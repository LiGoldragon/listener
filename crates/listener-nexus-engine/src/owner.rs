//! One owner serializes the durable transitions and their live publication.
//! Hearing and desktop effects return to it as events; they hold no store lock.
use crate::{
    effects::{Effects, HandingText, HandingWork, HearingJob, HearingWork, Notifying},
    journal::{ConfigurationChange, Journal, PublishingListener},
};
use listener_emission::{InterruptionCause, Notice, RecordingInterrupted_Data};
use listener_library::{AudioLocation, Handing, Hearing, JobId, Moment, Options, Ready};
use listener_memory::{
    ExecutingMemory, InspectingMemory, Memory, operations as memory, state::Audio,
};
use listener_place::{AudioLocating, random_bytes};
use listener_recording::{
    Capture, ControllingRecording, End, Recording, Refusal, RestoringAudio, StartingRecording,
};
use meta_signal_listener as meta;
use nexus::Capacity;
use signal_listener as ordinary;
use std::{
    fs, io,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};

pub enum Event {
    Ordinary(ordinary::Input, oneshot::Sender<Reply>),
    Meta(meta::Input, oneshot::Sender<MetaReply>),
    Replied(Deferred),
    Heard(JobId, Hearing),
    Handed(JobId, Handing),
    CaptureEnded(JobId, End),
}

pub enum Deferred {
    Nothing,
    Prune,
    Hear(JobId),
    Advance,
}

pub struct Reply {
    pub answer: ordinary::Answer,
    pub after: Deferred,
}
pub struct MetaReply {
    pub answer: meta::Answer,
    pub after: Deferred,
}
impl From<ordinary::Output> for Reply {
    fn from(output: ordinary::Output) -> Self {
        Self {
            answer: ordinary::Answer::Once(output),
            after: Deferred::Nothing,
        }
    }
}
impl From<meta::Output> for MetaReply {
    fn from(output: meta::Output) -> Self {
        Self {
            answer: meta::Answer::Once(output),
            after: Deferred::Nothing,
        }
    }
}
struct ActiveCapture {
    id: JobId,
    recording: Recording,
}
pub struct Owner {
    memory: Memory,
    journal: Journal,
    events: mpsc::UnboundedSender<Event>,
    effects: Arc<Effects>,
    capture: Option<ActiveCapture>,
}

trait Committing {
    fn commit(&self, input: memory::Input) -> Result<memory::Output, listener_memory::Error>;
}
impl Committing for Owner {
    fn commit(&self, input: memory::Input) -> Result<memory::Output, listener_memory::Error> {
        let committed = self.memory.execute(input.execution()?)?;
        for change in committed.changes.job_change_vector {
            self.journal.job_changed(change);
        }
        if let Some(set) = committed.changes.set_option {
            self.journal.settings_changed(ConfigurationChange {
                configuration: self.memory.inspect()?.configuration,
                set,
            });
        }
        Ok(committed.output)
    }
}

trait Executing {
    fn execution(self) -> Result<memory::Execution, io::Error>;
}
impl Executing for memory::Input {
    fn execution(self) -> Result<memory::Execution, io::Error> {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_secs();
        let moment = Moment(i64::try_from(seconds).map_err(io::Error::other)?);
        let job_id_option = if matches!(self, Self::Turn(_) | Self::Import(_)) {
            Some(JobId(format!(
                "{:032x}",
                u128::from_ne_bytes(random_bytes()?)
            )))
        } else {
            None
        };
        Ok(memory::Execution {
            input: self,
            moment,
            job_id_option,
        })
    }
}

pub trait Running {
    fn recover(
        memory: Memory,
        capacity: Capacity,
        events: mpsc::UnboundedSender<Event>,
        effects: Effects,
    ) -> Result<(Self, memory::Recovery), Box<dyn std::error::Error>>
    where
        Self: Sized;
    async fn run(self, recovery: memory::Recovery, inbox: mpsc::UnboundedReceiver<Event>);
}
impl Running for Owner {
    fn recover(
        memory: Memory,
        capacity: Capacity,
        events: mpsc::UnboundedSender<Event>,
        effects: Effects,
    ) -> Result<(Self, memory::Recovery), Box<dyn std::error::Error>> {
        let memo = memory.execute(memory::Input::Recover.execution()?)?;
        let memory::Output::Recovered(recovery) = memo.output else {
            return Err(io::Error::from(io::ErrorKind::InvalidData).into());
        };
        let jobs = memory.execute(memory::Input::Pending.execution()?)?;
        let memory::Output::Standing(jobs) = jobs.output else {
            return Err(io::Error::from(io::ErrorKind::InvalidData).into());
        };
        let journal = Journal::start(jobs, memory.inspect()?.configuration, capacity)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok((
            Self {
                memory,
                journal,
                events,
                effects: Arc::new(effects),
                capture: None,
            },
            recovery,
        ))
    }

    async fn run(mut self, recovery: memory::Recovery, mut inbox: mpsc::UnboundedReceiver<Event>) {
        for job_id in recovery.hear.0 {
            if let Some(location) = self.audio(&job_id) {
                location.recover_audio();
            }
            self.hear(job_id);
        }
        self.deliver(recovery.next);

        while let Some(event) = inbox.recv().await {
            match event {
                Event::Ordinary(input, answer) => {
                    if matches!(
                        input,
                        ordinary::Input::Recall | ordinary::Input::RecallJob(_)
                    ) {
                        self.recall(input, answer);
                    } else {
                        let response = self.ordinary(input).await;
                        if let Err(unsent) = answer.send(response) {
                            self.after(unsent.after);
                        }
                    }
                }
                Event::Meta(input, answer) => {
                    let response = self.meta(input);
                    if let Err(unsent) = answer.send(response) {
                        self.after(unsent.after);
                    }
                }
                Event::Replied(after) => self.after(after),
                Event::Heard(job_id, hearing) => {
                    self.next(self.commit(memory::Input::Record(memory::Record_Data {
                        job_id,
                        hearing,
                    })));
                }
                Event::Handed(job_id, handing) => {
                    self.next(self.commit(memory::Input::Finish(memory::Finish_Data {
                        job_id,
                        handing,
                    })));
                }
                Event::CaptureEnded(id, end) => {
                    if self
                        .capture
                        .as_ref()
                        .is_some_and(|capture| capture.id == id)
                    {
                        self.capture = None;
                        if matches!(
                            self.commit(memory::Input::Interrupt(id.clone())),
                            Ok(memory::Output::Interrupting(
                                memory::Interrupting::Interrupted(_)
                            ))
                        ) {
                            self.interrupted(id.clone(), end);
                            self.hear(id);
                        }
                    }
                }
            }
        }
    }
}

trait Dispatching {
    fn after(&self, action: Deferred);
    fn next(&self, output: Result<memory::Output, listener_memory::Error>);
    fn audio(&self, id: &JobId) -> Option<AudioLocation>;
    fn hear(&self, id: JobId);
    fn deliver(&self, next: memory::Next);
    fn hand_work(&self, ready: Ready) -> HandingWork;
    fn interrupted(&self, id: JobId, end: End);
}

impl Dispatching for Owner {
    fn after(&self, action: Deferred) {
        match action {
            Deferred::Nothing => {}
            Deferred::Advance => self.next(self.commit(memory::Input::Advance)),
            Deferred::Hear(job_id) => self.hear(job_id),
            Deferred::Prune => {
                if let Ok(memory::Output::Pruned(locations)) = self.commit(memory::Input::Prune) {
                    let erased = locations
                        .into_iter()
                        .filter(|location| match fs::remove_file(location.audio_path()) {
                            Ok(()) => true,
                            Err(error) => error.kind() == io::ErrorKind::NotFound,
                        })
                        .collect();
                    let _ = self.commit(memory::Input::Forget(erased));
                }
            }
        }
    }
    fn next(&self, output: Result<memory::Output, listener_memory::Error>) {
        if let Ok(memory::Output::Next(next)) = output {
            self.deliver(next);
        }
    }
    fn audio(&self, id: &JobId) -> Option<AudioLocation> {
        let record = self.memory.inspect().ok()?;
        record
            .job_vector
            .into_iter()
            .find_map(|job| match job.audio {
                Audio::Kept(directory) if &job.job_id == id => Some(AudioLocation {
                    directory,
                    job_id: id.clone(),
                }),
                _ => None,
            })
    }
    fn hear(&self, id: JobId) {
        let Ok(memory::Output::Trial(trial)) = self.commit(memory::Input::Tries(id.clone())) else {
            return;
        };
        let Some(location) = self.audio(&id) else {
            return;
        };
        let work = HearingWork {
            program: self.journal.settings().secret_program.0,
            location,
            backends: trial.backend_vector,
            terms: trial.term_vector,
        };
        let effects = self.effects.clone();
        let events = self.events.clone();
        tokio::spawn(async move {
            let hearing = effects.hear(work).await;
            let _ = events.send(Event::Heard(id, hearing));
        });
    }
    fn deliver(&self, next: memory::Next) {
        if let memory::Next::Deliverable(ready) = next {
            let id = ready.job_id.clone();
            let work = self.hand_work(ready);
            let effects = self.effects.clone();
            let events = self.events.clone();
            tokio::spawn(async move {
                let result = effects.hand(work).await;
                let _ = events.send(Event::Handed(id, result));
            });
        }
    }
    fn hand_work(&self, ready: Ready) -> HandingWork {
        let setting = self.journal.settings();
        HandingWork {
            ready,
            paste_program: setting.paste_program.0,
            copy_program: setting.copy_program.0,
        }
    }

    fn interrupted(&self, id: JobId, end: End) {
        let interruption_cause = match end {
            End::Stopped => return,
            End::MicrophoneLost => InterruptionCause::MicrophoneLost,
            End::Unwritable => InterruptionCause::Unwritable,
            End::Gap => InterruptionCause::Gap,
        };
        let effects = self.effects.clone();
        tokio::spawn(async move {
            effects
                .notify(Notice::RecordingInterrupted(RecordingInterrupted_Data {
                    job_id: id,
                    interruption_cause,
                }))
                .await;
        });
    }
}

trait Answering {
    async fn ordinary(&mut self, input: ordinary::Input) -> Reply;
    fn meta(&self, input: meta::Input) -> MetaReply;
    fn recall(&self, input: ordinary::Input, answer: oneshot::Sender<Reply>);
    async fn turn(
        &mut self,
        options: Options,
        toggled: fn(ordinary::Toggling) -> ordinary::Output,
    ) -> Reply;
}
impl Answering for Owner {
    async fn ordinary(&mut self, input: ordinary::Input) -> Reply {
        use ordinary::{Input, Output, Refused_Data as Refusal};
        match input {
            Input::Toggle => {
                self.turn(
                    Options {
                        delivery_option: None,
                        backend_name_vector_option: None,
                    },
                    Output::Toggled,
                )
                .await
            }
            Input::ToggleCustom(options) => self.turn(options, Output::CustomToggled).await,
            Input::Cancel => {
                if let Some(active) = self.capture.take() {
                    let _ = active.recording.stop().await;
                }
                match self.commit(memory::Input::Drop) {
                    Ok(memory::Output::Dropping(memory::Dropping::Dropped(id))) => Reply {
                        answer: ordinary::Answer::Once(Output::Cancelled(id)),
                        after: Deferred::Advance,
                    },
                    Ok(memory::Output::Dropping(memory::Dropping::NothingRecording)) => {
                        Output::Refused(Refusal::NothingRecording).into()
                    }
                    _ => Output::Refused(Refusal::Unwritable).into(),
                }
            }
            Input::Retranscribe(id) => match self.commit(memory::Input::Requeue(id)) {
                Ok(memory::Output::Requeueing(memory::Requeueing::Requeued(id))) => Reply {
                    answer: ordinary::Answer::Once(Output::Retranscribing(id.clone())),
                    after: Deferred::Hear(id),
                },
                Ok(memory::Output::Requeueing(memory::Requeueing::NoSuchJob(id))) => {
                    Output::Refused(Refusal::NoSuchJob(id)).into()
                }
                Ok(memory::Output::Requeueing(memory::Requeueing::NoAudio(id))) => {
                    Output::Refused(Refusal::NoAudio(id)).into()
                }
                Ok(memory::Output::Requeueing(memory::Requeueing::InProgress(job))) => {
                    Output::Refused(Refusal::InProgress(job)).into()
                }
                _ => Output::Refused(Refusal::Unwritable).into(),
            },
            Input::Jobs => Output::JobsListed(self.journal.jobs()).into(),
            Input::History => match self.commit(memory::Input::Recollect) {
                Ok(memory::Output::Recollected(history)) => Output::HistoryListed(history).into(),
                _ => Output::Refused(Refusal::Unwritable).into(),
            },
            Input::Subscribe => Reply {
                answer: ordinary::Answer::Subscribed(self.journal.job_subscription()),
                after: Deferred::Nothing,
            },
            Input::Recall | Input::RecallJob(_) => unreachable!(),
        }
    }
    fn meta(&self, input: meta::Input) -> MetaReply {
        use meta::{Input, Output, Refused_Data as Refusal};
        match input {
            Input::Configure(set) => match self.commit(memory::Input::Apply(set)) {
                Ok(memory::Output::Applying(memory::Applying::Applied(set))) => {
                    let after = if matches!(set, meta::Set::Sequence(_)) {
                        Deferred::Advance
                    } else {
                        Deferred::Nothing
                    };
                    MetaReply {
                        answer: meta::Answer::Once(Output::Configured(set)),
                        after,
                    }
                }
                Ok(memory::Output::Applying(memory::Applying::UnknownBackend(name))) => {
                    Output::Refused(Refusal::UnknownBackend(name)).into()
                }
                _ => Output::Refused(Refusal::InvalidValue).into(),
            },
            Input::Configuration => Output::Configuration(self.journal.settings()).into(),
            Input::Remember(remember) => {
                match self.commit(memory::Input::Import(memory::Import_Data {
                    moment: remember.moment,
                    text: remember.text,
                })) {
                    Ok(memory::Output::Imported(id)) => Output::Remembered(id).into(),
                    _ => Output::Refused(Refusal::InvalidValue).into(),
                }
            }
            Input::Subscribe => MetaReply {
                answer: meta::Answer::Subscribed(self.journal.settings_subscription()),
                after: Deferred::Nothing,
            },
        }
    }
    fn recall(&self, input: ordinary::Input, answer: oneshot::Sender<Reply>) {
        use ordinary::{Input, Output, Refused_Data as Refusal};
        let (request, output): (_, fn(_) -> _) = match input {
            Input::Recall => (memory::Input::Last, Output::Recalled),
            Input::RecallJob(id) => (memory::Input::Find(id), Output::JobRecalled),
            _ => unreachable!(),
        };
        let ready = match self.commit(request) {
            Ok(
                memory::Output::Lasting(memory::Lasting::Ready(ready))
                | memory::Output::Finding(memory::Finding::Ready(ready)),
            ) => Ok(ready),
            Ok(memory::Output::Finding(memory::Finding::NoSuchJob(id))) => {
                Err(Refusal::NoSuchJob(id))
            }
            Ok(
                memory::Output::Lasting(memory::Lasting::NothingKept)
                | memory::Output::Finding(memory::Finding::NothingKept),
            ) => Err(Refusal::NothingKept),
            _ => Err(Refusal::Unwritable),
        };
        match ready {
            Ok(ready) => {
                let text = ready.text.clone();
                let work = self.hand_work(ready);
                let effects = self.effects.clone();
                tokio::spawn(async move {
                    effects.hand(work).await;
                    let _ = answer.send(output(text).into());
                });
            }
            Err(refusal) => {
                let _ = answer.send(Output::Refused(refusal).into());
            }
        }
    }
    async fn turn(
        &mut self,
        options: Options,
        toggled: fn(ordinary::Toggling) -> ordinary::Output,
    ) -> Reply {
        use ordinary::{Output, Refused_Data, Toggling};
        match self.commit(memory::Input::Turn(options)) {
            Ok(memory::Output::Turning(memory::Turning::Fresh(fresh))) => {
                let id = fresh.audio_location.job_id.clone();
                let request = Capture {
                    audio_location: fresh.audio_location,
                    kilobits: fresh.kilobits,
                    source: fresh.source,
                    recording_settings: fresh.recording_settings,
                };
                match request.begin().await {
                    Ok(recording) => {
                        let mut outcome = recording.ending();
                        let events = self.events.clone();
                        let watching = id.clone();
                        tokio::spawn(async move {
                            if let Ok(end) = outcome.wait_for(Option::is_some).await
                                && let Some(end) = *end
                            {
                                let _ = events.send(Event::CaptureEnded(watching, end));
                            }
                        });
                        self.capture = Some(ActiveCapture {
                            id: id.clone(),
                            recording,
                        });
                        Reply {
                            answer: ordinary::Answer::Once(toggled(Toggling::Began(id))),
                            after: Deferred::Prune,
                        }
                    }
                    Err(refusal) => {
                        let _ = self.commit(memory::Input::AbortStart(id));
                        let refusal = match refusal {
                            Refusal::NoMicrophone => Refused_Data::NoMicrophone,
                            Refusal::Unwritable => Refused_Data::Unwritable,
                        };
                        Reply {
                            answer: ordinary::Answer::Once(Output::Refused(refusal)),
                            after: Deferred::Advance,
                        }
                    }
                }
            }
            Ok(memory::Output::Turning(memory::Turning::Running(id))) => {
                if let Some(active) = self.capture.take() {
                    let end = active.recording.stop().await;
                    self.interrupted(id.clone(), end);
                }
                Reply {
                    answer: ordinary::Answer::Once(toggled(Toggling::Ended(id.clone()))),
                    after: Deferred::Hear(id),
                }
            }
            Ok(memory::Output::Turning(memory::Turning::NoBackend)) => {
                Output::Refused(Refused_Data::NoBackend).into()
            }
            Ok(memory::Output::Turning(memory::Turning::UnknownBackend(name))) => {
                Output::Refused(Refused_Data::UnknownBackend(name)).into()
            }
            _ => Output::Refused(Refused_Data::Unwritable).into(),
        }
    }
}
