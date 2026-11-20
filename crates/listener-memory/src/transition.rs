use crate::{
    ApplyingConfiguration, Error, Transitioning, ValidatingConfiguration, ViewingJob,
    operations::*, state::*,
};
use listener_library::*;
use meta_signal_listener::Set;

impl ViewingJob for Job {
    fn text(&self) -> Option<&Text> {
        self.imported_text.0.as_ref().or_else(|| {
            self.transcript_vector
                .last()
                .map(|transcript| &transcript.text)
        })
    }
    fn ready(&self) -> Option<Ready> {
        self.text().map(|text| Ready {
            job_id: self.job_id.clone(),
            text: text.clone(),
            delivery: self.delivery.clone(),
        })
    }
    fn terminal(&self) -> bool {
        matches!(
            self.state,
            State::Delivered | State::Failed | State::Cancelled
        )
    }
    fn status(&self) -> JobState {
        JobState {
            job_id: self.job_id.clone(),
            state: self.state.clone(),
        }
    }
}
impl ValidatingConfiguration for Configuration {
    fn validate(&self) -> Result<(), Error> {
        for name in &self.order.0 {
            if !self
                .backend_vector
                .iter()
                .any(|backend| &backend.backend_name == name)
            {
                return Err(Error::UnknownBackend(name.clone()));
            }
        }
        let settings = &self.recording_settings;
        let channels = match settings.channels {
            Channels::Mono => 1,
            Channels::Stereo => 2,
        };
        let bad = self.budget.0.reading_steps.0 <= 0
            || self.budget.0.reading_depth.0 <= 0
            || self.kilobits.0 <= 0
            || self.history_length.0 < 0
            || self
                .backend_vector
                .iter()
                .any(|backend| backend.timeout.0.0 <= 0)
            || self
                .backend_vector
                .iter()
                .enumerate()
                .any(|(index, backend)| {
                    self.backend_vector[..index]
                        .iter()
                        .any(|prior| prior.backend_name == backend.backend_name)
                })
            || matches!(&self.audio_kept.0, Retention::Days(days) if days.0 < 0)
            || matches!(&self.text_kept.0, Retention::Days(days) if days.0 < 0)
            || !matches!(settings.sample_rate.0, 8000 | 12000 | 16000 | 24000 | 48000)
            || settings.capture_fragment_bytes.0 <= 0
            || settings.capture_fragment_bytes.0 % (channels * 4) != 0
            || settings.capture_buffer_bytes.0 < settings.capture_fragment_bytes.0
            || settings.capture_buffer_bytes.0 % (channels * 4) != 0
            || settings.pending_pcm_frames.0 <= 0
            || settings.page_packets.0 <= 0
            || !(0..=10).contains(&settings.encoding_complexity.0)
            || !(0..=100).contains(&settings.expected_packet_loss.0);
        if bad {
            return Err(Error::InvalidConfiguration);
        }
        Ok(())
    }
}
impl ApplyingConfiguration for Configuration {
    fn apply(&mut self, set: &Set) -> Result<(), Error> {
        match set {
            Set::Backend(backend) => {
                if let Some(prior) = self
                    .backend_vector
                    .iter_mut()
                    .find(|prior| prior.backend_name == backend.backend_name)
                {
                    *prior = backend.clone();
                } else {
                    self.backend_vector.push(backend.clone());
                }
            }
            Set::RemoveBackend(name) => {
                let Some(index) = self
                    .backend_vector
                    .iter()
                    .position(|backend| &backend.backend_name == name)
                else {
                    return Err(Error::UnknownBackend(name.clone()));
                };
                self.backend_vector.remove(index);
                self.order.0.retain(|kept| kept != name);
            }
            Set::Order(order) => self.order = Order(order.clone()),
            Set::Delivery(delivery) => self.delivery = delivery.clone(),
            Set::Sequence(sequence) => self.sequence = sequence.clone(),
            Set::AudioKept(retention) => self.audio_kept = AudioKept(retention.clone()),
            Set::TextKept(retention) => self.text_kept = TextKept(retention.clone()),
            Set::AddTerm(term) => {
                if !self.term_vector.contains(term) {
                    self.term_vector.push(term.clone());
                }
            }
            Set::RemoveTerm(term) => self.term_vector.retain(|kept| kept != term),
            Set::Kilobits(kilobits) => self.kilobits = *kilobits,
            Set::Source(source) => self.source = source.clone(),
            Set::HistoryLength(length) => self.history_length = *length,
            Set::PasteProgram(program) => self.paste_program = PasteProgram(program.clone()),
            Set::CopyProgram(program) => self.copy_program = CopyProgram(program.clone()),
            Set::SecretProgram(program) => self.secret_program = SecretProgram(program.clone()),
            Set::AudioDirectory(directory) => {
                self.audio_directory = AudioDirectory(directory.clone())
            }
            Set::Budget(budget) => self.budget = Budget(budget.clone()),
            Set::RecordingSettings(settings) => self.recording_settings = settings.clone(),
        }
        self.validate()
    }
}
impl Transitioning for Record {
    fn next(&mut self) -> Next {
        if self.delivery_claim.0.is_some() {
            return Next::Nothing;
        }
        for job in &self.job_vector {
            if self.configuration.sequence == Sequence::AsRecorded
                && matches!(job.state, State::Recording | State::Transcribing)
            {
                return Next::Nothing;
            }
            if job.state == State::Waiting {
                if let Some(ready) = job.ready() {
                    self.delivery_claim.0 = Some(job.job_id.clone());
                    return Next::Deliverable(ready);
                }
            }
        }
        Next::Nothing
    }
    fn transition(&mut self, execution: &Execution) -> Result<Output, Error> {
        match &execution.input {
            Input::Turn(options) => {
                if let Some(job) = self
                    .job_vector
                    .iter_mut()
                    .find(|job| job.state == State::Recording)
                {
                    job.state = State::Transcribing;
                    return Ok(Output::Turning(Turning::Running(job.job_id.clone())));
                }
                let backends = options
                    .backend_name_vector_option
                    .as_ref()
                    .unwrap_or(&self.configuration.order.0)
                    .clone();
                if backends.is_empty() {
                    return Ok(Output::Turning(Turning::NoBackend));
                }
                for name in &backends {
                    if !self
                        .configuration
                        .backend_vector
                        .iter()
                        .any(|backend| &backend.backend_name == name)
                    {
                        return Err(Error::UnknownBackend(name.clone()));
                    }
                }
                let id = execution
                    .job_id_option
                    .clone()
                    .ok_or(Error::MissingIdentity)?;
                if self.job_vector.iter().any(|job| job.job_id == id) {
                    return Err(Error::DuplicateJob(id));
                }
                let location = AudioLocation {
                    directory: self.configuration.audio_directory.0.clone(),
                    job_id: id.clone(),
                };
                self.job_vector.push(Job {
                    job_id: id,
                    moment: execution.moment,
                    state: State::Recording,
                    delivery: options
                        .delivery_option
                        .clone()
                        .unwrap_or(self.configuration.delivery.clone()),
                    backend_name_vector: backends,
                    transcript_vector: Vec::new(),
                    try_vector: Vec::new(),
                    handing_option: None,
                    audio: Audio::Kept(location.directory.clone()),
                    recording_settings: self.configuration.recording_settings.clone(),
                    imported_text: ImportedText(None),
                });
                Ok(Output::Turning(Turning::Fresh(Fresh_Data {
                    kilobits: self.configuration.kilobits,
                    source: self.configuration.source.clone(),
                    audio_location: location,
                    recording_settings: self.configuration.recording_settings.clone(),
                })))
            }
            Input::Drop => {
                let Some(job) = self
                    .job_vector
                    .iter_mut()
                    .find(|job| job.state == State::Recording)
                else {
                    return Ok(Output::Dropping(Dropping::NothingRecording));
                };
                job.state = State::Cancelled;
                Ok(Output::Dropping(Dropping::Dropped(job.job_id.clone())))
            }
            Input::Interrupt(id) => {
                let Some(job) = self
                    .job_vector
                    .iter_mut()
                    .find(|job| &job.job_id == id && job.state == State::Recording)
                else {
                    return Ok(Output::Interrupting(Interrupting::NothingRecording));
                };
                job.state = State::Transcribing;
                Ok(Output::Interrupting(Interrupting::Interrupted(id.clone())))
            }
            Input::Requeue(id) => {
                let Some(job) = self.job_vector.iter_mut().find(|job| &job.job_id == id) else {
                    return Ok(Output::Requeueing(Requeueing::NoSuchJob(
                        signal_listener::NoSuchJob(id.clone()),
                    )));
                };
                if matches!(job.audio, Audio::Erased) {
                    return Ok(Output::Requeueing(Requeueing::NoAudio(
                        signal_listener::NoAudio(id.clone()),
                    )));
                }
                if !job.terminal() {
                    return Err(Error::WrongState(id.clone()));
                }
                job.state = State::Transcribing;
                job.handing_option = None;
                job.try_vector.clear();
                Ok(Output::Requeueing(Requeueing::Requeued(id.clone())))
            }
            Input::Tries(id) => {
                let job = self
                    .job_vector
                    .iter()
                    .find(|job| &job.job_id == id)
                    .ok_or_else(|| Error::NoSuchJob(id.clone()))?;
                let mut backends = Vec::new();
                for name in &job.backend_name_vector {
                    backends.push(
                        self.configuration
                            .backend_vector
                            .iter()
                            .find(|backend| &backend.backend_name == name)
                            .cloned()
                            .ok_or_else(|| Error::UnknownBackend(name.clone()))?,
                    );
                }
                Ok(Output::Trial(Trial_Data {
                    backend_vector: backends,
                    term_vector: self.configuration.term_vector.clone(),
                }))
            }
            Input::Record(result) => {
                let job = self
                    .job_vector
                    .iter_mut()
                    .find(|job| job.job_id == result.job_id)
                    .ok_or_else(|| Error::NoSuchJob(result.job_id.clone()))?;
                if job.state != State::Transcribing {
                    return Err(Error::WrongState(result.job_id.clone()));
                }
                match &result.hearing {
                    Hearing::Heard(heard) => {
                        job.transcript_vector.push(heard.transcript.clone());
                        job.try_vector = heard.try_vector.clone();
                        job.state = State::Waiting;
                    }
                    Hearing::Exhausted(tries) => {
                        job.try_vector = tries.clone();
                        job.state = State::Failed;
                    }
                }
                Ok(Output::Next(self.next()))
            }
            Input::Finish(finish) => {
                if self.delivery_claim.0.as_ref() != Some(&finish.job_id) {
                    return Err(Error::WrongState(finish.job_id.clone()));
                }
                let job = self
                    .job_vector
                    .iter_mut()
                    .find(|job| job.job_id == finish.job_id)
                    .ok_or_else(|| Error::NoSuchJob(finish.job_id.clone()))?;
                if job.state != State::Waiting {
                    return Err(Error::WrongState(finish.job_id.clone()));
                }
                job.handing_option = Some(finish.handing.clone());
                job.state = State::Delivered;
                self.delivery_claim.0 = None;
                Ok(Output::Next(self.next()))
            }
            Input::Prune => {
                let mut locations = Vec::new();
                for job in &mut self.job_vector {
                    if !job.terminal() {
                        continue;
                    }
                    if self
                        .configuration
                        .text_kept
                        .0
                        .expired(job.moment, execution.moment)
                    {
                        job.transcript_vector.clear();
                        job.imported_text.0 = None;
                    }
                    if self
                        .configuration
                        .audio_kept
                        .0
                        .expired(job.moment, execution.moment)
                    {
                        if let Audio::Kept(directory) = &job.audio {
                            locations.push(AudioLocation {
                                directory: directory.clone(),
                                job_id: job.job_id.clone(),
                            });
                        }
                    }
                }
                self.job_vector.retain(|job| {
                    !job.terminal() || job.text().is_some() || matches!(job.audio, Audio::Kept(_))
                });
                Ok(Output::Pruned(locations))
            }
            Input::Forget(locations) => {
                for job in &mut self.job_vector {
                    if !job.terminal() {
                        continue;
                    }
                    if let Audio::Kept(directory) = &job.audio {
                        if locations.iter().any(|location| {
                            location.job_id == job.job_id && &location.directory == directory
                        }) {
                            job.audio = Audio::Erased;
                        }
                    }
                }
                self.job_vector.retain(|job| {
                    !job.terminal() || job.text().is_some() || matches!(job.audio, Audio::Kept(_))
                });
                Ok(Output::Forgotten)
            }
            Input::Last => Ok(Output::Lasting(
                self.job_vector
                    .iter()
                    .filter(|job| job.text().is_some())
                    .max_by_key(|job| job.moment)
                    .and_then(ViewingJob::ready)
                    .map_or(Lasting::NothingKept, Lasting::Ready),
            )),
            Input::Find(id) => Ok(Output::Finding(
                match self.job_vector.iter().find(|job| &job.job_id == id) {
                    None => Finding::NoSuchJob(signal_listener::NoSuchJob(id.clone())),
                    Some(job) => job.ready().map_or(Finding::NothingKept, Finding::Ready),
                },
            )),
            Input::Pending => Ok(Output::Standing(
                self.job_vector
                    .iter()
                    .filter(|job| !job.terminal())
                    .map(ViewingJob::status)
                    .collect(),
            )),
            Input::Recollect => {
                let mut history: Vec<_> = self
                    .job_vector
                    .iter()
                    .filter_map(|job| {
                        job.text().map(|text| Past {
                            job_id: job.job_id.clone(),
                            moment: job.moment,
                            text: text.clone(),
                        })
                    })
                    .collect();
                history.sort_by_key(|past| std::cmp::Reverse(past.moment));
                history.truncate(
                    usize::try_from(self.configuration.history_length.0)
                        .map_err(|_| Error::InvalidConfiguration)?,
                );
                Ok(Output::Recollected(history))
            }
            Input::Apply(set) => {
                let mut configuration = self.configuration.clone();
                match configuration.apply(set) {
                    Ok(()) => {
                        self.configuration = configuration;
                        Ok(Output::Applying(Applying::Applied(set.clone())))
                    }
                    Err(Error::UnknownBackend(name)) => Ok(Output::Applying(
                        Applying::UnknownBackend(meta_signal_listener::UnknownBackend(name)),
                    )),
                    Err(error) => Err(error),
                }
            }
            Input::Settings => Ok(Output::Configuration(self.configuration.clone())),
            Input::Import(import) => {
                let id = execution
                    .job_id_option
                    .clone()
                    .ok_or(Error::MissingIdentity)?;
                if self.job_vector.iter().any(|job| job.job_id == id) {
                    return Err(Error::DuplicateJob(id));
                }
                self.job_vector.push(Job {
                    job_id: id.clone(),
                    moment: import.moment,
                    state: State::Delivered,
                    delivery: Delivery::Kept,
                    backend_name_vector: Vec::new(),
                    transcript_vector: Vec::new(),
                    try_vector: Vec::new(),
                    handing_option: Some(Handing::Kept),
                    audio: Audio::Erased,
                    recording_settings: self.configuration.recording_settings.clone(),
                    imported_text: ImportedText(Some(import.text.clone())),
                });
                Ok(Output::Imported(id))
            }
            Input::Recover => {
                let mut hearing = Vec::new();
                self.delivery_claim.0 = None;
                for job in &mut self.job_vector {
                    if matches!(job.state, State::Recording | State::Transcribing) {
                        job.state = State::Transcribing;
                        hearing.push(job.job_id.clone());
                    }
                }
                Ok(Output::Recovered(Recovery {
                    hear: Hear(hearing),
                    next: self.next(),
                }))
            }
        }
    }
}
trait Expiring {
    fn expired(&self, started: Moment, now: Moment) -> bool;
}
impl Expiring for Retention {
    fn expired(&self, started: Moment, now: Moment) -> bool {
        match self {
            Retention::Forever => false,
            // Moment is Unix seconds; a day is the duration unit's invariant.
            Retention::Days(days) => {
                now.0.saturating_sub(started.0) >= days.0.saturating_mul(86_400)
            }
        }
    }
}
