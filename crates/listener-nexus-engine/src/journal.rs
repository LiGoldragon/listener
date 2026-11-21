//! The store owner publishes committed changes. The shared publisher takes each
//! snapshot and its cursor together and replaces a lagged reader's snapshot.
use futures_util::StreamExt;
use listener_library::{Configuration, JobState, State};
use meta_signal_listener::Set;
use nexus::{Capacity, Publisher, Publishing, Subscribable};
use signal::{Following, Frame, Stream};
use signal_listener::JobChange;

#[derive(Clone)]
pub struct Jobs(pub Vec<JobState>);
impl Following for Jobs {
    type Change = JobChange;

    fn after(mut prior: Self, change: JobChange) -> Self {
        match change {
            JobChange::JobState(job) => {
                let position = prior.0.iter().position(|kept| kept.job_id == job.job_id);
                match (position, &job.state) {
                    (Some(index), State::Delivered) => {
                        prior.0.remove(index);
                    }
                    (None, State::Delivered) => {}
                    (Some(index), _) => prior.0[index] = job,
                    (None, _) => prior.0.push(job),
                }
            }
            JobChange::Forgotten(id) => prior.0.retain(|kept| kept.job_id != id),
        }
        prior
    }
}

#[derive(Clone)]
pub struct Settings(pub Configuration);
#[derive(Clone)]
pub struct ConfigurationChange {
    pub configuration: Configuration,
    pub set: Set,
}
impl Following for Settings {
    type Change = ConfigurationChange;
    fn after(_: Self, change: Self::Change) -> Self {
        Self(change.configuration)
    }
}

pub struct Journal {
    jobs: Publisher<Jobs, JobChange>,
    settings: Publisher<Settings, ConfigurationChange>,
}
pub trait PublishingListener {
    fn start(jobs: Vec<JobState>, configuration: Configuration, capacity: Capacity) -> Option<Self>
    where
        Self: Sized;
    fn job_changed(&self, change: JobChange);
    fn settings_changed(&self, change: ConfigurationChange);
    fn jobs(&self) -> Vec<JobState>;
    fn settings(&self) -> Configuration;
    fn job_subscription(&self) -> Stream<Vec<JobState>, JobChange>;
    fn settings_subscription(&self) -> Stream<Configuration, Set>;
}
impl PublishingListener for Journal {
    fn start(
        jobs: Vec<JobState>,
        configuration: Configuration,
        capacity: Capacity,
    ) -> Option<Self> {
        Some(Self {
            jobs: Publisher::create(Jobs(jobs), capacity).ok()?,
            settings: Publisher::create(Settings(configuration), capacity).ok()?,
        })
    }
    fn job_changed(&self, change: JobChange) {
        self.jobs.publish(change);
    }
    fn settings_changed(&self, change: ConfigurationChange) {
        self.settings.publish(change);
    }
    fn jobs(&self) -> Vec<JobState> {
        self.jobs.state().0
    }
    fn settings(&self) -> Configuration {
        self.settings.state().0
    }
    fn job_subscription(&self) -> Stream<Vec<JobState>, JobChange> {
        Stream::from(Box::pin(self.jobs.open().map(|frame| match frame {
            Frame::Opened(state) => Frame::Opened(state.0),
            Frame::Lagged(state) => Frame::Lagged(state.0),
            Frame::Changed(change) => Frame::Changed(change),
        }))
            as std::pin::Pin<Box<dyn futures_util::Stream<Item = _> + Send>>)
    }
    fn settings_subscription(&self) -> Stream<Configuration, Set> {
        Stream::from(Box::pin(self.settings.open().map(|frame| match frame {
            Frame::Opened(state) => Frame::Opened(state.0),
            Frame::Lagged(state) => Frame::Lagged(state.0),
            Frame::Changed(change) => Frame::Changed(change.set),
        }))
            as std::pin::Pin<Box<dyn futures_util::Stream<Item = _> + Send>>)
    }
}
