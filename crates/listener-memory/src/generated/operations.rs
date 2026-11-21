#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Record_Data {
    pub job_id: listener_library::JobId,
    pub hearing: listener_library::Hearing,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Finish_Data {
    pub job_id: listener_library::JobId,
    pub handing: listener_library::Handing,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Import_Data {
    pub moment: listener_library::Moment,
    pub text: listener_library::Text,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Input {
    Turn(listener_library::Options),
    Drop,
    Interrupt(listener_library::JobId),
    Requeue(listener_library::JobId),
    Tries(listener_library::JobId),
    Record(Record_Data),
    Finish(Finish_Data),
    Prune,
    Forget(std::vec::Vec<listener_library::AudioLocation>),
    Last,
    Find(listener_library::JobId),
    Pending,
    Recollect,
    Apply(meta_signal_listener::Set),
    Settings,
    Import(Import_Data),
    Recover,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Trial_Data {
    pub backend_vector: std::vec::Vec<listener_library::Backend>,
    pub term_vector: std::vec::Vec<listener_library::Term>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Output {
    Turning(Turning),
    Dropping(Dropping),
    Interrupting(Interrupting),
    Requeueing(Requeueing),
    Trial(Trial_Data),
    Next(Next),
    Pruned(std::vec::Vec<listener_library::AudioLocation>),
    Forgotten,
    Lasting(Lasting),
    Finding(Finding),
    Standing(std::vec::Vec<listener_library::JobState>),
    Recollected(std::vec::Vec<listener_library::Past>),
    Applying(Applying),
    Configuration(listener_library::Configuration),
    Imported(listener_library::JobId),
    Recovered(Recovery),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Execution {
    pub input: Input,
    pub moment: listener_library::Moment,
    pub job_id_option: std::option::Option<listener_library::JobId>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Retired(pub std::vec::Vec<listener_library::JobId>);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Changes {
    pub job_state_vector: std::vec::Vec<listener_library::JobState>,
    pub retired: Retired,
    pub set_option: std::option::Option<meta_signal_listener::Set>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Committed {
    pub output: Output,
    pub changes: Changes,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Hear(pub std::vec::Vec<listener_library::JobId>);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Recovery {
    pub hear: Hear,
    pub next: Next,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Fresh_Data {
    pub kilobits: listener_library::Kilobits,
    pub source: listener_library::Source,
    pub audio_location: listener_library::AudioLocation,
    pub recording_settings: listener_library::RecordingSettings,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Turning {
    Fresh(Fresh_Data),
    Running(listener_library::JobId),
    NoBackend,
    UnknownBackend(listener_library::UnknownBackend),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Dropping {
    Dropped(listener_library::JobId),
    NothingRecording,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Interrupting {
    Interrupted(listener_library::JobId),
    NothingRecording,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Requeueing {
    Requeued(listener_library::JobId),
    NoSuchJob(signal_listener::NoSuchJob),
    NoAudio(signal_listener::NoAudio),
    InProgress(listener_library::JobState),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Next {
    Deliverable(listener_library::Ready),
    Nothing,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Lasting {
    Ready(listener_library::Ready),
    NothingKept,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Finding {
    Ready(listener_library::Ready),
    NoSuchJob(signal_listener::NoSuchJob),
    NothingKept,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Applying {
    Applied(meta_signal_listener::Set),
    UnknownBackend(listener_library::UnknownBackend),
    InvalidValue,
}
