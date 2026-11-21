#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DeliveryFailed_Data {
    pub job_id: listener_library::JobId,
    pub delivery: listener_library::Delivery,
    pub failing: listener_library::Failing,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RecordingInterrupted_Data {
    pub job_id: listener_library::JobId,
    pub interruption_cause: InterruptionCause,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Notice {
    BackendFailed(listener_library::Try),
    Exhausted(listener_library::JobId),
    Copied(listener_library::JobId),
    DeliveryFailed(DeliveryFailed_Data),
    RecordingInterrupted(RecordingInterrupted_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum InterruptionCause {
    MicrophoneLost,
    Unwritable,
    Gap,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Transcribe_Data {
    pub program: listener_library::Program,
    pub audio_location: listener_library::AudioLocation,
    pub backend: listener_library::Backend,
    pub term_vector: std::vec::Vec<listener_library::Term>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Paste_Data {
    pub program: listener_library::Program,
    pub text: listener_library::Text,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Copy_Data {
    pub program: listener_library::Program,
    pub text: listener_library::Text,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Emission {
    Transcribe(Transcribe_Data),
    Paste(Paste_Data),
    Copy(Copy_Data),
    Notify(Notice),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Transcription_Data_Heard_Data {
    pub text: listener_library::Text,
    pub provider_id_option: std::option::Option<listener_library::ProviderId>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Transcription_Data {
    Heard(Transcription_Data_Heard_Data),
    Failure(listener_library::Failure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Pasting_Data {
    Pasted,
    Failure(listener_library::Failure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Copying_Data {
    Copied,
    Failure(listener_library::Failure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Notifying_Data {
    Notified,
    Failure(listener_library::Failure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    Transcription(Transcription_Data),
    Pasting(Pasting_Data),
    Copying(Copying_Data),
    Notifying(Notifying_Data),
}
#[rustfmt::skip]
pub trait Emitted {
    type Outcome;
}
#[rustfmt::skip]
pub trait Emittable<Translator>: Emitted {
    fn emit(&self, translator: &Translator) -> Self::Outcome;
}
#[rustfmt::skip]
impl Emitted for Transcribe_Data {
    type Outcome = Transcription_Data;
}
#[rustfmt::skip]
impl Emitted for Paste_Data {
    type Outcome = Pasting_Data;
}
#[rustfmt::skip]
impl Emitted for Copy_Data {
    type Outcome = Copying_Data;
}
#[rustfmt::skip]
impl Emitted for Notice {
    type Outcome = Notifying_Data;
}
