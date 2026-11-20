#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DeliveryClaim(pub std::option::Option<listener_library::JobId>);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Record {
    pub job_vector: std::vec::Vec<Job>,
    pub configuration: listener_library::Configuration,
    pub delivery_claim: DeliveryClaim,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ImportedText(pub std::option::Option<listener_library::Text>);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Job {
    pub job_id: listener_library::JobId,
    pub moment: listener_library::Moment,
    pub state: listener_library::State,
    pub delivery: listener_library::Delivery,
    pub backend_name_vector: std::vec::Vec<listener_library::BackendName>,
    pub transcript_vector: std::vec::Vec<listener_library::Transcript>,
    pub try_vector: std::vec::Vec<listener_library::Try>,
    pub handing_option: std::option::Option<listener_library::Handing>,
    pub audio: Audio,
    pub recording_settings: listener_library::RecordingSettings,
    pub imported_text: ImportedText,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Audio {
    Kept(listener_library::Directory),
    Erased,
}
