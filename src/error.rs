use std::path::Path;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration archive encode failed")]
    ConfigurationEncode,

    #[error("configuration archive decode failed")]
    ConfigurationDecode,

    #[error("transcription customization archive encode failed")]
    TranscriptionCustomizationEncode,

    #[error("transcription customization archive decode failed")]
    TranscriptionCustomizationDecode,

    #[error("transcription customization archive header is incomplete")]
    TranscriptionCustomizationArchiveHeader,

    #[error("transcription customization archive magic mismatch")]
    TranscriptionCustomizationArchiveMagic,

    #[error(
        "unsupported transcription customization archive version {version}; expected {expected}"
    )]
    TranscriptionCustomizationArchiveVersion { version: u32, expected: u32 },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("signal exchange frame: {0}")]
    SignalExchangeFrame(#[from] signal_frame::FrameError),

    #[error("unexpected contract frame: expected {expected}, got {got}")]
    UnexpectedContractFrame { expected: &'static str, got: String },

    #[error("contract frame carries {count} operations; listener accepts exactly one")]
    UnsupportedContractBatch { count: usize },

    #[error("contract reply carries {count} operation replies; listener accepts exactly one")]
    UnsupportedContractReplyBatch { count: usize },

    #[error("reply exchange mismatch: expected {expected:?}, got {actual:?}")]
    ReplyExchangeMismatch {
        expected: signal_frame::ExchangeIdentifier,
        actual: signal_frame::ExchangeIdentifier,
    },

    #[error("invalid command: {message}")]
    InvalidCommand { message: String },

    #[error("invalid capture session {value}: {message}")]
    InvalidCaptureSession { value: String, message: String },

    #[error("capture session {session} is already active")]
    CaptureAlreadyActive { session: u64 },

    #[error("no active capture")]
    NoActiveCapture,

    #[error("capture session mismatch: active {active}, requested {requested}")]
    CaptureSessionMismatch { active: u64, requested: u64 },

    #[error("capture session sequence exhausted after {last_session}")]
    CaptureSessionSequenceExhausted { last_session: u64 },

    #[error("audio backend unavailable: {message}")]
    AudioBackendUnavailable { message: String },

    #[error("capture process did not expose stdout")]
    CaptureProcessStdoutUnavailable,

    #[error("capture writer thread failed")]
    CaptureWriterThread,

    #[error("invalid audio format: {message}")]
    InvalidAudioFormat { message: String },

    #[error("invalid recording log at {path}: {message}")]
    InvalidRecordingLog { path: String, message: String },

    #[error("recording log already exists: {path}")]
    RecordingLogAlreadyExists { path: String },

    #[error(
        "incomplete PCM frame: {remaining_bytes} trailing bytes for {bytes_per_frame}-byte frames"
    )]
    IncompletePcmFrame {
        remaining_bytes: usize,
        bytes_per_frame: u16,
    },

    #[error("system clock is before the Unix epoch: {message}")]
    SystemClockBeforeUnixEpoch { message: String },

    #[error("transcription backend unavailable: {message}")]
    TranscriptionBackendUnavailable { message: String },

    #[error("compact audio encode failed: {message}")]
    CompactAudioEncode { message: String },

    #[error("compact audio artifact is invalid: {path}")]
    CompactAudioInvalid { path: String },

    #[error("capture session {session} does not exist")]
    CaptureNotFound { session: u64 },

    #[error("transcription actor unavailable: {message}")]
    TranscriptionActorUnavailable { message: String },

    #[error("output target rejected transcript: {message}")]
    OutputTargetRejected { message: String },

    #[error("transcript history entry encode failed: {message}")]
    HistoryEntryEncode { message: String },

    #[error("transcript history entry decode failed: {message}")]
    HistoryEntryDecode { message: String },

    #[error("invalid transcript history retention policy {variable}={value}")]
    InvalidHistoryRetentionPolicy { variable: String, value: String },

    #[error("invalid capture retention policy {variable}={value}")]
    InvalidCaptureRetentionPolicy { variable: String, value: String },

    #[error("recall selector `{program}` unavailable: {message}")]
    RecallSelectorUnavailable { program: String, message: String },

    #[error("daemon socket is already accepting connections at {path}")]
    DaemonAlreadyRunning { path: String },

    #[error("status socket is already accepting connections at {path}")]
    StatusSocketAlreadyRunning { path: String },

    #[error("status event encode failed: {message}")]
    StatusEventEncode { message: String },

    #[error("daemon socket path has no parent directory: {path}")]
    SocketParentMissing { path: String },

    #[error("path has no parent directory: {path}")]
    PathParentMissing { path: String },

    #[error("{surface} is scaffolded but not implemented")]
    NotImplemented { surface: &'static str },
}

impl Error {
    pub fn invalid_recording_log(path: &Path, message: impl Into<String>) -> Self {
        Self::InvalidRecordingLog {
            path: path.display().to_string(),
            message: message.into(),
        }
    }

    pub fn recording_log_already_exists(path: &Path) -> Self {
        Self::RecordingLogAlreadyExists {
            path: path.display().to_string(),
        }
    }

    pub fn is_recording_log_already_exists(&self) -> bool {
        matches!(self, Self::RecordingLogAlreadyExists { .. })
    }
}
