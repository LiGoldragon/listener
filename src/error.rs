use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration archive encode failed")]
    ConfigurationEncode,

    #[error("configuration archive decode failed")]
    ConfigurationDecode,

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

    #[error("audio backend unavailable: {message}")]
    AudioBackendUnavailable { message: String },

    #[error("capture process did not expose stdout")]
    CaptureProcessStdoutUnavailable,

    #[error("capture writer thread failed")]
    CaptureWriterThread,

    #[error("transcription backend unavailable: {message}")]
    TranscriptionBackendUnavailable { message: String },

    #[error("output target rejected transcript: {message}")]
    OutputTargetRejected { message: String },

    #[error("daemon socket is already accepting connections at {path}")]
    DaemonAlreadyRunning { path: String },

    #[error("daemon socket path has no parent directory: {path}")]
    SocketParentMissing { path: String },

    #[error("path has no parent directory: {path}")]
    PathParentMissing { path: String },

    #[error("{surface} is scaffolded but not implemented")]
    NotImplemented { surface: &'static str },
}
