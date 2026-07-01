//! Listener speech-to-text runtime.
//!
//! The Listener component owns speech capture, durable capture writes, batch
//! transcription on stop, and configured output delivery. Its public wire
//! vocabularies live in `signal-listener` and `meta-signal-listener`.

pub mod capture;
pub mod client;
#[cfg(feature = "nota-text")]
pub mod command;
pub mod configuration;
pub mod daemon;
pub mod delivery;
pub mod error;
#[cfg(feature = "nota-text")]
pub mod meta;
pub mod runtime;
pub mod transcription;
pub mod transport;

pub use capture::{
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, CaptureStore,
    ProcessAudioCaptureBackend,
};
pub use client::ListenerClient;
#[cfg(feature = "nota-text")]
pub use command::CommandLine;
pub use configuration::{Configuration, ConfigurationEnvironment};
pub use daemon::ListenerDaemon;
pub use delivery::{
    ClipboardDelivery, OutputTargetDispatcher, TranscriptDelivery, TranscriptDeliveryRequest,
};
pub use error::{Error, Result};
#[cfg(feature = "nota-text")]
pub use meta::MetaCommandLine;
pub use runtime::ListenerRuntime;
pub use transcription::{
    BatchTranscriber, BatchTranscriptionRequest, ConfiguredBatchTranscriber, HonestStubTranscriber,
};
pub use transport::{ContractFrameCodec, ContractFrameStream, MaximumFrameLength};
