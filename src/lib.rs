//! Listener speech-to-text runtime.
//!
//! The Listener component owns speech capture, durable capture writes, batch
//! transcription on stop, and configured output delivery. Its public wire
//! vocabularies live in `signal-listener` and `meta-signal-listener`.

mod artifact_privacy;
pub mod capture;
pub mod client;
#[cfg(feature = "nota-text")]
pub mod command;
pub mod compact_audio;
pub mod configuration;
pub mod daemon;
pub mod delivery;
pub mod error;
pub mod history;
pub mod latency;
pub mod maintenance;
#[cfg(feature = "nota-text")]
pub mod meta;
pub mod notification;
pub mod recall;
pub mod recording_log;
pub mod runtime;
pub mod status;
pub mod transcription;
pub mod transport;

pub use capture::{
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, CaptureMaintenanceSnapshot,
    CaptureRetentionAge, CaptureRetentionByteLimit, CaptureRetentionPolicy, CaptureStore,
    ProcessAudioCaptureBackend, RecoveredCaptureRecordings, TerminalCaptureState,
};
pub use client::{ListenerClient, ListenerMaintenanceClient};
#[cfg(feature = "nota-text")]
pub use command::CommandLine;
pub use compact_audio::{CompactAudioArtifact, LiveOpusWebmEncoder, OpusWebmEncoder};
pub use configuration::{Configuration, ConfigurationEnvironment};
pub use daemon::ListenerDaemon;
pub use delivery::{
    ClipboardCommand, ClipboardDelivery, OutputTargetDispatcher, TranscriptDelivery,
    TranscriptDeliveryRequest,
};
pub use error::{Error, Result};
pub use history::{
    HistoryByteLimit, HistoryLimit, HistoryRetentionAge, HistoryRetentionPolicy, HistoryTimestamp,
    TranscriptHistoryEntry, TranscriptHistoryStore,
};
pub use latency::LatencyInstrumentation;
pub use maintenance::CaptureMaintenance;
#[cfg(feature = "nota-text")]
pub use meta::MetaCommandLine;
pub use notification::{
    ClipboardSuccessNotification, FreedesktopDbusNotificationTransport,
    FreedesktopNotificationTransport, FreedesktopSuccessNotifier, SilentSuccessNotifier,
    SuccessNotifier,
};
pub use recall::{RecallOutcome, RecallSelector, TranscriptRecall};
pub use recording_log::{
    RawPcmExport, RecordingAudioFormat, RecordingInputSource, RecordingLog, RecordingLogDurability,
    RecordingLogDurabilityPolicy, RecordingLogHeader, RecordingLogRecordCommit, RecordingLogWriter,
    RecordingSampleFormat, RecordingStartTime, RecoveredRecordingLog,
};
pub use runtime::{DeliveryOwnershipAdmission, ListenerRuntime, RuntimeFinalizationFeedback};
pub use status::{
    ListenerStatusEvent, ListenerStatusState, MicrophoneLevel, StatusEventRecorder,
    StatusPublisher, StatusStreamServer,
};
pub use transcription::{
    BatchTranscriber, BatchTranscriptionInput, BatchTranscriptionInputFormat,
    BatchTranscriptionRequest, ConfiguredBatchTranscriber, HonestStubTranscriber,
    OpenAiBatchTranscriptionActor, OpenAiCredentialSource, OpenAiRestTranscriber,
    OpenAiTranscriptionRequestConfiguration,
    TRANSCRIPTION_CUSTOMIZATION_ARCHIVE_ENVIRONMENT_VARIABLE, TranscriptionCustomization,
    TranscriptionCustomizationEnvironment, TranscriptionCustomizationTextSource,
    TranscriptionPrompt,
};
pub use transport::{ContractFrameCodec, ContractFrameStream, MaximumFrameLength};
