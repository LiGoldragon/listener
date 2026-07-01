use signal_listener::{
    ActiveCapture, ActiveCaptureSession, CaptureSession, CaptureStarted, CaptureStatus,
    CaptureStopped, Input, OperationKind, Output, Reason, RequestUnimplemented, StartCapture,
    StartedSession, StatusRequest, StopCapture, StoppedSession, UnimplementedOperationKind,
    UnimplementedReason,
};

use crate::{
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, CaptureStore, Configuration,
    ConfiguredBatchTranscriber, Error, OutputTargetDispatcher, Result,
};
use crate::{BatchTranscriber, BatchTranscriptionRequest, ProcessAudioCaptureBackend};

pub struct ListenerRuntime {
    configuration: Configuration,
    capture_store: CaptureStore,
    capture_backend: Box<dyn AudioCaptureBackend>,
    transcriber: Box<dyn BatchTranscriber>,
    output_target_dispatcher: OutputTargetDispatcher,
    session_sequence: CaptureSessionSequence,
    active_capture: Option<RuntimeActiveCapture>,
}

impl ListenerRuntime {
    pub fn from_configuration(configuration: Configuration) -> Self {
        Self::with_dependencies(
            configuration,
            Box::new(ProcessAudioCaptureBackend::from_environment()),
            Box::new(ConfiguredBatchTranscriber::from_environment()),
            OutputTargetDispatcher::from_environment(),
        )
    }

    pub fn with_dependencies(
        configuration: Configuration,
        capture_backend: Box<dyn AudioCaptureBackend>,
        transcriber: Box<dyn BatchTranscriber>,
        output_target_dispatcher: OutputTargetDispatcher,
    ) -> Self {
        let capture_store = CaptureStore::from_configuration(&configuration);
        Self {
            configuration,
            capture_store,
            capture_backend,
            transcriber,
            output_target_dispatcher,
            session_sequence: CaptureSessionSequence::new(1),
            active_capture: None,
        }
    }

    pub fn handle_input(&mut self, input: Input) -> Output {
        match input {
            Input::Start(start) => self
                .start(start)
                .unwrap_or_else(|error| error.into_unimplemented_reply(OperationKind::Start)),
            Input::Stop(stop) => self
                .stop(stop)
                .unwrap_or_else(|error| error.into_unimplemented_reply(OperationKind::Stop)),
            Input::Status(status) => self
                .status(status)
                .unwrap_or_else(|error| error.into_unimplemented_reply(OperationKind::Status)),
        }
    }

    pub fn start(&mut self, _request: StartCapture) -> Result<Output> {
        if let Some(active_capture) = &self.active_capture {
            return Err(Error::CaptureAlreadyActive {
                session: active_capture.session().value(),
            });
        }

        self.capture_store.prepare()?;
        let session = self.session_sequence.next_session();
        let artifact = self.capture_store.artifact_for_session(&session);
        let capture = self.capture_backend.start(AudioCaptureStart::new(
            session.clone(),
            artifact.clone(),
            self.configuration.input_source(),
        ))?;
        self.active_capture = Some(RuntimeActiveCapture::new(
            session.clone(),
            artifact,
            capture,
        ));

        Ok(Output::Started(CaptureStarted::new(StartedSession::new(
            session,
        ))))
    }

    pub fn stop(&mut self, request: StopCapture) -> Result<Output> {
        let requested_session = request.into_payload();
        let Some(active_capture) = self.active_capture.take() else {
            return Err(Error::NoActiveCapture);
        };

        if active_capture.session() != &requested_session {
            let active_session = active_capture.session().value();
            self.active_capture = Some(active_capture);
            return Err(Error::CaptureSessionMismatch {
                active: active_session,
                requested: requested_session.value(),
            });
        }

        let stopped_capture = active_capture.stop()?;
        let transcript_text = self.transcriber.transcribe(BatchTranscriptionRequest::new(
            stopped_capture.artifact().clone(),
        ))?;
        let delivery_outcomes = self
            .output_target_dispatcher
            .deliver(self.configuration.output_targets(), &transcript_text);

        Ok(Output::Stopped(CaptureStopped {
            stopped_session: StoppedSession::new(stopped_capture.session().clone()),
            durable_audio_artifact: stopped_capture.artifact().clone(),
            transcript_text,
            delivery_outcomes,
        }))
    }

    pub fn status(&self, _request: StatusRequest) -> Result<Output> {
        Ok(Output::status_reported(
            self.active_capture
                .as_ref()
                .map(RuntimeActiveCapture::status)
                .unwrap_or(CaptureStatus::Idle),
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureSessionSequence {
    next: u64,
}

impl CaptureSessionSequence {
    pub fn new(first: u64) -> Self {
        Self { next: first }
    }

    pub fn next_session(&mut self) -> CaptureSession {
        let session = CaptureSession::new(self.next);
        self.next += 1;
        session
    }
}

pub struct RuntimeActiveCapture {
    session: CaptureSession,
    artifact: signal_listener::DurableAudioArtifact,
    capture: Box<dyn ActiveAudioCapture>,
}

impl RuntimeActiveCapture {
    pub fn new(
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
        capture: Box<dyn ActiveAudioCapture>,
    ) -> Self {
        Self {
            session,
            artifact,
            capture,
        }
    }

    pub fn session(&self) -> &CaptureSession {
        &self.session
    }

    pub fn artifact(&self) -> &signal_listener::DurableAudioArtifact {
        &self.artifact
    }

    pub fn status(&self) -> CaptureStatus {
        CaptureStatus::Capturing(ActiveCapture {
            active_capture_session: ActiveCaptureSession::new(self.session.clone()),
            durable_audio_artifact: self.artifact.clone(),
        })
    }

    pub fn stop(self) -> Result<StoppedCapture> {
        let artifact = self.capture.stop()?;
        Ok(StoppedCapture::new(self.session, artifact))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoppedCapture {
    session: CaptureSession,
    artifact: signal_listener::DurableAudioArtifact,
}

impl StoppedCapture {
    pub fn new(session: CaptureSession, artifact: signal_listener::DurableAudioArtifact) -> Self {
        Self { session, artifact }
    }

    pub fn session(&self) -> &CaptureSession {
        &self.session
    }

    pub fn artifact(&self) -> &signal_listener::DurableAudioArtifact {
        &self.artifact
    }
}

impl Error {
    pub fn into_unimplemented_reply(self, operation_kind: OperationKind) -> Output {
        Output::RequestUnimplemented(RequestUnimplemented {
            unimplemented_operation_kind: UnimplementedOperationKind::new(operation_kind),
            reason: Reason::new(self.unimplemented_reason()),
        })
    }

    fn unimplemented_reason(&self) -> UnimplementedReason {
        match self {
            Self::AudioBackendUnavailable { .. } | Self::CaptureProcessStdoutUnavailable => {
                UnimplementedReason::AudioBackendUnavailable
            }
            Self::TranscriptionBackendUnavailable { .. } => {
                UnimplementedReason::TranscriptionBackendUnavailable
            }
            Self::OutputTargetRejected { .. } => UnimplementedReason::OutputTargetUnavailable,
            Self::Io(_) => UnimplementedReason::StoreUnavailable,
            _ => UnimplementedReason::NotBuiltYet,
        }
    }
}
