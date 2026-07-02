use signal_listener::{
    ActiveCapture, ActiveCaptureSession, CancelCapture, CancelledSession, CaptureAlreadyActive,
    CaptureCancelled, CaptureSession, CaptureSessionMismatch, CaptureStarted, CaptureStatus,
    CaptureStopped, DeliveryOutcome, DeliveryOutcomes, Input, NoActiveCapture, OperationKind,
    Output, Reason, RequestUnimplemented, RequestedCaptureSession, StartCapture, StartedSession,
    StatusRequest, StopCapture, StoppedSession, UnimplementedOperationKind, UnimplementedReason,
};

use crate::{
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, CaptureStore, Configuration, Error,
    OpenAiBatchTranscriptionActor, OutputTargetDispatcher, RecordingLog,
    RecoveredCaptureRecordings, Result,
};
use crate::{
    BatchTranscriber, BatchTranscriptionInput, BatchTranscriptionRequest,
    ProcessAudioCaptureBackend, StatusPublisher,
};

pub struct ListenerRuntime {
    configuration: Configuration,
    capture_store: CaptureStore,
    capture_backend: Box<dyn AudioCaptureBackend>,
    transcriber: Box<dyn BatchTranscriber>,
    output_target_dispatcher: OutputTargetDispatcher,
    status_publisher: StatusPublisher,
    session_sequence: CaptureSessionSequence,
    active_capture: Option<RuntimeActiveCapture>,
    orphaned_recordings: RecoveredCaptureRecordings,
}

impl ListenerRuntime {
    pub fn from_configuration(configuration: Configuration) -> Self {
        Self::from_configuration_with_status(configuration, StatusPublisher::silent())
    }

    pub fn from_configuration_with_status(
        configuration: Configuration,
        status_publisher: StatusPublisher,
    ) -> Self {
        Self::with_dependencies(
            configuration,
            Box::new(ProcessAudioCaptureBackend::from_environment()),
            Box::new(OpenAiBatchTranscriptionActor::from_environment(
                status_publisher.clone(),
            )),
            OutputTargetDispatcher::from_environment(),
            status_publisher,
        )
    }

    pub fn with_dependencies(
        configuration: Configuration,
        capture_backend: Box<dyn AudioCaptureBackend>,
        transcriber: Box<dyn BatchTranscriber>,
        output_target_dispatcher: OutputTargetDispatcher,
        status_publisher: StatusPublisher,
    ) -> Self {
        let capture_store = CaptureStore::from_configuration(&configuration);
        Self {
            configuration,
            capture_store,
            capture_backend,
            transcriber,
            output_target_dispatcher,
            status_publisher,
            session_sequence: CaptureSessionSequence::new(1),
            active_capture: None,
            orphaned_recordings: RecoveredCaptureRecordings::empty(),
        }
    }

    pub fn handle_input(&mut self, input: Input) -> Output {
        match input {
            Input::Start(start) => self.start(start).unwrap_or_else(Error::into_start_reply),
            Input::Stop(stop) => self.stop(stop).unwrap_or_else(Error::into_stop_reply),
            Input::Cancel(cancel) => self.cancel(cancel).unwrap_or_else(Error::into_cancel_reply),
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
        self.recover_orphaned_recordings()?;
        self.start_next_available_capture()
    }

    pub fn stop(&mut self, request: StopCapture) -> Result<Output> {
        let active_capture = self.take_active_capture(request.into_payload())?;

        let stopped_capture = match active_capture.stop() {
            Ok(stopped_capture) => stopped_capture,
            Err(error) => {
                self.status_publisher.publish_error();
                return Err(error);
            }
        };
        let recovered_log = match RecordingLog::new(stopped_capture.artifact_path()).recover() {
            Ok(recovered_log) => recovered_log,
            Err(error) => {
                self.status_publisher.publish_error();
                return Err(error);
            }
        };
        let raw_pcm_export = match recovered_log.export_raw_pcm(
            self.capture_store
                .raw_pcm_export_for_artifact(stopped_capture.artifact()),
        ) {
            Ok(raw_pcm_export) => raw_pcm_export,
            Err(error) => {
                self.status_publisher.publish_error();
                return Err(error);
            }
        };
        let transcription_input = BatchTranscriptionInput::signed_sixteen_bit_little_endian_pcm(
            raw_pcm_export.path().to_path_buf(),
            raw_pcm_export.audio_format(),
        );
        let transcript_text =
            match self
                .transcriber
                .transcribe(BatchTranscriptionRequest::new_with_input(
                    stopped_capture.artifact().clone(),
                    transcription_input,
                )) {
                Ok(transcript_text) => transcript_text,
                Err(error) => {
                    self.status_publisher.publish_error();
                    return Err(error);
                }
            };
        let delivery_outcomes = self
            .output_target_dispatcher
            .deliver(self.configuration.output_targets(), &transcript_text);
        RuntimeDeliveryStatus::from_outcomes(&delivery_outcomes).publish(&self.status_publisher);

        Ok(Output::Stopped(CaptureStopped {
            stopped_session: StoppedSession::new(stopped_capture.session().clone()),
            durable_audio_artifact: stopped_capture.artifact().clone(),
            transcript_text,
            delivery_outcomes,
        }))
    }

    pub fn cancel(&mut self, request: CancelCapture) -> Result<Output> {
        let active_capture = self.take_active_capture(request.into_payload())?;

        let stopped_capture = match active_capture.stop() {
            Ok(stopped_capture) => stopped_capture,
            Err(error) => {
                self.status_publisher.publish_error();
                return Err(error);
            }
        };
        self.status_publisher.publish_cancelled();

        Ok(Output::Cancelled(CaptureCancelled {
            cancelled_session: CancelledSession::new(stopped_capture.session().clone()),
            durable_audio_artifact: stopped_capture.artifact().clone(),
        }))
    }

    pub fn status(&mut self, _request: StatusRequest) -> Result<Output> {
        if self.active_capture.is_none() {
            self.recover_orphaned_recordings()?;
        }

        Ok(Output::status_reported(
            self.active_capture
                .as_ref()
                .map(RuntimeActiveCapture::status)
                .unwrap_or(CaptureStatus::Idle),
        ))
    }

    pub fn orphaned_recordings(&self) -> &RecoveredCaptureRecordings {
        &self.orphaned_recordings
    }

    fn take_active_capture(
        &mut self,
        requested_session: CaptureSession,
    ) -> Result<RuntimeActiveCapture> {
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

        Ok(active_capture)
    }

    fn recover_orphaned_recordings(&mut self) -> Result<()> {
        let orphaned_recordings = self.capture_store.recover_recording_logs()?;
        self.session_sequence
            .advance_to_at_least(orphaned_recordings.next_session_value());
        self.orphaned_recordings = orphaned_recordings;
        Ok(())
    }

    fn start_next_available_capture(&mut self) -> Result<Output> {
        loop {
            let session = self.session_sequence.next_session()?;
            let artifact = self.capture_store.artifact_for_session(&session);
            match self.capture_backend.start(AudioCaptureStart::new(
                session.clone(),
                artifact.clone(),
                self.configuration.input_source(),
                self.status_publisher.clone(),
            )) {
                Ok(capture) => {
                    self.active_capture = Some(RuntimeActiveCapture::new(
                        session.clone(),
                        artifact,
                        capture,
                    ));
                    self.status_publisher
                        .publish_recording_level(crate::MicrophoneLevel::silent());
                    return Ok(Output::Started(CaptureStarted::new(StartedSession::new(
                        session,
                    ))));
                }
                Err(error) if error.is_recording_log_already_exists() => {
                    let next_session_value = self
                        .capture_store
                        .next_session_value_after_existing_artifacts()?;
                    self.session_sequence
                        .advance_to_at_least(next_session_value);
                }
                Err(error) => {
                    self.status_publisher.publish_error();
                    return Err(error);
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeDeliveryStatus {
    Delivered,
    Failed,
    NoTargets,
}

impl RuntimeDeliveryStatus {
    fn from_outcomes(outcomes: &DeliveryOutcomes) -> Self {
        let mut delivered_count = 0_usize;
        for outcome in outcomes.as_slice() {
            match outcome {
                DeliveryOutcome::Delivered(_) => delivered_count += 1,
                DeliveryOutcome::Failed(_) => return Self::Failed,
            }
        }
        if delivered_count == 0 {
            Self::NoTargets
        } else {
            Self::Delivered
        }
    }

    fn publish(&self, status_publisher: &StatusPublisher) {
        match self {
            Self::Delivered => status_publisher.publish_copied(),
            Self::Failed => status_publisher.publish_error(),
            Self::NoTargets => status_publisher.publish_idle(),
        }
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

    pub fn next_session(&mut self) -> Result<CaptureSession> {
        let session = CaptureSession::new(self.next);
        self.next = self
            .next
            .checked_add(1)
            .ok_or(Error::CaptureSessionSequenceExhausted {
                last_session: self.next,
            })?;
        Ok(session)
    }

    pub fn advance_to_at_least(&mut self, next: u64) {
        self.next = self.next.max(next);
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

    pub fn artifact_path(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(self.artifact.path().as_str())
    }
}

impl Error {
    pub fn into_start_reply(self) -> Output {
        match self {
            Self::CaptureAlreadyActive { session } => Output::CaptureAlreadyActive(
                CaptureAlreadyActive::new(ActiveCaptureSession::new(CaptureSession::new(session))),
            ),
            error => error.into_unimplemented_reply(OperationKind::Start),
        }
    }

    pub fn into_stop_reply(self) -> Output {
        match self {
            Self::NoActiveCapture => Output::NoActiveCapture(NoActiveCapture {}),
            Self::CaptureSessionMismatch { active, requested } => {
                Output::CaptureSessionMismatch(CaptureSessionMismatch {
                    active_capture_session: ActiveCaptureSession::new(CaptureSession::new(active)),
                    requested_capture_session: RequestedCaptureSession::new(CaptureSession::new(
                        requested,
                    )),
                })
            }
            error => error.into_unimplemented_reply(OperationKind::Stop),
        }
    }

    pub fn into_cancel_reply(self) -> Output {
        match self {
            Self::NoActiveCapture => Output::NoActiveCapture(NoActiveCapture {}),
            Self::CaptureSessionMismatch { active, requested } => {
                Output::CaptureSessionMismatch(CaptureSessionMismatch {
                    active_capture_session: ActiveCaptureSession::new(CaptureSession::new(active)),
                    requested_capture_session: RequestedCaptureSession::new(CaptureSession::new(
                        requested,
                    )),
                })
            }
            error => error.into_unimplemented_reply(OperationKind::Cancel),
        }
    }

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
            Self::TranscriptionBackendUnavailable { .. }
            | Self::TranscriptionActorUnavailable { .. } => {
                UnimplementedReason::TranscriptionBackendUnavailable
            }
            Self::OutputTargetRejected { .. } => UnimplementedReason::OutputTargetUnavailable,
            Self::Io(_)
            | Self::InvalidAudioFormat { .. }
            | Self::InvalidRecordingLog { .. }
            | Self::RecordingLogAlreadyExists { .. }
            | Self::CaptureSessionSequenceExhausted { .. }
            | Self::IncompletePcmFrame { .. }
            | Self::SystemClockBeforeUnixEpoch { .. } => UnimplementedReason::StoreUnavailable,
            _ => UnimplementedReason::NotBuiltYet,
        }
    }
}
