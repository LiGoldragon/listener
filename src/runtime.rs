use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

use signal_listener::{
    ActiveCapture, ActiveCaptureSession, CancelCapture, CancellationRequestedSession,
    CancelledSession, CaptureAlreadyActive, CaptureArtifactBytes,
    CaptureArtifactDurationMilliseconds, CaptureArtifactState, CaptureArtifactStateValue,
    CaptureCancellationRequested, CaptureCancelled, CaptureListReport, CaptureRetried,
    CaptureSession, CaptureSessionMismatch, CaptureStarted, CaptureStatus, CaptureStopped,
    CaptureSummaries, CaptureSummary, DeliveryOutcome, DeliveryOutcomes, Input,
    ListCapturesRequest, NoActiveCapture, OperationKind, Output, OutputTargets, Reason,
    RequestUnimplemented, RequestedCaptureSession, RetriedSession, RetryCapture, StartCapture,
    StartedSession, StatusRequest, StopCapture, StoppedSession, ToggleCapture, TranscriptText,
    UnimplementedOperationKind, UnimplementedReason,
};

use crate::{
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, CaptureStore, Configuration, Error,
    FreedesktopSuccessNotifier, LatencyInstrumentation, OpenAiBatchTranscriptionActor,
    OutputTargetDispatcher, RecoveredCaptureRecordings, Result, SilentSuccessNotifier,
    SuccessNotifier, TranscriptHistoryEntry, TranscriptHistoryStore,
};
use crate::{
    BatchTranscriber, BatchTranscriptionInput, BatchTranscriptionRequest,
    ProcessAudioCaptureBackend, StatusPublisher,
};

pub struct ListenerRuntime {
    configuration: Configuration,
    capture_store: CaptureStore,
    capture_backend: Arc<dyn AudioCaptureBackend>,
    transcriber: Arc<dyn BatchTranscriber>,
    output_target_dispatcher: OutputTargetDispatcher,
    history_store: TranscriptHistoryStore,
    success_notifier: Arc<dyn SuccessNotifier>,
    status_publisher: StatusPublisher,
    latency_instrumentation: LatencyInstrumentation,
    session_sequence: CaptureSessionSequence,
    active_capture: Option<RuntimeActiveCapture>,
    orphaned_recordings: RecoveredCaptureRecordings,
}

struct RuntimeDependencies {
    success_notifier: Arc<dyn SuccessNotifier>,
    status_publisher: StatusPublisher,
    latency_instrumentation: LatencyInstrumentation,
}

impl ListenerRuntime {
    pub fn from_configuration(configuration: Configuration) -> Result<Self> {
        Self::from_configuration_with_status(configuration, StatusPublisher::silent())
    }

    pub fn from_configuration_with_status(
        configuration: Configuration,
        status_publisher: StatusPublisher,
    ) -> Result<Self> {
        Self::from_configuration_with_status_and_latency(
            configuration,
            status_publisher,
            LatencyInstrumentation::disabled(),
        )
    }

    pub fn from_configuration_with_status_and_latency(
        configuration: Configuration,
        status_publisher: StatusPublisher,
        latency_instrumentation: LatencyInstrumentation,
    ) -> Result<Self> {
        Ok(Self::with_dependencies_and_notifier_and_latency(
            configuration,
            Box::new(ProcessAudioCaptureBackend::from_environment()),
            Box::new(OpenAiBatchTranscriptionActor::from_environment(
                status_publisher.clone(),
            )?),
            OutputTargetDispatcher::from_environment(),
            TranscriptHistoryStore::from_environment()?,
            RuntimeDependencies {
                success_notifier: Arc::new(FreedesktopSuccessNotifier),
                status_publisher,
                latency_instrumentation,
            },
        ))
    }

    pub fn with_dependencies(
        configuration: Configuration,
        capture_backend: Box<dyn AudioCaptureBackend>,
        transcriber: Box<dyn BatchTranscriber>,
        output_target_dispatcher: OutputTargetDispatcher,
        history_store: TranscriptHistoryStore,
        status_publisher: StatusPublisher,
    ) -> Self {
        Self::with_dependencies_and_notifier_and_latency(
            configuration,
            capture_backend,
            transcriber,
            output_target_dispatcher,
            history_store,
            RuntimeDependencies {
                success_notifier: Arc::new(SilentSuccessNotifier),
                status_publisher,
                latency_instrumentation: LatencyInstrumentation::disabled(),
            },
        )
    }

    pub fn with_dependencies_and_notifier(
        configuration: Configuration,
        capture_backend: Box<dyn AudioCaptureBackend>,
        transcriber: Box<dyn BatchTranscriber>,
        output_target_dispatcher: OutputTargetDispatcher,
        history_store: TranscriptHistoryStore,
        success_notifier: Arc<dyn SuccessNotifier>,
        status_publisher: StatusPublisher,
    ) -> Self {
        Self::with_dependencies_and_notifier_and_latency(
            configuration,
            capture_backend,
            transcriber,
            output_target_dispatcher,
            history_store,
            RuntimeDependencies {
                success_notifier,
                status_publisher,
                latency_instrumentation: LatencyInstrumentation::disabled(),
            },
        )
    }

    pub fn with_dependencies_and_latency(
        configuration: Configuration,
        capture_backend: Box<dyn AudioCaptureBackend>,
        transcriber: Box<dyn BatchTranscriber>,
        output_target_dispatcher: OutputTargetDispatcher,
        history_store: TranscriptHistoryStore,
        status_publisher: StatusPublisher,
        latency_instrumentation: LatencyInstrumentation,
    ) -> Self {
        Self::with_dependencies_and_notifier_and_latency(
            configuration,
            capture_backend,
            transcriber,
            output_target_dispatcher,
            history_store,
            RuntimeDependencies {
                success_notifier: Arc::new(SilentSuccessNotifier),
                status_publisher,
                latency_instrumentation,
            },
        )
    }

    fn with_dependencies_and_notifier_and_latency(
        configuration: Configuration,
        capture_backend: Box<dyn AudioCaptureBackend>,
        transcriber: Box<dyn BatchTranscriber>,
        output_target_dispatcher: OutputTargetDispatcher,
        history_store: TranscriptHistoryStore,
        dependencies: RuntimeDependencies,
    ) -> Self {
        let RuntimeDependencies {
            success_notifier,
            status_publisher,
            latency_instrumentation,
        } = dependencies;
        let capture_store = CaptureStore::from_configuration(&configuration);
        Self {
            configuration,
            capture_store,
            capture_backend: Arc::from(capture_backend),
            transcriber: Arc::from(transcriber),
            output_target_dispatcher,
            history_store,
            success_notifier,
            status_publisher,
            latency_instrumentation,
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
            Input::ListCaptures(request) => self.list_captures(request).unwrap_or_else(|error| {
                error.into_unimplemented_reply(OperationKind::ListCaptures)
            }),
            Input::Retry(request) => self
                .retry_capture(request)
                .unwrap_or_else(|error| error.into_unimplemented_reply(OperationKind::Retry)),
            Input::Toggle(request) => self
                .toggle(request)
                .unwrap_or_else(Error::into_toggle_reply),
            Input::AcquireMaintenance(_) => Error::NotImplemented {
                surface: "listener maintenance lease actor",
            }
            .into_unimplemented_reply(OperationKind::AcquireMaintenance),
            Input::ReleaseMaintenance(_) => Error::NotImplemented {
                surface: "listener maintenance lease actor",
            }
            .into_unimplemented_reply(OperationKind::ReleaseMaintenance),
        }
    }

    pub fn start(&mut self, _request: StartCapture) -> Result<Output> {
        loop {
            let start = self.begin_capture_start()?;
            match start.start() {
                Ok(capture) => return Ok(self.install_started_capture(start, capture)),
                Err(error) if error.is_recording_log_already_exists() => {
                    self.advance_past_existing_capture_artifacts()?;
                }
                Err(error) => {
                    self.status_publisher.publish_error();
                    return Err(error);
                }
            }
        }
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
        self.capture_store.mark_terminal_capture(
            stopped_capture.session(),
            crate::TerminalCaptureState::Ready,
        )?;
        let compact_artifact = match self.compact_artifact_after_stop(&stopped_capture) {
            Ok(artifact) => artifact,
            Err(error) => {
                self.capture_store
                    .mark_transcription_failed(stopped_capture.session())
                    .ok();
                self.status_publisher.publish_error();
                return Err(error);
            }
        };
        let transcript_text = match self
            .transcribe_compact_capture(stopped_capture.session(), compact_artifact.clone())
        {
            Ok(transcript_text) => transcript_text,
            Err(error) => {
                self.capture_store
                    .mark_transcription_failed(stopped_capture.session())
                    .ok();
                self.status_publisher.publish_error();
                return Err(error);
            }
        };
        if self.record_transcript_history(stopped_capture.session(), &transcript_text) {
            self.capture_store.mark_terminal_capture(
                stopped_capture.session(),
                crate::TerminalCaptureState::Completed,
            )?;
        }
        let delivery_outcomes = self
            .output_target_dispatcher
            .deliver(self.configuration.output_targets(), &transcript_text);
        publish_delivery_feedback(
            &delivery_outcomes,
            &transcript_text,
            &self.success_notifier,
            &self.status_publisher,
        );

        Ok(Output::Stopped(CaptureStopped {
            stopped_session: StoppedSession::new(stopped_capture.session().clone()),
            durable_audio_artifact: compact_artifact,
            transcript_text,
            delivery_outcomes,
        }))
    }

    /// Atomically chooses the next capture transition from daemon-owned state.
    /// An active capture is finalized, transcribed, and delivered through the
    /// same graceful path as an explicit stop.
    pub fn toggle(&mut self, _request: ToggleCapture) -> Result<Output> {
        match self
            .active_capture
            .as_ref()
            .map(RuntimeActiveCapture::session)
            .cloned()
        {
            Some(session) => self.stop(StopCapture::new(session)),
            None => self.start(StartCapture {}),
        }
    }

    pub fn list_captures(&mut self, _request: ListCapturesRequest) -> Result<Output> {
        self.capture_store.prepare()?;
        let mut summaries = Vec::new();
        for session in self.capture_store.known_sessions()? {
            let compact_path = self.capture_store.compact_audio_path_for_session(&session);
            let log_artifact = self.capture_store.artifact_for_session(&session);
            let failed = matches!(
                self.capture_store.terminal_capture_state(&session)?,
                Some(crate::TerminalCaptureState::Failed)
            ) || self
                .capture_store
                .failed_marker_path_for_session(&session)
                .exists();
            let completed = self.history_store.contains_session(&session)?;
            let state = if completed {
                CaptureArtifactState::Completed
            } else if failed {
                CaptureArtifactState::Failed
            } else if compact_path.exists() {
                CaptureArtifactState::Retryable
            } else {
                CaptureArtifactState::Recovering
            };
            let artifact = if compact_path.exists() {
                self.capture_store.compact_artifact_for_session(&session)
            } else {
                log_artifact
            };
            let bytes = std::fs::metadata(artifact.path().as_str())?.len();
            summaries.push(CaptureSummary {
                capture_session: session,
                capture_artifact_state_value: CaptureArtifactStateValue::new(state),
                durable_audio_artifact: artifact,
                capture_artifact_bytes: CaptureArtifactBytes::new(bytes),
                capture_artifact_duration_milliseconds: CaptureArtifactDurationMilliseconds::new(0),
            });
        }
        Ok(Output::CapturesListed(CaptureListReport::new(
            CaptureSummaries::new(summaries),
        )))
    }

    pub fn retry_capture(&mut self, request: RetryCapture) -> Result<Output> {
        let session = request.into_payload();
        if self.history_store.contains_session(&session)? {
            return Err(Error::CaptureNotFound {
                session: session.value(),
            });
        }
        let compact_artifact = self.capture_store.compact_audio_for_session(&session)?;
        let transcript_text = match self.transcribe_compact_capture(&session, compact_artifact) {
            Ok(transcript) => transcript,
            Err(error) => {
                self.capture_store.mark_transcription_failed(&session).ok();
                return Err(error);
            }
        };
        if self.record_transcript_history(&session, &transcript_text) {
            self.capture_store
                .mark_terminal_capture(&session, crate::TerminalCaptureState::Completed)?;
        }
        let outcomes = self
            .output_target_dispatcher
            .deliver(self.configuration.output_targets(), &transcript_text);
        publish_delivery_feedback(
            &outcomes,
            &transcript_text,
            &self.success_notifier,
            &self.status_publisher,
        );
        Ok(Output::Retried(CaptureRetried {
            retried_session: RetriedSession::new(session),
            transcript_text,
            delivery_outcomes: outcomes,
        }))
    }

    pub fn cancel(&mut self, request: CancelCapture) -> Result<Output> {
        let active_capture = self.take_active_capture(request.into_payload())?;

        let stopped_capture = match active_capture.cancel() {
            Ok(stopped_capture) => stopped_capture,
            Err(error) => {
                self.status_publisher.publish_error();
                return Err(error);
            }
        };
        self.capture_store.mark_terminal_capture(
            stopped_capture.session(),
            crate::TerminalCaptureState::Cancelled,
        )?;
        self.status_publisher.publish_cancelled();

        Ok(Output::Cancelled(CaptureCancelled {
            cancelled_session: CancelledSession::new(stopped_capture.session().clone()),
            durable_audio_artifact: stopped_capture.artifact().clone(),
        }))
    }

    /// Append the finished transcript to the local history store. This is a
    /// best-effort convenience projection: the transcript is already in the stop
    /// reply and about to be delivered, so a history-write failure must not abort
    /// the stop or lose the transcript. A cancelled capture never reaches here.
    fn compact_artifact_after_stop(
        &self,
        stopped_capture: &StoppedCapture,
    ) -> Result<signal_listener::DurableAudioArtifact> {
        if stopped_capture
            .artifact()
            .path()
            .as_str()
            .ends_with(".webm")
        {
            self.capture_store
                .finalize_live_compact_for_session(stopped_capture.session())
        } else {
            self.capture_store
                .compact_audio_for_session(stopped_capture.session())
        }
    }

    fn transcribe_compact_capture(
        &self,
        session: &CaptureSession,
        compact_artifact: signal_listener::DurableAudioArtifact,
    ) -> Result<TranscriptText> {
        let input = BatchTranscriptionInput::webm_opus(std::path::PathBuf::from(
            compact_artifact.path().as_str(),
        ));
        let transcript = self
            .transcriber
            .transcribe(BatchTranscriptionRequest::new_with_input(
                compact_artifact,
                input,
            ))?;
        self.capture_store.clear_transcription_failure(session)?;
        Ok(transcript)
    }

    fn record_transcript_history(
        &self,
        session: &CaptureSession,
        transcript_text: &TranscriptText,
    ) -> bool {
        TranscriptHistoryEntry::recorded_now(session.clone(), transcript_text.clone())
            .and_then(|entry| self.history_store.append(&entry))
            .is_ok()
    }

    /// Return only the runtime-owned active slot. This must remain O(1):
    /// recovery, migration, and retention run in the finite startup task.
    pub fn status(&mut self, _request: StatusRequest) -> Result<Output> {
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

    /// Advance the first capture allocation past the daemon-start snapshot.
    /// This is a constant-time handoff that prevents a new recovery log from
    /// sharing any retained capture artifact's session name.
    pub fn advance_session_sequence(&mut self, next_session_value: u64) {
        self.session_sequence
            .advance_to_at_least(next_session_value);
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

    pub fn begin_capture_start(&mut self) -> Result<RuntimeCaptureStartWork> {
        if let Some(active_capture) = &self.active_capture {
            return Err(Error::CaptureAlreadyActive {
                session: active_capture.session().value(),
            });
        }

        self.status_publisher.publish_starting();
        self.capture_store.prepare()?;
        loop {
            let session = self.session_sequence.next_session()?;
            if !self.capture_store.session_is_occupied(&session)? {
                let artifact = self.capture_store.artifact_for_session(&session);
                return Ok(RuntimeCaptureStartWork::new(
                    session,
                    artifact,
                    self.configuration.input_source(),
                    Arc::clone(&self.capture_backend),
                    self.status_publisher.clone(),
                    self.latency_instrumentation.clone(),
                ));
            }
        }
    }

    pub fn install_started_capture(
        &mut self,
        start: RuntimeCaptureStartWork,
        capture: Box<dyn ActiveAudioCapture>,
    ) -> Output {
        let session = start.session().clone();
        self.active_capture = Some(RuntimeActiveCapture::new(
            session.clone(),
            start.artifact().clone(),
            capture,
        ));
        Output::Started(CaptureStarted::new(StartedSession::new(session)))
    }

    pub fn begin_capture_cancellation(
        &mut self,
        requested_session: CaptureSession,
    ) -> Result<RuntimeCaptureCancellationWork> {
        let active_capture = self.take_active_capture(requested_session)?;
        Ok(RuntimeCaptureCancellationWork::new(
            active_capture,
            self.capture_store.clone(),
            self.status_publisher.clone(),
        ))
    }

    pub fn begin_capture_finalization(
        &mut self,
        requested_session: CaptureSession,
    ) -> Result<RuntimeCaptureFinalizationWork> {
        let active_capture = self.take_active_capture(requested_session)?;
        Ok(RuntimeCaptureFinalizationWork::new(
            active_capture,
            self.capture_store.clone(),
            Arc::clone(&self.transcriber),
            self.output_target_dispatcher.clone(),
            self.history_store.clone(),
            self.configuration.output_targets().clone(),
            RuntimeDeliveryFeedback {
                success_notifier: Arc::clone(&self.success_notifier),
                status_publisher: self.status_publisher.clone(),
            },
        ))
    }

    pub fn publish_finalizing(&self) {
        self.status_publisher.publish_finalizing();
    }

    pub fn publish_cancelling(&self) {
        self.status_publisher.publish_cancelling();
    }

    fn advance_past_existing_capture_artifacts(&mut self) -> Result<()> {
        let next_session_value = self
            .capture_store
            .next_session_value_after_existing_artifacts()?;
        self.session_sequence
            .advance_to_at_least(next_session_value);
        Ok(())
    }
}

#[derive(Clone)]
pub struct CaptureCancellationSignal {
    requested: Arc<AtomicBool>,
}

impl CaptureCancellationSignal {
    pub fn new() -> Self {
        Self {
            requested: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

impl Default for CaptureCancellationSignal {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RuntimeCaptureStartWork {
    session: CaptureSession,
    artifact: signal_listener::DurableAudioArtifact,
    input_source: signal_listener::InputSource,
    capture_backend: Arc<dyn AudioCaptureBackend>,
    status_publisher: StatusPublisher,
    latency_instrumentation: LatencyInstrumentation,
}

impl RuntimeCaptureStartWork {
    fn new(
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
        input_source: signal_listener::InputSource,
        capture_backend: Arc<dyn AudioCaptureBackend>,
        status_publisher: StatusPublisher,
        latency_instrumentation: LatencyInstrumentation,
    ) -> Self {
        Self {
            session,
            artifact,
            input_source,
            capture_backend,
            status_publisher,
            latency_instrumentation,
        }
    }

    pub fn session(&self) -> &CaptureSession {
        &self.session
    }

    pub fn artifact(&self) -> &signal_listener::DurableAudioArtifact {
        &self.artifact
    }

    pub fn clone_for_worker(&self) -> Self {
        Self {
            session: self.session.clone(),
            artifact: self.artifact.clone(),
            input_source: self.input_source,
            capture_backend: Arc::clone(&self.capture_backend),
            status_publisher: self.status_publisher.clone(),
            latency_instrumentation: self.latency_instrumentation.clone(),
        }
    }

    pub fn start(&self) -> Result<Box<dyn ActiveAudioCapture>> {
        self.capture_backend.start(
            AudioCaptureStart::new(
                self.session.clone(),
                self.artifact.clone(),
                self.input_source,
                self.status_publisher.clone(),
            )
            .with_latency_instrumentation(self.latency_instrumentation.clone()),
        )
    }
}

pub struct RuntimeCaptureCancellationWork {
    active_capture: RuntimeActiveCapture,
    completion: RuntimeCancellationCompletion,
}

impl RuntimeCaptureCancellationWork {
    fn new(
        active_capture: RuntimeActiveCapture,
        capture_store: CaptureStore,
        status_publisher: StatusPublisher,
    ) -> Self {
        Self {
            active_capture,
            completion: RuntimeCancellationCompletion::new(capture_store, status_publisher),
        }
    }

    pub fn session(&self) -> &CaptureSession {
        self.active_capture.session()
    }

    pub fn artifact(&self) -> &signal_listener::DurableAudioArtifact {
        self.active_capture.artifact()
    }

    pub fn requested_reply(&self) -> Output {
        self.completion
            .requested_reply(self.session().clone(), self.artifact().clone())
    }

    pub fn execute(self) -> Output {
        match self.active_capture.cancel() {
            Ok(stopped_capture) => self.completion.complete(stopped_capture),
            Err(error) => {
                self.completion.status_publisher.publish_error();
                error.into_cancel_reply()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureFinalizationPhase {
    Finalizing,
    Transcribing,
}

struct RuntimeDeliveryFeedback {
    success_notifier: Arc<dyn SuccessNotifier>,
    status_publisher: StatusPublisher,
}

pub struct RuntimeCaptureFinalizationWork {
    active_capture: RuntimeActiveCapture,
    capture_store: CaptureStore,
    transcriber: Arc<dyn BatchTranscriber>,
    output_target_dispatcher: OutputTargetDispatcher,
    history_store: TranscriptHistoryStore,
    output_targets: OutputTargets,
    feedback: RuntimeDeliveryFeedback,
    completion: RuntimeCancellationCompletion,
}

impl RuntimeCaptureFinalizationWork {
    fn new(
        active_capture: RuntimeActiveCapture,
        capture_store: CaptureStore,
        transcriber: Arc<dyn BatchTranscriber>,
        output_target_dispatcher: OutputTargetDispatcher,
        history_store: TranscriptHistoryStore,
        output_targets: OutputTargets,
        feedback: RuntimeDeliveryFeedback,
    ) -> Self {
        let completion = RuntimeCancellationCompletion::new(
            capture_store.clone(),
            feedback.status_publisher.clone(),
        );
        Self {
            active_capture,
            capture_store: capture_store.clone(),
            transcriber,
            output_target_dispatcher,
            history_store,
            output_targets,
            feedback,
            completion,
        }
    }

    pub fn session(&self) -> &CaptureSession {
        self.active_capture.session()
    }

    pub fn artifact(&self) -> &signal_listener::DurableAudioArtifact {
        self.active_capture.artifact()
    }

    pub fn execute(
        self,
        cancellation: CaptureCancellationSignal,
        phase_sender: mpsc::Sender<CaptureFinalizationPhase>,
    ) -> Output {
        let RuntimeCaptureFinalizationWork {
            active_capture,
            capture_store,
            transcriber,
            output_target_dispatcher,
            history_store,
            output_targets,
            feedback,
            completion,
        } = self;
        let stopped_capture = match active_capture.stop() {
            Ok(stopped_capture) => stopped_capture,
            Err(error) => {
                completion.status_publisher.publish_error();
                return error.into_stop_reply();
            }
        };
        if cancellation.is_requested() {
            return completion.complete(stopped_capture);
        }
        if let Err(error) = capture_store.mark_terminal_capture(
            stopped_capture.session(),
            crate::TerminalCaptureState::Ready,
        ) {
            completion.status_publisher.publish_error();
            return error.into_stop_reply();
        }
        let compact_artifact =
            match Self::compact_artifact_after_stop(&capture_store, &stopped_capture) {
                Ok(artifact) => artifact,
                Err(error) => {
                    capture_store
                        .mark_transcription_failed(stopped_capture.session())
                        .ok();
                    completion.status_publisher.publish_error();
                    return error.into_stop_reply();
                }
            };
        if cancellation.is_requested() {
            return completion.cancelled(stopped_capture.session().clone(), compact_artifact);
        }
        let _ = phase_sender.send(CaptureFinalizationPhase::Transcribing);
        let transcript_text = match Self::transcribe(
            &capture_store,
            &transcriber,
            stopped_capture.session(),
            compact_artifact.clone(),
        ) {
            Ok(transcript_text) => transcript_text,
            Err(error) => {
                capture_store
                    .mark_transcription_failed(stopped_capture.session())
                    .ok();
                completion.status_publisher.publish_error();
                return error.into_stop_reply();
            }
        };
        if cancellation.is_requested() {
            return completion.cancelled(stopped_capture.session().clone(), compact_artifact);
        }
        if Self::record_history(&history_store, stopped_capture.session(), &transcript_text)
            && let Err(error) = capture_store.mark_terminal_capture(
                stopped_capture.session(),
                crate::TerminalCaptureState::Completed,
            )
        {
            completion.status_publisher.publish_error();
            return error.into_stop_reply();
        }
        if cancellation.is_requested() {
            return completion.cancelled(stopped_capture.session().clone(), compact_artifact);
        }
        let delivery_outcomes = output_target_dispatcher.deliver(&output_targets, &transcript_text);
        publish_delivery_feedback(
            &delivery_outcomes,
            &transcript_text,
            &feedback.success_notifier,
            &feedback.status_publisher,
        );
        Output::Stopped(CaptureStopped {
            stopped_session: StoppedSession::new(stopped_capture.session().clone()),
            durable_audio_artifact: compact_artifact,
            transcript_text,
            delivery_outcomes,
        })
    }

    fn compact_artifact_after_stop(
        capture_store: &CaptureStore,
        stopped_capture: &StoppedCapture,
    ) -> Result<signal_listener::DurableAudioArtifact> {
        if stopped_capture
            .artifact()
            .path()
            .as_str()
            .ends_with(".webm")
        {
            capture_store.finalize_live_compact_for_session(stopped_capture.session())
        } else {
            capture_store.compact_audio_for_session(stopped_capture.session())
        }
    }

    fn transcribe(
        capture_store: &CaptureStore,
        transcriber: &Arc<dyn BatchTranscriber>,
        session: &CaptureSession,
        compact_artifact: signal_listener::DurableAudioArtifact,
    ) -> Result<TranscriptText> {
        let input = BatchTranscriptionInput::webm_opus(std::path::PathBuf::from(
            compact_artifact.path().as_str(),
        ));
        let transcript = transcriber.transcribe(BatchTranscriptionRequest::new_with_input(
            compact_artifact,
            input,
        ))?;
        capture_store.clear_transcription_failure(session)?;
        Ok(transcript)
    }

    fn record_history(
        history_store: &TranscriptHistoryStore,
        session: &CaptureSession,
        transcript_text: &TranscriptText,
    ) -> bool {
        TranscriptHistoryEntry::recorded_now(session.clone(), transcript_text.clone())
            .and_then(|entry| history_store.append(&entry))
            .is_ok()
    }
}

struct RuntimeCancellationCompletion {
    capture_store: CaptureStore,
    status_publisher: StatusPublisher,
}

impl RuntimeCancellationCompletion {
    fn new(capture_store: CaptureStore, status_publisher: StatusPublisher) -> Self {
        Self {
            capture_store,
            status_publisher,
        }
    }

    fn requested_reply(
        &self,
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    ) -> Output {
        Output::CancellationRequested(CaptureCancellationRequested {
            cancellation_requested_session: CancellationRequestedSession::new(session),
            durable_audio_artifact: artifact,
        })
    }

    fn complete(&self, stopped_capture: StoppedCapture) -> Output {
        self.cancelled(
            stopped_capture.session().clone(),
            stopped_capture.artifact().clone(),
        )
    }

    fn cancelled(
        &self,
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    ) -> Output {
        match self
            .capture_store
            .mark_terminal_capture(&session, crate::TerminalCaptureState::Cancelled)
        {
            Ok(()) => {
                self.status_publisher.publish_cancelled();
                Output::Cancelled(CaptureCancelled {
                    cancelled_session: CancelledSession::new(session),
                    durable_audio_artifact: artifact,
                })
            }
            Err(error) => {
                self.status_publisher.publish_error();
                error.into_cancel_reply()
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

fn publish_delivery_feedback(
    delivery_outcomes: &DeliveryOutcomes,
    transcript_text: &TranscriptText,
    success_notifier: &Arc<dyn SuccessNotifier>,
    status_publisher: &StatusPublisher,
) {
    let status = RuntimeDeliveryStatus::from_outcomes(delivery_outcomes);
    status.publish(status_publisher);
    if status == RuntimeDeliveryStatus::Delivered {
        success_notifier.notify(transcript_text);
    }
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
            Self::Delivered => status_publisher.publish_delivered(),
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

    pub fn cancel(self) -> Result<StoppedCapture> {
        let artifact = self.capture.cancel()?;
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
            Self::CaptureAlreadyActive { session } => Output::AlreadyActive(
                CaptureAlreadyActive::new(ActiveCaptureSession::new(CaptureSession::new(session))),
            ),
            error => error.into_unimplemented_reply(OperationKind::Start),
        }
    }

    pub fn into_stop_reply(self) -> Output {
        match self {
            Self::NoActiveCapture => Output::NoActive(NoActiveCapture {}),
            Self::CaptureSessionMismatch { active, requested } => {
                Output::SessionMismatch(CaptureSessionMismatch {
                    active_capture_session: ActiveCaptureSession::new(CaptureSession::new(active)),
                    requested_capture_session: RequestedCaptureSession::new(CaptureSession::new(
                        requested,
                    )),
                })
            }
            error => error.into_unimplemented_reply(OperationKind::Stop),
        }
    }

    pub fn into_toggle_reply(self) -> Output {
        self.into_unimplemented_reply(OperationKind::Toggle)
    }

    pub fn into_cancel_reply(self) -> Output {
        match self {
            Self::NoActiveCapture => Output::NoActive(NoActiveCapture {}),
            Self::CaptureSessionMismatch { active, requested } => {
                Output::SessionMismatch(CaptureSessionMismatch {
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
        Output::Unimplemented(RequestUnimplemented {
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
            | Self::TranscriptionActorUnavailable { .. }
            | Self::CompactAudioEncode { .. }
            | Self::CompactAudioInvalid { .. } => {
                UnimplementedReason::TranscriptionBackendUnavailable
            }
            Self::OutputTargetRejected { .. } => UnimplementedReason::OutputTargetUnavailable,
            Self::Io(_)
            | Self::InvalidAudioFormat { .. }
            | Self::InvalidRecordingLog { .. }
            | Self::RecordingLogAlreadyExists { .. }
            | Self::CaptureSessionSequenceExhausted { .. }
            | Self::IncompletePcmFrame { .. }
            | Self::HistoryEntryEncode { .. }
            | Self::HistoryEntryDecode { .. }
            | Self::InvalidHistoryRetentionPolicy { .. }
            | Self::InvalidCaptureRetentionPolicy { .. }
            | Self::SystemClockBeforeUnixEpoch { .. }
            | Self::CaptureNotFound { .. } => UnimplementedReason::StoreUnavailable,
            _ => UnimplementedReason::NotBuiltYet,
        }
    }
}
