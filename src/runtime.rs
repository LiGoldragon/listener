use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
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
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, CaptureStore, CommittedSampleSink,
    Configuration,
    DurableProviderFinalizer, Error, FreedesktopSuccessNotifier, LatencyInstrumentation,
    MetaProviderPolicyService, OpenAiBatchProvider, OpenAiBatchTranscriptionActor,
    OutputTargetDispatcher, ProviderCircuitBreaker, ProviderJobStore, ProviderPolicy,
    ProviderRouter,
    RecoveredCaptureRecordings, Result, SilentSuccessNotifier, SuccessNotifier,
    TranscriptHistoryStore,
};
use crate::{BatchTranscriber, ProcessAudioCaptureBackend, StatusPublisher, TranscriptProvider};

pub struct ListenerRuntime {
    configuration: Configuration,
    capture_store: CaptureStore,
    capture_backend: Arc<dyn AudioCaptureBackend>,
    transcriber: Arc<dyn BatchTranscriber>,
    output_target_dispatcher: OutputTargetDispatcher,
    provider_finalizer: std::result::Result<DurableProviderFinalizer, String>,
    provider_policy_service: Option<Arc<MetaProviderPolicyService>>,
    history_store: TranscriptHistoryStore,
    success_notifier: Arc<dyn SuccessNotifier>,
    status_publisher: StatusPublisher,
    latency_instrumentation: LatencyInstrumentation,
    delivery_ownership_admission: Arc<dyn DeliveryOwnershipAdmission>,
    session_sequence: CaptureSessionSequence,
    active_capture: Option<RuntimeActiveCapture>,
    orphaned_recordings: RecoveredCaptureRecordings,
}

struct RuntimeDependencies {
    success_notifier: Arc<dyn SuccessNotifier>,
    status_publisher: StatusPublisher,
    latency_instrumentation: LatencyInstrumentation,
    delivery_ownership_admission: Arc<dyn DeliveryOwnershipAdmission>,
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
                success_notifier: Arc::new(FreedesktopSuccessNotifier::default()),
                status_publisher,
                latency_instrumentation,
                delivery_ownership_admission: Arc::new(ImmediateDeliveryOwnershipAdmission),
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
                delivery_ownership_admission: Arc::new(ImmediateDeliveryOwnershipAdmission),
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
                delivery_ownership_admission: Arc::new(ImmediateDeliveryOwnershipAdmission),
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
                delivery_ownership_admission: Arc::new(ImmediateDeliveryOwnershipAdmission),
            },
        )
    }

    pub fn with_dependencies_and_finalization_feedback(
        configuration: Configuration,
        capture_backend: Box<dyn AudioCaptureBackend>,
        transcriber: Box<dyn BatchTranscriber>,
        output_target_dispatcher: OutputTargetDispatcher,
        history_store: TranscriptHistoryStore,
        status_publisher: StatusPublisher,
        feedback: RuntimeFinalizationFeedback,
    ) -> Self {
        Self::with_dependencies_and_notifier_and_latency(
            configuration,
            capture_backend,
            transcriber,
            output_target_dispatcher,
            history_store,
            RuntimeDependencies {
                success_notifier: feedback.success_notifier,
                status_publisher,
                latency_instrumentation: LatencyInstrumentation::disabled(),
                delivery_ownership_admission: feedback.delivery_ownership_admission,
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
            delivery_ownership_admission,
        } = dependencies;
        let capture_store = CaptureStore::from_configuration(&configuration);
        let transcriber: Arc<dyn BatchTranscriber> = Arc::from(transcriber);
        let provider_finalizer = Self::provider_finalizer(
            &configuration,
            ProviderRouter::new(vec![Arc::new(OpenAiBatchProvider::new(Arc::clone(
                &transcriber,
            )))]),
            output_target_dispatcher.clone(),
            history_store.clone(),
        );
        Self {
            configuration,
            capture_store,
            capture_backend: Arc::from(capture_backend),
            transcriber,
            output_target_dispatcher,
            provider_finalizer,
            provider_policy_service: None,
            history_store,
            success_notifier,
            status_publisher,
            latency_instrumentation,
            delivery_ownership_admission,
            session_sequence: CaptureSessionSequence::new(1),
            active_capture: None,
            orphaned_recordings: RecoveredCaptureRecordings::empty(),
        }
    }

    fn provider_finalizer(
        configuration: &Configuration,
        router: ProviderRouter,
        dispatcher: OutputTargetDispatcher,
        history: TranscriptHistoryStore,
    ) -> std::result::Result<DurableProviderFinalizer, String> {
        ProviderJobStore::open(
            configuration
                .capture_store_directory()
                .join("transcription-provider-jobs.sema"),
        )
        .map(|jobs| {
            DurableProviderFinalizer::new(
                jobs,
                router,
                dispatcher,
                history,
            )
        })
        .map_err(|error| error.to_string())
    }

    /// The daemon shares its serialized durable policy owner with the runtime;
    /// every new job snapshots it once before its first provider attempt.
    pub fn use_provider_policy_service(&mut self, service: Arc<MetaProviderPolicyService>) {
        self.provider_policy_service = Some(service);
    }

    /// Installs a host-composed router finalizer. This is the production
    /// injection seam for the credential-free Wispr session/transport boundary
    /// and for synthetic provider fixtures; it never accepts secret bytes.
    pub fn use_provider_finalizer(&mut self, finalizer: DurableProviderFinalizer) {
        self.provider_finalizer = Ok(finalizer);
    }

    /// Installs Wispr first and the already-hosted OpenAI worker second. The
    /// durable meta policy still selects which configured provider may run.
    pub fn use_wispr_then_openai_provider(
        &mut self,
        wispr: Arc<dyn TranscriptProvider>,
        circuit_breaker: Arc<ProviderCircuitBreaker>,
    ) {
        let router = ProviderRouter::with_circuit_breaker(
            vec![
                wispr,
                Arc::new(OpenAiBatchProvider::new(Arc::clone(&self.transcriber))),
            ],
            circuit_breaker,
        );
        self.provider_finalizer = Self::provider_finalizer(
            &self.configuration,
            router,
            self.output_target_dispatcher.clone(),
            self.history_store.clone(),
        );
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
        let raw_artifact = self.finalization_source_artifact(
            stopped_capture.session(),
            stopped_capture.artifact().clone(),
        );
        let finalization =
            match self.finalize_capture(stopped_capture.session(), raw_artifact.clone()) {
                Ok(finalization) => finalization,
                Err(error) => {
                    self.capture_store
                        .mark_transcription_failed(stopped_capture.session())
                        .ok();
                    self.status_publisher.publish_error();
                    return Err(error);
                }
            };
        self.capture_store.mark_terminal_capture(
            stopped_capture.session(),
            crate::TerminalCaptureState::Completed,
        )?;
        let durable_audio_artifact = self
            .compact_artifact_after_stop(&stopped_capture)
            .unwrap_or(raw_artifact);
        let transcript_text = finalization.transcript().clone();
        let delivery_outcomes = finalization.delivery_outcomes().clone();
        publish_delivery_feedback(
            &delivery_outcomes,
            &transcript_text,
            &self.success_notifier,
            &self.status_publisher,
        );

        Ok(Output::Stopped(CaptureStopped {
            stopped_session: StoppedSession::new(stopped_capture.session().clone()),
            durable_audio_artifact,
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
        let raw_artifact = self.finalization_source_artifact(
            &session,
            self.capture_store.compact_artifact_for_session(&session),
        );
        let finalization = match self.finalize_capture(&session, raw_artifact.clone()) {
            Ok(finalization) => finalization,
            Err(error) => {
                self.capture_store.mark_transcription_failed(&session).ok();
                return Err(error);
            }
        };
        self.capture_store
            .mark_terminal_capture(&session, crate::TerminalCaptureState::Completed)?;
        let _ = self.capture_store.compact_audio_for_session(&session);
        let transcript_text = finalization.transcript().clone();
        let outcomes = finalization.delivery_outcomes().clone();
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

    fn finalize_capture(
        &self,
        session: &CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    ) -> Result<crate::ProviderFinalizationOutcome> {
        let policy = self.current_provider_policy()?;
        let finalizer = self.provider_finalizer.as_ref().map_err(|message| {
            Error::ProviderFinalizationUnavailable {
                message: message.clone(),
            }
        })?;
        let prepared =
            Self::prepare_artifact(finalizer, session, artifact, policy).map_err(|_| {
                Error::TranscriptionBackendUnavailable {
                    message: "all configured providers unavailable; audio preserved".to_owned(),
                }
            })?;
        finalizer
            .record_history(session, prepared.transcript())
            .map_err(|error| Error::ProviderFinalizationUnavailable {
                message: error.to_string(),
            })?;
        let delivery_outcomes = finalizer
            .deliver(
                session,
                self.configuration.output_targets(),
                prepared.transcript(),
            )
            .map_err(|error| Error::ProviderFinalizationUnavailable {
                message: error.to_string(),
            })?;
        self.capture_store.clear_transcription_failure(session)?;
        Ok(crate::ProviderFinalizationOutcome::from_prepared(
            prepared,
            delivery_outcomes,
        ))
    }

    fn finalization_source_artifact(
        &self,
        session: &CaptureSession,
        fallback: signal_listener::DurableAudioArtifact,
    ) -> signal_listener::DurableAudioArtifact {
        let raw = self.capture_store.artifact_for_session(session);
        std::path::Path::new(raw.path().as_str())
            .is_file()
            .then_some(raw)
            .unwrap_or(fallback)
    }

    fn prepare_artifact(
        finalizer: &DurableProviderFinalizer,
        session: &CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
        policy: ProviderPolicy,
    ) -> std::result::Result<crate::PreparedProviderFinalization, crate::ProviderFinalizationError>
    {
        if artifact.path().as_str().ends_with(".listenerlog") {
            finalizer.prepare_recording_log_segments(session, artifact, policy)
        } else {
            finalizer.prepare(session, artifact, policy)
        }
    }

    fn current_provider_policy(&self) -> Result<ProviderPolicy> {
        self.provider_policy_service
            .as_ref()
            .map(|service| service.current())
            .transpose()
            .map_err(|error| Error::ProviderPolicyUnavailable {
                message: error.to_string(),
            })?
            .flatten()
            .or_else(|| Some(ProviderPolicy::wispr_then_openai()))
            .ok_or_else(|| Error::ProviderPolicyUnavailable {
                message: "no valid provider policy".to_owned(),
            })
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
            self.provider_finalizer.clone(),
            self.provider_policy_service.clone(),
            self.configuration.output_targets().clone(),
            RuntimeDeliveryFeedback {
                success_notifier: Arc::clone(&self.success_notifier),
                status_publisher: self.status_publisher.clone(),
                delivery_ownership_admission: Arc::clone(&self.delivery_ownership_admission),
            },
        ))
    }

    /// Builds bounded background work for ranges that are already durably
    /// committed while capture continues. It never delivers or compacts; Stop
    /// remains the sole finalizer for the tail and assembled transcript.
    pub fn begin_committed_segment_finalization(
        &self,
        session: CaptureSession,
        committed_sample_end: u64,
    ) -> Result<RuntimeCommittedSegmentWork> {
        let finalizer = self.provider_finalizer.as_ref().map_err(|message| {
            Error::ProviderFinalizationUnavailable {
                message: message.clone(),
            }
        })?;
        Ok(RuntimeCommittedSegmentWork {
            session: session.clone(),
            artifact: self.capture_store.artifact_for_session(&session),
            finalizer: finalizer.clone(),
            policy: self.current_provider_policy()?,
            committed_sample_end,
        })
    }

    pub fn publish_finalizing(&self) {
        self.status_publisher.publish_finalizing();
    }

    pub fn publish_starting(&self) {
        self.status_publisher.publish_starting();
    }

    pub fn publish_transcribing(&self) {
        self.status_publisher.publish_transcribing();
    }

    pub fn publish_recording(&self) {
        self.status_publisher
            .publish_recording_level(crate::MicrophoneLevel::silent());
    }

    pub fn publish_idle(&self) {
        self.status_publisher.publish_idle();
    }

    pub fn publish_delivered(&self) {
        self.status_publisher.publish_delivered();
    }

    pub fn publish_error(&self) {
        self.status_publisher.publish_error();
    }

    pub fn set_in_flight_transcriptions(&self, in_flight_transcriptions: usize) {
        self.status_publisher
            .set_in_flight_transcriptions(in_flight_transcriptions);
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
    ownership: Arc<AtomicU8>,
}

impl CaptureCancellationSignal {
    const CANCELLABLE: u8 = 0;
    const CANCELLATION_REQUESTED: u8 = 1;
    const DELIVERY_OWNED: u8 = 2;

    pub fn new() -> Self {
        Self {
            ownership: Arc::new(AtomicU8::new(Self::CANCELLABLE)),
        }
    }

    /// Returns whether cancellation owns finalization. Delivery ownership is
    /// irrevocable, so a late cancellation must continue awaiting completion.
    pub fn request(&self) -> bool {
        match self.ownership.compare_exchange(
            Self::CANCELLABLE,
            Self::CANCELLATION_REQUESTED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(Self::CANCELLATION_REQUESTED) => true,
            Err(Self::DELIVERY_OWNED) => false,
            Err(state) => unreachable!("unknown capture finalization ownership state: {state}"),
        }
    }

    pub fn is_requested(&self) -> bool {
        self.ownership.load(Ordering::Acquire) == Self::CANCELLATION_REQUESTED
    }

    fn claim_delivery_ownership(&self) -> bool {
        match self.ownership.compare_exchange(
            Self::CANCELLABLE,
            Self::DELIVERY_OWNED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(Self::CANCELLATION_REQUESTED) => false,
            Err(Self::DELIVERY_OWNED) => {
                unreachable!("delivery ownership can only be claimed once")
            }
            Err(state) => unreachable!("unknown capture finalization ownership state: {state}"),
        }
    }
}

impl Default for CaptureCancellationSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// The explicit boundary immediately before irreversible delivery effects.
pub trait DeliveryOwnershipAdmission: Send + Sync {
    fn await_delivery_ownership(&self);
}

struct ImmediateDeliveryOwnershipAdmission;

impl DeliveryOwnershipAdmission for ImmediateDeliveryOwnershipAdmission {
    fn await_delivery_ownership(&self) {}
}

pub struct RuntimeCaptureStartWork {
    session: CaptureSession,
    artifact: signal_listener::DurableAudioArtifact,
    input_source: signal_listener::InputSource,
    capture_backend: Arc<dyn AudioCaptureBackend>,
    status_publisher: StatusPublisher,
    latency_instrumentation: LatencyInstrumentation,
    committed_sample_sink: Option<Arc<dyn CommittedSampleSink>>,
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
            committed_sample_sink: None,
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
            committed_sample_sink: self.committed_sample_sink.clone(),
        }
    }

    pub fn with_committed_sample_sink(
        mut self,
        committed_sample_sink: Arc<dyn CommittedSampleSink>,
    ) -> Self {
        self.committed_sample_sink = Some(committed_sample_sink);
        self
    }

    pub fn start(&self) -> Result<Box<dyn ActiveAudioCapture>> {
        let request = AudioCaptureStart::new(
                self.session.clone(),
                self.artifact.clone(),
                self.input_source,
                self.status_publisher.clone(),
            )
            .with_latency_instrumentation(self.latency_instrumentation.clone());
        let request = match &self.committed_sample_sink {
            Some(sink) => request.with_committed_sample_sink(Arc::clone(sink)),
            None => request,
        };
        self.capture_backend.start(request)
    }
}

pub struct RuntimeCommittedSegmentWork {
    session: CaptureSession,
    artifact: signal_listener::DurableAudioArtifact,
    finalizer: DurableProviderFinalizer,
    policy: ProviderPolicy,
    committed_sample_end: u64,
}

impl RuntimeCommittedSegmentWork {
    pub fn execute(self) -> Result<()> {
        self.finalizer
            .prepare_recording_log_completed_segments(
                &self.session,
                self.artifact,
                self.policy,
                self.committed_sample_end,
            )
            .map(|_| ())
            .map_err(|error| Error::ProviderFinalizationUnavailable {
                message: error.to_string(),
            })
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
    delivery_ownership_admission: Arc<dyn DeliveryOwnershipAdmission>,
}

pub struct RuntimeFinalizationFeedback {
    success_notifier: Arc<dyn SuccessNotifier>,
    delivery_ownership_admission: Arc<dyn DeliveryOwnershipAdmission>,
}

impl RuntimeFinalizationFeedback {
    pub fn new(
        success_notifier: Arc<dyn SuccessNotifier>,
        delivery_ownership_admission: Arc<dyn DeliveryOwnershipAdmission>,
    ) -> Self {
        Self {
            success_notifier,
            delivery_ownership_admission,
        }
    }
}

pub struct RuntimeCaptureFinalizationWork {
    active_capture: RuntimeActiveCapture,
    capture_store: CaptureStore,
    provider_finalizer: std::result::Result<DurableProviderFinalizer, String>,
    provider_policy_service: Option<Arc<MetaProviderPolicyService>>,
    output_targets: OutputTargets,
    feedback: RuntimeDeliveryFeedback,
    completion: RuntimeCancellationCompletion,
}

impl RuntimeCaptureFinalizationWork {
    fn new(
        active_capture: RuntimeActiveCapture,
        capture_store: CaptureStore,
        provider_finalizer: std::result::Result<DurableProviderFinalizer, String>,
        provider_policy_service: Option<Arc<MetaProviderPolicyService>>,
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
            provider_finalizer,
            provider_policy_service,
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

    pub fn execute<PublishPhase>(
        self,
        cancellation: CaptureCancellationSignal,
        publish_phase: PublishPhase,
    ) -> Output
    where
        PublishPhase: Fn(CaptureFinalizationPhase),
    {
        let RuntimeCaptureFinalizationWork {
            active_capture,
            capture_store,
            provider_finalizer,
            provider_policy_service,
            output_targets,
            feedback,
            completion,
        } = self;
        let RuntimeDeliveryFeedback {
            success_notifier,
            status_publisher,
            delivery_ownership_admission,
        } = feedback;
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
        let raw_artifact = Self::finalization_source_artifact(
            &capture_store,
            stopped_capture.session(),
            stopped_capture.artifact().clone(),
        );
        if cancellation.is_requested() {
            return completion.cancelled(stopped_capture.session().clone(), raw_artifact);
        }
        publish_phase(CaptureFinalizationPhase::Transcribing);
        let policy = match provider_policy_service
            .as_ref()
            .map(|service| service.current())
            .transpose()
            .map_err(|error| Error::ProviderPolicyUnavailable {
                message: error.to_string(),
            })
            .map(|policy| {
                policy
                    .flatten()
                    .unwrap_or_else(ProviderPolicy::wispr_then_openai)
            }) {
            Ok(policy) => policy,
            Err(error) => {
                capture_store
                    .mark_transcription_failed(stopped_capture.session())
                    .ok();
                completion.status_publisher.publish_error();
                return error.into_stop_reply();
            }
        };
        let finalizer = match provider_finalizer {
            Ok(finalizer) => finalizer,
            Err(message) => {
                capture_store
                    .mark_transcription_failed(stopped_capture.session())
                    .ok();
                completion.status_publisher.publish_error();
                return Error::ProviderFinalizationUnavailable { message }.into_stop_reply();
            }
        };
        let prepared = match Self::prepare_artifact(
            &finalizer,
            stopped_capture.session(),
            raw_artifact.clone(),
            policy,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                capture_store
                    .mark_transcription_failed(stopped_capture.session())
                    .ok();
                completion.status_publisher.publish_error();
                return Error::TranscriptionBackendUnavailable {
                    message: format!(
                        "all configured providers unavailable; audio preserved ({error})"
                    ),
                }
                .into_stop_reply();
            }
        };
        let transcript_text = prepared.transcript().clone();
        delivery_ownership_admission.await_delivery_ownership();
        if !cancellation.claim_delivery_ownership() {
            return completion.cancelled(stopped_capture.session().clone(), raw_artifact);
        }
        if let Err(error) = finalizer.record_history(stopped_capture.session(), &transcript_text) {
            completion.status_publisher.publish_error();
            return Error::ProviderFinalizationUnavailable {
                message: error.to_string(),
            }
            .into_stop_reply();
        }
        let delivery_outcomes =
            match finalizer.deliver(stopped_capture.session(), &output_targets, &transcript_text) {
                Ok(outcomes) => outcomes,
                Err(error) => {
                    completion.status_publisher.publish_error();
                    return Error::ProviderFinalizationUnavailable {
                        message: error.to_string(),
                    }
                    .into_stop_reply();
                }
            };
        if let Err(error) = capture_store.mark_terminal_capture(
            stopped_capture.session(),
            crate::TerminalCaptureState::Completed,
        ) {
            completion.status_publisher.publish_error();
            return error.into_stop_reply();
        }
        let durable_audio_artifact =
            Self::compact_artifact_after_stop(&capture_store, &stopped_capture)
                .unwrap_or(raw_artifact);
        publish_delivery_feedback(
            &delivery_outcomes,
            &transcript_text,
            &success_notifier,
            &status_publisher,
        );
        Output::Stopped(CaptureStopped {
            stopped_session: StoppedSession::new(stopped_capture.session().clone()),
            durable_audio_artifact,
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

    fn finalization_source_artifact(
        capture_store: &CaptureStore,
        session: &CaptureSession,
        fallback: signal_listener::DurableAudioArtifact,
    ) -> signal_listener::DurableAudioArtifact {
        let raw = capture_store.artifact_for_session(session);
        std::path::Path::new(raw.path().as_str())
            .is_file()
            .then_some(raw)
            .unwrap_or(fallback)
    }

    fn prepare_artifact(
        finalizer: &DurableProviderFinalizer,
        session: &CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
        policy: ProviderPolicy,
    ) -> std::result::Result<crate::PreparedProviderFinalization, crate::ProviderFinalizationError>
    {
        if artifact.path().as_str().ends_with(".listenerlog") {
            finalizer.prepare_recording_log_segments(session, artifact, policy)
        } else {
            finalizer.prepare(session, artifact, policy)
        }
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
