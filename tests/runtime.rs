use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use listener::daemon::ListenerSocketServer;
use listener::{
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, BatchTranscriber,
    BatchTranscriptionInput, BatchTranscriptionInputFormat, BatchTranscriptionRequest,
    Configuration, HistoryLimit, ListenerMaintenanceClient, ListenerRuntime,
    OutputTargetDispatcher, RecordingAudioFormat, RecordingInputSource, RecordingLog,
    RecordingLogHeader, RecordingLogWriter, RecordingStartTime, StatusEventRecorder,
    StatusPublisher, StatusStreamServer, SuccessNotifier, TranscriptDelivery,
    TranscriptDeliveryRequest, TranscriptHistoryStore,
};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use signal_listener::{
    ActiveCapture, CaptureCancellationRequested, CaptureSession, CaptureStatus, DeliveryFailure,
    DeliveryFailureReason, DeliveryOutcome, DurableAudioArtifact, Frame, FrameBody, Input,
    InputSource, ListCapturesRequest, ListenerDaemonConfiguration, MetaSocketMode, MetaSocketPath,
    OperationKind, Output, OutputTarget, OutputTargets, RetryCapture, SocketMode, StartCapture,
    StatusRequest, TranscriptText, TranscriptionMode, UnimplementedReason, WirePath,
    WorkingSocketMode, WorkingSocketPath,
};
use tempfile::TempDir;

struct RuntimeFixture {
    directory: TempDir,
    deliveries: Arc<Mutex<Vec<String>>>,
    transcription_inputs: Arc<Mutex<Vec<BatchTranscriptionInput>>>,
    status_publisher: StatusPublisher,
    status_events: StatusEventRecorder,
}

impl RuntimeFixture {
    fn new() -> Self {
        let (status_publisher, status_events) = StatusPublisher::recorder();
        Self {
            directory: TempDir::new().expect("temp directory"),
            deliveries: Arc::new(Mutex::new(Vec::new())),
            transcription_inputs: Arc::new(Mutex::new(Vec::new())),
            status_publisher,
            status_events,
        }
    }

    fn runtime(&self) -> ListenerRuntime {
        self.runtime_with_capture_backend(Box::new(FileAudioCaptureBackend))
    }

    fn runtime_with_capture_backend(
        &self,
        capture_backend: Box<dyn AudioCaptureBackend>,
    ) -> ListenerRuntime {
        self.runtime_with_capture_backend_and_transcriber(
            capture_backend,
            Box::new(FixedBatchTranscriber::new(
                "transcribed text",
                Arc::clone(&self.transcription_inputs),
                self.status_publisher.clone(),
            )),
        )
    }

    fn runtime_with_transcriber(&self, transcriber: Box<dyn BatchTranscriber>) -> ListenerRuntime {
        self.runtime_with_capture_backend_and_transcriber(
            Box::new(FileAudioCaptureBackend),
            transcriber,
        )
    }

    fn runtime_with_notifier(&self, success_notifier: Arc<dyn SuccessNotifier>) -> ListenerRuntime {
        self.runtime_with_transcriber_and_notifier(
            Box::new(FixedBatchTranscriber::new(
                "transcribed text",
                Arc::clone(&self.transcription_inputs),
                self.status_publisher.clone(),
            )),
            success_notifier,
        )
    }

    fn runtime_with_capture_backend_and_notifier(
        &self,
        capture_backend: Box<dyn AudioCaptureBackend>,
        success_notifier: Arc<dyn SuccessNotifier>,
    ) -> ListenerRuntime {
        ListenerRuntime::with_dependencies_and_notifier(
            self.configuration(),
            capture_backend,
            Box::new(FixedBatchTranscriber::new(
                "transcribed text",
                Arc::clone(&self.transcription_inputs),
                self.status_publisher.clone(),
            )),
            OutputTargetDispatcher::new(Box::new(RecordingDelivery::new(Arc::clone(
                &self.deliveries,
            )))),
            self.history_store(),
            success_notifier,
            self.status_publisher.clone(),
        )
    }

    fn runtime_with_transcriber_and_notifier(
        &self,
        transcriber: Box<dyn BatchTranscriber>,
        success_notifier: Arc<dyn SuccessNotifier>,
    ) -> ListenerRuntime {
        self.runtime_with_notifier_and_delivery(
            transcriber,
            success_notifier,
            Box::new(RecordingDelivery::new(Arc::clone(&self.deliveries))),
        )
    }

    fn runtime_with_notifier_and_delivery(
        &self,
        transcriber: Box<dyn BatchTranscriber>,
        success_notifier: Arc<dyn SuccessNotifier>,
        delivery: Box<dyn TranscriptDelivery>,
    ) -> ListenerRuntime {
        ListenerRuntime::with_dependencies_and_notifier(
            self.configuration(),
            Box::new(FileAudioCaptureBackend),
            transcriber,
            OutputTargetDispatcher::new(delivery),
            self.history_store(),
            success_notifier,
            self.status_publisher.clone(),
        )
    }

    fn runtime_with_capture_backend_and_transcriber(
        &self,
        capture_backend: Box<dyn AudioCaptureBackend>,
        transcriber: Box<dyn BatchTranscriber>,
    ) -> ListenerRuntime {
        ListenerRuntime::with_dependencies(
            self.configuration(),
            capture_backend,
            transcriber,
            OutputTargetDispatcher::new(Box::new(RecordingDelivery::new(Arc::clone(
                &self.deliveries,
            )))),
            self.history_store(),
            self.status_publisher.clone(),
        )
    }

    fn history_path(&self) -> PathBuf {
        self.directory.path().join("history.jsonl")
    }

    fn history_store(&self) -> TranscriptHistoryStore {
        TranscriptHistoryStore::new(self.history_path())
    }

    fn recorded_history(&self) -> Vec<String> {
        self.history_store()
            .read_recent(HistoryLimit::new(64))
            .expect("read transcript history")
            .iter()
            .map(|entry| entry.transcript_text().as_str().to_owned())
            .collect()
    }

    fn configuration(&self) -> Configuration {
        Configuration::new(ListenerDaemonConfiguration {
            working_socket_path: WorkingSocketPath::new(Self::wire_path(
                self.directory.path().join("listener.sock"),
            )),
            working_socket_mode: WorkingSocketMode::new(SocketMode::new(0o660)),
            meta_socket_path: MetaSocketPath::new(Self::wire_path(
                self.directory.path().join("listener-meta.sock"),
            )),
            meta_socket_mode: MetaSocketMode::new(SocketMode::new(0o600)),
            capture_store_directory: signal_listener::CaptureStoreDirectory::new(Self::wire_path(
                self.directory.path().join("captures"),
            )),
            input_source: InputSource::SystemDefault,
            transcription_mode: TranscriptionMode::BatchOnStop,
            output_targets: OutputTargets::new(vec![OutputTarget::SystemClipboard]),
        })
    }

    fn wire_path(path: impl AsRef<Path>) -> WirePath {
        WirePath::new(path.as_ref().to_string_lossy().into_owned())
    }

    fn capture_path(&self, session_value: u64) -> PathBuf {
        self.directory
            .path()
            .join("captures")
            .join(format!("capture-{session_value}.listenerlog"))
    }

    fn write_recording_log(&self, session_value: u64, payload: &[u8]) -> PathBuf {
        let path = self.capture_path(session_value);
        fs::create_dir_all(path.parent().expect("capture parent")).expect("create capture parent");
        let header = RecordingLogHeader::new(
            CaptureSession::new(session_value),
            RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
            RecordingInputSource::SystemDefault,
            RecordingStartTime::from_unix_parts(1_700_000_002, 0),
            8192,
        )
        .expect("recording header");
        let mut writer = RecordingLogWriter::create(&path, header).expect("create recording log");
        writer.append_record(payload).expect("append recording log");
        writer.finish().expect("finish recording log");
        path
    }

    fn delivered_texts(&self) -> Vec<String> {
        self.deliveries.lock().expect("deliveries").clone()
    }

    fn transcription_inputs(&self) -> Vec<BatchTranscriptionInput> {
        self.transcription_inputs
            .lock()
            .expect("transcription inputs")
            .clone()
    }

    fn status_events(&self) -> Vec<listener::ListenerStatusEvent> {
        self.status_events.events()
    }
}

struct FileAudioCaptureBackend;

impl FileAudioCaptureBackend {
    fn start_with_shutdown_gates(
        &self,
        request: AudioCaptureStart,
        stop_gate: Option<BlockingGate>,
        cancellation_gate: Option<BlockingGate>,
    ) -> listener::Result<Box<dyn ActiveAudioCapture>> {
        let artifact_path = request.artifact_path();
        fs::create_dir_all(artifact_path.parent().expect("artifact parent"))?;
        let header = RecordingLogHeader::from_capture_start(
            request.session(),
            request.input_source(),
            RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
        )?;
        let mut writer = RecordingLogWriter::create(&artifact_path, header)?;
        writer.append_record(ACTIVE_AUDIO_BYTES)?;
        request.status_publisher().publish_recording_level(
            listener::MicrophoneLevel::from_recording_payload(
                ACTIVE_AUDIO_BYTES,
                RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz().sample_format(),
            ),
        );
        Ok(Box::new(FileAudioCapture::new_with_shutdown_gates(
            request.artifact().clone(),
            writer,
            request.status_publisher(),
            stop_gate,
            cancellation_gate,
        )))
    }
}

impl AudioCaptureBackend for FileAudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> listener::Result<Box<dyn ActiveAudioCapture>> {
        self.start_with_shutdown_gates(request, None, None)
    }
}

struct BlockingStartupAudioCaptureBackend {
    startup_gate: BlockingGate,
}

impl BlockingStartupAudioCaptureBackend {
    fn new(startup_gate: BlockingGate) -> Self {
        Self { startup_gate }
    }
}

impl AudioCaptureBackend for BlockingStartupAudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> listener::Result<Box<dyn ActiveAudioCapture>> {
        self.startup_gate.enter_and_wait();
        FileAudioCaptureBackend.start(request)
    }
}

struct BlockingCancellationAudioCaptureBackend {
    cancellation_gate: BlockingGate,
}

impl BlockingCancellationAudioCaptureBackend {
    fn new(cancellation_gate: BlockingGate) -> Self {
        Self { cancellation_gate }
    }
}

impl AudioCaptureBackend for BlockingCancellationAudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> listener::Result<Box<dyn ActiveAudioCapture>> {
        FileAudioCaptureBackend.start_with_shutdown_gates(
            request,
            None,
            Some(self.cancellation_gate.clone()),
        )
    }
}

const ACTIVE_AUDIO_BYTES: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
const STOPPED_AUDIO_BYTES: &[u8] = &[8, 9, 10, 11];

struct BlockingFinalizationAudioCaptureBackend {
    finalization_gate: BlockingGate,
}

impl BlockingFinalizationAudioCaptureBackend {
    fn new(finalization_gate: BlockingGate) -> Self {
        Self { finalization_gate }
    }
}

impl AudioCaptureBackend for BlockingFinalizationAudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> listener::Result<Box<dyn ActiveAudioCapture>> {
        FileAudioCaptureBackend.start_with_shutdown_gates(
            request,
            Some(self.finalization_gate.clone()),
            None,
        )
    }
}

struct RacingCollisionAudioCaptureBackend {
    collided: Arc<Mutex<bool>>,
}

impl RacingCollisionAudioCaptureBackend {
    fn new() -> Self {
        Self {
            collided: Arc::new(Mutex::new(false)),
        }
    }
}

impl AudioCaptureBackend for RacingCollisionAudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> listener::Result<Box<dyn ActiveAudioCapture>> {
        let mut collided = self.collided.lock().expect("collision state");
        if !*collided {
            *collided = true;
            let artifact_path = request.artifact_path();
            fs::create_dir_all(artifact_path.parent().expect("artifact parent"))?;
            let header = RecordingLogHeader::from_capture_start(
                request.session(),
                request.input_source(),
                RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
            )?;
            let mut writer = RecordingLogWriter::create(&artifact_path, header)?;
            writer.append_record(&[40, 41, 42, 43])?;
            writer.finish()?;
            return Err(listener::Error::recording_log_already_exists(
                &artifact_path,
            ));
        }
        FileAudioCaptureBackend.start(request)
    }
}

struct FileAudioCapture {
    artifact: DurableAudioArtifact,
    writer: RecordingLogWriter,
    status_publisher: StatusPublisher,
    stop_gate: Option<BlockingGate>,
    cancellation_gate: Option<BlockingGate>,
}

impl FileAudioCapture {
    fn new_with_shutdown_gates(
        artifact: DurableAudioArtifact,
        writer: RecordingLogWriter,
        status_publisher: StatusPublisher,
        stop_gate: Option<BlockingGate>,
        cancellation_gate: Option<BlockingGate>,
    ) -> Self {
        Self {
            artifact,
            writer,
            status_publisher,
            stop_gate,
            cancellation_gate,
        }
    }

    fn finish(
        artifact: DurableAudioArtifact,
        mut writer: RecordingLogWriter,
        status_publisher: StatusPublisher,
    ) -> listener::Result<DurableAudioArtifact> {
        writer.append_record(STOPPED_AUDIO_BYTES)?;
        status_publisher.publish_recording_level(
            listener::MicrophoneLevel::from_recording_payload(
                STOPPED_AUDIO_BYTES,
                RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz().sample_format(),
            ),
        );
        writer.finish()?;
        Ok(artifact)
    }
}

impl ActiveAudioCapture for FileAudioCapture {
    fn artifact(&self) -> &DurableAudioArtifact {
        &self.artifact
    }

    fn stop(self: Box<Self>) -> listener::Result<DurableAudioArtifact> {
        let FileAudioCapture {
            artifact,
            writer,
            status_publisher,
            stop_gate,
            cancellation_gate: _,
        } = *self;
        if let Some(stop_gate) = stop_gate {
            stop_gate.enter_and_wait();
        }
        FileAudioCapture::finish(artifact, writer, status_publisher)
    }

    fn cancel(self: Box<Self>) -> listener::Result<DurableAudioArtifact> {
        let FileAudioCapture {
            artifact,
            writer,
            status_publisher,
            stop_gate: _,
            cancellation_gate,
        } = *self;
        if let Some(cancellation_gate) = cancellation_gate {
            cancellation_gate.enter_and_wait();
        }
        FileAudioCapture::finish(artifact, writer, status_publisher)
    }
}

struct FixedBatchTranscriber {
    text: String,
    inputs: Arc<Mutex<Vec<BatchTranscriptionInput>>>,
    status_publisher: StatusPublisher,
}

impl FixedBatchTranscriber {
    fn new(
        text: impl Into<String>,
        inputs: Arc<Mutex<Vec<BatchTranscriptionInput>>>,
        status_publisher: StatusPublisher,
    ) -> Self {
        Self {
            text: text.into(),
            inputs,
            status_publisher,
        }
    }
}

impl BatchTranscriber for FixedBatchTranscriber {
    fn transcribe(&self, request: BatchTranscriptionRequest) -> listener::Result<TranscriptText> {
        self.status_publisher.publish_transcribing();
        self.inputs
            .lock()
            .expect("transcription inputs")
            .push(request.input().clone());
        Ok(TranscriptText::new(self.text.clone()))
    }
}

struct BlockingBatchTranscriber {
    gate: BlockingGate,
    inputs: Arc<Mutex<Vec<BatchTranscriptionInput>>>,
    status_publisher: StatusPublisher,
}

impl BlockingBatchTranscriber {
    fn new(
        gate: BlockingGate,
        inputs: Arc<Mutex<Vec<BatchTranscriptionInput>>>,
        status_publisher: StatusPublisher,
    ) -> Self {
        Self {
            gate,
            inputs,
            status_publisher,
        }
    }
}

impl BatchTranscriber for BlockingBatchTranscriber {
    fn transcribe(&self, request: BatchTranscriptionRequest) -> listener::Result<TranscriptText> {
        self.status_publisher.publish_transcribing();
        self.inputs
            .lock()
            .expect("transcription inputs")
            .push(request.input().clone());
        self.gate.enter_and_wait();
        Ok(TranscriptText::new("blocked transcription"))
    }
}

#[derive(Clone)]
struct BlockingGate {
    state: Arc<(Mutex<BlockingGateState>, Condvar)>,
}

struct BlockingGateState {
    entered: bool,
    released: bool,
}

impl BlockingGate {
    fn new() -> Self {
        Self {
            state: Arc::new((
                Mutex::new(BlockingGateState {
                    entered: false,
                    released: false,
                }),
                Condvar::new(),
            )),
        }
    }

    fn enter_and_wait(&self) {
        let (state, condition) = &*self.state;
        let mut state = state.lock().expect("blocking gate state");
        state.entered = true;
        condition.notify_all();
        while !state.released {
            state = condition.wait(state).expect("blocking gate wait");
        }
    }

    fn wait_until_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let (state, condition) = &*self.state;
        let mut state = state.lock().expect("blocking gate state");
        while !state.entered {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .expect("blocking gate did not enter before timeout");
            let (next, timeout) = condition
                .wait_timeout(state, remaining)
                .expect("blocking gate timed wait");
            state = next;
            assert!(
                !timeout.timed_out(),
                "blocking gate did not enter before timeout"
            );
        }
    }

    fn release(&self) {
        let (state, condition) = &*self.state;
        state.lock().expect("blocking gate state").released = true;
        condition.notify_all();
    }
}

struct CancellationProbe;

impl CancellationProbe {
    fn assert_requested(server: &ListenerSocketServer, input: Input, session: &CaptureSession) {
        match server
            .handle_input(input)
            .expect("request cancellation through actor")
        {
            Output::CancellationRequested(CaptureCancellationRequested {
                cancellation_requested_session,
                ..
            }) => assert_eq!(cancellation_requested_session.payload(), session),
            other => panic!("expected cancellation acknowledgement, got {other:?}"),
        }
    }

    fn active_session(server: &ListenerSocketServer) -> CaptureSession {
        match server
            .handle_input(Input::Status(StatusRequest {}))
            .expect("read active lifecycle status through actor")
        {
            Output::StatusReported(report) => match report.status() {
                CaptureStatus::Capturing(active)
                | CaptureStatus::Finalizing(active)
                | CaptureStatus::Transcribing(active) => {
                    active.active_capture_session.payload().clone()
                }
                other => panic!("expected active lifecycle status, got {other:?}"),
            },
            other => panic!("expected status reply, got {other:?}"),
        }
    }

    fn assert_pending_for_session(server: &ListenerSocketServer, session: &CaptureSession) {
        assert_eq!(Self::active_session(server), *session);
    }

    fn assert_completion_requested(
        server: &ListenerSocketServer,
        input: Input,
        session: &CaptureSession,
    ) {
        match server
            .handle_input(input)
            .expect("request graceful completion through actor")
        {
            Output::CompletionRequested(requested) => {
                assert_eq!(requested.completion_requested_session.payload(), session)
            }
            other => panic!("expected completion acknowledgement, got {other:?}"),
        }
    }

    fn assert_toggle_preserves_active(server: &ListenerSocketServer, session: &CaptureSession) {
        match server
            .handle_input(Input::Toggle(signal_listener::ToggleCapture {}))
            .expect("toggle through actor")
        {
            Output::AlreadyActive(active) => assert_eq!(active.payload().payload(), session),
            other => panic!("expected active capture to remain intact, got {other:?}"),
        }
    }

    fn wait_until_idle(server: &ListenerSocketServer) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match server
                .handle_input(Input::Status(StatusRequest {}))
                .expect("read cancellation completion status through actor")
            {
                Output::StatusReported(report) if report.status() == &CaptureStatus::Idle => return,
                Output::StatusReported(report) if Instant::now() < deadline => {
                    let _ = report;
                    thread::sleep(Duration::from_millis(10));
                }
                Output::StatusReported(report) => {
                    panic!("expected idle after cancellation, got {report:?}")
                }
                other => panic!("expected status reply, got {other:?}"),
            }
        }
    }

    fn wait_until_delivered(server: &ListenerSocketServer, session: &CaptureSession) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match server
                .handle_input(Input::Status(StatusRequest {}))
                .expect("read completion status through actor")
            {
                Output::StatusReported(report)
                    if report.status() == &CaptureStatus::Delivered(session.clone()) =>
                {
                    return;
                }
                Output::StatusReported(_) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                Output::StatusReported(report) => {
                    panic!("expected delivered completion status, got {report:?}")
                }
                other => panic!("expected status reply, got {other:?}"),
            }
        }
    }

    fn assert_immediate<F>(request: F)
    where
        F: FnOnce(),
    {
        let started = Instant::now();
        request();
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "cancellation acknowledgement must not wait for blocked work"
        );
    }
}

struct ActorUnavailableBatchTranscriber;

impl BatchTranscriber for ActorUnavailableBatchTranscriber {
    fn transcribe(&self, _request: BatchTranscriptionRequest) -> listener::Result<TranscriptText> {
        Err(listener::Error::TranscriptionActorUnavailable {
            message: "test actor unavailable".to_owned(),
        })
    }
}

struct RecordingDelivery {
    deliveries: Arc<Mutex<Vec<String>>>,
}

impl RecordingDelivery {
    fn new(deliveries: Arc<Mutex<Vec<String>>>) -> Self {
        Self { deliveries }
    }
}

impl TranscriptDelivery for RecordingDelivery {
    fn deliver(&self, request: TranscriptDeliveryRequest) -> DeliveryOutcome {
        self.deliveries
            .lock()
            .expect("deliveries")
            .push(request.transcript_text().as_str().to_owned());
        DeliveryOutcome::delivered(request.target())
    }
}

struct UnavailableDelivery;

impl TranscriptDelivery for UnavailableDelivery {
    fn deliver(&self, request: TranscriptDeliveryRequest) -> DeliveryOutcome {
        DeliveryOutcome::Failed(DeliveryFailure {
            output_target: request.target(),
            delivery_failure_reason: DeliveryFailureReason::TargetUnavailable,
        })
    }
}

struct CountingSuccessNotifier {
    notifications: Arc<AtomicUsize>,
}

impl CountingSuccessNotifier {
    fn new(notifications: Arc<AtomicUsize>) -> Self {
        Self { notifications }
    }
}

impl SuccessNotifier for CountingSuccessNotifier {
    fn notify(&self, _transcript_text: &TranscriptText) {
        self.notifications.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn start_writes_active_capture_artifact_before_stop() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let start_reply = runtime.handle_input(Input::Start(StartCapture {}));
    let session = match start_reply {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    let status_reply = runtime.handle_input(Input::Status(StatusRequest {}));
    let artifact = match status_reply {
        Output::StatusReported(report) => match report.status() {
            CaptureStatus::Capturing(ActiveCapture {
                durable_audio_artifact,
                ..
            }) => durable_audio_artifact.clone(),
            other => panic!("expected active capture status, got {other:?}"),
        },
        other => panic!("expected status reply, got {other:?}"),
    };

    let active_export = RecordingLog::new(artifact.path().as_str())
        .recover()
        .expect("recover active recording log")
        .export_raw_pcm(fixture.directory.path().join("active.raw.s16le"))
        .expect("export active raw pcm");
    let active_bytes = fs::read(active_export.path()).expect("active artifact bytes");
    assert_eq!(active_bytes, ACTIVE_AUDIO_BYTES);

    let stop_reply = runtime.handle_input(Input::stop(session));
    let stopped = match stop_reply {
        Output::Stopped(stopped) => stopped,
        other => panic!("expected stopped reply, got {other:?}"),
    };
    assert!(
        stopped
            .durable_audio_artifact
            .path()
            .as_str()
            .ends_with(".webm"),
        "the stop reply names the compact artifact produced for transcription"
    );
    assert!(
        !Path::new(artifact.path().as_str()).exists(),
        "the crash-recovery listener log is removed after compact validation"
    );
    assert!(
        Path::new(stopped.durable_audio_artifact.path().as_str()).exists(),
        "a durable transcript retains exactly one canonical compact audio artifact for three days"
    );
}

#[test]
fn successful_capture_retains_canonical_opus_artifact_and_is_not_retryable() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();
    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };
    match runtime.handle_input(Input::stop(session.clone())) {
        Output::Stopped(_) => {}
        other => panic!("expected stopped reply, got {other:?}"),
    }

    match runtime.handle_input(Input::ListCaptures(ListCapturesRequest {})) {
        Output::CapturesListed(report) => {
            let captures = report.payload().payload();
            assert_eq!(
                captures.len(),
                1,
                "the terminal canonical audio is retained"
            );
            assert!(
                captures[0]
                    .durable_audio_artifact
                    .path()
                    .as_str()
                    .ends_with(".webm"),
                "the retained audio is the canonical WebM/Opus artifact"
            );
        }
        other => panic!("expected capture list, got {other:?}"),
    }

    match runtime.handle_input(Input::Retry(RetryCapture::new(session))) {
        Output::Unimplemented(unimplemented) => assert_eq!(
            unimplemented.reason.payload(),
            &UnimplementedReason::StoreUnavailable,
            "a terminally converted capture has no retry media"
        ),
        other => panic!("expected store-unavailable retry reply, got {other:?}"),
    }
}

#[test]
fn repeated_successful_captures_retain_one_canonical_opus_artifact_each() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    for expected_session in [1, 2] {
        let session = match runtime.handle_input(Input::Start(StartCapture {})) {
            Output::Started(started) => started.payload().payload().clone(),
            other => panic!("expected started reply, got {other:?}"),
        };
        assert_eq!(session.value(), expected_session);
        match runtime.handle_input(Input::stop(session.clone())) {
            Output::Stopped(_) => {}
            other => panic!("expected stopped reply, got {other:?}"),
        }
        assert!(
            !fixture.capture_path(session.value()).exists(),
            "successful capture {expected_session} left a recovery log"
        );
        assert!(
            fixture
                .directory
                .path()
                .join("captures")
                .join(format!("capture-{}.webm", session.value()))
                .exists(),
            "successful capture {expected_session} retains its canonical compact audio"
        );
    }

    assert_eq!(
        fixture.recorded_history(),
        vec!["transcribed text".to_owned(), "transcribed text".to_owned()]
    );
}

#[test]
fn start_skips_existing_artifacts_without_running_maintenance() {
    let fixture = RuntimeFixture::new();
    let existing_path = fixture.write_recording_log(1, &[20, 21, 22, 23]);
    let canonical_path = fixture.directory.path().join("captures/capture-1.webm");
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };
    assert_eq!(session.value(), 2);
    assert!(
        existing_path.exists(),
        "interactive start must not synchronously migrate an earlier recording"
    );
    assert!(
        !canonical_path.exists(),
        "only the one-shot daemon-start maintenance may create the canonical artifact"
    );

    let status_reply = runtime.handle_input(Input::Status(StatusRequest {}));
    let artifact = match status_reply {
        Output::StatusReported(report) => match report.status() {
            CaptureStatus::Capturing(ActiveCapture {
                durable_audio_artifact,
                ..
            }) => durable_audio_artifact.clone(),
            other => panic!("expected active capture status, got {other:?}"),
        },
        other => panic!("expected status reply, got {other:?}"),
    };
    assert_eq!(
        PathBuf::from(artifact.path().as_str()),
        fixture.capture_path(2)
    );

    runtime.handle_input(Input::stop(session));
}

#[test]
fn start_skips_existing_compact_artifact_before_writing_recovery_log() {
    let fixture = RuntimeFixture::new();
    let retained_compact = fixture.directory.path().join("captures/capture-1.webm");
    fs::create_dir_all(retained_compact.parent().expect("capture parent"))
        .expect("create capture parent");
    fs::write(&retained_compact, b"retained compact artifact").expect("write retained compact");
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    assert_eq!(
        session.value(),
        2,
        "a compact-only retained artifact must reserve its session before start"
    );
    assert!(
        retained_compact.exists(),
        "allocation must not alter an existing compact artifact"
    );
    assert!(
        !fixture.capture_path(1).exists(),
        "allocation must not create a recovery log beside another capture family"
    );

    runtime.handle_input(Input::stop(session));
}

#[test]
fn start_retries_when_artifact_appears_after_allocation() {
    let fixture = RuntimeFixture::new();
    let mut runtime =
        fixture.runtime_with_capture_backend(Box::new(RacingCollisionAudioCaptureBackend::new()));

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    assert_eq!(session.value(), 2);
    assert!(
        fixture.capture_path(1).exists(),
        "expected simulated racing artifact to exist"
    );
    match runtime.handle_input(Input::Status(StatusRequest {})) {
        Output::StatusReported(report) => match report.status() {
            CaptureStatus::Capturing(ActiveCapture {
                durable_audio_artifact,
                ..
            }) => {
                assert_eq!(
                    PathBuf::from(durable_audio_artifact.path().as_str()),
                    fixture.capture_path(2)
                );
            }
            other => panic!("expected active capture status, got {other:?}"),
        },
        other => panic!("expected status reply, got {other:?}"),
    }

    runtime.handle_input(Input::stop(session));
}

#[test]
fn idle_status_is_in_memory_and_leaves_maintenance_for_daemon_startup() {
    let fixture = RuntimeFixture::new();
    let existing_path = fixture.write_recording_log(1, &[30, 31, 32, 33]);
    OpenOptions::new()
        .append(true)
        .open(&existing_path)
        .expect("open orphan log for torn tail")
        .write_all(b"torn listener tail")
        .expect("append torn tail");
    let original_length = fs::metadata(&existing_path).expect("orphan metadata").len();
    let canonical_path = fixture.directory.path().join("captures/capture-1.webm");
    let mut runtime = fixture.runtime();

    for _ in 0..2 {
        match runtime.handle_input(Input::Status(StatusRequest {})) {
            Output::StatusReported(report) => assert_eq!(report.status(), &CaptureStatus::Idle),
            other => panic!("expected idle status reply, got {other:?}"),
        }
    }
    assert!(
        existing_path.exists(),
        "status must not open, recover, migrate, or delete capture logs"
    );
    assert_eq!(
        fs::metadata(&existing_path)
            .expect("orphan metadata after status")
            .len(),
        original_length,
        "status must leave a torn tail untouched for background maintenance"
    );
    assert!(
        !canonical_path.exists(),
        "status must not invoke the encoder or create a canonical artifact"
    );
}

#[test]
fn stop_returns_artifact_transcript_and_delivery_outcome() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    let stop_reply = runtime.handle_input(Input::stop(session));
    let stopped = match stop_reply {
        Output::Stopped(stopped) => stopped,
        other => panic!("expected stopped reply, got {other:?}"),
    };

    assert!(
        stopped
            .durable_audio_artifact
            .path()
            .as_str()
            .ends_with(".webm")
    );
    let transcription_inputs = fixture.transcription_inputs();
    assert_eq!(transcription_inputs.len(), 1);
    assert!(
        transcription_inputs[0]
            .path()
            .to_string_lossy()
            .ends_with(".webm")
    );
    match transcription_inputs[0].format() {
        BatchTranscriptionInputFormat::WebmOpus => {}
        other => panic!("expected compact WebM/Opus transcription input, got {other:?}"),
    }
    assert_eq!(stopped.transcript_text.as_str(), "transcribed text");
    assert_eq!(
        fixture.delivered_texts(),
        vec!["transcribed text".to_owned()]
    );
    assert_eq!(
        fixture.recorded_history(),
        vec!["transcribed text".to_owned()],
        "a successful stop must append the transcript to history"
    );
    assert_eq!(stopped.delivery_outcomes.as_slice().len(), 1);
    match &stopped.delivery_outcomes.as_slice()[0] {
        DeliveryOutcome::Delivered(delivered) => {
            assert_eq!(delivered.payload(), &OutputTarget::SystemClipboard);
        }
        other => panic!("expected clipboard delivery, got {other:?}"),
    }
}

#[test]
fn cancel_stops_capture_retains_artifact_and_skips_transcription_and_delivery() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };
    let artifact = match runtime.handle_input(Input::Status(StatusRequest {})) {
        Output::StatusReported(report) => match report.status() {
            CaptureStatus::Capturing(ActiveCapture {
                durable_audio_artifact,
                ..
            }) => durable_audio_artifact.clone(),
            other => panic!("expected active capture status, got {other:?}"),
        },
        other => panic!("expected status reply, got {other:?}"),
    };

    match runtime.handle_input(Input::cancel(session.clone())) {
        Output::Cancelled(cancelled) => {
            assert_eq!(cancelled.cancelled_session.payload(), &session);
            assert_eq!(&cancelled.durable_audio_artifact, &artifact);
        }
        other => panic!("expected cancelled reply, got {other:?}"),
    }

    let mut stop_export_path = PathBuf::from(artifact.path().as_str());
    stop_export_path.set_extension("raw.s16le");
    assert!(
        !stop_export_path.exists(),
        "cancel must not create the normal stop-time transcription export"
    );
    let retained_export = RecordingLog::new(artifact.path().as_str())
        .recover()
        .expect("recover retained cancelled recording log")
        .export_raw_pcm(fixture.directory.path().join("cancelled.raw.s16le"))
        .expect("export cancelled raw pcm");
    let retained_bytes = fs::read(retained_export.path()).expect("cancelled raw bytes");
    let capture_directory_mode = fs::metadata(
        PathBuf::from(artifact.path().as_str())
            .parent()
            .expect("artifact parent"),
    )
    .expect("capture directory metadata")
    .permissions()
    .mode()
        & 0o777;
    let retained_artifact_mode = fs::metadata(artifact.path().as_str())
        .expect("retained artifact metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        capture_directory_mode, 0o700,
        "cancel-retained capture directory must stay owner-only"
    );
    assert_eq!(
        retained_artifact_mode, 0o600,
        "cancel-retained artifact must stay owner-only"
    );
    let mut expected = Vec::new();
    expected.extend_from_slice(ACTIVE_AUDIO_BYTES);
    expected.extend_from_slice(STOPPED_AUDIO_BYTES);
    assert_eq!(retained_bytes, expected);
    assert!(
        fixture.transcription_inputs().is_empty(),
        "cancel must not send transcription input"
    );
    assert!(
        fixture.delivered_texts().is_empty(),
        "cancel must not deliver transcript text"
    );
    assert!(
        fixture.recorded_history().is_empty(),
        "cancel must not append a transcript history entry"
    );
    assert!(
        !fixture.history_path().exists(),
        "cancel must not create the transcript history store"
    );
    match runtime.handle_input(Input::Status(StatusRequest {})) {
        Output::StatusReported(report) => assert_eq!(report.status(), &CaptureStatus::Idle),
        other => panic!("expected idle status reply after cancel, got {other:?}"),
    }

    let events = fixture.status_events();
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Cancelled),
        "expected cancelled status event, got {events:?}"
    );
    assert!(
        events
            .iter()
            .filter(|event| event.state() == listener::ListenerStatusState::Cancelled)
            .all(|event| event.json_line().expect("cancelled status json")
                == "{\"state\":\"cancelled\",\"level\":0.0}\n"),
        "expected UI-safe cancelled status JSON, got {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Transcribing),
        "cancel must not publish transcribing status, got {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Delivered),
        "cancel must not publish delivered status, got {events:?}"
    );
    for event in events {
        let line = event.json_line().expect("status event json");
        assert!(!line.contains("transcribed text"));
        assert!(!line.contains("transcript"));
    }
}

#[test]
fn status_events_cover_recording_transcribing_copied_without_transcript_text() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };
    let stop_reply = runtime.handle_input(Input::stop(session));
    match stop_reply {
        Output::Stopped(stopped) => {
            assert_eq!(stopped.transcript_text.as_str(), "transcribed text");
        }
        other => panic!("expected stopped reply, got {other:?}"),
    }

    let events = fixture.status_events();
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Recording),
        "expected at least one recording status event, got {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Transcribing),
        "expected transcribing status event, got {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Delivered),
        "expected delivered status event, got {events:?}"
    );
    assert!(
        events
            .iter()
            .filter(|event| event.state() == listener::ListenerStatusState::Recording)
            .any(|event| event.level().value() > 0.0),
        "expected nonzero microphone level while recording, got {events:?}"
    );
    for event in events {
        let line = event.json_line().expect("status event json");
        assert!(!line.contains("transcribed text"));
        assert!(!line.contains("transcript"));
    }
}

#[test]
fn stop_actor_unavailable_returns_transcription_backend_unavailable_reply() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime_with_transcriber(Box::new(ActorUnavailableBatchTranscriber));

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    match runtime.handle_input(Input::stop(session)) {
        Output::Unimplemented(unimplemented) => {
            assert_eq!(
                unimplemented.unimplemented_operation_kind.payload(),
                &OperationKind::Stop
            );
            assert_eq!(
                unimplemented.reason.payload(),
                &UnimplementedReason::TranscriptionBackendUnavailable
            );
        }
        other => panic!("expected transcription backend unavailable reply, got {other:?}"),
    }

    let events = fixture.status_events();
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Error),
        "expected error status event after actor unavailable failure, got {events:?}"
    );
}

#[test]
fn actor_toggle_and_cancel_acknowledge_startup_cancellation_without_blocking() {
    let fixture = RuntimeFixture::new();
    let gate = BlockingGate::new();
    let runtime = fixture.runtime_with_capture_backend(Box::new(
        BlockingStartupAudioCaptureBackend::new(gate.clone()),
    ));
    let server = Arc::new(ListenerSocketServer::new(fixture.configuration(), runtime));
    let first_toggle_server = Arc::clone(&server);
    let first_toggle = thread::spawn(move || {
        first_toggle_server
            .handle_input(Input::Toggle(signal_listener::ToggleCapture {}))
            .expect("start through actor")
    });
    gate.wait_until_entered();

    let session = CancellationProbe::active_session(&server);
    CancellationProbe::assert_immediate(|| {
        CancellationProbe::assert_requested(&server, Input::cancel(session.clone()), &session)
    });
    CancellationProbe::assert_toggle_preserves_active(&server, &session);
    CancellationProbe::assert_pending_for_session(&server, &session);

    gate.release();
    match first_toggle.join().expect("start thread joins") {
        Output::Started(_) => {}
        other => panic!("expected first toggle to report its started transition, got {other:?}"),
    }
    CancellationProbe::wait_until_idle(&server);
    assert!(fixture.capture_path(session.value()).exists());
    assert!(fixture.transcription_inputs().is_empty());
    assert!(fixture.delivered_texts().is_empty());
}

#[test]
fn actor_toggle_and_cancel_repeat_recording_cancellation_without_starting_another_capture() {
    let fixture = RuntimeFixture::new();
    let gate = BlockingGate::new();
    let runtime = fixture.runtime_with_capture_backend(Box::new(
        BlockingCancellationAudioCaptureBackend::new(gate.clone()),
    ));
    let server = ListenerSocketServer::new(fixture.configuration(), runtime);
    let session = match server
        .handle_input(Input::Toggle(signal_listener::ToggleCapture {}))
        .expect("start through actor")
    {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    CancellationProbe::assert_immediate(|| {
        CancellationProbe::assert_requested(&server, Input::cancel(session.clone()), &session)
    });
    CancellationProbe::assert_requested(
        &server,
        Input::Toggle(signal_listener::ToggleCapture {}),
        &session,
    );
    CancellationProbe::assert_requested(&server, Input::cancel(session.clone()), &session);
    CancellationProbe::assert_pending_for_session(&server, &session);
    gate.release();

    CancellationProbe::wait_until_idle(&server);
    assert!(fixture.capture_path(session.value()).exists());
    assert!(fixture.transcription_inputs().is_empty());
    assert!(fixture.delivered_texts().is_empty());
}

#[test]
fn actor_toggle_and_cancel_repeat_finalizing_cancellation_without_transcribing() {
    let fixture = RuntimeFixture::new();
    let notifications = Arc::new(AtomicUsize::new(0));
    let gate = BlockingGate::new();
    let runtime = fixture.runtime_with_capture_backend_and_notifier(
        Box::new(BlockingFinalizationAudioCaptureBackend::new(gate.clone())),
        Arc::new(CountingSuccessNotifier::new(Arc::clone(&notifications))),
    );
    let server = Arc::new(ListenerSocketServer::new(fixture.configuration(), runtime));
    let session = match server
        .handle_input(Input::Start(StartCapture {}))
        .expect("start through actor")
    {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    let stopping_server = Arc::clone(&server);
    let stopping_session = session.clone();
    let stop_thread = thread::spawn(move || {
        stopping_server
            .handle_input(Input::stop(stopping_session))
            .expect("stop through actor")
    });
    gate.wait_until_entered();

    CancellationProbe::assert_immediate(|| {
        CancellationProbe::assert_completion_requested(
            &server,
            Input::Toggle(signal_listener::ToggleCapture {}),
            &session,
        )
    });
    CancellationProbe::assert_requested(&server, Input::cancel(session.clone()), &session);
    CancellationProbe::assert_pending_for_session(&server, &session);
    gate.release();

    match stop_thread.join().expect("stop thread joins") {
        Output::CompletionRequested(requested) => {
            assert_eq!(requested.completion_requested_session.payload(), &session)
        }
        other => panic!("expected prompt completion acknowledgement, got {other:?}"),
    }
    CancellationProbe::wait_until_idle(&server);
    assert!(fixture.capture_path(session.value()).exists());
    assert!(fixture.transcription_inputs().is_empty());
    assert!(fixture.delivered_texts().is_empty());
    assert!(fixture.recorded_history().is_empty());
    assert_eq!(notifications.load(Ordering::SeqCst), 0);
}

#[test]
fn actor_toggle_and_cancel_repeat_transcribing_cancellation_without_delivery_or_history() {
    let fixture = RuntimeFixture::new();
    let notifications = Arc::new(AtomicUsize::new(0));
    let gate = BlockingGate::new();
    let runtime = fixture.runtime_with_transcriber_and_notifier(
        Box::new(BlockingBatchTranscriber::new(
            gate.clone(),
            Arc::clone(&fixture.transcription_inputs),
            fixture.status_publisher.clone(),
        )),
        Arc::new(CountingSuccessNotifier::new(Arc::clone(&notifications))),
    );
    let server = Arc::new(ListenerSocketServer::new(fixture.configuration(), runtime));
    let session = match server
        .handle_input(Input::Start(StartCapture {}))
        .expect("start through actor")
    {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    let stopping_server = Arc::clone(&server);
    let stopping_session = session.clone();
    let stop_thread = thread::spawn(move || {
        stopping_server
            .handle_input(Input::stop(stopping_session))
            .expect("stop through actor")
    });
    gate.wait_until_entered();

    CancellationProbe::assert_immediate(|| {
        CancellationProbe::assert_completion_requested(
            &server,
            Input::Toggle(signal_listener::ToggleCapture {}),
            &session,
        )
    });
    CancellationProbe::assert_requested(&server, Input::cancel(session.clone()), &session);
    CancellationProbe::assert_pending_for_session(&server, &session);
    gate.release();

    match stop_thread.join().expect("stop thread joins") {
        Output::CompletionRequested(requested) => {
            assert_eq!(requested.completion_requested_session.payload(), &session)
        }
        other => panic!("expected prompt completion acknowledgement, got {other:?}"),
    }
    CancellationProbe::wait_until_idle(&server);
    assert_eq!(fixture.transcription_inputs().len(), 1);
    assert!(fixture.delivered_texts().is_empty());
    assert!(fixture.recorded_history().is_empty());
    assert_eq!(notifications.load(Ordering::SeqCst), 0);
    let events = fixture.status_events();
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Cancelling)
    );
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Cancelled)
    );
}

#[test]
fn toggle_starts_then_gracefully_completes_the_active_capture() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let first = runtime.handle_input(Input::Toggle(signal_listener::ToggleCapture {}));
    let session = match first {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected toggle to start capture, got {other:?}"),
    };

    let second = runtime.handle_input(Input::Toggle(signal_listener::ToggleCapture {}));
    match second {
        Output::Stopped(stopped) => assert_eq!(stopped.stopped_session.payload(), &session),
        other => panic!("expected toggle to gracefully complete the same capture, got {other:?}"),
    }
    assert_eq!(fixture.transcription_inputs().len(), 1);
    assert_eq!(
        fixture.delivered_texts(),
        vec!["transcribed text".to_owned()]
    );
    assert_eq!(
        fixture.recorded_history(),
        vec!["transcribed text".to_owned()]
    );
}

#[test]
fn actor_completion_notifies_once_after_clipboard_delivery() {
    let fixture = RuntimeFixture::new();
    let notifications = Arc::new(AtomicUsize::new(0));
    let runtime = fixture.runtime_with_notifier(Arc::new(CountingSuccessNotifier::new(
        Arc::clone(&notifications),
    )));
    let server = ListenerSocketServer::new(fixture.configuration(), runtime);

    let session = match server
        .handle_input(Input::Start(StartCapture {}))
        .expect("start through actor")
    {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected start reply, got {other:?}"),
    };

    CancellationProbe::assert_completion_requested(&server, Input::stop(session.clone()), &session);
    CancellationProbe::wait_until_delivered(&server, &session);

    assert!(!fixture.delivered_texts().is_empty());
    assert_eq!(notifications.load(Ordering::SeqCst), 1);
}

#[test]
fn clipboard_delivery_failure_skips_success_notifier() {
    let fixture = RuntimeFixture::new();
    let notifications = Arc::new(AtomicUsize::new(0));
    let runtime = fixture.runtime_with_notifier_and_delivery(
        Box::new(FixedBatchTranscriber::new(
            "generated fixture delivery failure",
            Arc::clone(&fixture.transcription_inputs),
            fixture.status_publisher.clone(),
        )),
        Arc::new(CountingSuccessNotifier::new(Arc::clone(&notifications))),
        Box::new(UnavailableDelivery),
    );
    let mut runtime = runtime;

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected start reply, got {other:?}"),
    };
    match runtime.handle_input(Input::stop(session)) {
        Output::Stopped(_) => {}
        other => panic!("expected stopped reply, got {other:?}"),
    }

    assert_eq!(notifications.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .status_events()
            .iter()
            .any(|event| { event.state() == listener::ListenerStatusState::Error })
    );
}

#[test]
fn actor_toggle_completes_recording_with_one_transcription_and_delivery() {
    let fixture = RuntimeFixture::new();
    let server = ListenerSocketServer::new(fixture.configuration(), fixture.runtime());

    let session = match server
        .handle_input(Input::Toggle(signal_listener::ToggleCapture {}))
        .expect("start through actor")
    {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected first toggle to start capture, got {other:?}"),
    };

    CancellationProbe::assert_completion_requested(
        &server,
        Input::Toggle(signal_listener::ToggleCapture {}),
        &session,
    );
    CancellationProbe::wait_until_delivered(&server, &session);
    let events = fixture.status_events();
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Finalizing),
        "expected finalizing state, got {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Transcribing),
        "expected transcribing state, got {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.state() == listener::ListenerStatusState::Delivered),
        "expected delivered state, got {events:?}"
    );
    assert_eq!(fixture.transcription_inputs().len(), 1);
    assert_eq!(
        fixture.delivered_texts(),
        vec!["transcribed text".to_owned()]
    );
    assert_eq!(
        fixture.recorded_history(),
        vec!["transcribed text".to_owned()]
    );
}

#[test]
fn connection_bound_maintenance_lease_gates_starts_until_explicit_release() {
    let fixture = RuntimeFixture::new();
    let server = Arc::new(ListenerSocketServer::new(
        fixture.configuration(),
        fixture.runtime(),
    ));
    let session = match server
        .handle_input(Input::Start(StartCapture {}))
        .expect("start capture")
    {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started capture, got {other:?}"),
    };

    let (client_stream, daemon_stream) = UnixStream::pair().expect("maintenance socket pair");
    let persistent_server = Arc::clone(&server);
    let persistent_connection = thread::spawn(move || {
        persistent_server
            .handle_persistent_connection(daemon_stream)
            .expect("persistent connection")
    });
    let acquire = thread::spawn(move || {
        let mut client = ListenerMaintenanceClient::from_stream(client_stream);
        let epoch = client.acquire().expect("grant after active capture stops");
        (client, epoch)
    });

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match server
            .handle_input(Input::Start(StartCapture {}))
            .expect("check start gate")
        {
            Output::MaintenanceLeaseActive(_) => break,
            Output::AlreadyActive(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            other => panic!("expected queued lease to gate starts, got {other:?}"),
        }
    }

    match server
        .handle_input(Input::stop(session.clone()))
        .expect("stop capture")
    {
        Output::CompletionRequested(requested) => {
            assert_eq!(requested.completion_requested_session.payload(), &session)
        }
        other => panic!("expected prompt completion acknowledgement, got {other:?}"),
    }
    let (mut client, epoch) = acquire.join().expect("maintenance acquire joins");
    assert!(epoch.payload().payload() > &0);
    CancellationProbe::wait_until_delivered(&server, &session);
    assert!(matches!(
        server
            .handle_input(Input::Start(StartCapture {}))
            .expect("check held lease"),
        Output::MaintenanceLeaseActive(_)
    ));

    client.release().expect("explicit lease release");
    drop(client);
    persistent_connection
        .join()
        .expect("persistent connection joins");
    let restarted = match server
        .handle_input(Input::Start(StartCapture {}))
        .expect("start after release")
    {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected start after release, got {other:?}"),
    };
    match server
        .handle_input(Input::cancel(restarted))
        .expect("cancel cleanup")
    {
        Output::CancellationRequested(_) => {}
        other => panic!("expected cleanup cancellation acknowledgement, got {other:?}"),
    }
    CancellationProbe::wait_until_idle(&server);
}

#[test]
fn starting_state_precedes_recording_and_never_claims_audio_before_a_capture_commit() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(_) => {}
        other => panic!("expected started reply, got {other:?}"),
    }

    let events = fixture.status_events();
    let starting = events
        .iter()
        .position(|event| event.state() == listener::ListenerStatusState::Starting)
        .expect("start must publish starting immediately");
    let recording = events
        .iter()
        .position(|event| event.state() == listener::ListenerStatusState::Recording)
        .expect("capture writer must publish recording after an audio commit");
    assert!(
        starting < recording,
        "starting must precede recording so the UI never claims audio is ready early"
    );
}

#[test]
fn status_stream_sends_newline_json_frames() {
    let fixture = RuntimeFixture::new();
    let status_socket = fixture.directory.path().join("status.sock");
    let (server, publisher) = StatusStreamServer::new(&status_socket);
    let _server_thread = server.spawn().expect("status server starts");
    let stream = StatusStreamClientProbe::connect(&status_socket);
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    reader.read_line(&mut line).expect("read initial status");
    assert_eq!(line, "{\"state\":\"idle\",\"level\":0.0}\n");

    line.clear();
    publisher.publish_recording_level(listener::MicrophoneLevel::new(0.5));
    reader.read_line(&mut line).expect("read recording status");
    assert_eq!(line, "{\"state\":\"recording\",\"level\":0.5}\n");
}

#[test]
fn start_while_active_returns_typed_conflict_reply() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::AlreadyActive(conflict) => {
            assert_eq!(conflict.payload().payload(), &session);
        }
        other => panic!("expected active-capture conflict, got {other:?}"),
    }

    runtime.handle_input(Input::stop(session));
}

#[test]
fn stop_while_idle_returns_typed_conflict_reply() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    match runtime.handle_input(Input::stop(CaptureSession::new(1))) {
        Output::NoActive(_) => {}
        other => panic!("expected no-active-capture conflict, got {other:?}"),
    }
}

#[test]
fn stop_with_wrong_session_returns_typed_conflict_reply_and_preserves_active_capture() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };
    let requested_session = CaptureSession::new(session.value() + 1);

    match runtime.handle_input(Input::stop(requested_session.clone())) {
        Output::SessionMismatch(conflict) => {
            assert_eq!(conflict.active_capture_session.payload(), &session);
            assert_eq!(
                conflict.requested_capture_session.payload(),
                &requested_session
            );
        }
        other => panic!("expected session-mismatch conflict, got {other:?}"),
    }

    match runtime.handle_input(Input::Status(StatusRequest {})) {
        Output::StatusReported(report) => match report.status() {
            CaptureStatus::Capturing(ActiveCapture {
                active_capture_session,
                ..
            }) => assert_eq!(active_capture_session.payload(), &session),
            other => panic!("expected active capture after wrong stop, got {other:?}"),
        },
        other => panic!("expected status reply, got {other:?}"),
    }

    runtime.handle_input(Input::stop(session));
}

#[test]
fn cancel_while_idle_returns_typed_conflict_reply() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    match runtime.handle_input(Input::cancel(CaptureSession::new(1))) {
        Output::NoActive(_) => {}
        other => panic!("expected no-active-capture conflict, got {other:?}"),
    }
}

#[test]
fn cancel_with_wrong_session_returns_typed_conflict_reply_and_preserves_active_capture() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };
    let requested_session = CaptureSession::new(session.value() + 1);

    match runtime.handle_input(Input::cancel(requested_session.clone())) {
        Output::SessionMismatch(conflict) => {
            assert_eq!(conflict.active_capture_session.payload(), &session);
            assert_eq!(
                conflict.requested_capture_session.payload(),
                &requested_session
            );
        }
        other => panic!("expected session-mismatch conflict, got {other:?}"),
    }

    match runtime.handle_input(Input::Status(StatusRequest {})) {
        Output::StatusReported(report) => match report.status() {
            CaptureStatus::Capturing(ActiveCapture {
                active_capture_session,
                ..
            }) => assert_eq!(active_capture_session.payload(), &session),
            other => panic!("expected active capture after wrong cancel, got {other:?}"),
        },
        other => panic!("expected status reply, got {other:?}"),
    }

    runtime.handle_input(Input::stop(session));
}

#[test]
fn output_target_dispatch_returns_one_outcome_per_configured_target() {
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let dispatcher =
        OutputTargetDispatcher::new(Box::new(RecordingDelivery::new(Arc::clone(&deliveries))));

    let outcomes = dispatcher.deliver(
        &OutputTargets::new(vec![
            OutputTarget::SystemClipboard,
            OutputTarget::SystemClipboard,
        ]),
        &TranscriptText::new("dispatch text"),
    );

    assert_eq!(outcomes.as_slice().len(), 2);
    assert_eq!(
        deliveries.lock().expect("deliveries").clone(),
        vec!["dispatch text".to_owned(), "dispatch text".to_owned()]
    );
}

#[test]
fn socket_server_answers_public_status_frame_with_matching_exchange() {
    let fixture = RuntimeFixture::new();
    let (mut client_stream, server_stream) = UnixStream::pair().expect("socket pair");
    let server = ListenerSocketServer::new(fixture.configuration(), fixture.runtime());
    let exchange = ExchangeIdentifier::new(
        SessionEpoch::new(5),
        ExchangeLane::Connector,
        LaneSequence::new(13),
    );

    let request = Input::Status(StatusRequest {})
        .into_frame(exchange)
        .encode_length_prefixed()
        .expect("public request frame encodes");
    client_stream
        .write_all(&request)
        .expect("write public request frame");
    server
        .handle_connection(server_stream)
        .expect("server reply");
    let response = read_length_prefixed_frame_bytes(&mut client_stream);
    let frame = Frame::decode_length_prefixed(&response).expect("public reply frame decodes");

    match frame.into_body() {
        FrameBody::Reply {
            exchange: reply_exchange,
            reply,
        } => {
            assert_eq!(reply_exchange, exchange);
            match reply {
                Reply::Accepted { per_operation, .. } => {
                    let (reply, additional_replies) = per_operation.into_head_and_tail();
                    assert!(
                        additional_replies.is_empty(),
                        "expected one reply payload, got {}",
                        1 + additional_replies.len()
                    );
                    match reply {
                        SubReply::Ok(Output::StatusReported(report)) => {
                            assert_eq!(report.status(), &CaptureStatus::Idle);
                        }
                        other => panic!("expected idle status reply, got {other:?}"),
                    }
                }
                other => panic!("expected accepted reply, got {other:?}"),
            }
        }
        other => panic!("expected public reply frame, got {other:?}"),
    }
}

#[test]
fn socket_server_accepts_atomic_toggle_frame_with_matching_exchange() {
    let fixture = RuntimeFixture::new();
    let (mut client_stream, server_stream) = UnixStream::pair().expect("socket pair");
    let server = ListenerSocketServer::new(fixture.configuration(), fixture.runtime());
    let exchange = ExchangeIdentifier::new(
        SessionEpoch::new(5),
        ExchangeLane::Connector,
        LaneSequence::new(14),
    );

    let request = Input::Toggle(signal_listener::ToggleCapture {})
        .into_frame(exchange)
        .encode_length_prefixed()
        .expect("public toggle request frame encodes");
    client_stream
        .write_all(&request)
        .expect("write public toggle request frame");
    server
        .handle_connection(server_stream)
        .expect("server toggle reply");
    let response = read_length_prefixed_frame_bytes(&mut client_stream);
    let frame =
        Frame::decode_length_prefixed(&response).expect("public toggle reply frame decodes");

    match frame.into_body() {
        FrameBody::Reply {
            exchange: reply_exchange,
            reply,
        } => {
            assert_eq!(reply_exchange, exchange);
            match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head_and_tail().0
                {
                    SubReply::Ok(Output::Started(_)) => {}
                    other => panic!("expected started toggle reply, got {other:?}"),
                },
                other => panic!("expected accepted reply, got {other:?}"),
            }
        }
        other => panic!("expected public reply frame, got {other:?}"),
    }
}

#[test]
fn socket_server_answers_public_conflict_frame_with_matching_exchange() {
    let fixture = RuntimeFixture::new();
    let (mut client_stream, server_stream) = UnixStream::pair().expect("socket pair");
    let server = ListenerSocketServer::new(fixture.configuration(), fixture.runtime());
    let exchange = ExchangeIdentifier::new(
        SessionEpoch::new(5),
        ExchangeLane::Connector,
        LaneSequence::new(15),
    );

    let request = Input::stop(CaptureSession::new(1))
        .into_frame(exchange)
        .encode_length_prefixed()
        .expect("public request frame encodes");
    client_stream
        .write_all(&request)
        .expect("write public request frame");
    server
        .handle_connection(server_stream)
        .expect("server reply");
    let response = read_length_prefixed_frame_bytes(&mut client_stream);
    let frame = Frame::decode_length_prefixed(&response).expect("public reply frame decodes");

    match frame.into_body() {
        FrameBody::Reply {
            exchange: reply_exchange,
            reply,
        } => {
            assert_eq!(reply_exchange, exchange);
            match reply {
                Reply::Accepted { per_operation, .. } => {
                    let (reply, additional_replies) = per_operation.into_head_and_tail();
                    assert!(
                        additional_replies.is_empty(),
                        "expected one reply payload, got {}",
                        1 + additional_replies.len()
                    );
                    match reply {
                        SubReply::Ok(Output::NoActive(_)) => {}
                        other => panic!("expected no-active-capture reply, got {other:?}"),
                    }
                }
                other => panic!("expected accepted reply, got {other:?}"),
            }
        }
        other => panic!("expected public reply frame, got {other:?}"),
    }
}

fn read_length_prefixed_frame_bytes(reader: &mut impl Read) -> Vec<u8> {
    let mut length_prefix = [0_u8; 4];
    reader
        .read_exact(&mut length_prefix)
        .expect("read reply length prefix");
    let frame_length = u32::from_be_bytes(length_prefix) as usize;
    let mut frame_bytes = Vec::with_capacity(4 + frame_length);
    frame_bytes.extend_from_slice(&length_prefix);
    frame_bytes.resize(4 + frame_length, 0);
    reader
        .read_exact(&mut frame_bytes[4..])
        .expect("read reply frame body");
    frame_bytes
}

struct StatusStreamClientProbe;

impl StatusStreamClientProbe {
    fn connect(path: &Path) -> UnixStream {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            match UnixStream::connect(path) {
                Ok(stream) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .expect("set status stream read timeout");
                    return stream;
                }
                Err(error) if std::time::Instant::now() < deadline => {
                    assert!(
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                        ),
                        "unexpected status socket connect error: {error}"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("status socket did not accept connection: {error}"),
            }
        }
    }
}
