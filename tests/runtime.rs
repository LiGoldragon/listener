use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use listener::daemon::ListenerSocketServer;
use listener::{
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, BatchTranscriber,
    BatchTranscriptionInput, BatchTranscriptionInputFormat, BatchTranscriptionRequest,
    Configuration, ListenerRuntime, OutputTargetDispatcher, RecordingAudioFormat,
    RecordingInputSource, RecordingLog, RecordingLogHeader, RecordingLogWriter, RecordingStartTime,
    TranscriptDelivery, TranscriptDeliveryRequest,
};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use signal_listener::{
    ActiveCapture, CaptureSession, CaptureStatus, DeliveryOutcome, DurableAudioArtifact, Frame,
    FrameBody, Input, InputSource, ListenerDaemonConfiguration, MetaSocketMode, MetaSocketPath,
    Output, OutputTarget, OutputTargets, SocketMode, StartCapture, StatusRequest, TranscriptText,
    TranscriptionMode, WirePath, WorkingSocketMode, WorkingSocketPath,
};
use tempfile::TempDir;

struct RuntimeFixture {
    directory: TempDir,
    deliveries: Arc<Mutex<Vec<String>>>,
    transcription_inputs: Arc<Mutex<Vec<BatchTranscriptionInput>>>,
}

impl RuntimeFixture {
    fn new() -> Self {
        Self {
            directory: TempDir::new().expect("temp directory"),
            deliveries: Arc::new(Mutex::new(Vec::new())),
            transcription_inputs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn runtime(&self) -> ListenerRuntime {
        self.runtime_with_capture_backend(Box::new(FileAudioCaptureBackend))
    }

    fn runtime_with_capture_backend(
        &self,
        capture_backend: Box<dyn AudioCaptureBackend>,
    ) -> ListenerRuntime {
        ListenerRuntime::with_dependencies(
            self.configuration(),
            capture_backend,
            Box::new(FixedBatchTranscriber::new(
                "transcribed text",
                Arc::clone(&self.transcription_inputs),
            )),
            OutputTargetDispatcher::new(Box::new(RecordingDelivery::new(Arc::clone(
                &self.deliveries,
            )))),
        )
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
}

struct FileAudioCaptureBackend;

impl AudioCaptureBackend for FileAudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> listener::Result<Box<dyn ActiveAudioCapture>> {
        let artifact_path = request.artifact_path();
        fs::create_dir_all(artifact_path.parent().expect("artifact parent"))?;
        let header = RecordingLogHeader::from_capture_start(
            request.session(),
            request.input_source(),
            RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
        )?;
        let mut writer = RecordingLogWriter::create(&artifact_path, header)?;
        writer.append_record(ACTIVE_AUDIO_BYTES)?;
        Ok(Box::new(FileAudioCapture::new(
            request.artifact().clone(),
            writer,
        )))
    }
}

const ACTIVE_AUDIO_BYTES: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
const STOPPED_AUDIO_BYTES: &[u8] = &[8, 9, 10, 11];

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
}

impl FileAudioCapture {
    fn new(artifact: DurableAudioArtifact, writer: RecordingLogWriter) -> Self {
        Self { artifact, writer }
    }
}

impl ActiveAudioCapture for FileAudioCapture {
    fn artifact(&self) -> &DurableAudioArtifact {
        &self.artifact
    }

    fn stop(self: Box<Self>) -> listener::Result<DurableAudioArtifact> {
        let FileAudioCapture {
            artifact,
            mut writer,
        } = *self;
        writer.append_record(STOPPED_AUDIO_BYTES)?;
        writer.finish()?;
        Ok(artifact)
    }
}

struct FixedBatchTranscriber {
    text: String,
    inputs: Arc<Mutex<Vec<BatchTranscriptionInput>>>,
}

impl FixedBatchTranscriber {
    fn new(text: impl Into<String>, inputs: Arc<Mutex<Vec<BatchTranscriptionInput>>>) -> Self {
        Self {
            text: text.into(),
            inputs,
        }
    }
}

impl BatchTranscriber for FixedBatchTranscriber {
    fn transcribe(&self, request: BatchTranscriptionRequest) -> listener::Result<TranscriptText> {
        self.inputs
            .lock()
            .expect("transcription inputs")
            .push(request.input().clone());
        Ok(TranscriptText::new(self.text.clone()))
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

    runtime.handle_input(Input::stop(session));
    let stopped_export = RecordingLog::new(artifact.path().as_str())
        .recover()
        .expect("recover stopped recording log")
        .export_raw_pcm(fixture.directory.path().join("stopped.raw.s16le"))
        .expect("export stopped raw pcm");
    let stopped_bytes = fs::read(stopped_export.path()).expect("stopped artifact bytes");
    let mut expected = Vec::new();
    expected.extend_from_slice(ACTIVE_AUDIO_BYTES);
    expected.extend_from_slice(STOPPED_AUDIO_BYTES);
    assert_eq!(stopped_bytes, expected);
}

#[test]
fn fresh_runtime_start_preserves_existing_listenerlog_and_allocates_next_artifact() {
    let fixture = RuntimeFixture::new();
    let existing_path = fixture.write_recording_log(1, &[20, 21, 22, 23]);
    let original_bytes = fs::read(&existing_path).expect("existing log bytes");
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };
    assert_eq!(session.value(), 2);
    assert_eq!(runtime.orphaned_recordings().as_slice().len(), 1);

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
    let active_path = PathBuf::from(artifact.path().as_str());
    assert_eq!(active_path, fixture.capture_path(2));
    assert_ne!(active_path, existing_path);
    assert_eq!(
        fs::read(&existing_path).expect("existing log after start"),
        original_bytes
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
fn idle_status_recovers_orphaned_listenerlog_idempotently_and_leaves_it_exportable() {
    let fixture = RuntimeFixture::new();
    let existing_path = fixture.write_recording_log(1, &[30, 31, 32, 33]);
    OpenOptions::new()
        .append(true)
        .open(&existing_path)
        .expect("open orphan log for torn tail")
        .write_all(b"torn listener tail")
        .expect("append torn tail");
    let torn_length = fs::metadata(&existing_path).expect("torn metadata").len();
    let mut runtime = fixture.runtime();

    match runtime.handle_input(Input::Status(StatusRequest {})) {
        Output::StatusReported(report) => assert_eq!(report.status(), &CaptureStatus::Idle),
        other => panic!("expected idle status reply, got {other:?}"),
    }

    let length_after_first_recovery = fs::metadata(&existing_path)
        .expect("metadata after first recovery")
        .len();
    {
        let recordings = runtime.orphaned_recordings();
        assert_eq!(recordings.next_session_value(), 2);
        assert_eq!(recordings.as_slice().len(), 1);
        let recovered = &recordings.as_slice()[0];
        assert_eq!(recovered.path(), existing_path.as_path());
        assert_eq!(recovered.truncated_from(), Some(torn_length));
        assert_eq!(recovered.records().len(), 1);
        let export = recovered
            .export_raw_pcm(fixture.directory.path().join("orphan.raw.s16le"))
            .expect("export recovered orphan");
        assert_eq!(
            fs::read(export.path()).expect("orphan raw bytes"),
            vec![30, 31, 32, 33]
        );
    }

    match runtime.handle_input(Input::Status(StatusRequest {})) {
        Output::StatusReported(report) => assert_eq!(report.status(), &CaptureStatus::Idle),
        other => panic!("expected second idle status reply, got {other:?}"),
    }
    let recordings = runtime.orphaned_recordings();
    assert_eq!(recordings.as_slice().len(), 1);
    assert_eq!(recordings.as_slice()[0].truncated_from(), None);
    assert_eq!(
        fs::metadata(&existing_path)
            .expect("metadata after second recovery")
            .len(),
        length_after_first_recovery
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
            .ends_with(".listenerlog")
    );
    let transcription_inputs = fixture.transcription_inputs();
    assert_eq!(transcription_inputs.len(), 1);
    assert!(
        transcription_inputs[0]
            .path()
            .to_string_lossy()
            .ends_with(".raw.s16le")
    );
    match transcription_inputs[0].format() {
        BatchTranscriptionInputFormat::SignedSixteenBitLittleEndianPcm { audio_format } => {
            assert_eq!(audio_format.sample_rate(), 16_000);
            assert_eq!(audio_format.channel_count(), 1);
        }
        other => panic!("expected raw PCM transcription input, got {other:?}"),
    }
    assert_eq!(stopped.transcript_text.as_str(), "transcribed text");
    assert_eq!(
        fixture.delivered_texts(),
        vec!["transcribed text".to_owned()]
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
fn start_while_active_returns_typed_conflict_reply() {
    let fixture = RuntimeFixture::new();
    let mut runtime = fixture.runtime();

    let session = match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::Started(started) => started.payload().payload().clone(),
        other => panic!("expected started reply, got {other:?}"),
    };

    match runtime.handle_input(Input::Start(StartCapture {})) {
        Output::CaptureAlreadyActive(conflict) => {
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
        Output::NoActiveCapture(_) => {}
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
        Output::CaptureSessionMismatch(conflict) => {
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
    let mut server = ListenerSocketServer::new(fixture.configuration(), fixture.runtime());
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
fn socket_server_answers_public_conflict_frame_with_matching_exchange() {
    let fixture = RuntimeFixture::new();
    let (mut client_stream, server_stream) = UnixStream::pair().expect("socket pair");
    let mut server = ListenerSocketServer::new(fixture.configuration(), fixture.runtime());
    let exchange = ExchangeIdentifier::new(
        SessionEpoch::new(5),
        ExchangeLane::Connector,
        LaneSequence::new(14),
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
                        SubReply::Ok(Output::NoActiveCapture(_)) => {}
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
