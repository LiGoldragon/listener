use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    sync::{Arc, Mutex},
};

use listener::daemon::ListenerSocketServer;
use listener::{
    ActiveAudioCapture, AudioCaptureBackend, AudioCaptureStart, BatchTranscriber,
    BatchTranscriptionRequest, Configuration, ListenerRuntime, OutputTargetDispatcher,
    TranscriptDelivery, TranscriptDeliveryRequest,
};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use signal_listener::{
    ActiveCapture, CaptureStatus, DeliveryOutcome, DurableAudioArtifact, Frame, FrameBody, Input,
    InputSource, ListenerDaemonConfiguration, MetaSocketMode, MetaSocketPath, Output, OutputTarget,
    OutputTargets, SocketMode, StartCapture, StatusRequest, TranscriptText, TranscriptionMode,
    WirePath, WorkingSocketMode, WorkingSocketPath,
};
use tempfile::TempDir;

struct RuntimeFixture {
    directory: TempDir,
    deliveries: Arc<Mutex<Vec<String>>>,
}

impl RuntimeFixture {
    fn new() -> Self {
        Self {
            directory: TempDir::new().expect("temp directory"),
            deliveries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn runtime(&self) -> ListenerRuntime {
        ListenerRuntime::with_dependencies(
            self.configuration(),
            Box::new(FileAudioCaptureBackend),
            Box::new(FixedBatchTranscriber::new("transcribed text")),
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

    fn delivered_texts(&self) -> Vec<String> {
        self.deliveries.lock().expect("deliveries").clone()
    }
}

struct FileAudioCaptureBackend;

impl AudioCaptureBackend for FileAudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> listener::Result<Box<dyn ActiveAudioCapture>> {
        let artifact_path = request.artifact_path();
        fs::create_dir_all(artifact_path.parent().expect("artifact parent"))?;
        let mut file = File::create(&artifact_path)?;
        file.write_all(b"active audio chunk\n")?;
        file.sync_all()?;
        Ok(Box::new(FileAudioCapture::new(request.artifact().clone())))
    }
}

struct FileAudioCapture {
    artifact: DurableAudioArtifact,
}

impl FileAudioCapture {
    fn new(artifact: DurableAudioArtifact) -> Self {
        Self { artifact }
    }
}

impl ActiveAudioCapture for FileAudioCapture {
    fn artifact(&self) -> &DurableAudioArtifact {
        &self.artifact
    }

    fn stop(self: Box<Self>) -> listener::Result<DurableAudioArtifact> {
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.artifact.path().as_str())?;
        file.write_all(b"stopped audio chunk\n")?;
        file.sync_all()?;
        Ok(self.artifact.clone())
    }
}

struct FixedBatchTranscriber {
    text: String,
}

impl FixedBatchTranscriber {
    fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

impl BatchTranscriber for FixedBatchTranscriber {
    fn transcribe(&self, _request: BatchTranscriptionRequest) -> listener::Result<TranscriptText> {
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

    let active_bytes = fs::read(artifact.path().as_str()).expect("active artifact bytes");
    assert_eq!(active_bytes, b"active audio chunk\n");

    runtime.handle_input(Input::stop(session));
    let stopped_bytes = fs::read(artifact.path().as_str()).expect("stopped artifact bytes");
    assert_eq!(stopped_bytes, b"active audio chunk\nstopped audio chunk\n");
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
            .ends_with(".s16le")
    );
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
