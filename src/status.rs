use std::{
    fs,
    io::{ErrorKind, Write},
    os::unix::fs::PermissionsExt,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::Serialize;

use crate::{Configuration, Error, LatencyInstrumentation, RecordingSampleFormat, Result};

const STATUS_SOCKET_MODE: u32 = 0o660;
const STATUS_IDLE_DELAY: Duration = Duration::from_millis(900);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerStatusState {
    Idle,
    Starting,
    Recording,
    Finalizing,
    Transcribing,
    Cancelling,
    Cancelled,
    Delivered,
    Error,
}

impl ListenerStatusState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Starting => "starting",
            Self::Recording => "recording",
            Self::Finalizing => "finalizing",
            Self::Transcribing => "transcribing",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Delivered => "delivered",
            Self::Error => "error",
        }
    }

    fn returns_to_idle(&self) -> bool {
        matches!(self, Self::Cancelled | Self::Delivered | Self::Error)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MicrophoneLevel {
    value: f32,
}

impl MicrophoneLevel {
    pub fn silent() -> Self {
        Self { value: 0.0 }
    }

    pub fn new(value: f32) -> Self {
        if value.is_finite() {
            Self {
                value: value.clamp(0.0, 1.0),
            }
        } else {
            Self::silent()
        }
    }

    pub fn from_signed_sixteen_bit_little_endian_pcm(bytes: &[u8]) -> Self {
        let samples = bytes.chunks_exact(2);
        let sample_count = samples.len();
        if sample_count == 0 {
            return Self::silent();
        }

        let square_sum = samples
            .map(|sample| {
                let value = i16::from_le_bytes([sample[0], sample[1]]) as f64 / i16::MAX as f64;
                value * value
            })
            .sum::<f64>();
        let rms = (square_sum / sample_count as f64).sqrt();
        Self::new((1.0 - (-rms * 18.0).exp()) as f32)
    }

    pub fn value(&self) -> f32 {
        self.value
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListenerStatusEvent {
    state: ListenerStatusState,
    level: MicrophoneLevel,
}

impl ListenerStatusEvent {
    pub fn idle() -> Self {
        Self::new(ListenerStatusState::Idle, MicrophoneLevel::silent())
    }

    pub fn starting() -> Self {
        Self::new(ListenerStatusState::Starting, MicrophoneLevel::silent())
    }

    pub fn recording(level: MicrophoneLevel) -> Self {
        Self::new(ListenerStatusState::Recording, level)
    }

    pub fn finalizing() -> Self {
        Self::new(ListenerStatusState::Finalizing, MicrophoneLevel::silent())
    }

    pub fn transcribing() -> Self {
        Self::new(ListenerStatusState::Transcribing, MicrophoneLevel::silent())
    }

    pub fn cancelling() -> Self {
        Self::new(ListenerStatusState::Cancelling, MicrophoneLevel::silent())
    }

    pub fn cancelled() -> Self {
        Self::new(ListenerStatusState::Cancelled, MicrophoneLevel::silent())
    }

    pub fn delivered() -> Self {
        Self::new(ListenerStatusState::Delivered, MicrophoneLevel::silent())
    }

    pub fn error() -> Self {
        Self::new(ListenerStatusState::Error, MicrophoneLevel::silent())
    }

    pub fn new(state: ListenerStatusState, level: MicrophoneLevel) -> Self {
        Self { state, level }
    }

    pub fn state(&self) -> ListenerStatusState {
        self.state
    }

    pub fn level(&self) -> MicrophoneLevel {
        self.level
    }

    pub fn json_line(&self) -> Result<String> {
        serde_json::to_string(&ListenerStatusEventFrame::from_event(self))
            .map(|json| format!("{json}\n"))
            .map_err(|error| Error::StatusEventEncode {
                message: error.to_string(),
            })
    }
}

#[derive(Serialize)]
struct ListenerStatusEventFrame<'a> {
    state: &'a str,
    level: f32,
}

impl<'a> ListenerStatusEventFrame<'a> {
    fn from_event(event: &'a ListenerStatusEvent) -> Self {
        Self {
            state: event.state().as_str(),
            level: event.level().value(),
        }
    }
}

#[derive(Clone)]
pub struct StatusPublisher {
    sink: StatusPublisherSink,
    latency_instrumentation: LatencyInstrumentation,
}

impl StatusPublisher {
    pub fn silent() -> Self {
        Self {
            sink: StatusPublisherSink::Silent,
            latency_instrumentation: LatencyInstrumentation::disabled(),
        }
    }

    pub fn recorder() -> (Self, StatusEventRecorder) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                sink: StatusPublisherSink::Recorder(Arc::clone(&events)),
                latency_instrumentation: LatencyInstrumentation::disabled(),
            },
            StatusEventRecorder::new(events),
        )
    }

    fn stream(
        sender: mpsc::Sender<StatusStreamMessage>,
        latency_instrumentation: LatencyInstrumentation,
    ) -> Self {
        Self {
            sink: StatusPublisherSink::Stream(sender),
            latency_instrumentation,
        }
    }

    pub fn publish(&self, event: ListenerStatusEvent) {
        self.latency_instrumentation
            .record_state_publication(event.state());
        match &self.sink {
            StatusPublisherSink::Silent => {}
            StatusPublisherSink::Stream(sender) => {
                let _ = sender.send(StatusStreamMessage::Publish(event));
            }
            StatusPublisherSink::Recorder(events) => {
                if let Ok(mut events) = events.lock() {
                    events.push(event);
                }
            }
        }
    }

    pub fn publish_idle(&self) {
        self.publish(ListenerStatusEvent::idle());
    }

    pub fn publish_starting(&self) {
        self.publish(ListenerStatusEvent::starting());
    }

    pub fn publish_recording_level(&self, level: MicrophoneLevel) {
        self.publish(ListenerStatusEvent::recording(level));
    }

    pub fn publish_finalizing(&self) {
        self.publish(ListenerStatusEvent::finalizing());
    }

    pub fn publish_transcribing(&self) {
        self.publish(ListenerStatusEvent::transcribing());
    }

    pub fn publish_cancelling(&self) {
        self.publish(ListenerStatusEvent::cancelling());
    }

    pub fn publish_cancelled(&self) {
        self.publish(ListenerStatusEvent::cancelled());
    }

    pub fn publish_delivered(&self) {
        self.publish(ListenerStatusEvent::delivered());
    }

    pub fn publish_error(&self) {
        self.publish(ListenerStatusEvent::error());
    }
}

#[derive(Clone)]
enum StatusPublisherSink {
    Silent,
    Stream(mpsc::Sender<StatusStreamMessage>),
    Recorder(Arc<Mutex<Vec<ListenerStatusEvent>>>),
}

#[derive(Clone)]
pub struct StatusEventRecorder {
    events: Arc<Mutex<Vec<ListenerStatusEvent>>>,
}

impl StatusEventRecorder {
    fn new(events: Arc<Mutex<Vec<ListenerStatusEvent>>>) -> Self {
        Self { events }
    }

    pub fn events(&self) -> Vec<ListenerStatusEvent> {
        self.events.lock().expect("status events").clone()
    }
}

pub struct StatusStreamServer {
    binding: StatusSocketBinding,
    receiver: mpsc::Receiver<StatusStreamMessage>,
    idle_delay: Duration,
}

impl StatusStreamServer {
    pub fn from_configuration(configuration: &Configuration) -> (Self, StatusPublisher) {
        Self::new(configuration.status_socket_path())
    }

    pub fn from_configuration_with_latency(
        configuration: &Configuration,
        latency_instrumentation: LatencyInstrumentation,
    ) -> (Self, StatusPublisher) {
        Self::new_with_latency(configuration.status_socket_path(), latency_instrumentation)
    }

    pub fn new(path: impl Into<PathBuf>) -> (Self, StatusPublisher) {
        Self::new_with_latency(path, LatencyInstrumentation::disabled())
    }

    pub fn new_with_latency(
        path: impl Into<PathBuf>,
        latency_instrumentation: LatencyInstrumentation,
    ) -> (Self, StatusPublisher) {
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                binding: StatusSocketBinding::new(path, STATUS_SOCKET_MODE),
                receiver,
                idle_delay: STATUS_IDLE_DELAY,
            },
            StatusPublisher::stream(sender, latency_instrumentation),
        )
    }

    pub fn spawn(self) -> Result<JoinHandle<()>> {
        self.binding.prepare()?;
        let listener = UnixListener::bind(self.binding.path())?;
        fs::set_permissions(
            self.binding.path(),
            fs::Permissions::from_mode(self.binding.mode()),
        )?;
        let shared_state = Arc::new(Mutex::new(StatusStreamSharedState::new()));
        StatusStreamAcceptor::new(listener, Arc::clone(&shared_state)).spawn();
        Ok(thread::spawn(move || {
            StatusStreamLoop::new(self.receiver, shared_state, self.idle_delay).run();
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusSocketBinding {
    path: PathBuf,
    mode: u32,
}

impl StatusSocketBinding {
    pub fn new(path: impl Into<PathBuf>, mode: u32) -> Self {
        Self {
            path: path.into(),
            mode,
        }
    }

    pub fn prepare(&self) -> Result<()> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| Error::SocketParentMissing {
                path: self.path.display().to_string(),
            })?;
        fs::create_dir_all(parent)?;
        self.remove_stale_socket_if_needed()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn mode(&self) -> u32 {
        self.mode
    }

    fn remove_stale_socket_if_needed(&self) -> Result<()> {
        match UnixStream::connect(&self.path) {
            Ok(_) => Err(Error::StatusSocketAlreadyRunning {
                path: self.path.display().to_string(),
            }),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) if self.path.exists() && error.kind() == ErrorKind::ConnectionRefused => {
                fs::remove_file(&self.path)?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

struct StatusStreamSharedState {
    current: ListenerStatusEvent,
    clients: Vec<UnixStream>,
}

impl StatusStreamSharedState {
    fn new() -> Self {
        Self {
            current: ListenerStatusEvent::idle(),
            clients: Vec::new(),
        }
    }
}

struct StatusStreamAcceptor {
    listener: UnixListener,
    shared_state: Arc<Mutex<StatusStreamSharedState>>,
}

impl StatusStreamAcceptor {
    fn new(listener: UnixListener, shared_state: Arc<Mutex<StatusStreamSharedState>>) -> Self {
        Self {
            listener,
            shared_state,
        }
    }

    fn spawn(self) {
        thread::spawn(move || self.accept_until_closed());
    }

    fn accept_until_closed(self) {
        for stream in self.listener.incoming() {
            let Ok(stream) = stream else {
                break;
            };
            self.admit(stream);
        }
    }

    fn admit(&self, mut stream: UnixStream) {
        if stream.set_nonblocking(true).is_err() {
            return;
        }
        let Ok(mut shared_state) = self.shared_state.lock() else {
            return;
        };
        let line = match shared_state.current.json_line() {
            Ok(line) => StatusStreamBroadcastLine::new(line),
            Err(_) => return,
        };
        if line.write_to(&mut stream).is_ok() {
            shared_state.clients.push(stream);
        }
    }
}

struct StatusStreamLoop {
    receiver: mpsc::Receiver<StatusStreamMessage>,
    shared_state: Arc<Mutex<StatusStreamSharedState>>,
    idle_delay: Duration,
    idle_deadline: Option<Instant>,
}

impl StatusStreamLoop {
    fn new(
        receiver: mpsc::Receiver<StatusStreamMessage>,
        shared_state: Arc<Mutex<StatusStreamSharedState>>,
        idle_delay: Duration,
    ) -> Self {
        Self {
            receiver,
            shared_state,
            idle_delay,
            idle_deadline: None,
        }
    }

    fn run(&mut self) {
        loop {
            if !self.receive_next_message() {
                return;
            }
        }
    }

    fn receive_next_message(&mut self) -> bool {
        let message = match self.idle_deadline {
            Some(deadline) => match self
                .receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(message) => Some(message),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.publish_idle_if_due();
                    None
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            },
            None => match self.receiver.recv() {
                Ok(message) => Some(message),
                Err(_) => return false,
            },
        };
        if let Some(StatusStreamMessage::Publish(event)) = message {
            self.broadcast(event);
        }
        true
    }

    fn broadcast(&mut self, event: ListenerStatusEvent) {
        self.idle_deadline = if event.state().returns_to_idle() {
            Some(Instant::now() + self.idle_delay)
        } else {
            None
        };
        let Ok(mut shared_state) = self.shared_state.lock() else {
            return;
        };
        shared_state.current = event;
        let line = match shared_state.current.json_line() {
            Ok(line) => StatusStreamBroadcastLine::new(line),
            Err(_) => return,
        };
        shared_state
            .clients
            .retain_mut(|stream| line.write_to(stream).is_ok());
    }

    fn publish_idle_if_due(&mut self) {
        if self
            .idle_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.idle_deadline = None;
            self.broadcast(ListenerStatusEvent::idle());
        }
    }
}

struct StatusStreamBroadcastLine {
    line: String,
}

impl StatusStreamBroadcastLine {
    fn new(line: String) -> Self {
        Self { line }
    }

    fn write_to(&self, stream: &mut UnixStream) -> std::io::Result<()> {
        loop {
            match stream.write(self.line.as_bytes()) {
                Ok(count) if count == self.line.len() => return Ok(()),
                Ok(_) => {
                    return Err(std::io::Error::new(
                        ErrorKind::WouldBlock,
                        "status client accepted a partial frame",
                    ));
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

enum StatusStreamMessage {
    Publish(ListenerStatusEvent),
}

impl MicrophoneLevel {
    pub fn from_recording_payload(bytes: &[u8], sample_format: RecordingSampleFormat) -> Self {
        match sample_format {
            RecordingSampleFormat::SignedSixteenBitLittleEndian => {
                Self::from_signed_sixteen_bit_little_endian_pcm(bytes)
            }
        }
    }
}
