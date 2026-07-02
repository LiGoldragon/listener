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

use crate::{Configuration, Error, RecordingSampleFormat, Result};

const STATUS_SOCKET_MODE: u32 = 0o660;
const STATUS_IDLE_DELAY: Duration = Duration::from_millis(900);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerStatusState {
    Idle,
    Recording,
    Transcribing,
    Cancelled,
    Copied,
    Error,
}

impl ListenerStatusState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Recording => "recording",
            Self::Transcribing => "transcribing",
            Self::Cancelled => "cancelled",
            Self::Copied => "copied",
            Self::Error => "error",
        }
    }

    fn returns_to_idle(&self) -> bool {
        matches!(self, Self::Cancelled | Self::Copied | Self::Error)
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

    pub fn recording(level: MicrophoneLevel) -> Self {
        Self::new(ListenerStatusState::Recording, level)
    }

    pub fn transcribing() -> Self {
        Self::new(ListenerStatusState::Transcribing, MicrophoneLevel::silent())
    }

    pub fn cancelled() -> Self {
        Self::new(ListenerStatusState::Cancelled, MicrophoneLevel::silent())
    }

    pub fn copied() -> Self {
        Self::new(ListenerStatusState::Copied, MicrophoneLevel::silent())
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
}

impl StatusPublisher {
    pub fn silent() -> Self {
        Self {
            sink: StatusPublisherSink::Silent,
        }
    }

    pub fn recorder() -> (Self, StatusEventRecorder) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                sink: StatusPublisherSink::Recorder(Arc::clone(&events)),
            },
            StatusEventRecorder::new(events),
        )
    }

    fn stream(sender: mpsc::Sender<StatusStreamMessage>) -> Self {
        Self {
            sink: StatusPublisherSink::Stream(sender),
        }
    }

    pub fn publish(&self, event: ListenerStatusEvent) {
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

    pub fn publish_recording_level(&self, level: MicrophoneLevel) {
        self.publish(ListenerStatusEvent::recording(level));
    }

    pub fn publish_transcribing(&self) {
        self.publish(ListenerStatusEvent::transcribing());
    }

    pub fn publish_cancelled(&self) {
        self.publish(ListenerStatusEvent::cancelled());
    }

    pub fn publish_copied(&self) {
        self.publish(ListenerStatusEvent::copied());
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

    pub fn new(path: impl Into<PathBuf>) -> (Self, StatusPublisher) {
        let (sender, receiver) = mpsc::channel();
        (
            Self {
                binding: StatusSocketBinding::new(path, STATUS_SOCKET_MODE),
                receiver,
                idle_delay: STATUS_IDLE_DELAY,
            },
            StatusPublisher::stream(sender),
        )
    }

    pub fn spawn(self) -> Result<JoinHandle<()>> {
        self.binding.prepare()?;
        let listener = UnixListener::bind(self.binding.path())?;
        fs::set_permissions(
            self.binding.path(),
            fs::Permissions::from_mode(self.binding.mode()),
        )?;
        listener.set_nonblocking(true)?;
        Ok(thread::spawn(move || {
            StatusStreamLoop::new(listener, self.receiver, self.idle_delay).run();
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

struct StatusStreamLoop {
    listener: UnixListener,
    receiver: mpsc::Receiver<StatusStreamMessage>,
    current: ListenerStatusEvent,
    clients: Vec<UnixStream>,
    idle_delay: Duration,
    idle_deadline: Option<Instant>,
}

impl StatusStreamLoop {
    fn new(
        listener: UnixListener,
        receiver: mpsc::Receiver<StatusStreamMessage>,
        idle_delay: Duration,
    ) -> Self {
        Self {
            listener,
            receiver,
            current: ListenerStatusEvent::idle(),
            clients: Vec::new(),
            idle_delay,
            idle_deadline: None,
        }
    }

    fn run(&mut self) {
        loop {
            self.accept_waiting_clients();
            self.receive_waiting_messages();
            self.publish_idle_if_due();
            thread::sleep(self.loop_delay());
        }
    }

    fn accept_waiting_clients(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((mut stream, _)) => {
                    if stream.set_nonblocking(true).is_ok()
                        && self
                            .write_event_to_stream(&mut stream, &self.current)
                            .is_ok()
                    {
                        self.clients.push(stream);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    fn receive_waiting_messages(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(StatusStreamMessage::Publish(event)) => self.broadcast(event),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }
    }

    fn broadcast(&mut self, event: ListenerStatusEvent) {
        self.idle_deadline = if event.state().returns_to_idle() {
            Some(Instant::now() + self.idle_delay)
        } else {
            None
        };
        self.current = event;
        let line = match self.current.json_line() {
            Ok(line) => line,
            Err(_) => return,
        };
        let line = StatusStreamBroadcastLine::new(line);
        self.clients
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

    fn loop_delay(&self) -> Duration {
        self.idle_deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_millis(25))
            .min(Duration::from_millis(25))
    }

    fn write_event_to_stream(
        &self,
        stream: &mut UnixStream,
        event: &ListenerStatusEvent,
    ) -> Result<()> {
        StatusStreamBroadcastLine::new(event.json_line()?).write_to(stream)?;
        Ok(())
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
