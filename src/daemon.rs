use std::{
    collections::VecDeque,
    fs,
    io::ErrorKind,
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::ExitCode,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use signal_frame::ExchangeIdentifier;
use signal_listener::{
    ActiveCapture, ActiveCaptureSession, CaptureCancellationRequested, CaptureCompletionRequested,
    CaptureSession, CaptureStatus, CompletionRequestedSession, DaemonEpoch, Input,
    MaintenanceLeaseAbsent, MaintenanceLeaseCancellation, MaintenanceLeaseEpoch,
    MaintenanceLeaseRelease, NoActiveCapture, Output, RequestedCaptureSession,
};

use crate::runtime::{
    CaptureCancellationSignal, CaptureFinalizationPhase, RuntimeCaptureCancellationWork,
    RuntimeCaptureFinalizationWork, RuntimeCaptureStartWork,
};
use crate::{
    CaptureMaintenance, Configuration, ContractFrameCodec, ContractFrameStream, Error,
    LatencyInstrumentation, ListenerRuntime, Result, StatusStreamServer,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenerDaemon {
    arguments: Vec<String>,
}

impl ListenerDaemon {
    pub fn from_environment() -> Self {
        Self {
            arguments: std::env::args().collect(),
        }
    }

    pub fn from_arguments(arguments: Vec<String>) -> Self {
        Self { arguments }
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn run(&self) -> Result<()> {
        let configuration = Configuration::from_environment();
        let latency = LatencyInstrumentation::from_environment();
        let (status_server, status_publisher) =
            StatusStreamServer::from_configuration_with_latency(&configuration, latency.clone());
        let _status_thread = status_server.spawn()?;
        let mut runtime = ListenerRuntime::from_configuration_with_status_and_latency(
            configuration.clone(),
            status_publisher,
            latency.clone(),
        )?;
        let maintenance = CaptureMaintenance::from_configuration(&configuration)?;
        runtime.advance_session_sequence(maintenance.snapshot().next_session_value());
        let _maintenance_thread = maintenance.spawn();
        ListenerSocketServer::new_with_latency(configuration, runtime, latency).serve()
    }

    pub fn run_to_exit_code() -> ExitCode {
        match Self::from_environment().run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("listener-daemon: {error}");
                ExitCode::FAILURE
            }
        }
    }
}

pub struct ListenerSocketServer {
    configuration: Configuration,
    mailbox: ListenerOperationMailbox,
    latency: LatencyInstrumentation,
    next_connection: AtomicU64,
}

impl ListenerSocketServer {
    pub fn new(configuration: Configuration, runtime: ListenerRuntime) -> Self {
        Self::new_with_latency(configuration, runtime, LatencyInstrumentation::disabled())
    }

    pub fn new_with_latency(
        configuration: Configuration,
        runtime: ListenerRuntime,
        latency: LatencyInstrumentation,
    ) -> Self {
        Self {
            configuration,
            mailbox: ListenerOperationActor::spawn(runtime),
            latency,
            next_connection: AtomicU64::new(1),
        }
    }

    pub fn serve(&self) -> Result<()> {
        let binding = DaemonSocketBinding::new(
            self.configuration.working_socket_path(),
            self.configuration.working_socket_mode(),
        );
        binding.prepare()?;
        let listener = UnixListener::bind(binding.path())?;
        fs::set_permissions(binding.path(), fs::Permissions::from_mode(binding.mode()))?;
        for stream in listener.incoming() {
            let connection = self.persistent_connection();
            thread::spawn(move || {
                let _ = connection.handle(stream?);
                Ok::<(), Error>(())
            });
        }
        Ok(())
    }

    pub fn handle_connection(&self, stream: UnixStream) -> Result<()> {
        let mut stream = ContractFrameStream::new(stream, ContractFrameCodec::listener_default());
        let request = stream.receive_request()?;
        self.latency.record_request_received();
        let output = self.mailbox.request(request.input().clone())?;
        stream.send_reply(request, output)
    }

    pub fn handle_persistent_connection(&self, stream: UnixStream) -> Result<()> {
        self.persistent_connection().handle(stream)
    }

    pub fn handle_input(&self, input: Input) -> Result<Output> {
        self.mailbox.request(input)
    }

    fn persistent_connection(&self) -> ListenerConnection {
        ListenerConnection::new(
            ConnectionIdentifier::new(self.next_connection.fetch_add(1, Ordering::Relaxed)),
            self.mailbox.clone(),
            self.latency.clone(),
        )
    }
}

struct ListenerConnection {
    identifier: ConnectionIdentifier,
    mailbox: ListenerOperationMailbox,
    latency: LatencyInstrumentation,
}

impl ListenerConnection {
    fn new(
        identifier: ConnectionIdentifier,
        mailbox: ListenerOperationMailbox,
        latency: LatencyInstrumentation,
    ) -> Self {
        Self {
            identifier,
            mailbox,
            latency,
        }
    }

    fn handle(&self, stream: UnixStream) -> Result<()> {
        let reader_stream = stream.try_clone()?;
        let (reply_sender, reply_receiver) = mpsc::channel();
        ListenerResponseWriter::new(stream, reply_receiver).spawn();
        let mut reader =
            ContractFrameStream::new(reader_stream, ContractFrameCodec::listener_default());
        loop {
            match reader.receive_request() {
                Ok(request) => {
                    self.latency.record_request_received();
                    self.mailbox.request_from_connection(
                        self.identifier,
                        request.exchange(),
                        request.input().clone(),
                        reply_sender.clone(),
                    )?;
                }
                Err(error) => {
                    self.mailbox.disconnect(self.identifier);
                    return ConnectionReadError::new(error).into_result();
                }
            }
        }
    }
}

struct ListenerResponseWriter {
    stream: UnixStream,
    receiver: mpsc::Receiver<ConnectionReply>,
}

impl ListenerResponseWriter {
    fn new(stream: UnixStream, receiver: mpsc::Receiver<ConnectionReply>) -> Self {
        Self { stream, receiver }
    }

    fn spawn(self) {
        thread::spawn(move || {
            let _ = self.run();
        });
    }

    fn run(self) -> Result<()> {
        let mut stream =
            ContractFrameStream::new(self.stream, ContractFrameCodec::listener_default());
        while let Ok(reply) = self.receiver.recv() {
            stream.send_reply_for_exchange(reply.exchange, reply.output)?;
        }
        Ok(())
    }
}

struct ConnectionReadError {
    error: Error,
}

impl ConnectionReadError {
    fn new(error: Error) -> Self {
        Self { error }
    }

    fn into_result(self) -> Result<()> {
        match self.error {
            Error::Io(error)
                if matches!(
                    error.kind(),
                    ErrorKind::UnexpectedEof
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionReset
                        | ErrorKind::BrokenPipe
                ) =>
            {
                Ok(())
            }
            error => Err(error),
        }
    }
}

#[derive(Clone)]
struct ListenerOperationMailbox {
    sender: mpsc::Sender<ListenerOperationMail>,
}

impl ListenerOperationMailbox {
    fn request(&self, input: Input) -> Result<Output> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.send(ListenerOperationMail::Request(
            ListenerOperationRequest::new(
                ConnectionIdentifier::in_process(),
                input,
                ListenerReplySink::direct(sender),
            ),
        ))?;
        receiver.recv().map_err(|_| Error::NotImplemented {
            surface: "listener operation actor",
        })
    }

    fn request_from_connection(
        &self,
        connection: ConnectionIdentifier,
        exchange: ExchangeIdentifier,
        input: Input,
        sender: mpsc::Sender<ConnectionReply>,
    ) -> Result<()> {
        self.send(ListenerOperationMail::Request(
            ListenerOperationRequest::new(
                connection,
                input,
                ListenerReplySink::connection(exchange, sender),
            ),
        ))
    }

    fn disconnect(&self, connection: ConnectionIdentifier) {
        let _ = self.send(ListenerOperationMail::Disconnected(connection));
    }

    fn send(&self, mail: ListenerOperationMail) -> Result<()> {
        self.sender.send(mail).map_err(|_| Error::NotImplemented {
            surface: "listener operation actor",
        })
    }
}

struct ListenerOperationActor {
    runtime: ListenerRuntime,
    receiver: mpsc::Receiver<ListenerOperationMail>,
    mailbox: ListenerOperationMailbox,
    operation: ListenerOperationState,
    leases: MaintenanceLeaseState,
    terminal_status: Option<CaptureStatus>,
}

impl ListenerOperationActor {
    fn spawn(runtime: ListenerRuntime) -> ListenerOperationMailbox {
        let (sender, receiver) = mpsc::channel();
        let mailbox = ListenerOperationMailbox { sender };
        let actor = Self {
            runtime,
            receiver,
            mailbox: mailbox.clone(),
            operation: ListenerOperationState::Idle,
            leases: MaintenanceLeaseState::for_current_daemon(),
            terminal_status: None,
        };
        thread::spawn(move || actor.run());
        mailbox
    }

    fn run(mut self) {
        while let Ok(mail) = self.receiver.recv() {
            self.handle_mail(mail);
        }
    }

    fn handle_mail(&mut self, mail: ListenerOperationMail) {
        match mail {
            ListenerOperationMail::Request(request) => self.handle_request(request),
            ListenerOperationMail::Disconnected(connection) => self.leases.disconnect(connection),
            ListenerOperationMail::StartCompleted(completion) => self.finish_start(completion),
            ListenerOperationMail::CancellationCompleted(output) => {
                self.finish_cancellation(output)
            }
            ListenerOperationMail::FinalizationPhase(phase) => self.advance_finalization(phase),
            ListenerOperationMail::FinalizationCompleted(output) => {
                self.finish_finalization(output)
            }
        }
        self.leases.grant_if_idle(self.operation.is_idle());
    }

    fn handle_request(&mut self, request: ListenerOperationRequest) {
        match request.input {
            Input::Start(_) => self.begin_start(request.reply),
            Input::Toggle(_) => self.toggle(request.reply),
            Input::Cancel(cancel) => self.cancel(cancel.into_payload(), request.reply),
            Input::Stop(stop) => self.stop(stop.into_payload(), request.reply),
            Input::Status(_) => {
                let output = self.status();
                self.reply(request.reply, output);
            }
            Input::AcquireMaintenance(_) => self.leases.acquire(request.connection, request.reply),
            Input::ReleaseMaintenance(_) => self.leases.release(request.connection, request.reply),
            input => {
                let output = self.runtime.handle_input(input);
                self.reply(request.reply, output);
            }
        }
    }

    fn begin_start(&mut self, reply: ListenerReplySink) {
        if self.leases.gates_new_starts() {
            self.reply(reply, self.leases.active_reply());
            return;
        }
        if let Some((session, _)) = self.operation.active_capture() {
            self.reply(reply, Self::already_active(session.clone()));
            return;
        }
        match self.runtime.begin_capture_start() {
            Ok(start) => {
                self.terminal_status = None;
                self.operation = ListenerOperationState::Starting {
                    start,
                    reply,
                    cancellation: CaptureCancellationSignal::new(),
                };
                self.spawn_start();
            }
            Err(error) => self.reply(reply, error.into_start_reply()),
        }
    }

    fn toggle(&mut self, reply: ListenerReplySink) {
        match &self.operation {
            ListenerOperationState::Idle => self.begin_start(reply),
            ListenerOperationState::Capturing { session, .. } => self.stop(session.clone(), reply),
            ListenerOperationState::Finalizing {
                session, artifact, ..
            } => self.reply(
                reply,
                Self::completion_requested(session.clone(), artifact.clone()),
            ),
            ListenerOperationState::Starting { start, .. } => {
                self.reply(reply, Self::already_active(start.session().clone()))
            }
            ListenerOperationState::Cancelling { session, artifact } => self.reply(
                reply,
                Self::cancellation_requested(session.clone(), artifact.clone()),
            ),
        }
    }

    fn cancel(&mut self, session: CaptureSession, reply: ListenerReplySink) {
        let Some((active, _)) = self.operation.active_capture() else {
            self.reply(reply, Output::NoActive(NoActiveCapture {}));
            return;
        };
        if active != &session {
            self.reply(reply, Self::session_mismatch(active.clone(), session));
            return;
        }
        self.request_cancellation(reply);
    }

    fn stop(&mut self, session: CaptureSession, reply: ListenerReplySink) {
        let operation = std::mem::replace(&mut self.operation, ListenerOperationState::Idle);
        match operation {
            ListenerOperationState::Capturing {
                session: active,
                artifact,
            } if active == session => match self.runtime.begin_capture_finalization(active.clone())
            {
                Ok(work) => {
                    self.runtime.publish_finalizing();
                    let cancellation = CaptureCancellationSignal::new();
                    self.operation = ListenerOperationState::Finalizing {
                        session: active.clone(),
                        artifact: artifact.clone(),
                        cancellation: cancellation.clone(),
                        phase: CaptureFinalizationPhase::Finalizing,
                    };
                    self.spawn_finalization(work, cancellation);
                    self.reply(reply, Self::completion_requested(active, artifact));
                }
                Err(error) => self.reply(reply, error.into_stop_reply()),
            },
            ListenerOperationState::Capturing {
                session: active,
                artifact,
            } => {
                self.operation = ListenerOperationState::Capturing {
                    session: active.clone(),
                    artifact,
                };
                self.reply(reply, Self::session_mismatch(active, session));
            }
            ListenerOperationState::Finalizing {
                session: active,
                artifact,
                cancellation,
                phase,
            } if active == session => {
                let response = Self::completion_requested(active.clone(), artifact.clone());
                self.operation = ListenerOperationState::Finalizing {
                    session: active,
                    artifact,
                    cancellation,
                    phase,
                };
                self.reply(reply, response);
            }
            other => {
                self.operation = other;
                self.reply(reply, Output::NoActive(NoActiveCapture {}));
            }
        }
    }

    fn request_cancellation(&mut self, reply: ListenerReplySink) {
        let operation = std::mem::replace(&mut self.operation, ListenerOperationState::Idle);
        match operation {
            ListenerOperationState::Starting {
                start,
                reply: start_reply,
                cancellation,
            } => {
                cancellation.request();
                self.runtime.publish_cancelling();
                let output =
                    Self::cancellation_requested(start.session().clone(), start.artifact().clone());
                self.operation = ListenerOperationState::Starting {
                    start,
                    reply: start_reply,
                    cancellation,
                };
                self.reply(reply, output);
            }
            ListenerOperationState::Capturing { session, artifact } => {
                self.runtime.publish_cancelling();
                match self.runtime.begin_capture_cancellation(session.clone()) {
                    Ok(work) => {
                        let output = work.requested_reply();
                        self.operation = ListenerOperationState::Cancelling { session, artifact };
                        self.spawn_cancellation(work);
                        self.reply(reply, output);
                    }
                    Err(error) => self.reply(reply, error.into_cancel_reply()),
                }
            }
            ListenerOperationState::Finalizing {
                session,
                artifact,
                cancellation,
                phase,
            } => {
                cancellation.request();
                self.runtime.publish_cancelling();
                let output = Self::cancellation_requested(session.clone(), artifact.clone());
                self.operation = ListenerOperationState::Finalizing {
                    session,
                    artifact,
                    cancellation,
                    phase,
                };
                self.reply(reply, output);
            }
            ListenerOperationState::Cancelling { session, artifact } => {
                let output = Self::cancellation_requested(session.clone(), artifact.clone());
                self.operation = ListenerOperationState::Cancelling { session, artifact };
                self.reply(reply, output);
            }
            ListenerOperationState::Idle => self.reply(reply, Output::NoActive(NoActiveCapture {})),
        }
    }

    fn finish_start(&mut self, completion: ListenerStartCompletion) {
        let operation = std::mem::replace(&mut self.operation, ListenerOperationState::Idle);
        let ListenerOperationState::Starting {
            start,
            reply,
            cancellation,
        } = operation
        else {
            return;
        };
        match completion.result {
            Ok(capture) => {
                let session = start.session().clone();
                let artifact = start.artifact().clone();
                let output = self.runtime.install_started_capture(start, capture);
                self.reply(reply, output);
                if cancellation.is_requested() {
                    self.runtime.publish_cancelling();
                    match self.runtime.begin_capture_cancellation(session.clone()) {
                        Ok(work) => {
                            self.operation =
                                ListenerOperationState::Cancelling { session, artifact };
                            self.spawn_cancellation(work);
                        }
                        Err(_) => self.operation = ListenerOperationState::Idle,
                    }
                } else {
                    self.operation = ListenerOperationState::Capturing { session, artifact };
                }
            }
            Err(error) if error.is_recording_log_already_exists() => {
                match self.runtime.begin_capture_start() {
                    Ok(start) => {
                        self.operation = ListenerOperationState::Starting {
                            start,
                            reply,
                            cancellation,
                        };
                        self.spawn_start();
                    }
                    Err(error) => self.reply(reply, error.into_start_reply()),
                }
            }
            Err(error) => self.reply(reply, error.into_start_reply()),
        }
    }

    fn finish_cancellation(&mut self, _output: Output) {
        if matches!(self.operation, ListenerOperationState::Cancelling { .. }) {
            self.operation = ListenerOperationState::Idle;
            self.terminal_status = None;
        }
    }

    fn advance_finalization(&mut self, phase: CaptureFinalizationPhase) {
        if let ListenerOperationState::Finalizing {
            phase: current,
            cancellation,
            ..
        } = &mut self.operation
            && !cancellation.is_requested()
        {
            *current = phase;
        }
    }

    fn finish_finalization(&mut self, output: Output) {
        let operation = std::mem::replace(&mut self.operation, ListenerOperationState::Idle);
        let ListenerOperationState::Finalizing { session, .. } = operation else {
            return;
        };
        self.terminal_status = match output {
            Output::Stopped(stopped) => Some(CaptureStatus::Delivered(
                stopped.stopped_session.payload().clone(),
            )),
            Output::Cancelled(_) => None,
            _ => Some(CaptureStatus::Error(session)),
        };
    }

    fn spawn_start(&self) {
        let ListenerOperationState::Starting { start, .. } = &self.operation else {
            return;
        };
        let start = RuntimeCaptureStartWork::from(start);
        let sender = self.mailbox.sender.clone();
        thread::spawn(move || {
            let _ = sender.send(ListenerOperationMail::StartCompleted(
                ListenerStartCompletion {
                    result: start.start(),
                },
            ));
        });
    }

    fn spawn_cancellation(&self, work: RuntimeCaptureCancellationWork) {
        let sender = self.mailbox.sender.clone();
        thread::spawn(move || {
            let _ = sender.send(ListenerOperationMail::CancellationCompleted(work.execute()));
        });
    }

    fn spawn_finalization(
        &self,
        work: RuntimeCaptureFinalizationWork,
        cancellation: CaptureCancellationSignal,
    ) {
        let sender = self.mailbox.sender.clone();
        let (phase_sender, phase_receiver) = mpsc::channel();
        let phase_mailbox = self.mailbox.sender.clone();
        thread::spawn(move || {
            while let Ok(phase) = phase_receiver.recv() {
                let _ = phase_mailbox.send(ListenerOperationMail::FinalizationPhase(phase));
            }
        });
        thread::spawn(move || {
            let output = work.execute(cancellation, phase_sender);
            let _ = sender.send(ListenerOperationMail::FinalizationCompleted(output));
        });
    }

    fn status(&self) -> Output {
        let status = match &self.operation {
            ListenerOperationState::Idle => {
                self.terminal_status.clone().unwrap_or(CaptureStatus::Idle)
            }
            ListenerOperationState::Starting { start, .. } => {
                CaptureStatus::Capturing(ActiveCapture {
                    active_capture_session: ActiveCaptureSession::new(start.session().clone()),
                    durable_audio_artifact: start.artifact().clone(),
                })
            }
            ListenerOperationState::Capturing { session, artifact }
            | ListenerOperationState::Cancelling { session, artifact } => {
                CaptureStatus::Capturing(ActiveCapture {
                    active_capture_session: ActiveCaptureSession::new(session.clone()),
                    durable_audio_artifact: artifact.clone(),
                })
            }
            ListenerOperationState::Finalizing {
                session,
                artifact,
                phase,
                ..
            } => {
                let capture = ActiveCapture {
                    active_capture_session: ActiveCaptureSession::new(session.clone()),
                    durable_audio_artifact: artifact.clone(),
                };
                match phase {
                    CaptureFinalizationPhase::Finalizing => CaptureStatus::Finalizing(capture),
                    CaptureFinalizationPhase::Transcribing => CaptureStatus::Transcribing(capture),
                }
            }
        };
        Output::status_reported(status)
    }

    fn reply(&self, reply: ListenerReplySink, output: Output) {
        reply.respond(output);
    }

    fn already_active(session: CaptureSession) -> Output {
        Output::AlreadyActive(signal_listener::CaptureAlreadyActive::new(
            ActiveCaptureSession::new(session),
        ))
    }

    fn session_mismatch(active: CaptureSession, requested: CaptureSession) -> Output {
        Output::SessionMismatch(signal_listener::CaptureSessionMismatch {
            active_capture_session: ActiveCaptureSession::new(active),
            requested_capture_session: RequestedCaptureSession::new(requested),
        })
    }

    fn completion_requested(
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    ) -> Output {
        Output::CompletionRequested(CaptureCompletionRequested {
            completion_requested_session: CompletionRequestedSession::new(session),
            durable_audio_artifact: artifact,
        })
    }

    fn cancellation_requested(
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    ) -> Output {
        Output::CancellationRequested(CaptureCancellationRequested {
            cancellation_requested_session: signal_listener::CancellationRequestedSession::new(
                session,
            ),
            durable_audio_artifact: artifact,
        })
    }
}

struct ListenerOperationRequest {
    connection: ConnectionIdentifier,
    input: Input,
    reply: ListenerReplySink,
}

impl ListenerOperationRequest {
    fn new(connection: ConnectionIdentifier, input: Input, reply: ListenerReplySink) -> Self {
        Self {
            connection,
            input,
            reply,
        }
    }
}

enum ListenerOperationMail {
    Request(ListenerOperationRequest),
    Disconnected(ConnectionIdentifier),
    StartCompleted(ListenerStartCompletion),
    CancellationCompleted(Output),
    FinalizationPhase(CaptureFinalizationPhase),
    FinalizationCompleted(Output),
}

struct ListenerStartCompletion {
    result: Result<Box<dyn crate::ActiveAudioCapture>>,
}

enum ListenerOperationState {
    Idle,
    Starting {
        start: RuntimeCaptureStartWork,
        reply: ListenerReplySink,
        cancellation: CaptureCancellationSignal,
    },
    Capturing {
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    },
    Finalizing {
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
        cancellation: CaptureCancellationSignal,
        phase: CaptureFinalizationPhase,
    },
    Cancelling {
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    },
}

impl ListenerOperationState {
    fn active_capture(&self) -> Option<(&CaptureSession, &signal_listener::DurableAudioArtifact)> {
        match self {
            Self::Idle => None,
            Self::Starting { start, .. } => Some((start.session(), start.artifact())),
            Self::Capturing { session, artifact }
            | Self::Finalizing {
                session, artifact, ..
            }
            | Self::Cancelling { session, artifact } => Some((session, artifact)),
        }
    }

    fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

#[derive(Clone)]
enum ListenerReplySink {
    Direct(mpsc::SyncSender<Output>),
    Connection(ConnectionReplySender),
}

impl ListenerReplySink {
    fn direct(sender: mpsc::SyncSender<Output>) -> Self {
        Self::Direct(sender)
    }

    fn connection(exchange: ExchangeIdentifier, sender: mpsc::Sender<ConnectionReply>) -> Self {
        Self::Connection(ConnectionReplySender { exchange, sender })
    }

    fn respond(&self, output: Output) {
        match self {
            Self::Direct(sender) => {
                let _ = sender.send(output);
            }
            Self::Connection(sender) => sender.respond(output),
        }
    }
}

#[derive(Clone)]
struct ConnectionReplySender {
    exchange: ExchangeIdentifier,
    sender: mpsc::Sender<ConnectionReply>,
}

impl ConnectionReplySender {
    fn respond(&self, output: Output) {
        let _ = self.sender.send(ConnectionReply {
            exchange: self.exchange,
            output,
        });
    }
}

struct ConnectionReply {
    exchange: ExchangeIdentifier,
    output: Output,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ConnectionIdentifier {
    value: u64,
}

impl ConnectionIdentifier {
    fn new(value: u64) -> Self {
        Self { value }
    }

    fn in_process() -> Self {
        Self::new(0)
    }
}

struct MaintenanceLeaseState {
    epoch: MaintenanceLeaseEpoch,
    holder: Option<ConnectionIdentifier>,
    waiters: VecDeque<MaintenanceLeaseWaiter>,
}

impl MaintenanceLeaseState {
    fn for_current_daemon() -> Self {
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        Self {
            epoch: MaintenanceLeaseEpoch::new(DaemonEpoch::new(epoch)),
            holder: None,
            waiters: VecDeque::new(),
        }
    }

    fn gates_new_starts(&self) -> bool {
        self.holder.is_some() || !self.waiters.is_empty()
    }

    fn active_reply(&self) -> Output {
        Output::maintenance_lease_active(self.epoch.clone())
    }

    fn acquire(&mut self, connection: ConnectionIdentifier, reply: ListenerReplySink) {
        if self.holder == Some(connection) {
            reply.respond(Output::maintenance_lease_granted(self.epoch.clone()));
        } else if self
            .waiters
            .iter()
            .any(|waiter| waiter.connection == connection)
        {
            reply.respond(self.active_reply());
        } else {
            self.waiters
                .push_back(MaintenanceLeaseWaiter { connection, reply });
        }
    }

    fn release(&mut self, connection: ConnectionIdentifier, reply: ListenerReplySink) {
        if self.holder == Some(connection) {
            self.holder = None;
            reply.respond(Output::maintenance_lease_released(
                MaintenanceLeaseRelease {},
            ));
        } else if let Some(position) = self
            .waiters
            .iter()
            .position(|waiter| waiter.connection == connection)
        {
            let waiter = self
                .waiters
                .remove(position)
                .expect("waiter position exists");
            waiter.reply.respond(Output::maintenance_lease_cancelled(
                MaintenanceLeaseCancellation {},
            ));
            reply.respond(Output::maintenance_lease_released(
                MaintenanceLeaseRelease {},
            ));
        } else {
            reply.respond(Output::maintenance_lease_not_held(
                MaintenanceLeaseAbsent {},
            ));
        }
    }

    fn disconnect(&mut self, connection: ConnectionIdentifier) {
        if self.holder == Some(connection) {
            self.holder = None;
        }
        self.waiters
            .retain(|waiter| waiter.connection != connection);
    }

    fn grant_if_idle(&mut self, idle: bool) {
        if idle && self.holder.is_none() {
            if let Some(waiter) = self.waiters.pop_front() {
                self.holder = Some(waiter.connection);
                waiter
                    .reply
                    .respond(Output::maintenance_lease_granted(self.epoch.clone()));
            }
        }
    }
}

struct MaintenanceLeaseWaiter {
    connection: ConnectionIdentifier,
    reply: ListenerReplySink,
}

impl From<&RuntimeCaptureStartWork> for RuntimeCaptureStartWork {
    fn from(start: &RuntimeCaptureStartWork) -> Self {
        start.clone_for_worker()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DaemonSocketBinding {
    path: PathBuf,
    mode: u32,
}

impl DaemonSocketBinding {
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

    pub fn mode(&self) -> u32 {
        self.mode
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn remove_stale_socket_if_needed(&self) -> Result<()> {
        match UnixStream::connect(&self.path) {
            Ok(_) => Err(Error::DaemonAlreadyRunning {
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

#[cfg(test)]
mod tests {
    use super::*;

    struct LeaseFixture;

    impl LeaseFixture {
        fn receiver() -> (ListenerReplySink, mpsc::Receiver<Output>) {
            let (sender, receiver) = mpsc::sync_channel(1);
            (ListenerReplySink::direct(sender), receiver)
        }
    }

    #[test]
    fn queued_maintenance_lease_gates_starts_before_idle() {
        let mut leases = MaintenanceLeaseState::for_current_daemon();
        let (reply, _receiver) = LeaseFixture::receiver();
        leases.acquire(ConnectionIdentifier::new(1), reply);
        assert!(leases.gates_new_starts());
        leases.grant_if_idle(false);
        assert!(leases.holder.is_none());
    }

    #[test]
    fn maintenance_lease_disconnect_preserves_fifo_grant() {
        let mut leases = MaintenanceLeaseState::for_current_daemon();
        let (first_reply, first_receiver) = LeaseFixture::receiver();
        let (second_reply, second_receiver) = LeaseFixture::receiver();
        leases.acquire(ConnectionIdentifier::new(1), first_reply);
        leases.acquire(ConnectionIdentifier::new(2), second_reply);
        leases.disconnect(ConnectionIdentifier::new(1));
        leases.grant_if_idle(true);
        assert!(first_receiver.try_recv().is_err());
        match second_receiver.recv().expect("second grant") {
            Output::MaintenanceLeaseGranted(grant) => assert_eq!(grant.payload(), &leases.epoch),
            other => panic!("expected second grant, got {other:?}"),
        }
    }
}
