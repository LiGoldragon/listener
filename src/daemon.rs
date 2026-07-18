use std::{
    fs,
    io::ErrorKind,
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::ExitCode,
    sync::mpsc,
    thread,
};

use signal_listener::{
    ActiveCapture, ActiveCaptureSession, CancellationRequestedSession, CaptureAlreadyActive,
    CaptureCancellationRequested, CaptureSession, CaptureStatus, Input, NoActiveCapture, Output,
    RequestedCaptureSession, StatusRequest,
};

use crate::runtime::{
    CaptureCancellationSignal, RuntimeCaptureCancellationWork, RuntimeCaptureFinalizationWork,
    RuntimeCaptureStartWork,
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
        let latency_instrumentation = LatencyInstrumentation::from_environment();
        let (status_server, status_publisher) = StatusStreamServer::from_configuration_with_latency(
            &configuration,
            latency_instrumentation.clone(),
        );
        let _status_thread = status_server.spawn()?;
        let mut runtime = ListenerRuntime::from_configuration_with_status_and_latency(
            configuration.clone(),
            status_publisher,
            latency_instrumentation.clone(),
        )?;
        let maintenance = CaptureMaintenance::from_configuration(&configuration)?;
        runtime.advance_session_sequence(maintenance.snapshot().next_session_value());
        let _maintenance_thread = maintenance.spawn();
        ListenerSocketServer::new_with_latency(configuration, runtime, latency_instrumentation)
            .serve()
    }

    pub fn run_to_exit_code() -> ExitCode {
        let daemon = Self::from_environment();
        match daemon.run() {
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
    latency_instrumentation: LatencyInstrumentation,
}

impl ListenerSocketServer {
    pub fn new(configuration: Configuration, runtime: ListenerRuntime) -> Self {
        Self::new_with_latency(configuration, runtime, LatencyInstrumentation::disabled())
    }

    pub fn new_with_latency(
        configuration: Configuration,
        runtime: ListenerRuntime,
        latency_instrumentation: LatencyInstrumentation,
    ) -> Self {
        Self {
            configuration,
            mailbox: ListenerOperationActor::spawn(runtime, latency_instrumentation.clone()),
            latency_instrumentation,
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
            let stream = stream?;
            let connection =
                ListenerConnection::new(self.mailbox.clone(), self.latency_instrumentation.clone());
            thread::spawn(move || {
                let _ = connection.handle(stream);
            });
        }

        Ok(())
    }

    pub fn handle_connection(&self, stream: UnixStream) -> Result<()> {
        ListenerConnection::new(self.mailbox.clone(), self.latency_instrumentation.clone())
            .handle(stream)
    }

    pub fn handle_input(&self, input: Input) -> Result<Output> {
        self.mailbox.request(input)
    }
}

struct ListenerConnection {
    mailbox: ListenerOperationMailbox,
    latency_instrumentation: LatencyInstrumentation,
}

impl ListenerConnection {
    fn new(
        mailbox: ListenerOperationMailbox,
        latency_instrumentation: LatencyInstrumentation,
    ) -> Self {
        Self {
            mailbox,
            latency_instrumentation,
        }
    }

    fn handle(&self, stream: UnixStream) -> Result<()> {
        let mut stream = ContractFrameStream::new(stream, ContractFrameCodec::listener_default());
        let request = stream.receive_request()?;
        self.latency_instrumentation.record_request_received();
        let output = self.mailbox.request(request.input().clone())?;
        stream.send_reply(request, output)
    }
}

#[derive(Clone)]
struct ListenerOperationMailbox {
    sender: mpsc::Sender<ListenerOperationMail>,
}

impl ListenerOperationMailbox {
    fn request(&self, input: Input) -> Result<Output> {
        let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
        self.sender
            .send(ListenerOperationMail::Request(
                ListenerOperationRequest::new(input, reply_sender),
            ))
            .map_err(|_| Error::NotImplemented {
                surface: "listener operation actor",
            })?;
        reply_receiver.recv().map_err(|_| Error::NotImplemented {
            surface: "listener operation actor",
        })
    }
}

struct ListenerOperationActor {
    runtime: ListenerRuntime,
    receiver: mpsc::Receiver<ListenerOperationMail>,
    mailbox: ListenerOperationMailbox,
    operation: ListenerOperationState,
}

impl ListenerOperationActor {
    fn spawn(
        runtime: ListenerRuntime,
        _latency_instrumentation: LatencyInstrumentation,
    ) -> ListenerOperationMailbox {
        let (sender, receiver) = mpsc::channel();
        let mailbox = ListenerOperationMailbox { sender };
        let actor = Self {
            runtime,
            receiver,
            mailbox: mailbox.clone(),
            operation: ListenerOperationState::Idle,
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
            ListenerOperationMail::StartCompleted(completion) => self.finish_start(completion),
            ListenerOperationMail::CancellationCompleted(output) => {
                self.finish_cancellation(output)
            }
            ListenerOperationMail::FinalizationCompleted(output) => {
                self.finish_finalization(output)
            }
        }
    }

    fn handle_request(&mut self, request: ListenerOperationRequest) {
        match request.input {
            Input::Start(_) => self.begin_start(request.reply_sender),
            Input::Toggle(_) => self.toggle(request.reply_sender),
            Input::Cancel(cancel) => self.cancel(cancel.into_payload(), request.reply_sender),
            Input::Stop(stop) => self.stop(stop.into_payload(), request.reply_sender),
            Input::Status(_) => {
                let output = self.status();
                self.reply(request.reply_sender, output);
            }
            input => {
                let output = self.runtime.handle_input(input);
                self.reply(request.reply_sender, output);
            }
        }
    }

    fn begin_start(&mut self, reply_sender: mpsc::SyncSender<Output>) {
        if let Some((session, _)) = self.operation.active_capture() {
            self.reply(
                reply_sender,
                Output::AlreadyActive(CaptureAlreadyActive::new(ActiveCaptureSession::new(
                    session.clone(),
                ))),
            );
            return;
        }
        match self.runtime.begin_capture_start() {
            Ok(start) => {
                let cancellation = CaptureCancellationSignal::new();
                self.operation = ListenerOperationState::Starting {
                    start,
                    reply_sender,
                    cancellation,
                };
                self.spawn_start();
            }
            Err(error) => self.reply(reply_sender, error.into_start_reply()),
        }
    }

    fn toggle(&mut self, reply_sender: mpsc::SyncSender<Output>) {
        match &self.operation {
            ListenerOperationState::Idle => self.begin_start(reply_sender),
            ListenerOperationState::Capturing { session, .. } => {
                self.stop(session.clone(), reply_sender)
            }
            _ => self.request_cancellation(reply_sender),
        }
    }

    fn cancel(&mut self, session: CaptureSession, reply_sender: mpsc::SyncSender<Output>) {
        let Some((active_session, _)) = self.operation.active_capture() else {
            self.reply(reply_sender, Output::NoActive(NoActiveCapture {}));
            return;
        };
        if active_session != &session {
            self.reply(
                reply_sender,
                Output::SessionMismatch(signal_listener::CaptureSessionMismatch {
                    active_capture_session: ActiveCaptureSession::new(active_session.clone()),
                    requested_capture_session: RequestedCaptureSession::new(session),
                }),
            );
            return;
        }
        self.request_cancellation(reply_sender);
    }

    fn stop(&mut self, session: CaptureSession, reply_sender: mpsc::SyncSender<Output>) {
        let operation = std::mem::replace(&mut self.operation, ListenerOperationState::Idle);
        match operation {
            ListenerOperationState::Capturing {
                session: active,
                artifact,
            } if active == session => {
                match self.runtime.begin_capture_finalization(active.clone()) {
                    Ok(work) => {
                        let cancellation = CaptureCancellationSignal::new();
                        self.operation = ListenerOperationState::Finalizing {
                            session: active,
                            artifact,
                            reply_sender,
                            cancellation: cancellation.clone(),
                        };
                        self.spawn_finalization(work, cancellation);
                    }
                    Err(error) => self.reply(reply_sender, error.into_stop_reply()),
                }
            }
            ListenerOperationState::Capturing {
                session: active,
                artifact,
            } => {
                self.operation = ListenerOperationState::Capturing {
                    session: active.clone(),
                    artifact,
                };
                self.reply(
                    reply_sender,
                    Output::SessionMismatch(signal_listener::CaptureSessionMismatch {
                        active_capture_session: ActiveCaptureSession::new(active),
                        requested_capture_session: RequestedCaptureSession::new(session),
                    }),
                );
            }
            other => {
                self.operation = other;
                self.reply(reply_sender, Output::NoActive(NoActiveCapture {}));
            }
        }
    }

    fn request_cancellation(&mut self, reply_sender: mpsc::SyncSender<Output>) {
        let operation = std::mem::replace(&mut self.operation, ListenerOperationState::Idle);
        match operation {
            ListenerOperationState::Starting {
                start,
                reply_sender: start_reply,
                cancellation,
            } => {
                cancellation.request();
                self.runtime.publish_cancelling();
                let output = ListenerOperationState::cancellation_requested(
                    start.session().clone(),
                    start.artifact().clone(),
                );
                self.operation = ListenerOperationState::Starting {
                    start,
                    reply_sender: start_reply,
                    cancellation,
                };
                self.reply(reply_sender, output);
            }
            ListenerOperationState::Capturing { session, artifact } => {
                self.runtime.publish_cancelling();
                match self.runtime.begin_capture_cancellation(session.clone()) {
                    Ok(work) => {
                        let output = work.requested_reply();
                        self.operation = ListenerOperationState::Cancelling { session, artifact };
                        self.spawn_cancellation(work);
                        self.reply(reply_sender, output);
                    }
                    Err(error) => self.reply(reply_sender, error.into_cancel_reply()),
                }
            }
            ListenerOperationState::Finalizing {
                session,
                artifact,
                reply_sender: stop_reply,
                cancellation,
            } => {
                cancellation.request();
                self.runtime.publish_cancelling();
                let output = ListenerOperationState::cancellation_requested(
                    session.clone(),
                    artifact.clone(),
                );
                self.operation = ListenerOperationState::Finalizing {
                    session,
                    artifact,
                    reply_sender: stop_reply,
                    cancellation,
                };
                self.reply(reply_sender, output);
            }
            ListenerOperationState::Cancelling { session, artifact } => {
                let output = ListenerOperationState::cancellation_requested(
                    session.clone(),
                    artifact.clone(),
                );
                self.operation = ListenerOperationState::Cancelling { session, artifact };
                self.reply(reply_sender, output);
            }
            ListenerOperationState::Idle => {
                self.reply(reply_sender, Output::NoActive(NoActiveCapture {}));
            }
        }
    }

    fn finish_start(&mut self, completion: ListenerStartCompletion) {
        let operation = std::mem::replace(&mut self.operation, ListenerOperationState::Idle);
        let ListenerOperationState::Starting {
            start,
            reply_sender,
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
                self.reply(reply_sender, output);
                if cancellation.is_requested() {
                    self.runtime.publish_cancelling();
                    match self.runtime.begin_capture_cancellation(session.clone()) {
                        Ok(work) => {
                            self.operation =
                                ListenerOperationState::Cancelling { session, artifact };
                            self.spawn_cancellation(work);
                        }
                        Err(error) => {
                            self.runtime.publish_cancelling();
                            self.operation = ListenerOperationState::Idle;
                            let _ = error;
                        }
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
                            reply_sender,
                            cancellation,
                        };
                        self.spawn_start();
                    }
                    Err(error) => self.reply(reply_sender, error.into_start_reply()),
                }
            }
            Err(error) => self.reply(reply_sender, error.into_start_reply()),
        }
    }

    fn finish_cancellation(&mut self, _output: Output) {
        if matches!(self.operation, ListenerOperationState::Cancelling { .. }) {
            self.operation = ListenerOperationState::Idle;
        }
    }

    fn finish_finalization(&mut self, output: Output) {
        let operation = std::mem::replace(&mut self.operation, ListenerOperationState::Idle);
        if let ListenerOperationState::Finalizing { reply_sender, .. } = operation {
            self.reply(reply_sender, output);
        }
    }

    fn spawn_start(&self) {
        let ListenerOperationState::Starting { start, .. } = &self.operation else {
            return;
        };
        let start = RuntimeCaptureStartWork::from(start);
        let sender = self.mailbox.sender.clone();
        thread::spawn(move || {
            let result = start.start();
            let _ = sender.send(ListenerOperationMail::StartCompleted(
                ListenerStartCompletion { result },
            ));
        });
    }

    fn spawn_cancellation(&self, work: RuntimeCaptureCancellationWork) {
        let sender = self.mailbox.sender.clone();
        thread::spawn(move || {
            let output = work.execute();
            let _ = sender.send(ListenerOperationMail::CancellationCompleted(output));
        });
    }

    fn spawn_finalization(
        &self,
        work: RuntimeCaptureFinalizationWork,
        cancellation: CaptureCancellationSignal,
    ) {
        let sender = self.mailbox.sender.clone();
        thread::spawn(move || {
            let output = work.execute(cancellation);
            let _ = sender.send(ListenerOperationMail::FinalizationCompleted(output));
        });
    }

    fn status(&mut self) -> Output {
        match self.operation.active_capture() {
            Some((session, artifact)) => {
                Output::status_reported(CaptureStatus::Capturing(ActiveCapture {
                    active_capture_session: ActiveCaptureSession::new(session.clone()),
                    durable_audio_artifact: artifact.clone(),
                }))
            }
            None => self
                .runtime
                .status(StatusRequest {})
                .unwrap_or_else(|error| {
                    error.into_unimplemented_reply(signal_listener::OperationKind::Status)
                }),
        }
    }

    fn reply(&self, reply_sender: mpsc::SyncSender<Output>, output: Output) {
        let _ = reply_sender.send(output);
    }
}

struct ListenerOperationRequest {
    input: Input,
    reply_sender: mpsc::SyncSender<Output>,
}

impl ListenerOperationRequest {
    fn new(input: Input, reply_sender: mpsc::SyncSender<Output>) -> Self {
        Self {
            input,
            reply_sender,
        }
    }
}

enum ListenerOperationMail {
    Request(ListenerOperationRequest),
    StartCompleted(ListenerStartCompletion),
    CancellationCompleted(Output),
    FinalizationCompleted(Output),
}

struct ListenerStartCompletion {
    result: Result<Box<dyn crate::ActiveAudioCapture>>,
}

enum ListenerOperationState {
    Idle,
    Starting {
        start: RuntimeCaptureStartWork,
        reply_sender: mpsc::SyncSender<Output>,
        cancellation: CaptureCancellationSignal,
    },
    Capturing {
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    },
    Finalizing {
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
        reply_sender: mpsc::SyncSender<Output>,
        cancellation: CaptureCancellationSignal,
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

    fn cancellation_requested(
        session: CaptureSession,
        artifact: signal_listener::DurableAudioArtifact,
    ) -> Output {
        Output::CancellationRequested(CaptureCancellationRequested {
            cancellation_requested_session: CancellationRequestedSession::new(session),
            durable_audio_artifact: artifact,
        })
    }
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
