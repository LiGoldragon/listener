use std::{
    fs,
    io::ErrorKind,
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::ExitCode,
};

use crate::{
    Configuration, ContractFrameCodec, ContractFrameStream, Error, ListenerRuntime, Result,
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
        let runtime = ListenerRuntime::from_configuration(configuration.clone());
        ListenerSocketServer::new(configuration, runtime).serve()
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
    runtime: ListenerRuntime,
    codec: ContractFrameCodec,
}

impl ListenerSocketServer {
    pub fn new(configuration: Configuration, runtime: ListenerRuntime) -> Self {
        Self {
            configuration,
            runtime,
            codec: ContractFrameCodec::listener_default(),
        }
    }

    pub fn serve(&mut self) -> Result<()> {
        let binding = DaemonSocketBinding::new(
            self.configuration.working_socket_path(),
            self.configuration.working_socket_mode(),
        );
        binding.prepare()?;
        let listener = UnixListener::bind(binding.path())?;
        fs::set_permissions(binding.path(), fs::Permissions::from_mode(binding.mode()))?;

        for stream in listener.incoming() {
            self.handle_connection(stream?)?;
        }

        Ok(())
    }

    pub fn handle_connection(&mut self, stream: UnixStream) -> Result<()> {
        let mut stream = ContractFrameStream::new(stream, self.codec);
        let input = stream.receive_input()?;
        let output = self.runtime.handle_input(input);
        stream.send_output(&output)
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
