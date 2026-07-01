use std::{os::unix::net::UnixStream, path::PathBuf};

use signal_listener::{Input, Output};

use crate::{Configuration, ContractFrameCodec, ContractFrameStream, Result};

pub struct ListenerClient {
    socket_path: PathBuf,
    codec: ContractFrameCodec,
}

impl ListenerClient {
    pub fn from_environment() -> Self {
        Self::new(Configuration::from_environment().working_socket_path())
    }

    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            codec: ContractFrameCodec::listener_default(),
        }
    }

    pub fn socket_path(&self) -> &PathBuf {
        &self.socket_path
    }

    pub fn call(&self, input: Input) -> Result<Output> {
        let stream = UnixStream::connect(&self.socket_path)?;
        let mut stream = ContractFrameStream::new(stream, self.codec);
        stream.send_input(input)?;
        stream.receive_output()
    }
}
