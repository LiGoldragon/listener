use std::{os::unix::net::UnixStream, path::PathBuf};

use signal_listener::{
    AcquireMaintenanceLease, Input, MaintenanceLeaseEpoch, Output, ReleaseMaintenanceLease,
};

use crate::{Configuration, ContractFrameCodec, ContractFrameStream, Error, Result};

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

pub struct ListenerMaintenanceClient {
    stream: ContractFrameStream,
}

impl ListenerMaintenanceClient {
    pub fn connect(socket_path: impl Into<PathBuf>) -> Result<Self> {
        let stream = UnixStream::connect(socket_path.into())?;
        Ok(Self::from_stream(stream))
    }

    pub fn from_stream(stream: UnixStream) -> Self {
        Self {
            stream: ContractFrameStream::new(stream, ContractFrameCodec::listener_default()),
        }
    }

    pub fn acquire(&mut self) -> Result<MaintenanceLeaseEpoch> {
        self.stream
            .send_input(Input::AcquireMaintenance(AcquireMaintenanceLease {}))?;
        match self.stream.receive_output()? {
            Output::MaintenanceLeaseGranted(grant) => Ok(grant.payload().clone()),
            reply => Err(Error::UnexpectedMaintenanceLeaseReply { reply }),
        }
    }

    pub fn release(&mut self) -> Result<()> {
        self.stream
            .send_input(Input::ReleaseMaintenance(ReleaseMaintenanceLease {}))?;
        match self.stream.receive_output()? {
            Output::MaintenanceLeaseReleased(_) => Ok(()),
            reply => Err(Error::UnexpectedMaintenanceLeaseReply { reply }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixStream;

    use super::*;

    #[test]
    fn lost_daemon_connection_fails_maintenance_acquire() {
        let (client_stream, daemon_stream) = UnixStream::pair().expect("socket pair");
        drop(daemon_stream);
        let mut client = ListenerMaintenanceClient::from_stream(client_stream);

        assert!(matches!(client.acquire(), Err(Error::Io(_))));
    }
}
