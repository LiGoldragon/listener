//! Privileged Listener meta-socket service and its thin textual client.
//!
//! Text exists only at the CLI boundary. The socket carries one generated,
//! length-prefixed `meta-signal-listener` frame per connection.

use std::{
    fs,
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::{UnixListener, UnixStream}},
    path::PathBuf,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    thread::{self, JoinHandle},
    time::Duration,
};

use meta_signal_listener::{Frame, FrameBody, Input, Output};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
};

use crate::{Configuration, Error, MetaProviderPolicyService, Result, daemon::DaemonSocketBinding};

const MAXIMUM_META_FRAME_BYTES: usize = 16 * 1024 * 1024;

pub struct MetaProviderPolicyClient {
    socket_path: PathBuf,
}

trait ExchangesMetaProviderPolicy {
    fn request(&self, input: Input) -> Result<Output>;
}

impl MetaProviderPolicyClient {
    // Exception: Too trivial. Construction fixes the owner socket endpoint.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }
}

impl ExchangesMetaProviderPolicy for MetaProviderPolicyClient {
    fn request(&self, input: Input) -> Result<Output> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        );
        MetaSignalFrameCodec::write(&mut stream, &input.into_frame(exchange))?;
        let frame = MetaSignalFrameCodec::read(&mut stream)?;
        let FrameBody::Reply {
            exchange: actual,
            reply,
        } = frame.into_body()
        else {
            return Err(Error::UnexpectedContractFrame {
                expected: "meta reply",
                got: "meta request".to_owned(),
            });
        };
        if actual != exchange {
            return Err(Error::ReplyExchangeMismatch {
                expected: exchange,
                actual,
            });
        }
        let Reply::Accepted { per_operation, .. } = reply else {
            return Err(Error::UnexpectedContractFrame {
                expected: "accepted meta reply",
                got: "rejected meta reply".to_owned(),
            });
        };
        let (reply, remaining) = per_operation.into_head_and_tail();
        if !remaining.is_empty() {
            return Err(Error::UnsupportedContractReplyBatch {
                count: remaining.len() + 1,
            });
        }
        match reply {
            SubReply::Ok(output)
            | SubReply::Failed {
                detail: Some(output),
                ..
            } => Ok(output),
            _ => Err(Error::UnexpectedContractFrame {
                expected: "meta reply payload",
                got: "empty meta reply payload".to_owned(),
            }),
        }
    }
}

impl MetaProviderPolicyClient {
    // Exception: Too trivial. This is the public client entrypoint.
    pub fn request(&self, input: Input) -> Result<Output> {
        ExchangesMetaProviderPolicy::request(self, input)
    }
}

pub struct MetaProviderPolicySocket {
    service: Arc<MetaProviderPolicyService>,
}

/// Bound privileged meta service. Its accept loop is owned by the Listener
/// daemon supervisor; it is never a fire-and-forget worker.
pub struct MetaProviderPolicyServer {
    listener: UnixListener,
    socket: MetaProviderPolicySocket,
}

impl MetaProviderPolicyServer {
    pub fn bind(
        path: impl Into<PathBuf>,
        mode: u32,
        service: Arc<MetaProviderPolicyService>,
    ) -> Result<Self> {
        let binding = DaemonSocketBinding::new(path, mode);
        binding.prepare()?;
        let listener = UnixListener::bind(binding.path())?;
        fs::set_permissions(binding.path(), fs::Permissions::from_mode(binding.mode()))?;
        listener.set_nonblocking(true)?;
        Ok(Self { listener, socket: MetaProviderPolicySocket::new(service) })
    }

    pub fn serve_until(self, stopping: Arc<AtomicBool>) -> Result<()> {
        while !stopping.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _)) => { let _ = self.socket.handle_connection(stream); }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(10)),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    pub fn spawn_until(self, stopping: Arc<AtomicBool>) -> JoinHandle<Result<()>> {
        thread::spawn(move || self.serve_until(stopping))
    }
}

trait ServesMetaProviderPolicySocket {
    fn handle_connection(&self, stream: UnixStream) -> Result<()>;
}

impl MetaProviderPolicySocket {
    // Exception: Too trivial. Construction binds one socket handler to its policy owner.
    pub fn new(service: Arc<MetaProviderPolicyService>) -> Self {
        Self { service }
    }
}

impl ServesMetaProviderPolicySocket for MetaProviderPolicySocket {
    fn handle_connection(&self, mut stream: UnixStream) -> Result<()> {
        let frame = MetaSignalFrameCodec::read(&mut stream)?;
        let FrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(Error::UnexpectedContractFrame {
                expected: "meta request",
                got: "meta reply".to_owned(),
            });
        };
        let (input, remaining) = request.payloads.into_head_and_tail();
        if !remaining.is_empty() {
            return Err(Error::UnsupportedContractBatch {
                count: remaining.len() + 1,
            });
        }
        let output = self.service.handle(input);
        MetaSignalFrameCodec::write(&mut stream, &output.into_reply_frame(exchange))
    }
}

impl MetaProviderPolicySocket {
    // Exception: Too trivial. This is the public socket-service entrypoint.
    pub fn handle_connection(&self, stream: UnixStream) -> Result<()> {
        ServesMetaProviderPolicySocket::handle_connection(self, stream)
    }
}

struct MetaSignalFrameCodec;

trait CodesMetaSignalFrames {
    fn read(reader: &mut impl Read) -> Result<Frame>;
    fn write(writer: &mut impl Write, frame: &Frame) -> Result<()>;
}

impl CodesMetaSignalFrames for MetaSignalFrameCodec {
    fn read(reader: &mut impl Read) -> Result<Frame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > MAXIMUM_META_FRAME_BYTES {
            return Err(Error::InvalidCommand {
                message: format!(
                    "meta contract frame is {length} bytes; maximum is {MAXIMUM_META_FRAME_BYTES}"
                ),
            });
        }
        let mut bytes = prefix.to_vec();
        bytes.resize(prefix.len() + length, 0);
        reader.read_exact(&mut bytes[prefix.len()..])?;
        Ok(Frame::decode_length_prefixed(&bytes)?)
    }

    fn write(writer: &mut impl Write, frame: &Frame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        if bytes.len().saturating_sub(4) > MAXIMUM_META_FRAME_BYTES {
            return Err(Error::InvalidCommand {
                message: format!(
                    "meta contract frame is too large; maximum is {MAXIMUM_META_FRAME_BYTES}"
                ),
            });
        }
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }
}

impl MetaSignalFrameCodec {
    // Exception: Too trivial. These are the private codec calls.
    fn read(reader: &mut impl Read) -> Result<Frame> {
        <Self as CodesMetaSignalFrames>::read(reader)
    }

    fn write(writer: &mut impl Write, frame: &Frame) -> Result<()> {
        <Self as CodesMetaSignalFrames>::write(writer, frame)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaCommandLine {
    arguments: Vec<String>,
}

impl MetaCommandLine {
    // Exception: Too trivial. This captures the process boundary once.
    pub fn from_environment() -> Self {
        Self {
            arguments: std::env::args().collect(),
        }
    }

    // Exception: Too trivial. Test construction supplies only process arguments.
    pub fn from_arguments(arguments: Vec<String>) -> Self {
        Self { arguments }
    }

    // Exception: Too trivial. This exposes the captured CLI boundary to tests.
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn run(&self, mut output: impl Write) -> Result<()> {
        let [_, source] = self.arguments.as_slice() else {
            return Err(Error::InvalidCommand {
                message: "meta-listener accepts exactly one typed input argument".to_owned(),
            });
        };
        let input = source.parse::<Input>().map_err(|error| Error::InvalidCommand {
            message: format!("invalid meta-listener input: {error}"),
        })?;
        let configuration = Configuration::from_environment();
        let output_value =
            MetaProviderPolicyClient::new(configuration.meta_socket_path()).request(input)?;
        writeln!(output, "{output_value}")?;
        Ok(())
    }
}
