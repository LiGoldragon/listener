use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
};

use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use signal_listener::{Frame, FrameBody, Input, Output};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaximumFrameLength {
    bytes: usize,
}

impl MaximumFrameLength {
    pub fn new(bytes: usize) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContractFrameCodec {
    maximum_frame_length: MaximumFrameLength,
}

impl ContractFrameCodec {
    pub fn new(maximum_frame_length: MaximumFrameLength) -> Self {
        Self {
            maximum_frame_length,
        }
    }

    pub fn listener_default() -> Self {
        Self::new(MaximumFrameLength::new(16 * 1024 * 1024))
    }

    pub fn read_frame(&self, reader: &mut impl Read) -> Result<Frame> {
        let bytes = self.read_length_prefixed_frame_bytes(reader)?;
        Ok(Frame::decode_length_prefixed(&bytes)?)
    }

    pub fn write_frame(&self, writer: &mut impl Write, frame: &Frame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        self.require_payload_length(bytes.len().saturating_sub(4))?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }

    fn read_length_prefixed_frame_bytes(&self, reader: &mut impl Read) -> Result<Vec<u8>> {
        let mut length_prefix = [0_u8; 4];
        reader.read_exact(&mut length_prefix)?;
        let frame_length = u32::from_be_bytes(length_prefix) as usize;
        self.require_payload_length(frame_length)?;

        let mut frame_bytes = Vec::with_capacity(4 + frame_length);
        frame_bytes.extend_from_slice(&length_prefix);
        frame_bytes.resize(4 + frame_length, 0);
        reader.read_exact(&mut frame_bytes[4..])?;
        Ok(frame_bytes)
    }

    fn require_payload_length(&self, frame_length: usize) -> Result<()> {
        if frame_length > self.maximum_frame_length.bytes() {
            return Err(Error::InvalidCommand {
                message: format!(
                    "contract frame is {frame_length} bytes; maximum is {}",
                    self.maximum_frame_length.bytes()
                ),
            });
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenerContractRequest {
    exchange: ExchangeIdentifier,
    input: Input,
}

impl ListenerContractRequest {
    pub fn from_frame(frame: Frame) -> Result<Self> {
        match frame.into_body() {
            FrameBody::Request { exchange, request } => {
                let (input, additional_inputs) = request.payloads.into_head_and_tail();
                if !additional_inputs.is_empty() {
                    return Err(Error::UnsupportedContractBatch {
                        count: 1 + additional_inputs.len(),
                    });
                }

                Ok(Self { exchange, input })
            }
            other => Err(Error::UnexpectedContractFrame {
                expected: "request",
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn exchange(&self) -> ExchangeIdentifier {
        self.exchange
    }

    pub fn input(&self) -> &Input {
        &self.input
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListenerContractReply {
    exchange: ExchangeIdentifier,
    output: Output,
}

impl ListenerContractReply {
    pub fn from_frame(frame: Frame) -> Result<Self> {
        match frame.into_body() {
            FrameBody::Reply { exchange, reply } => Self::from_reply(exchange, reply),
            other => Err(Error::UnexpectedContractFrame {
                expected: "reply",
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn exchange(&self) -> ExchangeIdentifier {
        self.exchange
    }

    pub fn into_output(self) -> Output {
        self.output
    }

    fn from_reply(exchange: ExchangeIdentifier, reply: Reply<Output>) -> Result<Self> {
        match reply {
            Reply::Accepted { per_operation, .. } => {
                let (reply, additional_replies) = per_operation.into_head_and_tail();
                if !additional_replies.is_empty() {
                    return Err(Error::UnsupportedContractReplyBatch {
                        count: 1 + additional_replies.len(),
                    });
                }

                Self::from_sub_reply(exchange, reply)
            }
            Reply::Rejected { reason } => Err(Error::UnexpectedContractFrame {
                expected: "accepted reply",
                got: format!("rejected reply: {reason:?}"),
            }),
        }
    }

    fn from_sub_reply(exchange: ExchangeIdentifier, reply: SubReply<Output>) -> Result<Self> {
        match reply {
            SubReply::Ok(output) => Ok(Self { exchange, output }),
            SubReply::Failed {
                detail: Some(output),
                ..
            } => Ok(Self { exchange, output }),
            other => Err(Error::UnexpectedContractFrame {
                expected: "reply payload",
                got: format!("{other:?}"),
            }),
        }
    }
}

pub struct ContractFrameStream {
    stream: UnixStream,
    codec: ContractFrameCodec,
    session_epoch: SessionEpoch,
    next_sequence: LaneSequence,
    pending_exchange: Option<ExchangeIdentifier>,
}

impl ContractFrameStream {
    pub fn new(stream: UnixStream, codec: ContractFrameCodec) -> Self {
        Self {
            stream,
            codec,
            session_epoch: SessionEpoch::new(0),
            next_sequence: LaneSequence::first(),
            pending_exchange: None,
        }
    }

    pub fn send_input(&mut self, input: Input) -> Result<()> {
        if self.pending_exchange.is_some() {
            return Err(Error::UnexpectedContractFrame {
                expected: "no pending request",
                got: "pending request".to_owned(),
            });
        }

        let exchange = self.next_exchange();
        let frame = input.into_frame(exchange);
        self.send_frame(&frame)?;
        self.pending_exchange = Some(exchange);
        Ok(())
    }

    pub fn receive_output(&mut self) -> Result<Output> {
        let reply = ListenerContractReply::from_frame(self.receive_frame()?)?;
        self.require_pending_exchange(reply.exchange())?;
        Ok(reply.into_output())
    }

    pub fn receive_request(&mut self) -> Result<ListenerContractRequest> {
        ListenerContractRequest::from_frame(self.receive_frame()?)
    }

    pub fn send_reply(&mut self, request: ListenerContractRequest, output: Output) -> Result<()> {
        let frame = output.into_reply_frame(request.exchange());
        self.send_frame(&frame)
    }

    pub fn receive_frame(&mut self) -> Result<Frame> {
        self.codec.read_frame(&mut self.stream)
    }

    pub fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        self.codec.write_frame(&mut self.stream, frame)
    }

    fn next_exchange(&mut self) -> ExchangeIdentifier {
        let exchange = ExchangeIdentifier::new(
            self.session_epoch,
            ExchangeLane::Connector,
            self.next_sequence,
        );
        self.next_sequence = self.next_sequence.next();
        exchange
    }

    fn require_pending_exchange(&mut self, actual: ExchangeIdentifier) -> Result<()> {
        let Some(expected) = self.pending_exchange.take() else {
            return Err(Error::UnexpectedContractFrame {
                expected: "reply matching a pending request",
                got: "reply without pending request".to_owned(),
            });
        };

        if expected != actual {
            return Err(Error::ReplyExchangeMismatch { expected, actual });
        }

        Ok(())
    }
}
