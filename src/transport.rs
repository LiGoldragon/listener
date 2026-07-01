use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
};

use signal_listener::{Input, Output};

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

    pub fn read_input(&self, reader: &mut impl Read) -> Result<Input> {
        let bytes = self.read_frame(reader)?;
        let (_route, input) = Input::decode_signal_frame(&bytes)?;
        Ok(input)
    }

    pub fn write_input(&self, writer: &mut impl Write, input: &Input) -> Result<()> {
        self.write_frame(writer, input.encode_signal_frame()?)
    }

    pub fn read_output(&self, reader: &mut impl Read) -> Result<Output> {
        let bytes = self.read_frame(reader)?;
        let (_route, output) = Output::decode_signal_frame(&bytes)?;
        Ok(output)
    }

    pub fn write_output(&self, writer: &mut impl Write, output: &Output) -> Result<()> {
        self.write_frame(writer, output.encode_signal_frame()?)
    }

    fn read_frame(&self, reader: &mut impl Read) -> Result<Vec<u8>> {
        let mut length_bytes = [0_u8; 4];
        reader.read_exact(&mut length_bytes)?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        if length > self.maximum_frame_length.bytes() {
            return Err(Error::InvalidCommand {
                message: format!(
                    "contract frame is {length} bytes; maximum is {}",
                    self.maximum_frame_length.bytes()
                ),
            });
        }

        let mut bytes = vec![0_u8; length];
        reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn write_frame(&self, writer: &mut impl Write, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() > self.maximum_frame_length.bytes() {
            return Err(Error::InvalidCommand {
                message: format!(
                    "contract frame is {} bytes; maximum is {}",
                    bytes.len(),
                    self.maximum_frame_length.bytes()
                ),
            });
        }

        writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }
}

pub struct ContractFrameStream {
    stream: UnixStream,
    codec: ContractFrameCodec,
}

impl ContractFrameStream {
    pub fn new(stream: UnixStream, codec: ContractFrameCodec) -> Self {
        Self { stream, codec }
    }

    pub fn send_input(&mut self, input: &Input) -> Result<()> {
        self.codec.write_input(&mut self.stream, input)
    }

    pub fn receive_input(&mut self) -> Result<Input> {
        self.codec.read_input(&mut self.stream)
    }

    pub fn send_output(&mut self, output: &Output) -> Result<()> {
        self.codec.write_output(&mut self.stream, output)
    }

    pub fn receive_output(&mut self) -> Result<Output> {
        self.codec.read_output(&mut self.stream)
    }
}
