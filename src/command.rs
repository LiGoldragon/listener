use std::io::Write;

use signal_listener::{CancelCapture, CaptureSession, Input, StartCapture, StatusRequest};

use crate::{Error, ListenerClient, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandLine {
    arguments: Vec<String>,
}

impl CommandLine {
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

    pub fn run(&self, mut output: impl Write) -> Result<()> {
        let command = ListenerCommand::from_arguments(&self.arguments)?;
        let reply = ListenerClient::from_environment().call(command.into_input())?;
        writeln!(output, "{reply}")?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListenerCommand {
    Start,
    Stop(CaptureSession),
    Cancel(CaptureSession),
    Status,
}

impl ListenerCommand {
    pub fn from_arguments(arguments: &[String]) -> Result<Self> {
        match arguments.get(1).map(String::as_str) {
            Some("start") => Ok(Self::Start),
            Some("stop") => Self::stop_from_arguments(arguments),
            Some("cancel") => Self::cancel_from_arguments(arguments),
            Some("status") => Ok(Self::Status),
            Some(command) => Err(Error::InvalidCommand {
                message: format!("unknown listener command `{command}`"),
            }),
            None => Err(Error::InvalidCommand {
                message: "expected one of: start, stop <session>, cancel <session>, status"
                    .to_owned(),
            }),
        }
    }

    pub fn into_input(self) -> Input {
        match self {
            Self::Start => Input::Start(StartCapture {}),
            Self::Stop(session) => Input::stop(session),
            Self::Cancel(session) => Input::Cancel(CancelCapture::new(session)),
            Self::Status => Input::Status(StatusRequest {}),
        }
    }

    fn stop_from_arguments(arguments: &[String]) -> Result<Self> {
        Ok(Self::Stop(Self::capture_session_from_arguments(
            arguments, "stop",
        )?))
    }

    fn cancel_from_arguments(arguments: &[String]) -> Result<Self> {
        Ok(Self::Cancel(Self::capture_session_from_arguments(
            arguments, "cancel",
        )?))
    }

    fn capture_session_from_arguments(
        arguments: &[String],
        command: &'static str,
    ) -> Result<CaptureSession> {
        let value = arguments
            .get(2)
            .ok_or_else(|| Error::InvalidCommand {
                message: format!("{command} requires a capture session integer"),
            })?
            .clone();
        value
            .parse::<u64>()
            .map(CaptureSession::new)
            .map_err(|error| Error::InvalidCaptureSession {
                value,
                message: error.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_command_builds_typed_cancel_input() {
        let command = ListenerCommand::from_arguments(&[
            "listener".to_owned(),
            "cancel".to_owned(),
            "7".to_owned(),
        ])
        .expect("parse cancel command");

        match command.into_input() {
            Input::Cancel(cancel) => assert_eq!(cancel.payload().value(), 7),
            other => panic!("expected typed cancel input, got {other:?}"),
        }
    }
}
