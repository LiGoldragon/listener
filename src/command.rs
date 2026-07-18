use std::io::Write;

use nota::NotaSource;
use signal_listener::Input;

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

    pub fn run(&self, output: impl Write) -> Result<()> {
        self.run_with_client(&ListenerClient::from_environment(), output)
    }

    pub fn run_with_client(&self, client: &ListenerClient, mut output: impl Write) -> Result<()> {
        let reply = client.call(self.parse_input()?)?;
        writeln!(output, "{reply}")?;
        Ok(())
    }

    pub fn parse_input(&self) -> Result<Input> {
        let request = self.request_text()?;
        NotaSource::new(request)
            .parse::<Input>()
            .map_err(|error| Error::InvalidCommand {
                message: format!("invalid Listener schema request `{request}`: {error}"),
            })
    }

    fn request_text(&self) -> Result<&str> {
        match self.arguments.as_slice() {
            [_program, request] if request.starts_with("--") => Err(Error::InvalidCommand {
                message: format!(
                    "listener accepts one schema-defined NOTA request object, not flag argument `{request}`"
                ),
            }),
            [_program, request] => Ok(request),
            [_program] => Err(Error::InvalidCommand {
                message: "listener expects one schema-defined NOTA request object".to_owned(),
            }),
            _ => Err(Error::InvalidCommand {
                message: format!(
                    "listener expects exactly one schema-defined NOTA request object, found {}",
                    self.arguments.len().saturating_sub(1)
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nota::NotaEncode;
    use signal_listener::{CancelCapture, CaptureSession, StopCapture, ToggleCapture};

    struct CommandLineFixture;

    impl CommandLineFixture {
        fn input(request: &str) -> Input {
            CommandLine::from_arguments(vec!["listener".to_owned(), request.to_owned()])
                .parse_input()
                .expect("parse schema request")
        }
    }

    #[test]
    fn canonical_schema_requests_parse_to_their_typed_inputs() {
        let toggle = CommandLineFixture::input("Toggle.{}");
        assert_eq!(toggle, Input::Toggle(ToggleCapture {}));
        assert_eq!(toggle.to_nota(), "Toggle.{}");

        let cancellation = CommandLineFixture::input("Cancel.7");
        assert_eq!(
            cancellation,
            Input::Cancel(CancelCapture::new(CaptureSession::new(7)))
        );
        assert_eq!(cancellation.to_nota(), "Cancel.7");

        let stop = CommandLineFixture::input("Stop.7");
        assert_eq!(stop, Input::Stop(StopCapture::new(CaptureSession::new(7))));
        assert_eq!(stop.to_nota(), "Stop.7");
    }

    #[test]
    fn positional_listener_commands_are_rejected() {
        let error = CommandLine::from_arguments(vec!["listener".to_owned(), "toggle".to_owned()])
            .parse_input()
            .expect_err("reject positional toggle");
        assert!(matches!(error, Error::InvalidCommand { .. }));

        let error = CommandLine::from_arguments(vec![
            "listener".to_owned(),
            "cancel".to_owned(),
            "7".to_owned(),
        ])
        .parse_input()
        .expect_err("reject positional cancel");
        assert!(matches!(error, Error::InvalidCommand { .. }));
    }
}
