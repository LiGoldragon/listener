use std::io::Write;

use crate::{Error, Result};

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
        writeln!(
            output,
            "listener CLI scaffold: signal-listener transport is not implemented"
        )?;
        Err(Error::NotImplemented {
            surface: "listener CLI",
        })
    }
}
