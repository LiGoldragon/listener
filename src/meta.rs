use std::io::Write;

use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaCommandLine {
    arguments: Vec<String>,
}

impl MetaCommandLine {
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
            "meta-listener CLI scaffold: meta-signal-listener transport is not implemented"
        )?;
        Err(Error::NotImplemented {
            surface: "meta-listener CLI",
        })
    }
}
