use std::process::ExitCode;

use crate::{Error, Result};

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
        Err(Error::NotImplemented {
            surface: "listener daemon runtime",
        })
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
