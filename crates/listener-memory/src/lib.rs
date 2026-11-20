//! Fresh Listener state and serialized durable operations; no external effects.
#![forbid(unsafe_code)]

#[path = "generated/operations.rs"]
pub mod operations;
#[path = "generated/state.rs"]
pub mod state;
mod store;
mod transition;

use listener_library::{Configuration, JobId};
use operations::{Committed, Execution};
use std::path::PathBuf;
pub use store::{Memory, MemoryDefinition};

#[derive(Debug)]
pub enum Error {
    Storage(memory::Error),
    Poisoned,
    MissingRecord,
    MissingIdentity,
    DuplicateJob(JobId),
    NoSuchJob(JobId),
    WrongState(JobId),
    InvalidConfiguration,
    UnknownBackend(listener_library::BackendName),
}
impl From<memory::Error> for Error {
    fn from(error: memory::Error) -> Self {
        Self::Storage(error)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, formatter)
    }
}
impl std::error::Error for Error {}

/// Opens only the configured new Memory; no legacy locations are accepted.
pub trait OpeningMemory {
    fn open(self, configuration: Configuration) -> Result<Memory, Error>;
}
/// Serializes state transformation, durable commit and returned change data.
pub trait ExecutingMemory {
    fn execute(&self, execution: Execution) -> Result<Committed, Error>;
}
/// Reads a transaction-owned snapshot without retaining another authority.
pub trait InspectingMemory {
    fn inspect(&self) -> Result<state::Record, Error>;
    fn path(&self) -> Result<PathBuf, Error>;
}
/// Validates configured references and recording ranges before a mutation.
pub trait ValidatingConfiguration {
    fn validate(&self) -> Result<(), Error>;
}
/// Applies one pure operation; persistence is the Memory handle's role.
pub(crate) trait Transitioning {
    fn transition(&mut self, execution: &Execution) -> Result<operations::Output, Error>;
    fn next(&mut self) -> operations::Next;
}
/// A job's retained text and status, independent of its audio lifetime.
pub(crate) trait ViewingJob {
    fn text(&self) -> Option<&listener_library::Text>;
    fn ready(&self) -> Option<listener_library::Ready>;
    fn terminal(&self) -> bool;
    fn status(&self) -> listener_library::JobState;
}
/// Configuration replacement and backend-reference integrity.
pub(crate) trait ApplyingConfiguration {
    fn apply(&mut self, set: &meta_signal_listener::Set) -> Result<(), Error>;
}
