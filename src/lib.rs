//! Listener runtime scaffold.
//!
//! The Listener component will own speech capture, durable capture writes,
//! batch transcription on stop, and configured output delivery. Its public wire
//! vocabularies live in `signal-listener` and `meta-signal-listener`.

#[cfg(feature = "nota-text")]
pub mod command;
pub mod configuration;
pub mod daemon;
pub mod error;
#[cfg(feature = "nota-text")]
pub mod meta;

#[cfg(feature = "nota-text")]
pub use command::CommandLine;
pub use configuration::Configuration;
pub use daemon::ListenerDaemon;
pub use error::{Error, Result};
#[cfg(feature = "nota-text")]
pub use meta::MetaCommandLine;
