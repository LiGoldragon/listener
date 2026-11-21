//! The Nexus-owned recording session and its local typed vocabulary.
#[rustfmt::skip]
#[path = "generated.rs"]
mod generated;
pub use generated::*;

mod capturing;
#[allow(unsafe_code)]
mod native;

pub use capturing::{
    ControllingRecording, End, Recording, Refusal, RestoringAudio, StartingRecording,
};
