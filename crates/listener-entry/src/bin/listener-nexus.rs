use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use listener_entry::{Listener, ReadingLaunch};
use listener_place::{ENGINE, Startup};
use signal::{ByteViewable, Signalizable};
use std::{
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::Command,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let startup = Listener::read_launch::<Startup>()?;
    let binary = std::env::args_os()
        .next()
        .map(PathBuf::from)
        .as_deref()
        .and_then(Path::parent)
        .filter(|path| !path.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from(ENGINE), |parent| parent.join(ENGINE));
    let encoded = STANDARD_NO_PAD.encode(startup.signalize()?.bytes());
    Err(Command::new(binary).arg(encoded).exec().into())
}
