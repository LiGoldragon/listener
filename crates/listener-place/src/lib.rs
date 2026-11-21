//! Local launch types and paths, shared by entry points and the Nexus.
#![forbid(unsafe_code)]
#[rustfmt::skip]
#[allow(clippy::large_enum_variant)] // Create carries the complete initial configuration.
mod generated;
pub use generated::*;

use listener_library::AudioLocation;
use rustix::fs::{FlockOperation, flock};
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io,
    os::unix::fs::{DirBuilderExt, FileTypeExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
use translator::Place;

pub const COMPONENT: &str = "listener";
pub const ENGINE: &str = "listener-nexus-engine";
pub const STORE_FILE: &str = "listener.memory";
pub const STORE_TABLE: &str = "listener";
pub const STORE_KEY: &str = "state";

pub trait AudioLocating {
    fn audio_path(&self) -> PathBuf;
}
impl AudioLocating for AudioLocation {
    fn audio_path(&self) -> PathBuf {
        Path::new(&self.directory.0).join(format!("{}.opus", self.job_id.0))
    }
}

pub trait Situated {
    fn place(&self) -> &Place;
}
impl Situated for Startup {
    fn place(&self) -> &Place {
        match self {
            Self::Create(create) => &create.place,
            Self::Open(open) => &open.place,
        }
    }
}

#[derive(Clone, Copy)]
pub enum Socket {
    OrdinaryNexus,
    MetaNexus,
    OrdinaryText,
    MetaText,
    Emission,
}
#[derive(Clone, Copy)]
pub enum Role {
    Nexus,
    Translator,
}

pub struct Lease {
    _file: File,
}
pub trait Preparing {
    fn prepare(&self, role: Role) -> io::Result<Lease>;
    fn socket(&self, socket: Socket) -> PathBuf;
}
impl Preparing for Place {
    fn prepare(&self, role: Role) -> io::Result<Lease> {
        if translator::placement::validate_place_name(&self.place_name.0).is_err()
            || !Path::new(&self.runtime.0).is_absolute()
            || !Path::new(&self.state.0).is_absolute()
            || self.runtime.0 == self.state.0
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&self.runtime.0)?;
        let (name, sockets): (&str, &[Socket]) = match role {
            Role::Nexus => (
                "listener-nexus.lock",
                &[Socket::OrdinaryNexus, Socket::MetaNexus],
            ),
            Role::Translator => (
                "listener-translator.lock",
                &[Socket::OrdinaryText, Socket::MetaText, Socket::Emission],
            ),
        };
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(Path::new(&self.runtime.0).join(name))?;
        flock(&file, FlockOperation::NonBlockingLockExclusive).map_err(io::Error::from)?;
        // The role's lease excludes a live instance before clearing its dead sockets.
        for &socket in sockets {
            let path = self.socket(socket);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_socket() => fs::remove_file(path)?,
                Ok(_) => return Err(io::ErrorKind::AlreadyExists.into()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(Lease { _file: file })
    }
    fn socket(&self, socket: Socket) -> PathBuf {
        let suffix = match socket {
            Socket::OrdinaryNexus => "",
            Socket::MetaNexus => "-meta",
            Socket::OrdinaryText => "-translator",
            Socket::MetaText => "-meta-translator",
            Socket::Emission => "-emission",
        };
        Path::new(&self.runtime.0).join(format!("{COMPONENT}{suffix}.sock"))
    }
}

/// Get a kernel-generated identity without sharing state between places.
pub fn random_bytes<const SIZE: usize>() -> io::Result<[u8; SIZE]> {
    let mut bytes = [0; SIZE];
    let mut position = 0;
    while position < SIZE {
        match rustix::rand::getrandom(
            &mut bytes[position..],
            rustix::rand::GetRandomFlags::empty(),
        ) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => position += count,
            Err(rustix::io::Errno::INTR) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(bytes)
}
