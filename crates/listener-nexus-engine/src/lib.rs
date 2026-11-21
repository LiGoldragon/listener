//! Listener's typed runtime, without a dependency on a text codec.
#![forbid(unsafe_code)]
mod effects;
mod journal;
mod owner;
mod serving;

use effects::Effects;
use listener_memory::{CreatingMemory, MemoryDefinition, OpeningMemory};
use listener_place::{
    Preparing, Role, STORE_FILE, STORE_KEY, STORE_TABLE, Situated, Socket, Startup,
};
use nexus::{Capacity, EmissionSender};
use owner::{Owner, Running};
use serving::{Serving, Sockets};
use std::{io, os::unix::fs::DirBuilderExt, path::Path};
use tokio::{
    net::UnixListener,
    sync::{Mutex, mpsc},
};

pub trait StartingNexus {
    fn start(
        self,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error>>> + Send;
}
impl StartingNexus for Startup {
    async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        let place = self.place().clone();
        let backlog = match &self {
            Self::Create(create) => &create.publication_capacity,
            Self::Open(open) => &open.publication_capacity,
        };
        if usize::try_from(backlog.0).ok().is_none_or(|size| size == 0) {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let capacity = Capacity(backlog.0);
        let _lease = place.prepare(Role::Nexus)?;
        let definition = MemoryDefinition {
            path: Path::new(&place.state.0).join(STORE_FILE),
            table_name: STORE_TABLE,
            record_key: STORE_KEY.to_owned(),
        };
        let memory = match self {
            Startup::Create(create) => {
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&place.state.0)?;
                definition.create(create.configuration)?
            }
            Startup::Open(_) => definition.open()?,
        };
        let sockets = Sockets {
            ordinary: UnixListener::bind(place.socket(Socket::OrdinaryNexus))?,
            meta: UnixListener::bind(place.socket(Socket::MetaNexus))?,
        };
        let effects = Effects {
            emission: EmissionSender {
                socket_path: place.socket(Socket::Emission),
                frame_capacity: signal::FrameCapacity::default(),
            },
            desktop: Mutex::new(()),
        };
        let (events, inbox) = mpsc::unbounded_channel();
        let (owner, recovery) = Owner::recover(memory, capacity, events.clone(), effects)?;
        tokio::select! {
            result = sockets.serve(events) => result?,
            () = owner.run(recovery, inbox) => {}
        }
        Ok(())
    }
}
