use std::thread::{self, JoinHandle};

use crate::{CaptureMaintenanceSnapshot, CaptureStore, Configuration, Result};

/// One finite maintenance pass over capture sessions that existed when the
/// daemon came up. It keeps recovery, migration, and retention off socket
/// request handling and does not poll once it has completed.
pub struct CaptureMaintenance {
    capture_store: CaptureStore,
    snapshot: CaptureMaintenanceSnapshot,
}

impl CaptureMaintenance {
    pub fn from_configuration(configuration: &Configuration) -> Result<Self> {
        let capture_store = CaptureStore::from_configuration(configuration);
        let snapshot = capture_store.maintenance_snapshot()?;
        Ok(Self {
            capture_store,
            snapshot,
        })
    }

    pub fn spawn(self) -> JoinHandle<()> {
        thread::spawn(move || {
            if let Err(error) = self.run() {
                eprintln!("listener-daemon maintenance: {error}");
            }
        })
    }

    pub fn run(&self) -> Result<()> {
        self.capture_store.maintain_snapshot(&self.snapshot)
    }

    pub fn snapshot(&self) -> &CaptureMaintenanceSnapshot {
        &self.snapshot
    }
}
