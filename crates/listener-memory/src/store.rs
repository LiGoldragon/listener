use crate::{
    CreatingMemory, Error, ExecutingMemory, InspectingMemory, OpeningMemory, Transitioning,
    ValidatingConfiguration, ViewingJob, operations, state,
};
use listener_library::Configuration;
use std::{
    ffi::OsStr,
    fs::{self, File},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Component-owned database identity; no process paths or table names are inferred.
pub struct MemoryDefinition {
    pub path: PathBuf,
    pub table_name: &'static str,
    pub record_key: String,
}
pub struct Memory {
    storage: Mutex<Storage>,
}
struct Storage {
    database: memory::Memory,
    table: memory::Table<&'static str, state::Record>,
    record_key: String,
}
const SCHEMA: memory::Schema = memory::Schema {
    version: memory::SchemaVersion::new(1),
};

struct StagedMemory {
    _directory: tempfile::TempDir,
    path: PathBuf,
}

trait StagingMemory {
    fn stage(parent: &Path, name: &OsStr) -> Result<Self, Error>
    where
        Self: Sized;
    fn stamp(&self) -> Result<memory::Memory, Error>;
    fn publish(self, destination: &Path, parent: &Path) -> Result<(), Error>;
}

impl StagingMemory for StagedMemory {
    fn stage(parent: &Path, name: &OsStr) -> Result<Self, Error> {
        let directory = tempfile::tempdir_in(parent)?;
        let path = directory.path().join(name);
        Ok(Self {
            _directory: directory,
            path,
        })
    }

    fn stamp(&self) -> Result<memory::Memory, Error> {
        Ok(memory::Memory::open_with_schema(&self.path, &SCHEMA)?)
    }

    fn publish(self, destination: &Path, parent: &Path) -> Result<(), Error> {
        File::open(&self.path)?.sync_all()?;
        match fs::hard_link(&self.path, destination) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                return Err(Error::StoreAlreadyExists);
            }
            Err(error) => return Err(error.into()),
        }
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

impl CreatingMemory for MemoryDefinition {
    fn create(self, configuration: Configuration) -> Result<Memory, Error> {
        configuration.validate()?;
        match fs::symlink_metadata(&self.path) {
            Ok(_) => return Err(Error::StoreAlreadyExists),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let parent = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or(Error::StoreNotRegular)?;
        fs::create_dir_all(parent)?;
        let name = self.path.file_name().ok_or(Error::StoreNotRegular)?;
        let stage = StagedMemory::stage(parent, name)?;
        let database = stage.stamp()?;
        let table = memory::Table::<&str, state::Record>::new(self.table_name);
        let record = state::Record {
            job_vector: Vec::new(),
            configuration,
            delivery_claim: state::DeliveryClaim(None),
        };
        database
            .write(|transaction| table.insert(transaction, self.record_key.as_str(), &record))?;
        drop(database);
        stage.publish(&self.path, parent)?;
        self.open()
    }
}

impl OpeningMemory for MemoryDefinition {
    fn open(self) -> Result<Memory, Error> {
        // The component owns this path. The current kernel combines open and
        // create, so this check is not an atomic guard against external unlink.
        match fs::symlink_metadata(&self.path) {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => return Err(Error::StoreNotRegular),
            Err(error) if error.kind() == ErrorKind::NotFound => return Err(Error::MissingStore),
            Err(error) => return Err(error.into()),
        }
        let database = memory::Memory::open_with_schema(&self.path, &SCHEMA)?;
        let table = memory::Table::<&str, state::Record>::new(self.table_name);
        database
            .read(|transaction| table.get(transaction, self.record_key.as_str()))?
            .ok_or(Error::MissingRecord)?;
        Ok(Memory {
            storage: Mutex::new(Storage {
                database,
                table,
                record_key: self.record_key,
            }),
        })
    }
}
impl InspectingMemory for Memory {
    fn inspect(&self) -> Result<state::Record, Error> {
        let storage = self.storage.lock().map_err(|_| Error::Poisoned)?;
        storage
            .database
            .read(|transaction| storage.table.get(transaction, storage.record_key.as_str()))?
            .ok_or(Error::MissingRecord)
    }
    fn path(&self) -> Result<PathBuf, Error> {
        let storage = self.storage.lock().map_err(|_| Error::Poisoned)?;
        Ok(storage.database.path().to_path_buf())
    }
}
impl ExecutingMemory for Memory {
    fn execute(&self, execution: operations::Execution) -> Result<operations::Committed, Error> {
        // One owner lock covers the kernel read and write: Table::get takes a
        // ReadTransaction. No other handle to this database is exposed.
        let storage = self.storage.lock().map_err(|_| Error::Poisoned)?;
        let before = storage
            .database
            .read(|transaction| storage.table.get(transaction, storage.record_key.as_str()))?
            .ok_or(Error::MissingRecord)?;
        let mut after = before.clone();
        let output = after.transition(&execution)?;
        let changes = operations::Changes {
            job_state_vector: after
                .job_vector
                .iter()
                .filter(|job| {
                    before
                        .job_vector
                        .iter()
                        .find(|prior| prior.job_id == job.job_id)
                        .is_none_or(|prior| prior.state != job.state)
                })
                .map(ViewingJob::status)
                .collect(),
            retired: operations::Retired(
                before
                    .job_vector
                    .iter()
                    .filter(|job| {
                        !after
                            .job_vector
                            .iter()
                            .any(|kept| kept.job_id == job.job_id)
                    })
                    .map(|job| job.job_id.clone())
                    .collect(),
            ),
            set_option: match &output {
                operations::Output::Applying(operations::Applying::Applied(set)) => {
                    Some(set.clone())
                }
                _ => None,
            },
        };
        if before != after {
            storage.database.write(|transaction| {
                storage
                    .table
                    .insert(transaction, storage.record_key.as_str(), &after)
            })?;
        }
        // Nothing escapes this method before the write has committed.
        Ok(operations::Committed { output, changes })
    }
}

#[cfg(test)]
mod tests {
    use super::{StagedMemory, StagingMemory};
    use std::{env, fs, path::Path, process::Command};

    #[test]
    fn abrupt_process_exit_before_publication_leaves_no_final_file() {
        for child in [
            "store::tests::exit_after_schema_stamp",
            "store::tests::exit_after_initial_commit",
        ] {
            let parent = tempfile::tempdir().unwrap();
            let destination = parent.path().join("state.memory");
            let status = Command::new(env::current_exe().unwrap())
                .args(["--ignored", "--exact", child])
                .current_dir(parent.path())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(73));
            assert!(!destination.exists());
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 1);
        }
    }

    #[test]
    #[ignore = "subprocess fixture exits without running destructors"]
    fn exit_after_schema_stamp() {
        let stage = StagedMemory::stage(Path::new("."), "state.memory".as_ref()).unwrap();
        let _database = stage.stamp().unwrap();
        std::process::exit(73);
    }

    #[test]
    #[ignore = "subprocess fixture exits without running destructors"]
    fn exit_after_initial_commit() {
        let stage = StagedMemory::stage(Path::new("."), "state.memory".as_ref()).unwrap();
        let database = stage.stamp().unwrap();
        let marker = memory::Table::<&str, u64>::new("creation-marker");
        database
            .write(|transaction| marker.insert(transaction, "initialized", &1))
            .unwrap();
        std::process::exit(73);
    }

    #[test]
    fn publication_does_not_replace_an_existing_destination() {
        let parent = tempfile::tempdir().unwrap();
        let destination = parent.path().join("state.memory");
        fs::write(&destination, "existing").unwrap();
        let stage = StagedMemory::stage(parent.path(), "state.memory".as_ref()).unwrap();
        drop(stage.stamp().unwrap());
        assert!(matches!(
            stage.publish(&destination, parent.path()),
            Err(super::Error::StoreAlreadyExists)
        ));
        assert_eq!(fs::read_to_string(&destination).unwrap(), "existing");
    }
}
