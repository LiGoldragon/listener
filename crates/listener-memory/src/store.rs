use crate::{
    Error, ExecutingMemory, InspectingMemory, OpeningMemory, Transitioning,
    ValidatingConfiguration, ViewingJob, operations, state,
};
use listener_library::Configuration;
use std::{path::PathBuf, sync::Mutex};

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
impl OpeningMemory for MemoryDefinition {
    fn open(self, configuration: Configuration) -> Result<Memory, Error> {
        configuration.validate()?;
        let fresh = !self.path.exists();
        // The version is the record layout's identity, not a running preference.
        let schema = memory::Schema {
            version: memory::SchemaVersion::new(1),
        };
        let database = memory::Memory::open_with_schema(&self.path, &schema)?;
        let table = memory::Table::new(self.table_name);
        let prior =
            database.read(|transaction| table.get(transaction, self.record_key.as_str()))?;
        if prior.is_none() {
            if !fresh {
                return Err(Error::MissingRecord);
            }
            let record = state::Record {
                job_vector: Vec::new(),
                configuration,
                delivery_claim: state::DeliveryClaim(None),
            };
            database.write(|transaction| {
                table.insert(transaction, self.record_key.as_str(), &record)
            })?;
        }
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
