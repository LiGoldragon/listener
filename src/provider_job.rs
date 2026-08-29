//! Durable, credential-free provider finalization jobs.
//!
//! A job captures its provider-policy generation before a provider call. The
//! result is durable before history or delivery intent. History is logically
//! once per stable capture session; physical delivery is at-least-once across
//! the unavoidable crash between external effect and receipt.

use std::{fs, path::Path, sync::{Arc, Mutex}};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference};
use signal_listener::TranscriptText;
use thiserror::Error;

use crate::{ProviderAttempt, ProviderAttemptState, ProviderIdentifier, ProviderPolicy};

const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const JOB_TABLE: TableName = TableName::new("listener_transcription_provider_jobs");

#[derive(Debug, Error)]
pub enum ProviderJobStoreError {
    #[error("provider job sema engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("provider job store path has no parent")]
    StorePathHasNoParent,
    #[error("provider job store filesystem: {0}")]
    Filesystem(#[from] std::io::Error),
    #[error("provider job store mutex is poisoned")]
    Poisoned,
    #[error("provider job has an invalid stored provider policy")]
    InvalidPolicy,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredProviderAttempt { provider: u8, state: u8 }

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredProviderJob {
    session: String,
    artifact_path: String,
    policy_generation: u64,
    providers: Vec<u8>,
    attempts: Vec<StoredProviderAttempt>,
    result: Option<String>,
    history_intent: bool,
    history_receipt: bool,
    delivery_intent: bool,
    delivery_receipt: bool,
}

impl EngineRecord for StoredProviderJob {
    fn record_key(&self) -> RecordKey { RecordKey::new(&self.session) }
}

struct ProviderJobStoreInner { engine: Engine, jobs: TableReference<StoredProviderJob> }

impl ProviderJobStoreInner {
    fn find(&self, session: &str) -> Result<Option<StoredProviderJob>, ProviderJobStoreError> {
        let records = self.engine.match_records(QueryPlan::all(self.jobs))?;
        let mut matches = records.records().iter().filter(|job| job.session == session);
        let job = matches.next().cloned();
        if matches.next().is_some() { return Err(ProviderJobStoreError::InvalidPolicy); }
        Ok(job)
    }

    fn save(&mut self, job: StoredProviderJob, existed: bool) -> Result<(), ProviderJobStoreError> {
        let commit = if existed {
            self.engine.begin_atomic_commit().mutate(self.jobs, job)
        } else {
            self.engine.begin_atomic_commit().assert(self.jobs, job)
        };
        self.engine.commit_atomic(commit)?;
        Ok(())
    }
}

/// The sole owner of durable provider-job state.
#[derive(Clone)]
pub struct ProviderJobStore { inner: Arc<Mutex<ProviderJobStoreInner>> }

/// One stable session/job identity. Handles contain no audio or credential
/// bytes and reload/write their durable row under the store owner mutex.
#[derive(Clone)]
pub struct ProviderJob { store: ProviderJobStore, session: String }

trait OwnsProviderJobs {
    fn begin_job(&self, session: &str, artifact_path: &str, policy: ProviderPolicy) -> Result<ProviderJob, ProviderJobStoreError>;
    fn find_job(&self, session: &str) -> Result<Option<ProviderJob>, ProviderJobStoreError>;
}

impl ProviderJobStore {
    // Exception: Too trivial. Opens the concrete Sema owner.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ProviderJobStoreError> {
        let path = path.as_ref();
        let parent = path.parent().ok_or(ProviderJobStoreError::StorePathHasNoParent)?;
        fs::create_dir_all(parent)?;
        let mut engine = Engine::open(EngineOpen::new(path.display().to_string(), SCHEMA_VERSION))?;
        let jobs = engine.register_table(TableDescriptor::new(
            JOB_TABLE,
            FamilyName::new("listener-transcription-provider-jobs"),
            SchemaHash::for_label("listener-transcription-provider-jobs-v1"),
        ))?;
        Ok(Self { inner: Arc::new(Mutex::new(ProviderJobStoreInner { engine, jobs })) })
    }

    // Exception: Too trivial. This is the public job-start operation.
    pub fn begin(&self, session: &str, artifact_path: &str, policy: ProviderPolicy) -> Result<ProviderJob, ProviderJobStoreError> {
        self.begin_job(session, artifact_path, policy)
    }

    // Exception: Too trivial. This reads one stable job identity.
    pub fn job(&self, session: &str) -> Result<Option<ProviderJob>, ProviderJobStoreError> { self.find_job(session) }
}

impl OwnsProviderJobs for ProviderJobStore {
    fn begin_job(&self, session: &str, artifact_path: &str, policy: ProviderPolicy) -> Result<ProviderJob, ProviderJobStoreError> {
        let mut inner = self.inner.lock().map_err(|_| ProviderJobStoreError::Poisoned)?;
        if inner.find(session)?.is_none() {
            let job = StoredProviderJob {
                session: session.to_owned(), artifact_path: artifact_path.to_owned(),
                policy_generation: policy.generation(), providers: policy.providers().iter().copied().map(provider_code).collect(),
                attempts: Vec::new(), result: None, history_intent: false, history_receipt: false,
                delivery_intent: false, delivery_receipt: false,
            };
            inner.save(job, false)?;
        }
        Ok(ProviderJob { store: self.clone(), session: session.to_owned() })
    }

    fn find_job(&self, session: &str) -> Result<Option<ProviderJob>, ProviderJobStoreError> {
        let inner = self.inner.lock().map_err(|_| ProviderJobStoreError::Poisoned)?;
        Ok(inner.find(session)?.map(|_| ProviderJob { store: self.clone(), session: session.to_owned() }))
    }
}

trait PersistsProviderJob {
    fn record_attempts(&self, attempts: &[ProviderAttempt]) -> Result<(), ProviderJobStoreError>;
    fn record_result_text(&self, transcript: &str) -> Result<(), ProviderJobStoreError>;
    fn stored_result(&self) -> Result<Option<String>, ProviderJobStoreError>;
    fn prepare_history_once(&self) -> Result<bool, ProviderJobStoreError>;
    fn receipt_history_once(&self) -> Result<(), ProviderJobStoreError>;
    fn prepare_delivery_once(&self) -> Result<bool, ProviderJobStoreError>;
    fn receipt_delivery_once(&self) -> Result<(), ProviderJobStoreError>;
}

impl ProviderJob {
    fn modify(&self, change: impl FnOnce(&mut StoredProviderJob)) -> Result<(), ProviderJobStoreError> {
        let mut inner = self.store.inner.lock().map_err(|_| ProviderJobStoreError::Poisoned)?;
        let mut job = inner.find(&self.session)?.ok_or(ProviderJobStoreError::InvalidPolicy)?;
        change(&mut job);
        inner.save(job, true)
    }

    fn read(&self) -> Result<StoredProviderJob, ProviderJobStoreError> {
        let inner = self.store.inner.lock().map_err(|_| ProviderJobStoreError::Poisoned)?;
        inner.find(&self.session)?.ok_or(ProviderJobStoreError::InvalidPolicy)
    }

    // Exception: Too trivial. Appends provider provenance before a final result exists.
    pub fn record_attempts(&self, attempts: &[ProviderAttempt]) -> Result<(), ProviderJobStoreError> { PersistsProviderJob::record_attempts(self, attempts) }
    // Exception: Too trivial. Result text is written before delivery intent.
    pub fn record_result(&self, transcript: impl AsRef<str>) -> Result<(), ProviderJobStoreError> { self.record_result_text(transcript.as_ref()) }
    // Exception: Too trivial. Reads the durable provider result for retry resumption.
    pub fn result(&self) -> Result<Option<String>, ProviderJobStoreError> { self.stored_result() }
    // Exception: Too trivial. Persists logical history intent before history projection.
    pub fn prepare_history(&self) -> Result<bool, ProviderJobStoreError> { self.prepare_history_once() }
    // Exception: Too trivial. Persists logical history completion by stable job identity.
    pub fn receipt_history(&self) -> Result<(), ProviderJobStoreError> { self.receipt_history_once() }
    // Exception: Too trivial. Persists delivery intent before the external target effect.
    pub fn prepare_delivery(&self) -> Result<bool, ProviderJobStoreError> { self.prepare_delivery_once() }
    // Exception: Too trivial. Persists delivery receipt after target completion.
    pub fn receipt_delivery(&self) -> Result<(), ProviderJobStoreError> { self.receipt_delivery_once() }
}

impl PersistsProviderJob for ProviderJob {
    fn record_attempts(&self, attempts: &[ProviderAttempt]) -> Result<(), ProviderJobStoreError> {
        self.modify(|job| job.attempts.extend(attempts.iter().map(|attempt| StoredProviderAttempt { provider: provider_code(attempt.provider()), state: state_code(attempt.state()) })))
    }

    fn record_result_text(&self, transcript: &str) -> Result<(), ProviderJobStoreError> {
        self.modify(|job| if job.result.is_none() { job.result = Some(transcript.to_owned()); })
    }

    fn stored_result(&self) -> Result<Option<String>, ProviderJobStoreError> { Ok(self.read()?.result) }

    fn prepare_history_once(&self) -> Result<bool, ProviderJobStoreError> {
        let job = self.read()?;
        if job.result.is_none() || job.history_receipt { return Ok(false); }
        self.modify(|job| job.history_intent = true)?;
        Ok(true)
    }

    fn receipt_history_once(&self) -> Result<(), ProviderJobStoreError> { self.modify(|job| { if job.history_intent { job.history_receipt = true; } }) }

    fn prepare_delivery_once(&self) -> Result<bool, ProviderJobStoreError> {
        let job = self.read()?;
        if job.result.is_none() || job.delivery_receipt { return Ok(false); }
        self.modify(|job| job.delivery_intent = true)?;
        Ok(true)
    }

    fn receipt_delivery_once(&self) -> Result<(), ProviderJobStoreError> { self.modify(|job| { if job.delivery_intent { job.delivery_receipt = true; } }) }
}

fn provider_code(provider: ProviderIdentifier) -> u8 { match provider { ProviderIdentifier::WisprFlow => 1, ProviderIdentifier::OpenAi => 2 } }
fn state_code(state: ProviderAttemptState) -> u8 { match state { ProviderAttemptState::Succeeded => 1, ProviderAttemptState::Unavailable => 2, ProviderAttemptState::Rejected => 3, ProviderAttemptState::TransientFailure => 4, ProviderAttemptState::ProtocolFailure => 5, ProviderAttemptState::SizeLimit => 6, ProviderAttemptState::AuthenticationExpired => 7, ProviderAttemptState::Cancelled => 8, ProviderAttemptState::LocalArtifactFailure => 9, ProviderAttemptState::AmbiguousAfterSubmit => 10 } }

#[allow(dead_code)]
fn transcript_from_stored(job: &StoredProviderJob) -> Option<TranscriptText> { job.result.clone().map(TranscriptText::new) }
