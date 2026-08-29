//! One durable finalization path for stop, retry, and daemon work.
//!
//! The stable capture session is the logical result/history key. The finalizer
//! saves provider attempts and the chosen transcript before either projection;
//! it uses a stable delivery intent id and writes the receipt after delivery.

use std::{path::PathBuf, sync::Arc};

use signal_listener::{
    AudioArtifactPath, CaptureSession, DeliveryOutcomes, DurableAudioArtifact, OutputTargets,
    TranscriptText, WirePath,
};
use thiserror::Error;

use crate::{
    OutputTargetDispatcher, ProviderAttempt, ProviderJobStore,
    ProviderPolicy, ProviderRouter, ProviderTranscriptRequest, TranscriptHistoryEntry,
    TranscriptHistoryStore, TranscriptProvider, ProviderIdentifier, ProviderAttemptState,
    BatchTranscriber, BatchTranscriptionInput, BatchTranscriptionRequest,
};
use crate::provider_job::ProviderJobStoreError;

/// Adapter for the existing bounded OpenAI worker. It makes the worker one
/// typed router provider; fallback artifacts still arrive by their durable
/// path, never by an in-memory provider buffer.
pub struct OpenAiBatchProvider {
    transcriber: Arc<dyn BatchTranscriber>,
}

impl OpenAiBatchProvider {
    pub fn new(transcriber: Arc<dyn BatchTranscriber>) -> Self { Self { transcriber } }
}

impl TranscriptProvider for OpenAiBatchProvider {
    fn identifier(&self) -> ProviderIdentifier { ProviderIdentifier::OpenAi }

    fn transcribe(
        &self,
        request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let path = request.artifact_path();
        let artifact = DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
            path.to_string_lossy().into_owned(),
        )));
        self.transcriber
            .transcribe(BatchTranscriptionRequest::new_with_input(
                artifact,
                BatchTranscriptionInput::webm_opus(path.clone()),
            ))
            .map_err(|_| ProviderAttemptState::Unavailable)
    }
}

#[derive(Debug, Error)]
pub enum ProviderFinalizationError {
    #[error("durable provider finalization state: {0}")]
    Job(#[from] ProviderJobStoreError),
    #[error("provider policy exhausted while audio remains preserved")]
    AllProvidersFailed,
    #[error("history projection: {0}")]
    History(#[from] crate::Error),
}

/// Result already made durable before it is exposed to stop/retry callers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderFinalizationOutcome {
    transcript: TranscriptText,
    attempts: Vec<ProviderAttempt>,
    delivery_outcomes: DeliveryOutcomes,
}

/// Durable result ready for its history/delivery projections. It contains no
/// credential material; provider provenance remains in the job store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedProviderFinalization {
    transcript: TranscriptText,
    attempts: Vec<ProviderAttempt>,
}

impl PreparedProviderFinalization {
    pub fn transcript(&self) -> &TranscriptText { &self.transcript }
    pub fn attempts(&self) -> &[ProviderAttempt] { &self.attempts }
}

impl ProviderFinalizationOutcome {
    pub(crate) fn from_prepared(
        prepared: PreparedProviderFinalization,
        delivery_outcomes: DeliveryOutcomes,
    ) -> Self {
        Self {
            transcript: prepared.transcript,
            attempts: prepared.attempts,
            delivery_outcomes,
        }
    }
    pub fn transcript(&self) -> &TranscriptText { &self.transcript }
    pub fn attempts(&self) -> &[ProviderAttempt] { &self.attempts }
    pub fn delivery_outcomes(&self) -> &DeliveryOutcomes { &self.delivery_outcomes }
}

/// The shared router path. It intentionally knows no credentials: providers
/// and their session/transport boundaries are injected above this type.
#[derive(Clone)]
pub struct DurableProviderFinalizer {
    jobs: ProviderJobStore,
    router: ProviderRouter,
    dispatcher: OutputTargetDispatcher,
    history: TranscriptHistoryStore,
}

impl DurableProviderFinalizer {
    pub fn new(
        jobs: ProviderJobStore,
        router: ProviderRouter,
        dispatcher: OutputTargetDispatcher,
        history: TranscriptHistoryStore,
    ) -> Self {
        Self { jobs, router, dispatcher, history }
    }

    pub fn finalize(
        &self,
        session: &CaptureSession,
        artifact: DurableAudioArtifact,
        policy: ProviderPolicy,
        output_targets: &OutputTargets,
    ) -> Result<ProviderFinalizationOutcome, ProviderFinalizationError> {
        let prepared = self.prepare(session, artifact, policy)?;
        self.record_history(session, prepared.transcript())?;
        let delivery_outcomes = self.deliver(session, output_targets, prepared.transcript())?;
        Ok(ProviderFinalizationOutcome {
            transcript: prepared.transcript,
            attempts: prepared.attempts,
            delivery_outcomes,
        })
    }

    /// Persist provider attempts and result before a caller crosses its own
    /// cancellation-to-delivery ownership boundary.
    pub fn prepare(
        &self,
        session: &CaptureSession,
        artifact: DurableAudioArtifact,
        policy: ProviderPolicy,
    ) -> Result<PreparedProviderFinalization, ProviderFinalizationError> {
        let job = self.jobs.begin(session.value().to_string().as_str(), artifact.path().as_str(), policy)?;
        let policy = job.policy()?;
        let (transcript, attempts) = match job.result()? {
            Some(text) => (TranscriptText::new(text), Vec::new()),
            None => {
                let outcome = self.router.transcribe(
                    policy,
                    ProviderTranscriptRequest::for_test(PathBuf::from(artifact.path().as_str())),
                );
                job.record_attempts(outcome.attempts())?;
                let transcript = outcome
                    .transcript()
                    .cloned()
                    .ok_or(ProviderFinalizationError::AllProvidersFailed)?;
                job.record_result(transcript.as_str())?;
                (transcript, outcome.attempts().to_vec())
            }
        };

        Ok(PreparedProviderFinalization { transcript, attempts })
    }

    /// Persist the logical history intent only after cancellation no longer
    /// owns the work. The history projection is session-deduplicated, covering
    /// a crash after append and before its durable receipt.
    pub fn record_history(
        &self,
        session: &CaptureSession,
        transcript: &TranscriptText,
    ) -> Result<(), ProviderFinalizationError> {
        let job = self.jobs.job(session.value().to_string().as_str())?
            .ok_or(ProviderFinalizationError::AllProvidersFailed)?;
        if job.prepare_history()? {
            if !self.history.contains_session(session)? {
                self.history.append(&TranscriptHistoryEntry::recorded_now(
                    session.clone(),
                    transcript.clone(),
                )?)?;
            }
            job.receipt_history()?;
        }
        Ok(())
    }

    /// Persist intent before the external target effect and receipt after it.
    /// A retry with an existing receipt does not touch the target; a crash
    /// after the target effect can repeat the stable idempotent payload.
    pub fn deliver(
        &self,
        session: &CaptureSession,
        output_targets: &OutputTargets,
        transcript: &TranscriptText,
    ) -> Result<DeliveryOutcomes, ProviderFinalizationError> {
        let job = self.jobs.job(session.value().to_string().as_str())?
            .ok_or(ProviderFinalizationError::AllProvidersFailed)?;
        let delivery_outcomes = if job.prepare_delivery()? {
            let outcomes = self
                .dispatcher
                .deliver_for_session(session, output_targets, &transcript);
            job.receipt_delivery()?;
            outcomes
        } else {
            DeliveryOutcomes::new(Vec::new())
        };
        Ok(delivery_outcomes)
    }
}
