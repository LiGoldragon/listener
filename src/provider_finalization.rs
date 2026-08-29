//! One durable finalization path for stop, retry, and daemon work.
//!
//! The stable capture session is the logical result/history key. The finalizer
//! saves provider attempts and the chosen transcript before either projection;
//! it uses a stable delivery intent id and writes the receipt after delivery.

use std::{collections::BTreeMap, io::Write, path::PathBuf, sync::Arc};

use signal_listener::{
    AudioArtifactPath, CaptureSession, DeliveryOutcomes, DurableAudioArtifact, OutputTargets,
    TranscriptText, WirePath,
};
use thiserror::Error;

use crate::provider_job::ProviderJobStoreError;
use crate::segmentation::{plan_raw_pcm_segments, stitch_transcripts};
use crate::{
    BatchTranscriber, BatchTranscriptionInput, BatchTranscriptionRequest, OutputTargetDispatcher,
    ProviderAttempt, ProviderAttemptState, ProviderIdentifier, ProviderJobStore, ProviderPolicy,
    ProviderRouter, ProviderTranscriptRequest, RecordingLog, SegmentSampleRange,
    TranscriptHistoryEntry, TranscriptHistoryStore, TranscriptProvider,
};

/// Adapter for the existing bounded OpenAI worker. It makes the worker one
/// typed router provider; fallback artifacts still arrive by their durable
/// path, never by an in-memory provider buffer.
pub struct OpenAiBatchProvider {
    transcriber: Arc<dyn BatchTranscriber>,
}

impl OpenAiBatchProvider {
    pub fn new(transcriber: Arc<dyn BatchTranscriber>) -> Self {
        Self { transcriber }
    }
}

impl TranscriptProvider for OpenAiBatchProvider {
    fn identifier(&self) -> ProviderIdentifier {
        ProviderIdentifier::OpenAi
    }

    fn transcribe(
        &self,
        request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let path = request.artifact_path();
        let artifact = DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
            path.to_string_lossy().into_owned(),
        )));
        match request.sample_range() {
            None => self
                .transcriber
                .transcribe(BatchTranscriptionRequest::new_with_input(
                    artifact,
                    BatchTranscriptionInput::webm_opus(path.clone()),
                ))
                .map_err(|_| ProviderAttemptState::Unavailable),
            Some(range) => self.transcribe_recording_log_range(artifact, path, range),
        }
    }
}

impl OpenAiBatchProvider {
    fn transcribe_recording_log_range(
        &self,
        artifact: DurableAudioArtifact,
        path: &PathBuf,
        range: SegmentSampleRange,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let recovered = RecordingLog::new(path)
            .recover()
            .map_err(|_| ProviderAttemptState::LocalArtifactFailure)?;
        let (format, pcm) = recovered
            .raw_pcm_bytes()
            .map_err(|_| ProviderAttemptState::LocalArtifactFailure)?;
        let start = usize::try_from(
            range
                .start()
                .checked_mul(u64::from(format.bytes_per_frame()))
                .ok_or(ProviderAttemptState::SizeLimit)?,
        )
        .map_err(|_| ProviderAttemptState::SizeLimit)?;
        let end = usize::try_from(
            range
                .end()
                .checked_mul(u64::from(format.bytes_per_frame()))
                .ok_or(ProviderAttemptState::SizeLimit)?,
        )
        .map_err(|_| ProviderAttemptState::SizeLimit)?;
        let segment = pcm
            .get(start..end)
            .ok_or(ProviderAttemptState::LocalArtifactFailure)?;
        let segment_path =
            path.with_extension(format!("segment-{}-{}.pcm", range.start(), range.end()));
        let mut file = crate::artifact_privacy::OwnerPrivateFile::new(&segment_path)
            .create_truncated_write()
            .map_err(|_| ProviderAttemptState::LocalArtifactFailure)?;
        file.write_all(segment)
            .map_err(|_| ProviderAttemptState::LocalArtifactFailure)?;
        file.sync_data()
            .map_err(|_| ProviderAttemptState::LocalArtifactFailure)?;
        drop(file);
        let result = self
            .transcriber
            .transcribe(BatchTranscriptionRequest::new_with_input(
                artifact,
                BatchTranscriptionInput::signed_sixteen_bit_little_endian_pcm(
                    segment_path.clone(),
                    format,
                ),
            ));
        let _ = std::fs::remove_file(segment_path);
        result.map_err(|_| ProviderAttemptState::Unavailable)
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
    segments: Vec<PreparedProviderSegment>,
}

/// One ordered durable raw-log range and its provider provenance. The ranges
/// can overlap for seam context; their transcript joins are conservative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedProviderSegment {
    range: SegmentSampleRange,
    attempts: Vec<ProviderAttempt>,
}

impl PreparedProviderSegment {
    pub fn range(&self) -> SegmentSampleRange {
        self.range
    }
    pub fn attempts(&self) -> &[ProviderAttempt] {
        &self.attempts
    }
}

impl PreparedProviderFinalization {
    pub fn transcript(&self) -> &TranscriptText {
        &self.transcript
    }
    pub fn attempts(&self) -> &[ProviderAttempt] {
        &self.attempts
    }
    pub fn segments(&self) -> &[PreparedProviderSegment] {
        &self.segments
    }
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
    pub fn transcript(&self) -> &TranscriptText {
        &self.transcript
    }
    pub fn attempts(&self) -> &[ProviderAttempt] {
        &self.attempts
    }
    pub fn delivery_outcomes(&self) -> &DeliveryOutcomes {
        &self.delivery_outcomes
    }
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
        Self {
            jobs,
            router,
            dispatcher,
            history,
        }
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
        let job = self.jobs.begin(
            session.value().to_string().as_str(),
            artifact.path().as_str(),
            policy,
        )?;
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

        Ok(PreparedProviderFinalization {
            transcript,
            attempts,
            segments: Vec::new(),
        })
    }

    /// Executes completed sample ranges from the committed raw recording log.
    /// Each range is durable before the assembled result, so a retry resumes
    /// finished chunks and repeats only the failed/unattempted range. The raw
    /// log remains the authority until result, history and delivery receipts.
    pub fn prepare_recording_log_segments(
        &self,
        session: &CaptureSession,
        artifact: DurableAudioArtifact,
        policy: ProviderPolicy,
    ) -> Result<PreparedProviderFinalization, ProviderFinalizationError> {
        let job = self.jobs.begin(
            session.value().to_string().as_str(),
            artifact.path().as_str(),
            policy,
        )?;
        let policy = job.policy()?;
        let existing = job
            .segments()?
            .into_iter()
            .map(|segment| ((segment.range().start(), segment.range().end()), segment))
            .collect::<BTreeMap<_, _>>();
        if let Some(text) = job.result()? {
            let segments = existing
                .into_values()
                .map(|segment| PreparedProviderSegment {
                    range: segment.range(),
                    attempts: segment.attempts().to_vec(),
                })
                .collect();
            return Ok(PreparedProviderFinalization {
                transcript: TranscriptText::new(text),
                attempts: Vec::new(),
                segments,
            });
        }

        let recovered = RecordingLog::new(artifact.path().as_str())
            .recover()
            .map_err(|_| ProviderFinalizationError::AllProvidersFailed)?;
        let (format, pcm) = recovered
            .raw_pcm_bytes()
            .map_err(|_| ProviderFinalizationError::AllProvidersFailed)?;
        if format != crate::RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz() {
            return Err(ProviderFinalizationError::AllProvidersFailed);
        }
        let mut transcripts = Vec::new();
        let mut attempts = Vec::new();
        let mut segments = Vec::new();
        for range in plan_raw_pcm_segments(&pcm) {
            let key = (range.start(), range.end());
            if let Some(existing) = existing.get(&key) {
                transcripts.push(existing.transcript().as_str().to_owned());
                attempts.extend_from_slice(existing.attempts());
                segments.push(PreparedProviderSegment {
                    range,
                    attempts: existing.attempts().to_vec(),
                });
                continue;
            }
            let request = ProviderTranscriptRequest::for_sample_range(
                PathBuf::from(artifact.path().as_str()),
                range,
            );
            let outcome = self.router.transcribe(policy.clone(), request);
            let segment_attempts = outcome.attempts().to_vec();
            attempts.extend(segment_attempts.clone());
            let transcript = match outcome.transcript().cloned() {
                Some(transcript) => transcript,
                None => {
                    job.record_attempts(&segment_attempts)?;
                    return Err(ProviderFinalizationError::AllProvidersFailed);
                }
            };
            job.record_segment(range, &segment_attempts, transcript.as_str())?;
            transcripts.push(transcript.as_str().to_owned());
            segments.push(PreparedProviderSegment {
                range,
                attempts: segment_attempts,
            });
        }
        let transcript = TranscriptText::new(stitch_transcripts(&transcripts));
        job.record_result(transcript.as_str())?;
        Ok(PreparedProviderFinalization {
            transcript,
            attempts,
            segments,
        })
    }

    /// Persist the logical history intent only after cancellation no longer
    /// owns the work. The history projection is session-deduplicated, covering
    /// a crash after append and before its durable receipt.
    pub fn record_history(
        &self,
        session: &CaptureSession,
        transcript: &TranscriptText,
    ) -> Result<(), ProviderFinalizationError> {
        let job = self
            .jobs
            .job(session.value().to_string().as_str())?
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
        let job = self
            .jobs
            .job(session.value().to_string().as_str())?
            .ok_or(ProviderFinalizationError::AllProvidersFailed)?;
        let delivery_outcomes = if job.prepare_delivery()? {
            let outcomes =
                self.dispatcher
                    .deliver_for_session(session, output_targets, &transcript);
            job.receipt_delivery()?;
            outcomes
        } else {
            DeliveryOutcomes::new(Vec::new())
        };
        Ok(delivery_outcomes)
    }
}
