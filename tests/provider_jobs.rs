use std::sync::{Arc, Mutex};

use listener::{
    DurableProviderFinalizer, OutputTargetDispatcher, ProviderAttemptState, ProviderIdentifier,
    ProviderJobStore, ProviderPolicy, ProviderRouter, ProviderTranscriptRequest,
    RecordingAudioFormat, RecordingInputSource, RecordingLogHeader, RecordingLogWriter,
    RecordingStartTime, TranscriptDelivery, TranscriptDeliveryRequest, TranscriptHistoryStore,
    TranscriptProvider,
};
use signal_listener::{
    AudioArtifactPath, CaptureSession, DeliveryOutcome, DurableAudioArtifact, OutputTarget,
    OutputTargets, TranscriptText, WirePath,
};

#[derive(Clone)]
struct SegmentedProvider {
    identifier: ProviderIdentifier,
    calls: Arc<Mutex<Vec<(ProviderIdentifier, Option<(u64, u64)>)>>>,
    failure_on_second_segment: bool,
}

impl TranscriptProvider for SegmentedProvider {
    fn identifier(&self) -> ProviderIdentifier {
        self.identifier
    }

    fn transcribe(
        &self,
        request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let range = request
            .sample_range()
            .map(|range| (range.start(), range.end()));
        self.calls
            .lock()
            .expect("calls")
            .push((self.identifier, range));
        let index = self
            .calls
            .lock()
            .expect("calls")
            .iter()
            .filter(|(provider, _)| *provider == self.identifier)
            .count();
        if self.failure_on_second_segment && index == 2 {
            return Err(ProviderAttemptState::TransientFailure);
        }
        let text = match (self.identifier, index) {
            (ProviderIdentifier::WisprFlow, 1) => "alpha beta",
            (ProviderIdentifier::OpenAi, _) => "alpha beta gamma",
            _ => "gamma delta",
        };
        Ok(TranscriptText::new(text))
    }
}

#[derive(Clone)]
struct CountingProvider {
    calls: Arc<Mutex<usize>>,
}

impl TranscriptProvider for CountingProvider {
    fn identifier(&self) -> ProviderIdentifier {
        ProviderIdentifier::OpenAi
    }

    fn transcribe(
        &self,
        _request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        *self.calls.lock().expect("provider calls") += 1;
        Ok(TranscriptText::new("durable transcript"))
    }
}

struct RecordingDelivery(Arc<Mutex<Vec<String>>>);

impl TranscriptDelivery for RecordingDelivery {
    fn deliver(&self, request: TranscriptDeliveryRequest) -> DeliveryOutcome {
        self.0
            .lock()
            .expect("deliveries")
            .push(request.delivery_id().as_str().to_owned());
        DeliveryOutcome::delivered(request.target())
    }
}

#[test]
fn finalizer_resumes_the_durable_result_without_another_provider_call_or_history() {
    let directory = tempfile::tempdir().expect("temporary durable finalizer");
    let calls = Arc::new(Mutex::new(0));
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let finalizer = DurableProviderFinalizer::new(
        ProviderJobStore::open(directory.path().join("jobs.sema")).expect("jobs"),
        ProviderRouter::new(vec![Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
        })]),
        OutputTargetDispatcher::new(Box::new(RecordingDelivery(Arc::clone(&deliveries)))),
        TranscriptHistoryStore::new(directory.path().join("history.jsonl")),
    );
    let session = CaptureSession::new(7);
    let artifact = DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
        directory
            .path()
            .join("capture.webm")
            .to_string_lossy()
            .into_owned(),
    )));
    let policy = ProviderPolicy::new(7, vec![ProviderIdentifier::OpenAi]).expect("policy");
    let targets = OutputTargets::new(vec![OutputTarget::SystemClipboard]);

    let first = finalizer
        .finalize(&session, artifact.clone(), policy.clone(), &targets)
        .expect("first finalization");
    let second = finalizer
        .finalize(&session, artifact, policy, &targets)
        .expect("retry finalization");

    assert_eq!(
        first.transcript(),
        &TranscriptText::new("durable transcript")
    );
    assert_eq!(second.transcript(), first.transcript());
    assert_eq!(*calls.lock().expect("provider calls"), 1);
    assert_eq!(
        deliveries.lock().expect("deliveries").as_slice(),
        ["listener:7:system-clipboard"]
    );
}

#[test]
fn a_provider_result_survives_restart_and_retries_only_delivery_work() {
    let directory = tempfile::tempdir().expect("temporary job store");
    let path = directory.path().join("jobs.sema");
    let policy = ProviderPolicy::new(
        7,
        vec![ProviderIdentifier::WisprFlow, ProviderIdentifier::OpenAi],
    )
    .expect("valid provider policy");
    let first = ProviderJobStore::open(&path).expect("open jobs");
    let job = first
        .begin("capture-7", "/durable/capture-7.webm", policy)
        .expect("durable job");
    job.record_result("transcript without another provider call")
        .expect("persisted result");
    assert!(job.prepare_delivery().expect("delivery intent"));
    drop(job);
    drop(first);

    let reopened = ProviderJobStore::open(&path).expect("reopen jobs");
    let recovered = reopened
        .job("capture-7")
        .expect("read durable job")
        .expect("job exists");
    assert_eq!(
        recovered.result().expect("read durable result"),
        Some("transcript without another provider call".into())
    );
    assert!(
        recovered
            .prepare_delivery()
            .expect("resume unreceipted delivery")
    );
    recovered
        .receipt_delivery()
        .expect("persist delivery receipt");
    assert!(
        !recovered
            .prepare_delivery()
            .expect("receipt prevents duplicate delivery")
    );
}

#[test]
fn raw_log_segments_are_lossless_fallback_provenanced_and_stitched_once() {
    let directory = tempfile::tempdir().expect("temporary continuous finalizer");
    let raw_log = directory.path().join("capture.listenerlog");
    let session = CaptureSession::new(41);
    let header = RecordingLogHeader::new(
        session.clone(),
        RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
        RecordingInputSource::SystemDefault,
        RecordingStartTime::from_unix_parts(1_700_000_002, 0),
        8192,
    )
    .expect("header");
    let mut writer = RecordingLogWriter::create(&raw_log, header).expect("raw log");
    let pause_start = 331 * 16_000;
    let samples = 351 * 16_000;
    let pcm = (0..samples)
        .map(|sample| {
            if (pause_start..pause_start + 8_000).contains(&sample) {
                0_i16
            } else {
                1_000_i16
            }
        })
        .flat_map(i16::to_le_bytes)
        .collect::<Vec<_>>();
    for chunk in pcm.chunks(8192) {
        writer
            .append_record(chunk)
            .expect("committed source record");
    }
    writer.finish().expect("finish source log");

    let calls = Arc::new(Mutex::new(Vec::new()));
    let finalizer = DurableProviderFinalizer::new(
        ProviderJobStore::open(directory.path().join("jobs.sema")).expect("jobs"),
        ProviderRouter::new(vec![
            Arc::new(SegmentedProvider {
                identifier: ProviderIdentifier::WisprFlow,
                calls: Arc::clone(&calls),
                failure_on_second_segment: true,
            }),
            Arc::new(SegmentedProvider {
                identifier: ProviderIdentifier::OpenAi,
                calls: Arc::clone(&calls),
                failure_on_second_segment: false,
            }),
        ]),
        OutputTargetDispatcher::new(Box::new(RecordingDelivery(Arc::new(
            Mutex::new(Vec::new()),
        )))),
        TranscriptHistoryStore::new(directory.path().join("history.jsonl")),
    );
    let artifact = DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
        raw_log.to_string_lossy().into_owned(),
    )));
    let policy = ProviderPolicy::wispr_then_openai();

    let prepared = finalizer
        .prepare_recording_log_segments(&session, artifact, policy)
        .expect("segmented finalization");

    assert_eq!(
        prepared.transcript(),
        &TranscriptText::new("alpha beta gamma")
    );
    assert_eq!(prepared.segments().len(), 2);
    assert_eq!(prepared.segments()[0].range().start(), 0);
    assert_eq!(prepared.segments()[0].range().end(), pause_start as u64);
    assert_eq!(
        prepared.segments()[1].range().start(),
        pause_start as u64 - 16_000
    );
    assert_eq!(prepared.segments()[1].range().end(), samples as u64);
    assert_eq!(
        calls.lock().expect("calls").as_slice(),
        &[
            (ProviderIdentifier::WisprFlow, Some((0, pause_start as u64))),
            (
                ProviderIdentifier::WisprFlow,
                Some((pause_start as u64 - 16_000, samples as u64))
            ),
            (
                ProviderIdentifier::OpenAi,
                Some((pause_start as u64 - 16_000, samples as u64))
            ),
        ]
    );
}
