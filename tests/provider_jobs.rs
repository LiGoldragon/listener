use std::sync::{Arc, Mutex};

use listener::{
    DurableProviderFinalizer, OutputTargetDispatcher, ProviderAttemptState, ProviderIdentifier,
    ProviderJobStore, ProviderPolicy, ProviderRouter, ProviderTranscriptRequest,
    TranscriptDelivery, TranscriptDeliveryRequest, TranscriptHistoryStore, TranscriptProvider,
};
use signal_listener::{
    AudioArtifactPath, CaptureSession, DeliveryOutcome, DurableAudioArtifact, OutputTarget,
    OutputTargets, TranscriptText, WirePath,
};

#[derive(Clone)]
struct CountingProvider {
    calls: Arc<Mutex<usize>>,
}

impl TranscriptProvider for CountingProvider {
    fn identifier(&self) -> ProviderIdentifier { ProviderIdentifier::OpenAi }

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
        self.0.lock().expect("deliveries").push(request.delivery_id().as_str().to_owned());
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
        ProviderRouter::new(vec![Arc::new(CountingProvider { calls: Arc::clone(&calls) })]),
        OutputTargetDispatcher::new(Box::new(RecordingDelivery(Arc::clone(&deliveries)))),
        TranscriptHistoryStore::new(directory.path().join("history.jsonl")),
    );
    let session = CaptureSession::new(7);
    let artifact = DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
        directory.path().join("capture.webm").to_string_lossy().into_owned(),
    )));
    let policy = ProviderPolicy::new(7, vec![ProviderIdentifier::OpenAi]).expect("policy");
    let targets = OutputTargets::new(vec![OutputTarget::SystemClipboard]);

    let first = finalizer
        .finalize(&session, artifact.clone(), policy.clone(), &targets)
        .expect("first finalization");
    let second = finalizer
        .finalize(&session, artifact, policy, &targets)
        .expect("retry finalization");

    assert_eq!(first.transcript(), &TranscriptText::new("durable transcript"));
    assert_eq!(second.transcript(), first.transcript());
    assert_eq!(*calls.lock().expect("provider calls"), 1);
    assert_eq!(deliveries.lock().expect("deliveries").as_slice(), ["listener:7:system-clipboard"]);
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
    let recovered = reopened.job("capture-7").expect("read durable job").expect("job exists");
    assert_eq!(
        recovered.result().expect("read durable result"),
        Some("transcript without another provider call".into())
    );
    assert!(recovered.prepare_delivery().expect("resume unreceipted delivery"));
    recovered.receipt_delivery().expect("persist delivery receipt");
    assert!(!recovered.prepare_delivery().expect("receipt prevents duplicate delivery"));
}
