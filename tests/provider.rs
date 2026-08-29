use std::sync::{Arc, Mutex};

use listener::{
    ProviderAttemptOutcome, ProviderAttemptState, ProviderIdentifier, ProviderPolicy,
    ProviderRouter, ProviderTranscriptRequest, TranscriptProvider,
};
use signal_listener::TranscriptText;

#[derive(Clone)]
struct FakeProvider {
    identifier: ProviderIdentifier,
    result: Result<TranscriptText, ProviderAttemptState>,
    attempts: Arc<Mutex<Vec<String>>>,
}

impl TranscriptProvider for FakeProvider {
    fn identifier(&self) -> ProviderIdentifier {
        self.identifier
    }

    fn transcribe(
        &self,
        _request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        self.attempts
            .lock()
            .expect("test attempt recorder")
            .push(self.identifier.as_str().to_owned());
        self.result.clone()
    }
}

#[test]
fn falls_back_in_policy_order_using_the_same_durable_artifact() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let router = ProviderRouter::new(vec![
        Arc::new(FakeProvider {
            identifier: ProviderIdentifier::WisprFlow,
            result: Err(ProviderAttemptState::Unavailable),
            attempts: Arc::clone(&attempts),
        }),
        Arc::new(FakeProvider {
            identifier: ProviderIdentifier::OpenAi,
            result: Ok(TranscriptText::new("fallback transcript")),
            attempts: Arc::clone(&attempts),
        }),
    ]);
    let request = ProviderTranscriptRequest::for_test("/durable/capture-7.listenerlog");

    let outcome = router.transcribe(ProviderPolicy::wispr_then_openai(), request.clone());

    assert_eq!(attempts.lock().unwrap().as_slice(), ["wispr-flow", "openai"]);
    assert_eq!(outcome.transcript(), Some(&TranscriptText::new("fallback transcript")));
    assert_eq!(outcome.attempts()[0].artifact_path(), request.artifact_path());
    assert_eq!(outcome.attempts()[1].artifact_path(), request.artifact_path());
    assert_eq!(outcome.attempts()[0].state(), ProviderAttemptState::Unavailable);
}

#[test]
fn cancellation_and_local_artifact_failure_do_not_fall_back() {
    for state in [ProviderAttemptState::Cancelled, ProviderAttemptState::LocalArtifactFailure] {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let router = ProviderRouter::new(vec![
            Arc::new(FakeProvider {
                identifier: ProviderIdentifier::WisprFlow,
                result: Err(state),
                attempts: Arc::clone(&attempts),
            }),
            Arc::new(FakeProvider {
                identifier: ProviderIdentifier::OpenAi,
                result: Ok(TranscriptText::new("must not run")),
                attempts: Arc::clone(&attempts),
            }),
        ]);

        let outcome = router.transcribe(ProviderPolicy::wispr_then_openai(), ProviderTranscriptRequest::for_test("/durable/a"));

        assert_eq!(attempts.lock().unwrap().as_slice(), ["wispr-flow"]);
        assert_eq!(outcome, ProviderAttemptOutcome::exhausted(vec![outcome.attempts()[0].clone()]));
    }
}

#[test]
fn ambiguous_submission_is_explicit_and_never_retried_by_the_router() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let router = ProviderRouter::new(vec![
        Arc::new(FakeProvider {
            identifier: ProviderIdentifier::WisprFlow,
            result: Err(ProviderAttemptState::AmbiguousAfterSubmit),
            attempts: Arc::clone(&attempts),
        }),
        Arc::new(FakeProvider {
            identifier: ProviderIdentifier::OpenAi,
            result: Ok(TranscriptText::new("fallback transcript")),
            attempts: Arc::clone(&attempts),
        }),
    ]);

    let outcome = router.transcribe(ProviderPolicy::wispr_then_openai(), ProviderTranscriptRequest::for_test("/durable/a"));

    assert_eq!(attempts.lock().unwrap().as_slice(), ["wispr-flow", "openai"]);
    assert_eq!(outcome.attempts()[0].state(), ProviderAttemptState::AmbiguousAfterSubmit);
}
