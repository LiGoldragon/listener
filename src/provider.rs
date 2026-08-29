//! Typed, redacted transcription-provider routing.
//!
//! The router deliberately owns only policy and attempt state.  Audio remains
//! the durable Listener artifact: a fallback receives the exact same request,
//! never an in-memory or provider-owned copy.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use signal_listener::TranscriptText;

use crate::SegmentSampleRange;

/// A stable provider identity for policy, provenance, and redacted status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProviderIdentifier {
    WisprFlow,
    OpenAi,
}

impl ProviderIdentifier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WisprFlow => "wispr-flow",
            Self::OpenAi => "openai",
        }
    }
}

/// Ordered provider policy captured with a transcription job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderPolicy {
    generation: u64,
    providers: Vec<ProviderIdentifier>,
}

impl ProviderPolicy {
    pub fn new(generation: u64, providers: Vec<ProviderIdentifier>) -> Option<Self> {
        if providers.is_empty()
            || providers.iter().copied().collect::<BTreeSet<_>>().len() != providers.len()
        {
            return None;
        }
        Some(Self {
            generation,
            providers,
        })
    }

    pub fn wispr_then_openai() -> Self {
        Self::new(
            1,
            vec![ProviderIdentifier::WisprFlow, ProviderIdentifier::OpenAi],
        )
        .expect("the built-in policy is valid")
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn providers(&self) -> &[ProviderIdentifier] {
        &self.providers
    }
}

/// The outcome known before delivery.  It is serializable by the durable job
/// store rather than inferred again after a crash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderAttemptState {
    Succeeded,
    Unavailable,
    Rejected,
    TransientFailure,
    ProtocolFailure,
    SizeLimit,
    AuthenticationExpired,
    Cancelled,
    LocalArtifactFailure,
    AmbiguousAfterSubmit,
}

impl ProviderAttemptState {
    pub fn permits_fallback(self) -> bool {
        !matches!(self, Self::Cancelled | Self::LocalArtifactFailure)
    }
}

/// Redacted provider-health change suitable for status, push, or desktop
/// notification. It has a stable provider and typed state, never provider
/// response text, request identifiers, artifact paths, or credentials.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderHealthEvent {
    Degraded {
        provider: ProviderIdentifier,
        state: ProviderAttemptState,
    },
    Recovered {
        provider: ProviderIdentifier,
    },
}

pub trait ProviderHealthSink: Send + Sync {
    fn publish(&self, event: ProviderHealthEvent);
}

/// One typed event can drive multiple local projections without giving the
/// router access to either transport. Every sink receives the same redacted
/// event value; no provider response, session, or artifact is fanned out.
pub struct ProviderHealthFanout {
    sinks: Vec<Arc<dyn ProviderHealthSink>>,
}

impl ProviderHealthFanout {
    pub fn new(sinks: Vec<Arc<dyn ProviderHealthSink>>) -> Self {
        Self { sinks }
    }
}

impl ProviderHealthSink for ProviderHealthFanout {
    fn publish(&self, event: ProviderHealthEvent) {
        for sink in &self.sinks {
            sink.publish(event);
        }
    }
}

struct SilentProviderHealthSink;
impl ProviderHealthSink for SilentProviderHealthSink {
    fn publish(&self, _event: ProviderHealthEvent) {}
}

#[derive(Clone, Copy)]
enum ProviderCircuitState {
    Closed,
    Open { retry_after: Instant },
    HalfOpen,
}

/// A single-probe circuit breaker. The first eligible caller changes Open to
/// HalfOpen under the mutex; concurrent callers are refused until that one
/// probe records success or failure.
pub struct ProviderCircuitBreaker {
    cooldown: Duration,
    states: Mutex<BTreeMap<ProviderIdentifier, ProviderCircuitState>>,
    sink: Arc<dyn ProviderHealthSink>,
}

impl ProviderCircuitBreaker {
    // Exception: Too trivial. Construction fixes the health event boundary.
    pub fn new(cooldown: Duration, sink: Arc<dyn ProviderHealthSink>) -> Self {
        Self {
            cooldown,
            states: Mutex::new(BTreeMap::new()),
            sink,
        }
    }

    pub fn permit(&self, provider: ProviderIdentifier) -> bool {
        let Ok(mut states) = self.states.lock() else {
            return false;
        };
        match states
            .get(&provider)
            .copied()
            .unwrap_or(ProviderCircuitState::Closed)
        {
            ProviderCircuitState::Closed => true,
            ProviderCircuitState::HalfOpen => false,
            ProviderCircuitState::Open { retry_after } if Instant::now() < retry_after => false,
            ProviderCircuitState::Open { .. } => {
                states.insert(provider, ProviderCircuitState::HalfOpen);
                true
            }
        }
    }

    pub fn record_failure(&self, provider: ProviderIdentifier, state: ProviderAttemptState) {
        let Ok(mut states) = self.states.lock() else {
            return;
        };
        let was_closed = matches!(
            states.get(&provider),
            None | Some(ProviderCircuitState::Closed)
        );
        states.insert(
            provider,
            ProviderCircuitState::Open {
                retry_after: Instant::now() + self.cooldown,
            },
        );
        drop(states);
        if was_closed {
            self.sink
                .publish(ProviderHealthEvent::Degraded { provider, state });
        }
    }

    pub fn record_success(&self, provider: ProviderIdentifier) {
        let Ok(mut states) = self.states.lock() else {
            return;
        };
        let was_degraded = matches!(
            states.get(&provider),
            Some(ProviderCircuitState::Open { .. } | ProviderCircuitState::HalfOpen)
        );
        states.insert(provider, ProviderCircuitState::Closed);
        drop(states);
        if was_degraded {
            self.sink
                .publish(ProviderHealthEvent::Recovered { provider });
        }
    }
}

impl Default for ProviderCircuitBreaker {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), Arc::new(SilentProviderHealthSink))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderTranscriptRequest {
    artifact_path: PathBuf,
    sample_range: Option<SegmentSampleRange>,
    preceding_transcript_tail: String,
    vocabulary: Vec<String>,
}

impl ProviderTranscriptRequest {
    pub fn new(
        artifact_path: PathBuf,
        preceding_transcript_tail: String,
        vocabulary: Vec<String>,
    ) -> Self {
        Self {
            artifact_path,
            sample_range: None,
            preceding_transcript_tail,
            vocabulary,
        }
    }

    pub fn for_test(path: impl Into<PathBuf>) -> Self {
        Self::new(path.into(), String::new(), Vec::new())
    }

    /// A provider request always names the durable capture authority. Segment
    /// bounds are sample indices into that source, never a provider-owned
    /// recording or mutable playback position.
    pub fn for_sample_range(path: impl Into<PathBuf>, sample_range: SegmentSampleRange) -> Self {
        Self {
            artifact_path: path.into(),
            sample_range: Some(sample_range),
            preceding_transcript_tail: String::new(),
            vocabulary: Vec::new(),
        }
    }

    pub fn artifact_path(&self) -> &PathBuf {
        &self.artifact_path
    }
    pub fn sample_range(&self) -> Option<SegmentSampleRange> {
        self.sample_range
    }
    pub fn preceding_transcript_tail(&self) -> &str {
        &self.preceding_transcript_tail
    }
    pub fn vocabulary(&self) -> &[String] {
        &self.vocabulary
    }
}

pub trait TranscriptProvider: Send + Sync {
    fn identifier(&self) -> ProviderIdentifier;
    fn transcribe(
        &self,
        request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAttempt {
    provider: ProviderIdentifier,
    artifact_path: PathBuf,
    sample_range: Option<SegmentSampleRange>,
    state: ProviderAttemptState,
}

impl ProviderAttempt {
    pub(crate) fn new(
        provider: ProviderIdentifier,
        request: &ProviderTranscriptRequest,
        state: ProviderAttemptState,
    ) -> Self {
        Self {
            provider,
            artifact_path: request.artifact_path().clone(),
            sample_range: request.sample_range(),
            state,
        }
    }
    pub fn provider(&self) -> ProviderIdentifier {
        self.provider
    }
    pub fn artifact_path(&self) -> &PathBuf {
        &self.artifact_path
    }
    pub fn sample_range(&self) -> Option<SegmentSampleRange> {
        self.sample_range
    }
    pub fn state(&self) -> ProviderAttemptState {
        self.state
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAttemptOutcome {
    transcript: Option<TranscriptText>,
    attempts: Vec<ProviderAttempt>,
}

impl ProviderAttemptOutcome {
    pub fn transcript(&self) -> Option<&TranscriptText> {
        self.transcript.as_ref()
    }
    pub fn attempts(&self) -> &[ProviderAttempt] {
        &self.attempts
    }
    pub fn exhausted(attempts: Vec<ProviderAttempt>) -> Self {
        Self {
            transcript: None,
            attempts,
        }
    }
}

/// A policy router with no provider-specific media or credential knowledge.
#[derive(Clone)]
pub struct ProviderRouter {
    providers: BTreeMap<ProviderIdentifier, Arc<dyn TranscriptProvider>>,
    circuit_breaker: Arc<ProviderCircuitBreaker>,
}

impl ProviderRouter {
    pub fn new(providers: Vec<Arc<dyn TranscriptProvider>>) -> Self {
        Self::with_circuit_breaker(providers, Arc::new(ProviderCircuitBreaker::default()))
    }

    pub fn with_circuit_breaker(
        providers: Vec<Arc<dyn TranscriptProvider>>,
        circuit_breaker: Arc<ProviderCircuitBreaker>,
    ) -> Self {
        Self {
            providers: providers
                .into_iter()
                .map(|provider| (provider.identifier(), provider))
                .collect(),
            circuit_breaker,
        }
    }

    pub fn transcribe(
        &self,
        policy: ProviderPolicy,
        request: ProviderTranscriptRequest,
    ) -> ProviderAttemptOutcome {
        let mut attempts = Vec::new();
        for provider_id in policy.providers() {
            let Some(provider) = self.providers.get(provider_id) else {
                attempts.push(ProviderAttempt::new(
                    *provider_id,
                    &request,
                    ProviderAttemptState::Unavailable,
                ));
                continue;
            };
            if !self.circuit_breaker.permit(*provider_id) {
                attempts.push(ProviderAttempt::new(
                    *provider_id,
                    &request,
                    ProviderAttemptState::Unavailable,
                ));
                continue;
            }
            match provider.transcribe(&request) {
                Ok(transcript) => {
                    self.circuit_breaker.record_success(*provider_id);
                    attempts.push(ProviderAttempt::new(
                        *provider_id,
                        &request,
                        ProviderAttemptState::Succeeded,
                    ));
                    return ProviderAttemptOutcome {
                        transcript: Some(transcript),
                        attempts,
                    };
                }
                Err(state) => {
                    if state.permits_fallback() {
                        self.circuit_breaker.record_failure(*provider_id, state);
                    }
                    attempts.push(ProviderAttempt::new(*provider_id, &request, state));
                    if !state.permits_fallback() {
                        break;
                    }
                }
            }
        }
        ProviderAttemptOutcome::exhausted(attempts)
    }
}

/// Private boundary for an expiring Wispr Flow session.  Implementations may
/// resolve a session only at request time; this API intentionally has no
/// string getter, Debug implementation, persistence projection, or logging.
pub trait WisprSessionBoundary: Send + Sync {
    fn with_session(
        &self,
        use_session: &mut dyn FnMut(&str) -> Result<TranscriptText, ProviderAttemptState>,
    ) -> Result<TranscriptText, ProviderAttemptState>;
}

/// Private provider adapter.  The network protocol is injected so tests use
/// synthetic fixtures and production credentials never enter router state.
pub struct WisprFlowProvider {
    session: Arc<dyn WisprSessionBoundary>,
    transport: Arc<dyn WisprFlowTransport>,
}

impl WisprFlowProvider {
    pub fn new(
        session: Arc<dyn WisprSessionBoundary>,
        transport: Arc<dyn WisprFlowTransport>,
    ) -> Self {
        Self { session, transport }
    }
}

impl TranscriptProvider for WisprFlowProvider {
    fn identifier(&self) -> ProviderIdentifier {
        ProviderIdentifier::WisprFlow
    }

    fn transcribe(
        &self,
        request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        self.session
            .with_session(&mut |session| self.transport.transcribe_wav(session, request))
    }
}

/// Wispr media/protocol adapter. Implementations must provide 16 kHz mono
/// PCM16 WAV bounded to the segment hard cut, and may retry one transient
/// failure before returning the classified result.
pub trait WisprFlowTransport: Send + Sync {
    fn transcribe_wav(
        &self,
        session: &str,
        request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState>;
}
