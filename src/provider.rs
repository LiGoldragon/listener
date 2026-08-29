//! Typed, redacted transcription-provider routing.
//!
//! The router deliberately owns only policy and attempt state.  Audio remains
//! the durable Listener artifact: a fallback receives the exact same request,
//! never an in-memory or provider-owned copy.

use std::{collections::{BTreeMap, BTreeSet}, path::PathBuf, sync::Arc};

use signal_listener::TranscriptText;

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
        Some(Self { generation, providers })
    }

    pub fn wispr_then_openai() -> Self {
        Self::new(1, vec![ProviderIdentifier::WisprFlow, ProviderIdentifier::OpenAi])
            .expect("the built-in policy is valid")
    }

    pub fn generation(&self) -> u64 { self.generation }
    pub fn providers(&self) -> &[ProviderIdentifier] { &self.providers }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderTranscriptRequest {
    artifact_path: PathBuf,
    preceding_transcript_tail: String,
    vocabulary: Vec<String>,
}

impl ProviderTranscriptRequest {
    pub fn new(artifact_path: PathBuf, preceding_transcript_tail: String, vocabulary: Vec<String>) -> Self {
        Self { artifact_path, preceding_transcript_tail, vocabulary }
    }

    pub fn for_test(path: impl Into<PathBuf>) -> Self {
        Self::new(path.into(), String::new(), Vec::new())
    }

    pub fn artifact_path(&self) -> &PathBuf { &self.artifact_path }
    pub fn preceding_transcript_tail(&self) -> &str { &self.preceding_transcript_tail }
    pub fn vocabulary(&self) -> &[String] { &self.vocabulary }
}

pub trait TranscriptProvider: Send + Sync {
    fn identifier(&self) -> ProviderIdentifier;
    fn transcribe(&self, request: &ProviderTranscriptRequest) -> Result<TranscriptText, ProviderAttemptState>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAttempt {
    provider: ProviderIdentifier,
    artifact_path: PathBuf,
    state: ProviderAttemptState,
}

impl ProviderAttempt {
    fn new(provider: ProviderIdentifier, artifact_path: PathBuf, state: ProviderAttemptState) -> Self {
        Self { provider, artifact_path, state }
    }
    pub fn provider(&self) -> ProviderIdentifier { self.provider }
    pub fn artifact_path(&self) -> &PathBuf { &self.artifact_path }
    pub fn state(&self) -> ProviderAttemptState { self.state }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAttemptOutcome {
    transcript: Option<TranscriptText>,
    attempts: Vec<ProviderAttempt>,
}

impl ProviderAttemptOutcome {
    pub fn transcript(&self) -> Option<&TranscriptText> { self.transcript.as_ref() }
    pub fn attempts(&self) -> &[ProviderAttempt] { &self.attempts }
    pub fn exhausted(attempts: Vec<ProviderAttempt>) -> Self { Self { transcript: None, attempts } }
}

/// A policy router with no provider-specific media or credential knowledge.
#[derive(Clone)]
pub struct ProviderRouter {
    providers: BTreeMap<ProviderIdentifier, Arc<dyn TranscriptProvider>>,
}

impl ProviderRouter {
    pub fn new(providers: Vec<Arc<dyn TranscriptProvider>>) -> Self {
        Self { providers: providers.into_iter().map(|provider| (provider.identifier(), provider)).collect() }
    }

    pub fn transcribe(&self, policy: ProviderPolicy, request: ProviderTranscriptRequest) -> ProviderAttemptOutcome {
        let mut attempts = Vec::new();
        for provider_id in policy.providers() {
            let Some(provider) = self.providers.get(provider_id) else {
                attempts.push(ProviderAttempt::new(*provider_id, request.artifact_path().clone(), ProviderAttemptState::Unavailable));
                continue;
            };
            match provider.transcribe(&request) {
                Ok(transcript) => {
                    attempts.push(ProviderAttempt::new(
                        *provider_id,
                        request.artifact_path().clone(),
                        ProviderAttemptState::Succeeded,
                    ));
                    return ProviderAttemptOutcome { transcript: Some(transcript), attempts };
                }
                Err(state) => {
                    attempts.push(ProviderAttempt::new(*provider_id, request.artifact_path().clone(), state));
                    if !state.permits_fallback() { break; }
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
    fn with_session(&self, use_session: &mut dyn FnMut(&str) -> Result<TranscriptText, ProviderAttemptState>) -> Result<TranscriptText, ProviderAttemptState>;
}

/// Private provider adapter.  The network protocol is injected so tests use
/// synthetic fixtures and production credentials never enter router state.
pub struct WisprFlowProvider {
    session: Arc<dyn WisprSessionBoundary>,
    transport: Arc<dyn WisprFlowTransport>,
}

impl WisprFlowProvider {
    pub fn new(session: Arc<dyn WisprSessionBoundary>, transport: Arc<dyn WisprFlowTransport>) -> Self {
        Self { session, transport }
    }
}

impl TranscriptProvider for WisprFlowProvider {
    fn identifier(&self) -> ProviderIdentifier { ProviderIdentifier::WisprFlow }

    fn transcribe(&self, request: &ProviderTranscriptRequest) -> Result<TranscriptText, ProviderAttemptState> {
        self.session.with_session(&mut |session| self.transport.transcribe_wav(session, request))
    }
}

/// Wispr media/protocol adapter. Implementations must provide 16 kHz mono
/// PCM16 WAV bounded to the segment hard cut, and may retry one transient
/// failure before returning the classified result.
pub trait WisprFlowTransport: Send + Sync {
    fn transcribe_wav(&self, session: &str, request: &ProviderTranscriptRequest) -> Result<TranscriptText, ProviderAttemptState>;
}
