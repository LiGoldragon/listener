//! Private Wispr Flow session and wire boundary.
//!
//! This module contains no credential lookup. A host-provided session source
//! resolves an opaque session at request time, retains it only in memory, and
//! refreshes it under one mutex. Tests use synthetic sessions and a synthetic
//! wire client; Listener never performs a live provider call in its test path.

use std::{path::Path, sync::{Arc, Mutex}, time::Instant};

use signal_listener::TranscriptText;

use crate::{ProviderAttemptState, ProviderTranscriptRequest, WisprFlowTransport, WisprSessionBoundary};

pub(crate) const TRANSCRIBE_STREAM_PATH: &str = "/flow_api.v1.TranscriptionService/TranscribeStream";
pub(crate) const WISPR_SAMPLE_RATE: u32 = 16_000;
pub(crate) const WISPR_MAXIMUM_SAMPLES: u64 = 350 * WISPR_SAMPLE_RATE as u64;

/// An opaque, expiring session. It deliberately exposes no value accessor and
/// does not implement Debug, Serialize, or Clone.
pub(crate) struct WisprSession {
    value: String,
    valid_until: Instant,
}

impl WisprSession {
    pub(crate) fn new(value: String, valid_until: Instant) -> Self { Self { value, valid_until } }
    fn is_current_at(&self, now: Instant) -> bool { self.valid_until > now }
}

/// Supported secret/session boundary. Implementors alone decide how a session
/// is acquired; they must never return it to logs or durable state.
pub(crate) trait WisprSessionSource: Send + Sync {
    fn refresh_session(&self) -> Result<WisprSession, ProviderAttemptState>;
}

struct SessionCache { session: Option<WisprSession> }

/// Single-flight refresh boundary. A rejected/expired session is discarded,
/// refreshed once, and then retried exactly once for the current submission.
pub(crate) struct RefreshingWisprSessionBoundary {
    source: Arc<dyn WisprSessionSource>,
    cache: Mutex<SessionCache>,
}

impl RefreshingWisprSessionBoundary {
    pub(crate) fn new(source: Arc<dyn WisprSessionSource>) -> Self {
        Self { source, cache: Mutex::new(SessionCache { session: None }) }
    }

    fn current_or_refresh<'a>(&self, cache: &'a mut SessionCache) -> Result<&'a WisprSession, ProviderAttemptState> {
        let stale = cache.session.as_ref().is_none_or(|session| !session.is_current_at(Instant::now()));
        if stale { cache.session = Some(self.source.refresh_session()?); }
        Ok(cache.session.as_ref().expect("refresh populated the session"))
    }
}

impl WisprSessionBoundary for RefreshingWisprSessionBoundary {
    fn with_session(&self, use_session: &mut dyn FnMut(&str) -> Result<TranscriptText, ProviderAttemptState>) -> Result<TranscriptText, ProviderAttemptState> {
        let mut cache = self.cache.lock().map_err(|_| ProviderAttemptState::Unavailable)?;
        let first = self.current_or_refresh(&mut cache)?;
        match use_session(&first.value) {
            Err(ProviderAttemptState::AuthenticationExpired) => {
                cache.session = None;
                let refreshed = self.current_or_refresh(&mut cache)?;
                use_session(&refreshed.value)
            }
            result => result,
        }
    }
}

/// A redacted protocol request. Its caller knows the inferred stream path,
/// fresh session/request identifiers, optional context, WAV payload, and final
/// commit; diagnostics contain only typed state.
pub(crate) struct WisprFlowWireRequest {
    session: String,
    request: ProviderTranscriptRequest,
    wav_pcm16: Vec<u8>,
}

impl WisprFlowWireRequest {
    pub(crate) fn new(session: &str, request: ProviderTranscriptRequest, wav_pcm16: Vec<u8>) -> Self {
        Self { session: session.to_owned(), request, wav_pcm16 }
    }
}

/// The only place transport implementations receive the opaque session.
/// Implementors must speak the private inferred streaming route, send init and
/// non-final commit followed by PCM16/WAV and final commit, ignore heartbeats,
/// and select HTML/plain/formatted/raw response text in that order.
pub(crate) trait WisprFlowWireClient: Send + Sync {
    fn transcribe_stream(&self, request: WisprFlowWireRequest) -> Result<TranscriptText, ProviderAttemptState>;
}

/// Provider-specific media adapter: it makes a short PCM16 WAV chunk from the
/// durable source without changing OpenAI's media behavior.
pub(crate) trait WisprMediaAdapter: Send + Sync {
    fn wav_pcm16(&self, artifact: &Path) -> Result<Vec<u8>, ProviderAttemptState>;
}

pub(crate) struct ProtocolWisprFlowTransport {
    media: Arc<dyn WisprMediaAdapter>,
    wire: Arc<dyn WisprFlowWireClient>,
}

impl ProtocolWisprFlowTransport {
    pub(crate) fn new(media: Arc<dyn WisprMediaAdapter>, wire: Arc<dyn WisprFlowWireClient>) -> Self {
        Self { media, wire }
    }
}

impl WisprFlowTransport for ProtocolWisprFlowTransport {
    fn transcribe_wav(&self, session: &str, request: &ProviderTranscriptRequest) -> Result<TranscriptText, ProviderAttemptState> {
        let wav_pcm16 = self.media.wav_pcm16(request.artifact_path())?;
        // The WAV header is 44 bytes. A complete maximum duration segment is
        // 11,200,000 PCM bytes; over-limit media is a provider-specific error.
        if wav_pcm16.len() > 44 + (WISPR_MAXIMUM_SAMPLES as usize * 2) {
            return Err(ProviderAttemptState::SizeLimit);
        }
        let wire_request = WisprFlowWireRequest::new(session, request.clone(), wav_pcm16);
        match self.wire.transcribe_stream(wire_request) {
            Err(ProviderAttemptState::TransientFailure) => {
                // A single retry is safe only before the wire layer has marked
                // the outcome ambiguous; that typed state never retries here.
                let wav_pcm16 = self.media.wav_pcm16(request.artifact_path())?;
                self.wire.transcribe_stream(WisprFlowWireRequest::new(session, request.clone(), wav_pcm16))
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::{Arc, Mutex}, time::Duration};

    use super::*;

    struct SyntheticSessionSource { calls: Arc<Mutex<u8>> }
    impl WisprSessionSource for SyntheticSessionSource {
        fn refresh_session(&self) -> Result<WisprSession, ProviderAttemptState> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            Ok(WisprSession::new(
                format!("synthetic-{calls}"),
                Instant::now() + Duration::from_secs(60),
            ))
        }
    }

    #[test]
    fn expired_session_refreshes_once_and_retries_without_persisting_a_token() {
        let refreshes = Arc::new(Mutex::new(0));
        let boundary = RefreshingWisprSessionBoundary::new(Arc::new(SyntheticSessionSource {
            calls: Arc::clone(&refreshes),
        }));
        let calls = Arc::new(Mutex::new(0));
        let result = boundary.with_session(&mut |_| {
            let mut calls = calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Err(ProviderAttemptState::AuthenticationExpired)
            } else {
                Ok(TranscriptText::new("synthetic"))
            }
        });
        assert_eq!(result.unwrap(), TranscriptText::new("synthetic"));
        assert_eq!(*refreshes.lock().unwrap(), 2);
    }
}
