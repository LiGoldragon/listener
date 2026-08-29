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
pub(crate) const WISPR_GRPC_HOST: &str = "inference.wisprflow.com";
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

/// Opaque request identity needed by the observed protobuf Init message. The
/// boundary returns identifiers only to the encoder; neither values nor the
/// authorization session are persisted or rendered in errors.
pub(crate) trait WisprWireIdentity: Send + Sync {
    fn user_id(&self) -> Result<String, ProviderAttemptState>;
    fn fresh_request_identifiers(&self) -> Result<WisprRequestIdentifiers, ProviderAttemptState>;
}

pub(crate) struct WisprRequestIdentifiers {
    session_id: String,
    request_id: String,
}

impl WisprRequestIdentifiers {
    // Exception: Too trivial. The identity boundary constructs opaque identifiers.
    pub(crate) fn new(session_id: String, request_id: String) -> Self {
        Self { session_id, request_id }
    }
}

/// A fully materialized raw gRPC stream call. The injected boundary owns TLS,
/// HTTP/2 and the actual connection; this type owns only the observed service
/// route, relevant metadata and already-encoded protobuf messages.
pub(crate) struct WisprGrpcStreamCall {
    host: &'static str,
    method: &'static str,
    metadata: Vec<(&'static str, String)>,
    messages: Vec<Vec<u8>>,
}

impl WisprGrpcStreamCall {
    pub(crate) fn host(&self) -> &'static str { self.host }
    pub(crate) fn method(&self) -> &'static str { self.method }
    pub(crate) fn metadata(&self) -> &[(&'static str, String)] { &self.metadata }
    pub(crate) fn messages(&self) -> &[Vec<u8>] { &self.messages }
}

/// Production networking is deliberately outside Listener's provider model.
/// This makes a concrete gRPC call inspectable with synthetic fixtures and
/// prevents tests from issuing a network request.
pub(crate) trait WisprGrpcStreamingBoundary: Send + Sync {
    fn stream(&self, call: WisprGrpcStreamCall) -> Result<Vec<Vec<u8>>, ProviderAttemptState>;
}

/// Concrete encoder/parser for the publicly observed Wispr Flow wire shape.
///
/// Field map provenance: `mathisarends/whisprflow-re` at
/// `2fa262cdd42200df70bef90b59257b29fbc8abaa`, `wisprflow/protocol.py`,
/// `models.py`, and `transport.py`; corroborated against MIT
/// `ThisisShashwat/wisprflow-sdk` at
/// `3faa6e1fd7db3d2563f1e2f4cece93eb4a925d6d`, `_core.py` and
/// `TECHNICAL_DETAILS.md`. This is independently expressed Rust, not copied
/// implementation text.
pub(crate) struct ObservedWisprGrpcClient {
    stream: Arc<dyn WisprGrpcStreamingBoundary>,
    identity: Arc<dyn WisprWireIdentity>,
}

impl ObservedWisprGrpcClient {
    // Exception: Too trivial. Construction joins the two injected boundaries.
    pub(crate) fn new(
        stream: Arc<dyn WisprGrpcStreamingBoundary>,
        identity: Arc<dyn WisprWireIdentity>,
    ) -> Self {
        Self { stream, identity }
    }
}

impl WisprFlowWireClient for ObservedWisprGrpcClient {
    fn transcribe_stream(&self, request: WisprFlowWireRequest) -> Result<TranscriptText, ProviderAttemptState> {
        let user_id = self.identity.user_id()?;
        let identifiers = self.identity.fresh_request_identifiers()?;
        let call = WisprGrpcStreamCall {
            host: WISPR_GRPC_HOST,
            method: TRANSCRIBE_STREAM_PATH,
            metadata: vec![
                ("authorization", format!("Bearer {}", request.session)),
                ("flow-debug", "false".into()),
                ("disable-formatting", "false".into()),
                ("content-type", "application/grpc".into()),
                ("te", "trailers".into()),
            ],
            messages: encode_observed_requests(&user_id, identifiers, &request)?,
        };
        let responses = self.stream.stream(call)?;
        decode_observed_responses(&responses)
    }
}

/// Encodes the inferred Init, optional cursor context, and final audio stream
/// messages. Commit 2 is non-final; Commit 1 seals the PCM16 WAV upload.
fn encode_observed_requests(
    user_id: &str,
    identifiers: WisprRequestIdentifiers,
    request: &WisprFlowWireRequest,
) -> Result<Vec<Vec<u8>>, ProviderAttemptState> {
    if !is_wispr_pcm16_wav(&request.wav_pcm16) {
        return Err(ProviderAttemptState::ProtocolFailure);
    }
    let version = protobuf_concat(&[
        protobuf_integer(1, 1),
        protobuf_integer(2, 6),
        protobuf_integer(3, 606),
    ]);
    let client = protobuf_concat(&[
        protobuf_string(1, "Wispr Flow"),
        protobuf_integer(2, 2),
        protobuf_message(3, &version),
    ]);
    let metadata = protobuf_concat(&[
        protobuf_string(1, user_id),
        protobuf_string(2, &identifiers.session_id),
        protobuf_string(3, &identifiers.request_id),
        protobuf_integer(4, 1),
        protobuf_integer(5, 1),
        protobuf_message(6, &client),
    ]);
    let vocabulary = request
        .request
        .vocabulary()
        .iter()
        .fold(Vec::new(), |mut fields, word| {
            fields.extend(protobuf_string(1, word));
            fields
        });
    let tagging = protobuf_message(3, &protobuf_integer(1, 0));
    let signature = protobuf_message(
        4,
        &protobuf_concat(&[protobuf_integer(2, 0), protobuf_integer(3, 0)]),
    );
    let style = protobuf_concat(&[tagging, signature, protobuf_integer(5, 1)]);
    let preferences = protobuf_concat(&[
        protobuf_message(1, &[]),
        protobuf_message(3, &vocabulary),
        protobuf_message(5, &style),
    ]);
    let init_body = protobuf_concat(&[
        protobuf_message(1, &metadata),
        protobuf_message(2, &preferences),
    ]);
    let init = protobuf_concat(&[protobuf_message(1, &init_body), protobuf_integer(4, 2)]);
    let mut messages = vec![init];
    if !request.request.preceding_transcript_tail().is_empty() {
        let textbox = protobuf_string(2, request.request.preceding_transcript_tail());
        let context = protobuf_message(2, &protobuf_message(2, &textbox));
        messages.push(protobuf_concat(&[context, protobuf_integer(4, 2)]));
    }
    let payload = protobuf_bytes(1, &request.wav_pcm16);
    let audio_file = protobuf_message(2, &payload);
    let audio = protobuf_concat(&[protobuf_message(3, &audio_file), protobuf_integer(4, 1)]);
    messages.push(audio);
    Ok(messages)
}

fn protobuf_varint(mut value: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    while value > 0x7f {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
    encoded
}

fn protobuf_field(number: u64, wire_type: u64, payload: &[u8]) -> Vec<u8> {
    let mut encoded = protobuf_varint((number << 3) | wire_type);
    encoded.extend(payload);
    encoded
}

fn protobuf_integer(number: u64, value: u64) -> Vec<u8> {
    protobuf_field(number, 0, &protobuf_varint(value))
}

fn protobuf_bytes(number: u64, value: &[u8]) -> Vec<u8> {
    let mut payload = protobuf_varint(value.len() as u64);
    payload.extend(value);
    protobuf_field(number, 2, &payload)
}

fn protobuf_string(number: u64, value: &str) -> Vec<u8> { protobuf_bytes(number, value.as_bytes()) }
fn protobuf_message(number: u64, value: &[u8]) -> Vec<u8> { protobuf_bytes(number, value) }
fn protobuf_concat(fields: &[Vec<u8>]) -> Vec<u8> {
    let mut joined = Vec::new();
    for field in fields { joined.extend(field); }
    joined
}

fn decode_observed_responses(responses: &[Vec<u8>]) -> Result<TranscriptText, ProviderAttemptState> {
    let mut best = ObservedResponseText::default();
    for response in responses {
        if response.first() == Some(&0x22) { continue; }
        best.merge(parse_observed_response(response)?);
    }
    best.final_text().map(TranscriptText::new).ok_or(ProviderAttemptState::ProtocolFailure)
}

#[derive(Default)]
struct ObservedResponseText { html: Option<String>, plaintext: Option<String>, formatted: Option<String>, raw: Option<String> }

impl ObservedResponseText {
    fn set_if_nonempty(slot: &mut Option<String>, value: String) { if !value.trim().is_empty() { *slot = Some(clean_observed_text(value)); } }
    fn merge(&mut self, incoming: Self) {
        if let Some(value) = incoming.html { Self::set_if_nonempty(&mut self.html, value); }
        if let Some(value) = incoming.plaintext { Self::set_if_nonempty(&mut self.plaintext, value); }
        if let Some(value) = incoming.formatted { Self::set_if_nonempty(&mut self.formatted, value); }
        if let Some(value) = incoming.raw { Self::set_if_nonempty(&mut self.raw, value); }
    }
    fn final_text(self) -> Option<String> { self.html.or(self.plaintext).or(self.formatted).or(self.raw) }
}

fn clean_observed_text(value: String) -> String {
    value.trim_start_matches('\u{fffd}').trim().chars().take_while(|character| !character.is_control() || *character == '\n' || *character == '\t').collect()
}

fn parse_observed_response(data: &[u8]) -> Result<ObservedResponseText, ProviderAttemptState> {
    let mut output = ObservedResponseText::default();
    for field in protobuf_fields(data)? {
        let ProtobufField::Bytes { number, value } = field else { continue; };
        match number {
            1 => for field in protobuf_fields(value)? {
                match field {
                    ProtobufField::Bytes { number: 1, value } => for field in protobuf_fields(value)? {
                        if let ProtobufField::Bytes { number: 1, value } = field {
                            for field in protobuf_fields(value)? {
                                if let ProtobufField::Bytes { number, value } = field {
                                    if number == 1 { output.html = Some(protobuf_text(value)?); }
                                    if number == 2 { output.plaintext = Some(protobuf_text(value)?); }
                                }
                            }
                        }
                    },
                    _ => {}
                }
            },
            2 => for field in protobuf_fields(value)? {
                if let ProtobufField::Bytes { number, value } = field {
                    if number == 2 { output.raw = Some(first_text_field(value)?); }
                    if number == 3 { output.formatted = Some(first_text_field(value)?); }
                }
            },
            _ => {}
        }
    }
    Ok(output)
}

enum ProtobufField<'a> { Integer { number: u64, value: u64 }, Bytes { number: u64, value: &'a [u8] } }

fn protobuf_fields(data: &[u8]) -> Result<Vec<ProtobufField<'_>>, ProviderAttemptState> {
    let mut fields = Vec::new();
    let mut position = 0;
    while position < data.len() {
        let tag = read_protobuf_varint(data, &mut position)?;
        let number = tag >> 3;
        match tag & 7 {
            0 => fields.push(ProtobufField::Integer { number, value: read_protobuf_varint(data, &mut position)? }),
            2 => {
                let length = read_protobuf_varint(data, &mut position)? as usize;
                let end = position.checked_add(length).filter(|end| *end <= data.len()).ok_or(ProviderAttemptState::ProtocolFailure)?;
                fields.push(ProtobufField::Bytes { number, value: &data[position..end] });
                position = end;
            }
            1 => position = position.checked_add(8).filter(|end| *end <= data.len()).ok_or(ProviderAttemptState::ProtocolFailure)?,
            5 => position = position.checked_add(4).filter(|end| *end <= data.len()).ok_or(ProviderAttemptState::ProtocolFailure)?,
            _ => return Err(ProviderAttemptState::ProtocolFailure),
        }
    }
    Ok(fields)
}

fn read_protobuf_varint(data: &[u8], position: &mut usize) -> Result<u64, ProviderAttemptState> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let byte = *data.get(*position).ok_or(ProviderAttemptState::ProtocolFailure)?;
        *position += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 { return Ok(value); }
    }
    Err(ProviderAttemptState::ProtocolFailure)
}

fn protobuf_text(data: &[u8]) -> Result<String, ProviderAttemptState> { String::from_utf8(data.to_vec()).map_err(|_| ProviderAttemptState::ProtocolFailure) }
fn first_text_field(data: &[u8]) -> Result<String, ProviderAttemptState> {
    protobuf_fields(data)?.into_iter().find_map(|field| match field { ProtobufField::Bytes { number: 1, value } => Some(protobuf_text(value)), _ => None }).transpose()?.ok_or(ProviderAttemptState::ProtocolFailure)
}

fn is_wispr_pcm16_wav(wav: &[u8]) -> bool {
    wav.len() >= 44
        && &wav[..4] == b"RIFF"
        && &wav[8..12] == b"WAVE"
        && &wav[12..16] == b"fmt "
        && u16::from_le_bytes([wav[20], wav[21]]) == 1
        && u16::from_le_bytes([wav[22], wav[23]]) == 1
        && u32::from_le_bytes([wav[24], wav[25], wav[26], wav[27]]) == WISPR_SAMPLE_RATE
        && u16::from_le_bytes([wav[34], wav[35]]) == 16
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
        if !is_wispr_pcm16_wav(&wav_pcm16)
            || wav_pcm16.len() > 44 + (WISPR_MAXIMUM_SAMPLES as usize * 2)
        {
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

    #[test]
    fn concrete_wire_uses_the_observed_grpc_route_and_discards_heartbeats() {
        let stream = SyntheticGrpcStream::with_responses(vec![
            vec![0x22],
            synthetic_result_response("<p>formatted</p>", "plain text"),
        ]);
        let client_stream: Arc<dyn WisprGrpcStreamingBoundary> = stream.clone();
        let client = ObservedWisprGrpcClient::new(
            client_stream,
            Arc::new(SyntheticWireIdentity),
        );
        let text = client
            .transcribe_stream(WisprFlowWireRequest::new(
                "synthetic-session",
                ProviderTranscriptRequest::new(
                    "/durable/audio.wav".into(),
                    "before cursor".into(),
                    vec!["project-name".into()],
                ),
                synthetic_wav(),
            ))
            .expect("synthetic protocol transcript");

        assert_eq!(text, TranscriptText::new("<p>formatted</p>"));
        let calls = stream.calls.lock().expect("synthetic stream calls");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].host(), "inference.wisprflow.com");
        assert_eq!(calls[0].method(), TRANSCRIBE_STREAM_PATH);
        assert_eq!(calls[0].messages().len(), 3);
        assert!(calls[0]
            .metadata()
            .iter()
            .any(|(name, value)| *name == "authorization" && value == "Bearer synthetic-session"));
    }

    struct SyntheticWireIdentity;

    impl WisprWireIdentity for SyntheticWireIdentity {
        fn user_id(&self) -> Result<String, ProviderAttemptState> { Ok("synthetic-user".into()) }

        fn fresh_request_identifiers(&self) -> Result<WisprRequestIdentifiers, ProviderAttemptState> {
            Ok(WisprRequestIdentifiers::new(
                "00000000-0000-4000-8000-000000000001".into(),
                "00000000-0000-4000-8000-000000000002".into(),
            ))
        }
    }

    struct SyntheticGrpcStream { calls: Mutex<Vec<WisprGrpcStreamCall>>, responses: Vec<Vec<u8>> }

    impl SyntheticGrpcStream {
        fn with_responses(responses: Vec<Vec<u8>>) -> Arc<Self> {
            Arc::new(Self { calls: Mutex::new(Vec::new()), responses })
        }
    }

    impl WisprGrpcStreamingBoundary for SyntheticGrpcStream {
        fn stream(&self, call: WisprGrpcStreamCall) -> Result<Vec<Vec<u8>>, ProviderAttemptState> {
            self.calls.lock().expect("synthetic call sink").push(call);
            Ok(self.responses.clone())
        }
    }

    fn synthetic_result_response(html: &str, plaintext: &str) -> Vec<u8> {
        let text = protobuf_concat(&[protobuf_string(1, html), protobuf_string(2, plaintext)]);
        let result = protobuf_message(1, &text);
        protobuf_message(
            1,
            &protobuf_message(
                1,
                &result,
            ),
        )
    }

    fn synthetic_wav() -> Vec<u8> {
        let mut wav = vec![0_u8; 46];
        wav[..4].copy_from_slice(b"RIFF");
        wav[8..12].copy_from_slice(b"WAVE");
        wav[12..16].copy_from_slice(b"fmt ");
        wav[16..20].copy_from_slice(&16_u32.to_le_bytes());
        wav[20..22].copy_from_slice(&1_u16.to_le_bytes());
        wav[22..24].copy_from_slice(&1_u16.to_le_bytes());
        wav[24..28].copy_from_slice(&WISPR_SAMPLE_RATE.to_le_bytes());
        wav[28..32].copy_from_slice(&(WISPR_SAMPLE_RATE * 2).to_le_bytes());
        wav[32..34].copy_from_slice(&2_u16.to_le_bytes());
        wav[34..36].copy_from_slice(&16_u16.to_le_bytes());
        wav[36..40].copy_from_slice(b"data");
        wav[40..44].copy_from_slice(&2_u32.to_le_bytes());
        wav
    }
}
