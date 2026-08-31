//! Private Wispr Flow session and wire boundary.
//!
//! This module contains no credential lookup. A host-provided session source
//! resolves an opaque session at request time, retains it only in memory, and
//! refreshes it under one mutex. Tests use synthetic sessions and a synthetic
//! wire client; Listener never performs a live provider call in its test path.

use std::{
    fs::File,
    io::Read,
    path::Path,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use signal_listener::TranscriptText;

use crate::{
    ProviderAttemptState, ProviderTranscriptRequest, RecordingLog, TranscriptProvider,
    WisprFlowProvider, WisprFlowTransport, WisprSessionBoundary,
};

pub(crate) const TRANSCRIBE_STREAM_PATH: &str =
    "/flow_api.v1.TranscriptionService/TranscribeStream";
pub(crate) const WISPR_GRPC_HOST: &str = "inference.wisprflow.com";
pub(crate) const WISPR_SAMPLE_RATE: u32 = 16_000;
pub(crate) const WISPR_MAXIMUM_SAMPLES: u64 = 350 * WISPR_SAMPLE_RATE as u64;
const WISPR_SESSION_SECRET_NAME: &str = "wispr-flow/session";
const WISPR_USER_ID_SECRET_NAME: &str = "wispr-flow/user-id";
const WISPR_SESSION_CACHE_LIFETIME: Duration = Duration::from_secs(300);
const WISPR_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
static NEXT_WISPR_REQUEST_IDENTIFIER: AtomicU64 = AtomicU64::new(1);
const DESKTOP_BUNDLE_MAXIMUM_BYTES: u64 = 384 * 1024 * 1024;
const DESKTOP_SESSION_MAXIMUM_BYTES: u64 = 256 * 1024;

/// An opaque, expiring session. It deliberately exposes no value accessor and
/// does not implement Debug, Serialize, or Clone.
pub(crate) struct WisprSession {
    value: String,
    valid_until: Instant,
}

impl WisprSession {
    pub(crate) fn new(value: String, valid_until: Instant) -> Self {
        Self { value, valid_until }
    }
    fn is_current_at(&self, now: Instant) -> bool {
        self.valid_until > now
    }
}

/// Supported secret/session boundary. Implementors alone decide how a session
/// is acquired; they must never return it to logs or durable state.
pub(crate) trait WisprSessionSource: Send + Sync {
    fn refresh_session(&self) -> Result<WisprSession, ProviderAttemptState>;
}

/// Local, request-time secret consumer for the opaque Wispr session. It never
/// stores, logs, serializes, or returns the token beyond `WisprSession`; the
/// gopass entry name is an identifier, not credential material.
pub(crate) struct GopassWisprSessionSource {
    secret_name: &'static str,
}

impl Default for GopassWisprSessionSource {
    fn default() -> Self {
        Self {
            secret_name: WISPR_SESSION_SECRET_NAME,
        }
    }
}

impl WisprSessionSource for GopassWisprSessionSource {
    fn refresh_session(&self) -> Result<WisprSession, ProviderAttemptState> {
        let value = local_secret(self.secret_name)?;
        Ok(WisprSession::new(
            value,
            Instant::now() + WISPR_SESSION_CACHE_LIFETIME,
        ))
    }
}

struct SessionCache {
    session: Option<WisprSession>,
}

/// Single-flight refresh boundary. A rejected/expired session is discarded,
/// refreshed once, and then retried exactly once for the current submission.
pub(crate) struct RefreshingWisprSessionBoundary {
    source: Arc<dyn WisprSessionSource>,
    cache: Mutex<SessionCache>,
}

impl RefreshingWisprSessionBoundary {
    pub(crate) fn new(source: Arc<dyn WisprSessionSource>) -> Self {
        Self {
            source,
            cache: Mutex::new(SessionCache { session: None }),
        }
    }

    fn current_or_refresh<'a>(
        &self,
        cache: &'a mut SessionCache,
    ) -> Result<&'a WisprSession, ProviderAttemptState> {
        let stale = cache
            .session
            .as_ref()
            .is_none_or(|session| !session.is_current_at(Instant::now()));
        if stale {
            cache.session = Some(self.source.refresh_session()?);
        }
        Ok(cache
            .session
            .as_ref()
            .expect("refresh populated the session"))
    }
}

impl WisprSessionBoundary for RefreshingWisprSessionBoundary {
    fn with_session(
        &self,
        use_session: &mut dyn FnMut(&str) -> Result<TranscriptText, ProviderAttemptState>,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| ProviderAttemptState::Unavailable)?;
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

/// The interoperability witness submits exactly once. It never refreshes or
/// retries an authenticated desktop session, so an ambiguous outcome cannot
/// become a second paid submission.
struct OneShotWisprSessionBoundary {
    source: Arc<dyn WisprSessionSource>,
}

impl OneShotWisprSessionBoundary {
    fn new(source: Arc<dyn WisprSessionSource>) -> Self {
        Self { source }
    }
}

impl WisprSessionBoundary for OneShotWisprSessionBoundary {
    fn with_session(
        &self,
        use_session: &mut dyn FnMut(&str) -> Result<TranscriptText, ProviderAttemptState>,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let session = self.source.refresh_session()?;
        use_session(&session.value)
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
    pub(crate) fn new(
        session: &str,
        request: ProviderTranscriptRequest,
        wav_pcm16: Vec<u8>,
    ) -> Self {
        Self {
            session: session.to_owned(),
            request,
            wav_pcm16,
        }
    }
}

/// The only place transport implementations receive the opaque session.
/// Implementors must speak the private streaming route, submit the desktop's
/// combined init/payload/final-commit shape for a one-shot WAV, ignore
/// heartbeats, and select HTML/plain/formatted/raw response text in that order.
pub(crate) trait WisprFlowWireClient: Send + Sync {
    fn transcribe_stream(
        &self,
        request: WisprFlowWireRequest,
    ) -> Result<TranscriptText, ProviderAttemptState>;
}

/// Opaque request identity needed by the observed protobuf Init message. The
/// boundary returns identifiers only to the encoder; neither values nor the
/// authorization session are persisted or rendered in errors.
pub(crate) trait WisprWireIdentity: Send + Sync {
    fn user_id(&self) -> Result<String, ProviderAttemptState>;
    fn fresh_request_identifiers(&self) -> Result<WisprRequestIdentifiers, ProviderAttemptState>;
}

/// The desktop-selected transport destination and its non-user client
/// metadata. This stays private so neither the client credential nor model
/// routing data can enter a status or durable-state surface.
pub(crate) struct WisprGrpcBackend {
    host: String,
    model_id: String,
    environment: String,
    baseten_authorization: Option<String>,
}

impl WisprGrpcBackend {
    fn inferred() -> Self {
        Self {
            host: WISPR_GRPC_HOST.into(),
            model_id: String::new(),
            environment: String::new(),
            baseten_authorization: None,
        }
    }

    fn metadata(&self) -> Vec<(&'static str, String)> {
        let mut metadata = vec![
            ("flow-debug", "false".into()),
            ("disable-formatting", "false".into()),
            ("content-type", "application/grpc".into()),
            ("te", "trailers".into()),
        ];
        if let Some(authorization) = &self.baseten_authorization {
            metadata.extend([
                ("baseten-authorization", authorization.clone()),
                ("baseten-model-id", self.model_id.clone()),
                ("x-baseten-environment", self.environment.clone()),
            ]);
        }
        metadata
    }
}

pub(crate) trait WisprGrpcBackendSource: Send + Sync {
    fn backend(&self) -> Result<WisprGrpcBackend, ProviderAttemptState>;
}

struct FixedWisprGrpcBackendSource;

impl WisprGrpcBackendSource for FixedWisprGrpcBackendSource {
    fn backend(&self) -> Result<WisprGrpcBackend, ProviderAttemptState> {
        Ok(WisprGrpcBackend::inferred())
    }
}

/// Resolves the non-public Wispr identity at submission time through the same
/// local secret boundary. The request/session identifiers are newly generated
/// opaque values and never enter durable provider-job state.
pub(crate) struct GopassWisprWireIdentity {
    user_id_secret_name: &'static str,
}

impl Default for GopassWisprWireIdentity {
    fn default() -> Self {
        Self {
            user_id_secret_name: WISPR_USER_ID_SECRET_NAME,
        }
    }
}

impl WisprWireIdentity for GopassWisprWireIdentity {
    fn user_id(&self) -> Result<String, ProviderAttemptState> {
        local_secret(self.user_id_secret_name)
    }

    fn fresh_request_identifiers(&self) -> Result<WisprRequestIdentifiers, ProviderAttemptState> {
        let sequence = NEXT_WISPR_REQUEST_IDENTIFIER.fetch_add(1, Ordering::Relaxed);
        let epoch_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ProviderAttemptState::Unavailable)?
            .as_nanos();
        Ok(WisprRequestIdentifiers::new(
            format!("listener-{epoch_nanos}-{sequence}"),
            format!("listener-request-{epoch_nanos}-{sequence}"),
        ))
    }
}

pub(crate) struct WisprRequestIdentifiers {
    session_id: String,
    request_id: String,
}

impl WisprRequestIdentifiers {
    fn desktop() -> Result<Self, ProviderAttemptState> {
        let sequence = NEXT_WISPR_REQUEST_IDENTIFIER.fetch_add(1, Ordering::Relaxed) as u128;
        let epoch_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ProviderAttemptState::Unavailable)?
            .as_nanos();
        Ok(Self::new(
            uuid_from_entropy(epoch_nanos, sequence),
            uuid_from_entropy(epoch_nanos.rotate_left(17), sequence.rotate_left(9)),
        ))
    }
}

fn uuid_from_entropy(first: u128, second: u128) -> String {
    let value = first ^ second.rotate_left(37);
    format!(
        "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
        (value >> 96) as u32,
        (value >> 80) as u16,
        ((value >> 64) & 0x0fff) as u16,
        ((value >> 48) & 0x0fff) as u16,
        value & 0x0000_0000_0000_ffff_ffff_ffff_ffff
    )
}

impl WisprRequestIdentifiers {
    // Exception: Too trivial. The identity boundary constructs opaque identifiers.
    pub(crate) fn new(session_id: String, request_id: String) -> Self {
        Self {
            session_id,
            request_id,
        }
    }
}

/// A fully materialized raw gRPC stream call. The injected boundary owns TLS,
/// HTTP/2 and the actual connection; this type owns only the observed service
/// route, relevant metadata and already-encoded protobuf messages.
pub(crate) struct WisprGrpcStreamCall {
    host: String,
    method: &'static str,
    metadata: Vec<(&'static str, String)>,
    messages: Vec<Vec<u8>>,
}

impl WisprGrpcStreamCall {
    pub(crate) fn host(&self) -> &str {
        &self.host
    }
    pub(crate) fn method(&self) -> &'static str {
        self.method
    }
    pub(crate) fn metadata(&self) -> &[(&'static str, String)] {
        &self.metadata
    }
    pub(crate) fn messages(&self) -> &[Vec<u8>] {
        &self.messages
    }
}

/// Production networking is deliberately outside Listener's provider model.
/// This makes a concrete gRPC call inspectable with synthetic fixtures and
/// prevents tests from issuing a network request.
pub(crate) trait WisprGrpcStreamingBoundary: Send + Sync {
    fn stream(&self, call: WisprGrpcStreamCall) -> Result<Vec<Vec<u8>>, ProviderAttemptState>;
}

/// Concrete TLS/HTTP2 gRPC boundary. It is intentionally narrow: call
/// metadata and encoded frames arrive from the private codec, while failures
/// leave as typed routing states without exposing server bodies or request
/// metadata. Tests exercise framing only and never invoke this boundary.
pub(crate) struct ReqwestWisprGrpcStreamingBoundary {
    client: reqwest::blocking::Client,
}

impl Default for ReqwestWisprGrpcStreamingBoundary {
    fn default() -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(WISPR_REQUEST_TIMEOUT)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|_| reqwest::blocking::Client::new());
        Self { client }
    }
}

impl WisprGrpcStreamingBoundary for ReqwestWisprGrpcStreamingBoundary {
    fn stream(&self, call: WisprGrpcStreamCall) -> Result<Vec<Vec<u8>>, ProviderAttemptState> {
        if (call.host != WISPR_GRPC_HOST && !call.host.ends_with(".grpc.api.baseten.co"))
            || call.method != TRANSCRIBE_STREAM_PATH
        {
            return Err(ProviderAttemptState::ProtocolFailure);
        }
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in call.metadata {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ProviderAttemptState::ProtocolFailure)?;
            let value = reqwest::header::HeaderValue::from_str(&value)
                .map_err(|_| ProviderAttemptState::ProtocolFailure)?;
            headers.insert(name, value);
        }
        let request_body = grpc_encode_messages(&call.messages);
        let response = self
            .client
            .post(format!("https://{}{}", call.host, call.method))
            .headers(headers)
            .body(request_body)
            .send()
            .map_err(|_| ProviderAttemptState::TransientFailure)?;
        if !response.status().is_success() {
            return Err(match response.status().as_u16() {
                401 | 403 => ProviderAttemptState::AuthenticationExpired,
                408 | 429 | 500..=599 => ProviderAttemptState::TransientFailure,
                _ => ProviderAttemptState::Rejected,
            });
        }
        let bytes = response
            .bytes()
            .map_err(|_| ProviderAttemptState::TransientFailure)?;
        grpc_decode_messages(&bytes)
    }
}

fn grpc_encode_messages(messages: &[Vec<u8>]) -> Vec<u8> {
    let capacity = messages.iter().map(|message| message.len() + 5).sum();
    let mut encoded = Vec::with_capacity(capacity);
    for message in messages {
        encoded.push(0);
        encoded.extend_from_slice(&(message.len() as u32).to_be_bytes());
        encoded.extend_from_slice(message);
    }
    encoded
}

fn grpc_decode_messages(bytes: &[u8]) -> Result<Vec<Vec<u8>>, ProviderAttemptState> {
    let mut messages = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        let compressed = *bytes
            .get(position)
            .ok_or(ProviderAttemptState::ProtocolFailure)?;
        position += 1;
        if compressed != 0 {
            return Err(ProviderAttemptState::ProtocolFailure);
        }
        let size = bytes
            .get(position..position + 4)
            .ok_or(ProviderAttemptState::ProtocolFailure)
            .map(|length| u32::from_be_bytes(length.try_into().expect("four bytes")) as usize)?;
        position += 4;
        let end = position
            .checked_add(size)
            .ok_or(ProviderAttemptState::ProtocolFailure)?;
        let message = bytes
            .get(position..end)
            .ok_or(ProviderAttemptState::ProtocolFailure)?;
        messages.push(message.to_vec());
        position = end;
    }
    Ok(messages)
}

fn local_secret(secret_name: &str) -> Result<String, ProviderAttemptState> {
    let output = Command::new("gopass")
        .args(["show", "-o", secret_name])
        .output()
        .map_err(|_| ProviderAttemptState::Unavailable)?;
    if !output.status.success() {
        return Err(ProviderAttemptState::Unavailable);
    }
    let value = String::from_utf8(output.stdout)
        .map_err(|_| ProviderAttemptState::Unavailable)?
        .trim()
        .to_owned();
    if value.is_empty() {
        return Err(ProviderAttemptState::Unavailable);
    }
    Ok(value)
}

/// Session material acquired by the sandbox consumer. It deliberately has no
/// Debug or serialization implementation, and is retained only while one
/// submission is being assembled.
struct DesktopWisprSession {
    access_token: String,
    user_id: String,
}

/// Reads the authenticated desktop session through an already-open descriptor.
/// The shell opens the protected file but never reads, transforms, or receives
/// its contents; this consumer selects the Supabase or WorkOS token in memory.
struct InheritedFdDesktopWisprSession {
    descriptor: i32,
    session: Mutex<Option<DesktopWisprSession>>,
}

impl InheritedFdDesktopWisprSession {
    fn new(descriptor: i32) -> Result<Self, ProviderAttemptState> {
        if descriptor < 3 {
            return Err(ProviderAttemptState::Unavailable);
        }
        Ok(Self {
            descriptor,
            session: Mutex::new(None),
        })
    }

    fn session(&self) -> Result<(String, String), ProviderAttemptState> {
        let mut cached = self
            .session
            .lock()
            .map_err(|_| ProviderAttemptState::Unavailable)?;
        if cached.is_none() {
            let bytes = inherited_descriptor_bytes(self.descriptor, DESKTOP_SESSION_MAXIMUM_BYTES)?;
            *cached = Some(desktop_session_from_json(&bytes)?);
        }
        let session = cached.as_ref().expect("desktop session was populated");
        Ok((session.access_token.clone(), session.user_id.clone()))
    }
}

impl WisprSessionSource for InheritedFdDesktopWisprSession {
    fn refresh_session(&self) -> Result<WisprSession, ProviderAttemptState> {
        let (access_token, _) = self.session()?;
        Ok(WisprSession::new(
            access_token,
            Instant::now() + WISPR_SESSION_CACHE_LIFETIME,
        ))
    }
}

impl WisprWireIdentity for InheritedFdDesktopWisprSession {
    fn user_id(&self) -> Result<String, ProviderAttemptState> {
        let (_, user_id) = self.session()?;
        Ok(user_id)
    }

    fn fresh_request_identifiers(&self) -> Result<WisprRequestIdentifiers, ProviderAttemptState> {
        WisprRequestIdentifiers::desktop()
    }
}

fn inherited_descriptor_bytes(
    descriptor: i32,
    maximum: u64,
) -> Result<Vec<u8>, ProviderAttemptState> {
    let path = format!("/proc/self/fd/{descriptor}");
    let mut file = File::open(path).map_err(|_| ProviderAttemptState::Unavailable)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ProviderAttemptState::Unavailable)?;
    if bytes.len() as u64 > maximum {
        return Err(ProviderAttemptState::Unavailable);
    }
    Ok(bytes)
}

fn desktop_session_from_json(bytes: &[u8]) -> Result<DesktopWisprSession, ProviderAttemptState> {
    let document: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| ProviderAttemptState::Unavailable)?;
    let access_token = first_json_string(&document, "access_token")
        .or_else(|| first_json_string(&document, "accessToken"))
        .ok_or(ProviderAttemptState::Unavailable)?;
    let user_id = desktop_user_identifier(&document)
        .or_else(|| jwt_subject(&access_token))
        .ok_or(ProviderAttemptState::Unavailable)?;
    if access_token.is_empty() || user_id.is_empty() {
        return Err(ProviderAttemptState::Unavailable);
    }
    Ok(DesktopWisprSession {
        access_token,
        user_id,
    })
}

fn desktop_user_identifier(document: &serde_json::Value) -> Option<String> {
    first_json_string(document, "user_id")
        .or_else(|| first_json_string(document, "userId"))
        .or_else(|| {
            ["user", "currentUser", "profile", "identity"]
                .into_iter()
                .filter_map(|key| document.get(key))
                .find_map(|value| {
                    first_json_string(value, "id")
                        .or_else(|| first_json_string(value, "user_id"))
                        .or_else(|| first_json_string(value, "userId"))
                })
        })
}

fn first_json_string(document: &serde_json::Value, key: &str) -> Option<String> {
    match document {
        serde_json::Value::Object(fields) => fields
            .get(key)
            .and_then(|value| value.as_str())
            .map(str::to_owned)
            .or_else(|| {
                fields
                    .values()
                    .find_map(|value| first_json_string(value, key))
            }),
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|value| first_json_string(value, key)),
        serde_json::Value::String(value) => serde_json::from_str(value)
            .ok()
            .and_then(|nested| first_json_string(&nested, key)),
        _ => None,
    }
}

fn jwt_subject(token: &str) -> Option<String> {
    use base64::{
        Engine as _,
        engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
    };

    let encoded_payload = token.split('.').nth(1)?;
    let payload = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .or_else(|_| URL_SAFE.decode(encoded_payload))
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    value.get("sub")?.as_str().map(str::to_owned)
}

/// Non-user desktop backend material selected from a packaged app bundle. The
/// static client authorization never leaves this value or reaches logs,
/// environment, arguments, the clipboard, or durable sandbox state.
struct DesktopWisprBackendSource {
    backend: WisprGrpcBackend,
}

impl DesktopWisprBackendSource {
    fn new(descriptor: i32) -> Result<Self, ProviderAttemptState> {
        if descriptor < 3 {
            return Err(ProviderAttemptState::Unavailable);
        }
        Ok(Self {
            backend: desktop_backend_from_bundle(&inherited_descriptor_bytes(
                descriptor,
                DESKTOP_BUNDLE_MAXIMUM_BYTES,
            )?)?,
        })
    }
}

impl WisprGrpcBackendSource for DesktopWisprBackendSource {
    fn backend(&self) -> Result<WisprGrpcBackend, ProviderAttemptState> {
        Ok(WisprGrpcBackend {
            host: self.backend.host.clone(),
            model_id: self.backend.model_id.clone(),
            environment: self.backend.environment.clone(),
            baseten_authorization: self.backend.baseten_authorization.clone(),
        })
    }
}

fn desktop_backend_from_bundle(bytes: &[u8]) -> Result<WisprGrpcBackend, ProviderAttemptState> {
    let source = String::from_utf8_lossy(bytes);
    let model_id = bundled_qwen_model_id(&source)?;
    let property = bundled_baseten_property(&source)?;
    let key = bundled_exported_string(&source, property)?;
    Ok(WisprGrpcBackend {
        host: format!("model-{model_id}.grpc.api.baseten.co"),
        model_id: format!("model-{model_id}"),
        environment: "production".into(),
        baseten_authorization: Some(format!("Api-Key {key}")),
    })
}

fn bundled_qwen_model_id(source: &str) -> Result<String, ProviderAttemptState> {
    let mappings = source
        .split_once("ASR_VARIANT_BASETEN_MODEL_IDS={")
        .and_then(|(_, remaining)| remaining.split_once("};"))
        .map(|(mappings, _)| mappings)
        .ok_or(ProviderAttemptState::ProtocolFailure)?;
    let (_, value) = mappings
        .split_once("QwenHttp]:\"")
        .ok_or(ProviderAttemptState::ProtocolFailure)?;
    value
        .split_once('"')
        .map(|(model_id, _)| model_id.to_owned())
        .filter(|model_id| !model_id.is_empty())
        .ok_or(ProviderAttemptState::ProtocolFailure)
}

fn bundled_baseten_property(source: &str) -> Result<&str, ProviderAttemptState> {
    let (_, remaining) = source
        .split_once("\"baseten-authorization\":`Api-Key ${")
        .ok_or(ProviderAttemptState::ProtocolFailure)?;
    let (expression, _) = remaining
        .split_once('}')
        .ok_or(ProviderAttemptState::ProtocolFailure)?;
    expression
        .rsplit_once('.')
        .map(|(_, property)| property)
        .filter(|property| !property.is_empty())
        .ok_or(ProviderAttemptState::ProtocolFailure)
}

fn bundled_exported_string(source: &str, property: &str) -> Result<String, ProviderAttemptState> {
    let export = format!("{property}:()=>");
    let (_, remaining) = source
        .split_once(&export)
        .ok_or(ProviderAttemptState::ProtocolFailure)?;
    let identifier: String = remaining
        .chars()
        .take_while(|character| {
            character.is_ascii_alphanumeric() || *character == '_' || *character == '$'
        })
        .collect();
    if identifier.is_empty() {
        return Err(ProviderAttemptState::ProtocolFailure);
    }
    for declaration in ["const", "let", "var"] {
        let binding = format!("{declaration} {identifier}=\"");
        if let Some((_, value)) = source.split_once(&binding) {
            if let Some((key, _)) = value.split_once('"') {
                if !key.is_empty() {
                    return Ok(key.to_owned());
                }
            }
        }
    }
    Err(ProviderAttemptState::ProtocolFailure)
}

/// The production host gets a fully concrete private provider, while its
/// secret/session values remain request-local. The provider's public router
/// interface has no credential getter or persistence projection.
pub(crate) fn production_wispr_provider() -> Arc<dyn TranscriptProvider> {
    let session = Arc::new(RefreshingWisprSessionBoundary::new(Arc::new(
        GopassWisprSessionSource::default(),
    )));
    let wire = Arc::new(ObservedWisprGrpcClient::new(
        Arc::new(ReqwestWisprGrpcStreamingBoundary::default()),
        Arc::new(GopassWisprWireIdentity::default()),
        Arc::new(FixedWisprGrpcBackendSource),
    ));
    let transport = Arc::new(ProtocolWisprFlowTransport::new(
        Arc::new(RecordingLogWisprMediaAdapter),
        wire,
    ));
    Arc::new(WisprFlowProvider::new(session, transport))
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
    backend: Arc<dyn WisprGrpcBackendSource>,
}

impl ObservedWisprGrpcClient {
    // Exception: Too trivial. Construction joins the two injected boundaries.
    pub(crate) fn new(
        stream: Arc<dyn WisprGrpcStreamingBoundary>,
        identity: Arc<dyn WisprWireIdentity>,
        backend: Arc<dyn WisprGrpcBackendSource>,
    ) -> Self {
        Self {
            stream,
            identity,
            backend,
        }
    }
}

impl WisprFlowWireClient for ObservedWisprGrpcClient {
    fn transcribe_stream(
        &self,
        request: WisprFlowWireRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let user_id = self.identity.user_id()?;
        let identifiers = self.identity.fresh_request_identifiers()?;
        let backend = self.backend.backend()?;
        let mut metadata = backend.metadata();
        metadata.push(("authorization", format!("Bearer {}", request.session)));
        let call = WisprGrpcStreamCall {
            host: backend.host,
            method: TRANSCRIBE_STREAM_PATH,
            metadata,
            messages: encode_observed_requests(&user_id, identifiers, &request)?,
        };
        let responses = self.stream.stream(call)?;
        decode_observed_responses(&responses)
    }
}

/// Encodes the desktop's one-shot Init, optional cursor context, PCM16/WAV
/// payload, and final Commit=True in one stream message.
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
        protobuf_integer(3, 7),
    ]);
    let client = protobuf_concat(&[
        protobuf_string(1, "Wispr Flow"),
        protobuf_integer(2, 0),
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
    let mut request_fields = vec![protobuf_message(1, &init_body)];
    if !request.request.preceding_transcript_tail().is_empty() {
        let textbox = protobuf_string(2, request.request.preceding_transcript_tail());
        let context = protobuf_message(2, &protobuf_message(2, &textbox));
        request_fields.push(context);
    }
    let payload = protobuf_bytes(1, &request.wav_pcm16);
    let audio_file = protobuf_message(2, &payload);
    request_fields.push(protobuf_message(3, &audio_file));
    request_fields.push(protobuf_integer(4, 1));
    Ok(vec![protobuf_concat(&request_fields)])
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

fn protobuf_string(number: u64, value: &str) -> Vec<u8> {
    protobuf_bytes(number, value.as_bytes())
}
fn protobuf_message(number: u64, value: &[u8]) -> Vec<u8> {
    protobuf_bytes(number, value)
}
fn protobuf_concat(fields: &[Vec<u8>]) -> Vec<u8> {
    let mut joined = Vec::new();
    for field in fields {
        joined.extend(field);
    }
    joined
}

fn decode_observed_responses(
    responses: &[Vec<u8>],
) -> Result<TranscriptText, ProviderAttemptState> {
    let mut best = ObservedResponseText::default();
    for response in responses {
        if response.first() == Some(&0x22) {
            continue;
        }
        best.merge(parse_observed_response(response)?);
    }
    best.final_text()
        .map(TranscriptText::new)
        .ok_or(ProviderAttemptState::ProtocolFailure)
}

#[derive(Default)]
struct ObservedResponseText {
    html: Option<String>,
    plaintext: Option<String>,
    formatted: Option<String>,
    raw: Option<String>,
}

impl ObservedResponseText {
    fn set_if_nonempty(slot: &mut Option<String>, value: String) {
        if !value.trim().is_empty() {
            *slot = Some(clean_observed_text(value));
        }
    }
    fn merge(&mut self, incoming: Self) {
        if let Some(value) = incoming.html {
            Self::set_if_nonempty(&mut self.html, value);
        }
        if let Some(value) = incoming.plaintext {
            Self::set_if_nonempty(&mut self.plaintext, value);
        }
        if let Some(value) = incoming.formatted {
            Self::set_if_nonempty(&mut self.formatted, value);
        }
        if let Some(value) = incoming.raw {
            Self::set_if_nonempty(&mut self.raw, value);
        }
    }
    fn final_text(self) -> Option<String> {
        self.html.or(self.plaintext).or(self.formatted).or(self.raw)
    }
}

fn clean_observed_text(value: String) -> String {
    value
        .trim_start_matches('\u{fffd}')
        .trim()
        .chars()
        .take_while(|character| !character.is_control() || *character == '\n' || *character == '\t')
        .collect()
}

fn parse_observed_response(data: &[u8]) -> Result<ObservedResponseText, ProviderAttemptState> {
    let mut output = ObservedResponseText::default();
    for field in protobuf_fields(data)? {
        let ProtobufField::Bytes { number, value } = field else {
            continue;
        };
        match number {
            1 => {
                for field in protobuf_fields(value)? {
                    match field {
                        ProtobufField::Bytes { number: 1, value } => {
                            for field in protobuf_fields(value)? {
                                if let ProtobufField::Bytes { number: 1, value } = field {
                                    for field in protobuf_fields(value)? {
                                        if let ProtobufField::Bytes { number, value } = field {
                                            if number == 1 {
                                                output.html = Some(protobuf_text(value)?);
                                            }
                                            if number == 2 {
                                                output.plaintext = Some(protobuf_text(value)?);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            2 => {
                for field in protobuf_fields(value)? {
                    if let ProtobufField::Bytes { number, value } = field {
                        if number == 2 {
                            output.raw = Some(first_text_field(value)?);
                        }
                        if number == 3 {
                            output.formatted = Some(first_text_field(value)?);
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(output)
}

enum ProtobufField<'a> {
    Integer { number: u64, value: u64 },
    Bytes { number: u64, value: &'a [u8] },
}

fn protobuf_fields(data: &[u8]) -> Result<Vec<ProtobufField<'_>>, ProviderAttemptState> {
    let mut fields = Vec::new();
    let mut position = 0;
    while position < data.len() {
        let tag = read_protobuf_varint(data, &mut position)?;
        let number = tag >> 3;
        match tag & 7 {
            0 => fields.push(ProtobufField::Integer {
                number,
                value: read_protobuf_varint(data, &mut position)?,
            }),
            2 => {
                let length = read_protobuf_varint(data, &mut position)? as usize;
                let end = position
                    .checked_add(length)
                    .filter(|end| *end <= data.len())
                    .ok_or(ProviderAttemptState::ProtocolFailure)?;
                fields.push(ProtobufField::Bytes {
                    number,
                    value: &data[position..end],
                });
                position = end;
            }
            1 => {
                position = position
                    .checked_add(8)
                    .filter(|end| *end <= data.len())
                    .ok_or(ProviderAttemptState::ProtocolFailure)?
            }
            5 => {
                position = position
                    .checked_add(4)
                    .filter(|end| *end <= data.len())
                    .ok_or(ProviderAttemptState::ProtocolFailure)?
            }
            _ => return Err(ProviderAttemptState::ProtocolFailure),
        }
    }
    Ok(fields)
}

fn read_protobuf_varint(data: &[u8], position: &mut usize) -> Result<u64, ProviderAttemptState> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let byte = *data
            .get(*position)
            .ok_or(ProviderAttemptState::ProtocolFailure)?;
        *position += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(ProviderAttemptState::ProtocolFailure)
}

fn protobuf_text(data: &[u8]) -> Result<String, ProviderAttemptState> {
    String::from_utf8(data.to_vec()).map_err(|_| ProviderAttemptState::ProtocolFailure)
}
fn first_text_field(data: &[u8]) -> Result<String, ProviderAttemptState> {
    protobuf_fields(data)?
        .into_iter()
        .find_map(|field| match field {
            ProtobufField::Bytes { number: 1, value } => Some(protobuf_text(value)),
            _ => None,
        })
        .transpose()?
        .ok_or(ProviderAttemptState::ProtocolFailure)
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
    fn wav_pcm16(
        &self,
        request: &ProviderTranscriptRequest,
    ) -> Result<Vec<u8>, ProviderAttemptState>;
}

/// Converts Listener's committed recording-log PCM authority directly to the
/// observed provider's 16 kHz mono signed-16-bit WAV. It has no network or
/// credential access and rejects any incompatible recording before encoding.
pub(crate) struct RecordingLogWisprMediaAdapter;

impl WisprMediaAdapter for RecordingLogWisprMediaAdapter {
    fn wav_pcm16(
        &self,
        request: &ProviderTranscriptRequest,
    ) -> Result<Vec<u8>, ProviderAttemptState> {
        let recovered = RecordingLog::new(request.artifact_path())
            .recover()
            .map_err(|_| ProviderAttemptState::LocalArtifactFailure)?;
        let (format, pcm) = recovered
            .raw_pcm_bytes()
            .map_err(|_| ProviderAttemptState::LocalArtifactFailure)?;
        if format.sample_rate() != WISPR_SAMPLE_RATE
            || format.channel_count() != 1
            || format.sample_format() != crate::RecordingSampleFormat::SignedSixteenBitLittleEndian
            || pcm.len() % 2 != 0
        {
            return Err(ProviderAttemptState::LocalArtifactFailure);
        }
        let pcm = match request.sample_range() {
            Some(range) => {
                let start = usize::try_from(
                    range
                        .start()
                        .checked_mul(2)
                        .ok_or(ProviderAttemptState::SizeLimit)?,
                )
                .map_err(|_| ProviderAttemptState::SizeLimit)?;
                let end = usize::try_from(
                    range
                        .end()
                        .checked_mul(2)
                        .ok_or(ProviderAttemptState::SizeLimit)?,
                )
                .map_err(|_| ProviderAttemptState::SizeLimit)?;
                pcm.get(start..end)
                    .ok_or(ProviderAttemptState::LocalArtifactFailure)?
                    .to_vec()
            }
            None => pcm,
        };
        let data_length = u32::try_from(pcm.len()).map_err(|_| ProviderAttemptState::SizeLimit)?;
        let riff_length = data_length
            .checked_add(36)
            .ok_or(ProviderAttemptState::SizeLimit)?;
        let mut wav = Vec::with_capacity(44 + pcm.len());
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&riff_length.to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16_u32.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&WISPR_SAMPLE_RATE.to_le_bytes());
        wav.extend_from_slice(&(WISPR_SAMPLE_RATE * 2).to_le_bytes());
        wav.extend_from_slice(&2_u16.to_le_bytes());
        wav.extend_from_slice(&16_u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_length.to_le_bytes());
        wav.extend_from_slice(&pcm);
        Ok(wav)
    }
}

pub(crate) struct ProtocolWisprFlowTransport {
    media: Arc<dyn WisprMediaAdapter>,
    wire: Arc<dyn WisprFlowWireClient>,
}

impl ProtocolWisprFlowTransport {
    pub(crate) fn new(
        media: Arc<dyn WisprMediaAdapter>,
        wire: Arc<dyn WisprFlowWireClient>,
    ) -> Self {
        Self { media, wire }
    }
}

impl WisprFlowTransport for ProtocolWisprFlowTransport {
    fn transcribe_wav(
        &self,
        session: &str,
        request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let wav_pcm16 = self.media.wav_pcm16(request)?;
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
                let wav_pcm16 = self.media.wav_pcm16(request)?;
                self.wire.transcribe_stream(WisprFlowWireRequest::new(
                    session,
                    request.clone(),
                    wav_pcm16,
                ))
            }
            result => result,
        }
    }
}

/// A non-retrying transport reserved for the explicit sandbox witness. The
/// normal provider transport may retry an unsubmitted transient error; this
/// one cannot distinguish that case from an accepted server submission, so it
/// reports the first result exactly as observed.
struct OneShotProtocolWisprFlowTransport {
    media: Arc<dyn WisprMediaAdapter>,
    wire: Arc<dyn WisprFlowWireClient>,
}

impl OneShotProtocolWisprFlowTransport {
    fn new(media: Arc<dyn WisprMediaAdapter>, wire: Arc<dyn WisprFlowWireClient>) -> Self {
        Self { media, wire }
    }
}

impl WisprFlowTransport for OneShotProtocolWisprFlowTransport {
    fn transcribe_wav(
        &self,
        session: &str,
        request: &ProviderTranscriptRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let wav_pcm16 = self.media.wav_pcm16(request)?;
        if !is_wispr_pcm16_wav(&wav_pcm16)
            || wav_pcm16.len() > 44 + (WISPR_MAXIMUM_SAMPLES as usize * 2)
        {
            return Err(ProviderAttemptState::SizeLimit);
        }
        self.wire.transcribe_stream(WisprFlowWireRequest::new(
            session,
            request.clone(),
            wav_pcm16,
        ))
    }
}

/// Runs one isolated, Wispr-only provider submission against a supplied WAV.
/// It does not construct ListenerRuntime, open audio devices or Listener
/// sockets, create a capture/history store, access the system clipboard, or
/// route to any fallback provider.
pub fn sandbox_wispr_transcribe(
    session_descriptor: i32,
    desktop_bundle_descriptor: i32,
    artifact_path: &Path,
) -> Result<TranscriptText, ProviderAttemptState> {
    let session = Arc::new(InheritedFdDesktopWisprSession::new(session_descriptor)?);
    let session_source: Arc<dyn WisprSessionSource> = session.clone();
    let identity: Arc<dyn WisprWireIdentity> = session;
    let wire = Arc::new(ObservedWisprGrpcClient::new(
        Arc::new(ReqwestWisprGrpcStreamingBoundary::default()),
        identity,
        Arc::new(DesktopWisprBackendSource::new(desktop_bundle_descriptor)?),
    ));
    let transport = Arc::new(OneShotProtocolWisprFlowTransport::new(
        Arc::new(RecordingLogWisprMediaAdapter),
        wire,
    ));
    WisprFlowProvider::new(
        Arc::new(OneShotWisprSessionBoundary::new(session_source)),
        transport,
    )
    .transcribe(&ProviderTranscriptRequest::for_test(artifact_path))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;

    struct SyntheticSessionSource {
        calls: Arc<Mutex<u8>>,
    }
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
            Arc::new(FixedWisprGrpcBackendSource),
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
        assert_eq!(calls[0].messages().len(), 1);
        assert!(
            calls[0].metadata().iter().any(
                |(name, value)| *name == "authorization" && value == "Bearer synthetic-session"
            )
        );
    }

    #[test]
    fn desktop_backend_descriptor_selects_the_packaged_default_without_exposing_its_key() {
        let descriptor = desktop_backend_from_bundle(
            br#"const basetenApiKey="synthetic-client-key";const Rt={Fo:()=>basetenApiKey};class G{static ASR_VARIANT_BASETEN_MODEL_IDS={[xt.eW.QwenHttp]:"synthetic-model"};static getRpcOptions(){return {"baseten-authorization":`Api-Key ${Rt.Fo}`}}}"#,
        )
        .expect("synthetic desktop bundle descriptor");

        assert_eq!(descriptor.host, "model-synthetic-model.grpc.api.baseten.co");
        assert_eq!(descriptor.model_id, "model-synthetic-model");
        assert_eq!(descriptor.environment, "production");
    }

    #[test]
    fn grpc_framing_round_trips_without_contacting_a_provider() {
        let encoded = grpc_encode_messages(&[vec![1, 2], vec![3, 4, 5]]);
        assert_eq!(
            grpc_decode_messages(&encoded),
            Ok(vec![vec![1, 2], vec![3, 4, 5]])
        );
        assert_eq!(
            grpc_decode_messages(&[1, 0, 0, 0, 0]),
            Err(ProviderAttemptState::ProtocolFailure),
        );
    }

    struct SyntheticWireIdentity;

    impl WisprWireIdentity for SyntheticWireIdentity {
        fn user_id(&self) -> Result<String, ProviderAttemptState> {
            Ok("synthetic-user".into())
        }

        fn fresh_request_identifiers(
            &self,
        ) -> Result<WisprRequestIdentifiers, ProviderAttemptState> {
            Ok(WisprRequestIdentifiers::new(
                "00000000-0000-4000-8000-000000000001".into(),
                "00000000-0000-4000-8000-000000000002".into(),
            ))
        }
    }

    struct SyntheticGrpcStream {
        calls: Mutex<Vec<WisprGrpcStreamCall>>,
        responses: Vec<Vec<u8>>,
    }

    impl SyntheticGrpcStream {
        fn with_responses(responses: Vec<Vec<u8>>) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                responses,
            })
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
        protobuf_message(1, &protobuf_message(1, &result))
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
