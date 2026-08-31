//! Private Wispr Flow session and wire boundary.
//!
//! This module contains no credential lookup. A host-provided session source
//! resolves an opaque session at request time, retains it only in memory, and
//! refreshes it under one mutex. Tests use synthetic sessions and a synthetic
//! wire client; Listener never performs a live provider call in its test path.

use std::{
    collections::BTreeMap,
    fs::File,
    future::Future,
    io::Read,
    net::SocketAddr,
    path::Path,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use serde::Serialize;
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
    #[cfg(test)]
    pub(crate) fn host(&self) -> &str {
        &self.host
    }
    #[cfg(test)]
    pub(crate) fn method(&self) -> &'static str {
        self.method
    }
    #[cfg(test)]
    pub(crate) fn metadata(&self) -> &[(&'static str, String)] {
        &self.metadata
    }
    #[cfg(test)]
    pub(crate) fn messages(&self) -> &[Vec<u8>] {
        &self.messages
    }
}

/// Production networking is deliberately outside Listener's provider model.
/// This makes a concrete gRPC call inspectable with synthetic fixtures and
/// prevents tests from issuing a network request.
pub(crate) trait WisprGrpcStreamingBoundary: Send + Sync {
    fn stream(
        &self,
        call: WisprGrpcStreamCall,
    ) -> Result<WisprGrpcStreamResponse, ProviderAttemptState>;
}

pub(crate) struct WisprGrpcStreamResponse {
    messages: Vec<Vec<u8>>,
    http_status: Option<u16>,
    grpc_status: Option<u32>,
    content_type: Option<String>,
    permission_category: PermissionCategory,
}

impl WisprGrpcStreamResponse {
    fn provider_state(&self) -> Result<(), ProviderAttemptState> {
        match (self.http_status, self.grpc_status) {
            (Some(200..=299), None | Some(0)) => Ok(()),
            (Some(401 | 403), _) | (_, Some(16)) => {
                Err(ProviderAttemptState::AuthenticationExpired)
            }
            (Some(408 | 429 | 500..=599), _) | (_, Some(4 | 8 | 14)) => {
                Err(ProviderAttemptState::TransientFailure)
            }
            _ => Err(ProviderAttemptState::Rejected),
        }
    }

    fn diagnostics(
        &self,
        stage: &'static str,
        bearer_state: BearerState,
    ) -> WisprWitnessDiagnostics {
        WisprWitnessDiagnostics::from_response(stage, bearer_state, self)
    }
}

/// The only bearer-lifetime classification allowed to cross the sandbox
/// boundary. Numeric expiry and provider evidence are consumed locally.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum BearerState {
    Fresh,
    NearExpiry,
    Expired,
    Unknown,
}

/// The only authorization-message classification allowed to cross the sandbox
/// boundary. The original gRPC message is consumed locally and discarded.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum PermissionCategory {
    Absent,
    BearerAuth,
    BasetenApiKey,
    ModelEntitlement,
    GenericPermission,
}

/// The sandbox's sole durable/output shape. It deliberately excludes every
/// request value and every response value; only numeric and structural facts
/// cross from the authenticated consumer into its caller.
#[derive(Serialize)]
pub struct WisprWitnessDiagnostics {
    local_stage: &'static str,
    http_status: Option<u16>,
    grpc_status: Option<u32>,
    content_type: Option<String>,
    bearer_state: BearerState,
    permission_category: PermissionCategory,
    response_frame_count: usize,
    response_frame_lengths: Vec<usize>,
    protobuf_top_level_tag_histograms: Vec<Vec<WisprProtobufTagCount>>,
}

/// A shared, static-only progress marker for the isolated witness. It is
/// deliberately unable to accept dynamic text, so it can cross the owned
/// runtime thread without widening the diagnostics boundary.
#[derive(Clone)]
pub struct WisprWitnessCheckpoint {
    stage: Arc<Mutex<&'static str>>,
}

impl WisprWitnessCheckpoint {
    pub fn new(stage: &'static str) -> Self {
        Self {
            stage: Arc::new(Mutex::new(stage)),
        }
    }

    pub fn advance(&self, stage: &'static str) {
        if let Ok(mut current) = self.stage.lock() {
            *current = stage;
        }
    }

    pub fn stage(&self) -> &'static str {
        self.stage
            .lock()
            .map(|current| *current)
            .unwrap_or("checkpoint-unavailable")
    }
}

#[derive(Serialize)]
struct WisprProtobufTagCount {
    tag: u64,
    count: usize,
}

impl WisprWitnessDiagnostics {
    /// Produces a structural-only outcome before any response can exist.
    ///
    /// Callers must use a static local-stage label; no error value belongs in
    /// the sandbox's durable diagnostics boundary.
    pub fn setup_failure(stage: &'static str) -> Self {
        Self::at(stage)
    }

    /// Whether the witness reached response headers. This is intentionally a
    /// boolean so the sandbox runner can select its exit status without
    /// inspecting or retaining any response value.
    pub fn has_completed_response(&self) -> bool {
        self.http_status.is_some()
    }

    fn at(stage: &'static str) -> Self {
        Self {
            local_stage: stage,
            http_status: None,
            grpc_status: None,
            content_type: None,
            bearer_state: BearerState::Unknown,
            permission_category: PermissionCategory::Absent,
            response_frame_count: 0,
            response_frame_lengths: Vec::new(),
            protobuf_top_level_tag_histograms: Vec::new(),
        }
    }

    fn from_response(
        stage: &'static str,
        bearer_state: BearerState,
        response: &WisprGrpcStreamResponse,
    ) -> Self {
        Self {
            local_stage: stage,
            http_status: response.http_status,
            grpc_status: response.grpc_status,
            content_type: response.content_type.clone(),
            bearer_state,
            permission_category: response.permission_category,
            response_frame_count: response.messages.len(),
            response_frame_lengths: response.messages.iter().map(Vec::len).collect(),
            protobuf_top_level_tag_histograms: response
                .messages
                .iter()
                .map(|message| protobuf_top_level_tag_histogram(message))
                .collect(),
        }
    }
}

fn grpc_message_permission_category(value: Option<&str>) -> PermissionCategory {
    let Some(value) = value else {
        return PermissionCategory::Absent;
    };
    let value = value.to_ascii_lowercase();
    if value.contains("baseten")
        && (value.contains("api-key") || value.contains("api key") || value.contains("apikey"))
    {
        PermissionCategory::BasetenApiKey
    } else if value.contains("model") && value.contains("entitlement") {
        PermissionCategory::ModelEntitlement
    } else if value.contains("bearer") {
        PermissionCategory::BearerAuth
    } else if ["permission", "unauthorized", "forbidden", "denied"]
        .into_iter()
        .any(|indicator| value.contains(indicator))
    {
        PermissionCategory::GenericPermission
    } else {
        PermissionCategory::Absent
    }
}

fn protobuf_top_level_tag_histogram(message: &[u8]) -> Vec<WisprProtobufTagCount> {
    let mut counts = BTreeMap::new();
    if let Ok(fields) = protobuf_fields(message) {
        for field in fields {
            let number = match field {
                ProtobufField::Integer { number } | ProtobufField::Bytes { number, .. } => number,
            };
            *counts.entry(number).or_insert(0) += 1;
        }
    }
    counts
        .into_iter()
        .map(|(tag, count)| WisprProtobufTagCount { tag, count })
        .collect()
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
    fn stream(
        &self,
        call: WisprGrpcStreamCall,
    ) -> Result<WisprGrpcStreamResponse, ProviderAttemptState> {
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
        let http_status = Some(response.status().as_u16());
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let grpc_status = response
            .headers()
            .get("grpc-status")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let permission_category = grpc_message_permission_category(
            response
                .headers()
                .get("grpc-message")
                .and_then(|value| value.to_str().ok()),
        );
        let bytes = response
            .bytes()
            .map_err(|_| ProviderAttemptState::TransientFailure)?;
        Ok(WisprGrpcStreamResponse {
            messages: grpc_decode_messages(&bytes)?,
            http_status,
            grpc_status,
            content_type,
            permission_category,
        })
    }
}

/// Native TLS/HTTP2 gRPC transport used only by the explicit sandbox witness.
/// It submits each protobuf request as its own gRPC message and half-closes
/// only after Commit=True. The h2 connection is driven independently while the
/// request stream is open, so server frames are not coupled to HTTP/1 buffering.
struct NativeWisprGrpcStreamingBoundary;

fn run_wispr_runtime<T: Send + 'static>(
    checkpoint: &WisprWitnessCheckpoint,
    future: impl Future<Output = Result<T, ProviderAttemptState>> + Send + 'static,
) -> Result<T, ProviderAttemptState> {
    checkpoint.advance("runtime-thread-spawn");
    let child_checkpoint = checkpoint.clone();
    std::thread::Builder::new()
        .name("listener-wispr-runtime".into())
        .spawn(move || {
            child_checkpoint.advance("runtime-child-entry");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .map_err(|_| ProviderAttemptState::Unavailable)?;
            child_checkpoint.advance("runtime-build");
            runtime.block_on(future)
        })
        .map_err(|_| ProviderAttemptState::Unavailable)?
        .join()
        .map_err(|_| ProviderAttemptState::Unavailable)?
}

impl NativeWisprGrpcStreamingBoundary {
    fn stream_checkpointed(
        &self,
        call: WisprGrpcStreamCall,
        checkpoint: &WisprWitnessCheckpoint,
    ) -> Result<WisprGrpcStreamResponse, ProviderAttemptState> {
        let child_checkpoint = checkpoint.clone();
        run_wispr_runtime(checkpoint, async move {
            tokio::time::timeout(
                WISPR_REQUEST_TIMEOUT,
                native_grpc_stream(call, child_checkpoint),
            )
            .await
            .map_err(|_| ProviderAttemptState::TransientFailure)?
        })
    }
}

impl WisprGrpcStreamingBoundary for NativeWisprGrpcStreamingBoundary {
    fn stream(
        &self,
        call: WisprGrpcStreamCall,
    ) -> Result<WisprGrpcStreamResponse, ProviderAttemptState> {
        let checkpoint = WisprWitnessCheckpoint::new("request-encoding");
        self.stream_checkpointed(call, &checkpoint)
    }
}

async fn native_grpc_stream(
    call: WisprGrpcStreamCall,
    checkpoint: WisprWitnessCheckpoint,
) -> Result<WisprGrpcStreamResponse, ProviderAttemptState> {
    checkpoint.advance("future-enter");
    tokio::task::yield_now().await;
    checkpoint.advance("future-first-poll");
    if (call.host != WISPR_GRPC_HOST && !call.host.ends_with(".grpc.api.baseten.co"))
        || call.method != TRANSCRIBE_STREAM_PATH
    {
        return Err(ProviderAttemptState::ProtocolFailure);
    }

    let addresses = resolve_wispr_addresses(&call.host, &checkpoint).await?;
    let tcp = dial_wispr_addresses(&addresses, &checkpoint).await?;
    let connector = wispr_tls_connector(&checkpoint)?;
    let server_name = wispr_server_name(call.host.clone(), &checkpoint)?;
    checkpoint.advance("tls-handshake-attempted");
    let tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(|_| ProviderAttemptState::TransientFailure)?;
    checkpoint.advance("tls-handshake-completed");
    checkpoint.advance("tls-alpn-verification-attempted");
    if tls.get_ref().1.alpn_protocol() != Some(b"h2") {
        return Err(ProviderAttemptState::ProtocolFailure);
    }
    checkpoint.advance("tls-alpn-verified");

    let (sender, connection) = h2::client::handshake(tls)
        .await
        .map_err(|_| ProviderAttemptState::TransientFailure)?;
    checkpoint.advance("http2-handshake");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut sender = sender
        .ready()
        .await
        .map_err(|_| ProviderAttemptState::TransientFailure)?;
    checkpoint.advance("http2-ready");
    let request = wispr_h2_request(&call, &checkpoint)?;
    checkpoint.advance("http2-send-request-attempted");
    let (response, mut upload) = sender
        .send_request(request, false)
        .map_err(|_| ProviderAttemptState::TransientFailure)?;
    checkpoint.advance("http2-send-request-completed");
    checkpoint.advance("http2-body-stream-opened");
    for message in &call.messages {
        native_send_grpc_message(&mut upload, message).await?;
        checkpoint.advance(wispr_outbound_frame_stage(message));
    }
    checkpoint.advance("http2-body-half-close-attempted");
    upload
        .send_data(Bytes::new(), true)
        .map_err(|_| ProviderAttemptState::TransientFailure)?;
    checkpoint.advance("http2-body-half-close-completed");

    let response = response
        .await
        .map_err(|_| ProviderAttemptState::TransientFailure)?;
    checkpoint.advance("response-headers");
    let (head, mut body) = response.into_parts();
    let http_status = Some(head.status.as_u16());
    let content_type = head
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut grpc_status = head
        .headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let mut permission_category = grpc_message_permission_category(
        head.headers
            .get("grpc-message")
            .and_then(|value| value.to_str().ok()),
    );
    let mut flow_control = body.flow_control().clone();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|_| ProviderAttemptState::TransientFailure)?;
        flow_control
            .release_capacity(chunk.len())
            .map_err(|_| ProviderAttemptState::ProtocolFailure)?;
        bytes.extend_from_slice(&chunk);
    }
    checkpoint.advance("response-body");
    if let Some(trailers) = body
        .trailers()
        .await
        .map_err(|_| ProviderAttemptState::TransientFailure)?
    {
        grpc_status = trailers
            .get("grpc-status")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .or(grpc_status);
        let trailer_permission_category = grpc_message_permission_category(
            trailers
                .get("grpc-message")
                .and_then(|value| value.to_str().ok()),
        );
        if trailer_permission_category != PermissionCategory::Absent {
            permission_category = trailer_permission_category;
        }
    }
    checkpoint.advance("response-trailers");
    Ok(WisprGrpcStreamResponse {
        messages: grpc_decode_messages(&bytes)?,
        http_status,
        grpc_status,
        content_type,
        permission_category,
    })
}

async fn resolve_wispr_addresses(
    host: &str,
    checkpoint: &WisprWitnessCheckpoint,
) -> Result<Vec<SocketAddr>, ProviderAttemptState> {
    checkpoint.advance("dns-resolution-attempted");
    let addresses = tokio::net::lookup_host((host, 443))
        .await
        .map_err(|_| ProviderAttemptState::TransientFailure)?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(ProviderAttemptState::TransientFailure);
    }
    checkpoint.advance("dns-resolution-completed");
    Ok(addresses)
}

async fn dial_wispr_addresses(
    addresses: &[SocketAddr],
    checkpoint: &WisprWitnessCheckpoint,
) -> Result<tokio::net::TcpStream, ProviderAttemptState> {
    checkpoint.advance("tcp-dial-attempted");
    let tcp = tokio::net::TcpStream::connect(addresses)
        .await
        .map_err(|_| ProviderAttemptState::TransientFailure)?;
    checkpoint.advance("tcp-dial-connected");
    Ok(tcp)
}

fn wispr_tls_connector(
    checkpoint: &WisprWitnessCheckpoint,
) -> Result<tokio_rustls::TlsConnector, ProviderAttemptState> {
    checkpoint.advance("tls-configuration-attempted");
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut configuration = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| ProviderAttemptState::ProtocolFailure)?
    .with_root_certificates(roots)
    .with_no_client_auth();
    configuration.alpn_protocols = vec![b"h2".to_vec()];
    checkpoint.advance("tls-configuration-completed");
    let connector = tokio_rustls::TlsConnector::from(Arc::new(configuration));
    checkpoint.advance("tls-connector-ready");
    Ok(connector)
}

fn wispr_server_name(
    host: String,
    checkpoint: &WisprWitnessCheckpoint,
) -> Result<rustls::pki_types::ServerName<'static>, ProviderAttemptState> {
    checkpoint.advance("tls-server-name-attempted");
    let server_name = rustls::pki_types::ServerName::try_from(host)
        .map_err(|_| ProviderAttemptState::ProtocolFailure)?;
    checkpoint.advance("tls-server-name-completed");
    Ok(server_name)
}

fn wispr_h2_request(
    call: &WisprGrpcStreamCall,
    checkpoint: &WisprWitnessCheckpoint,
) -> Result<http::Request<()>, ProviderAttemptState> {
    checkpoint.advance("http2-request-uri-attempted");
    let uri = http::Uri::builder()
        .scheme("https")
        .authority(call.host.as_str())
        .path_and_query(call.method)
        .build()
        .map_err(|_| ProviderAttemptState::ProtocolFailure)?;
    checkpoint.advance("http2-request-uri-completed");
    checkpoint.advance("http2-request-headers-attempted");
    let mut builder = http::Request::builder()
        .method(http::Method::POST)
        .uri(uri)
        .version(http::Version::HTTP_2)
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    for (name, value) in &call.metadata {
        builder = builder.header(*name, value);
    }
    checkpoint.advance("http2-request-headers-completed");
    let request = builder
        .body(())
        .map_err(|_| ProviderAttemptState::ProtocolFailure)?;
    checkpoint.advance("http2-request-built");
    Ok(request)
}

fn wispr_outbound_frame_stage(message: &[u8]) -> &'static str {
    match message.first() {
        Some(0x0a) => "client-init-frame-sent",
        Some(0x12) => "client-context-frame-sent",
        Some(0x1a) => "client-audio-frame-sent",
        Some(0x20) => "client-commit-frame-sent",
        _ => "client-unclassified-frame-sent",
    }
}

async fn native_send_grpc_message(
    upload: &mut h2::SendStream<Bytes>,
    message: &[u8],
) -> Result<(), ProviderAttemptState> {
    let mut frame = Bytes::from(grpc_encode_message(message));
    upload.reserve_capacity(frame.len());
    while !frame.is_empty() {
        let available = std::future::poll_fn(|context| upload.poll_capacity(context))
            .await
            .ok_or(ProviderAttemptState::TransientFailure)?
            .map_err(|_| ProviderAttemptState::TransientFailure)?;
        let length = available.min(frame.len());
        if length == 0 {
            continue;
        }
        let chunk = frame.split_to(length);
        upload
            .send_data(chunk, false)
            .map_err(|_| ProviderAttemptState::TransientFailure)?;
        upload.reserve_capacity(frame.len());
    }
    Ok(())
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

fn grpc_encode_message(message: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(message.len() + 5);
    encoded.push(0);
    encoded.extend_from_slice(&(message.len() as u32).to_be_bytes());
    encoded.extend_from_slice(message);
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
    bearer_state: BearerState,
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

    fn session(&self) -> Result<(String, String, BearerState), ProviderAttemptState> {
        let mut cached = self
            .session
            .lock()
            .map_err(|_| ProviderAttemptState::Unavailable)?;
        if cached.is_none() {
            let bytes = inherited_descriptor_bytes(self.descriptor, DESKTOP_SESSION_MAXIMUM_BYTES)?;
            *cached = Some(desktop_session_from_json(&bytes)?);
        }
        let session = cached.as_ref().expect("desktop session was populated");
        Ok((
            session.access_token.clone(),
            session.user_id.clone(),
            session.bearer_state,
        ))
    }
}

impl WisprSessionSource for InheritedFdDesktopWisprSession {
    fn refresh_session(&self) -> Result<WisprSession, ProviderAttemptState> {
        let (access_token, _, _) = self.session()?;
        Ok(WisprSession::new(
            access_token,
            Instant::now() + WISPR_SESSION_CACHE_LIFETIME,
        ))
    }
}

impl WisprWireIdentity for InheritedFdDesktopWisprSession {
    fn user_id(&self) -> Result<String, ProviderAttemptState> {
        let (_, user_id, _) = self.session()?;
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
    let bearer_state = desktop_bearer_state(&document, &access_token, SystemTime::now());
    drop(document);
    Ok(DesktopWisprSession {
        access_token,
        user_id,
        bearer_state,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DesktopAuthProvider {
    Supabase,
    Workos,
}

fn desktop_bearer_state(
    document: &serde_json::Value,
    access_token: &str,
    observed_at: SystemTime,
) -> BearerState {
    let provider = desktop_auth_provider(document, access_token);
    let Some(expiry_milliseconds) = desktop_expiry_milliseconds(document, access_token, provider)
    else {
        return BearerState::Unknown;
    };
    let Ok(now_milliseconds) = observed_at.duration_since(UNIX_EPOCH) else {
        return BearerState::Unknown;
    };
    let now_milliseconds = now_milliseconds.as_millis();
    if expiry_milliseconds <= now_milliseconds {
        return BearerState::Expired;
    }
    let refresh_window_milliseconds = match provider {
        DesktopAuthProvider::Supabase => 90_000,
        DesktopAuthProvider::Workos => 120_000,
    };
    if expiry_milliseconds - now_milliseconds <= refresh_window_milliseconds {
        BearerState::NearExpiry
    } else {
        BearerState::Fresh
    }
}

fn desktop_auth_provider(document: &serde_json::Value, access_token: &str) -> DesktopAuthProvider {
    if first_json_string(document, "authProviderId")
        .is_some_and(|provider| provider.eq_ignore_ascii_case("workos"))
        || jwt_issuer_is_workos(access_token)
    {
        DesktopAuthProvider::Workos
    } else {
        DesktopAuthProvider::Supabase
    }
}

fn desktop_expiry_milliseconds(
    document: &serde_json::Value,
    access_token: &str,
    provider: DesktopAuthProvider,
) -> Option<u128> {
    let session_expiry = match provider {
        DesktopAuthProvider::Supabase => first_json_u64(document, "expires_at")
            .and_then(seconds_to_milliseconds)
            .or_else(|| jwt_claim_u64(access_token, "exp").and_then(seconds_to_milliseconds))
            .or_else(|| first_json_u64(document, "expiresAt").map(u128::from)),
        DesktopAuthProvider::Workos => first_json_u64(document, "expiresAt")
            .map(u128::from)
            .or_else(|| first_json_u64(document, "expires_at").and_then(seconds_to_milliseconds))
            .or_else(|| jwt_claim_u64(access_token, "exp").and_then(seconds_to_milliseconds)),
    };
    session_expiry
}

fn seconds_to_milliseconds(seconds: u64) -> Option<u128> {
    u128::from(seconds).checked_mul(1_000)
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

fn first_json_u64(document: &serde_json::Value, key: &str) -> Option<u64> {
    match document {
        serde_json::Value::Object(fields) => fields
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .or_else(|| fields.values().find_map(|value| first_json_u64(value, key))),
        serde_json::Value::Array(values) => {
            values.iter().find_map(|value| first_json_u64(value, key))
        }
        _ => None,
    }
}

fn jwt_subject(token: &str) -> Option<String> {
    jwt_claim_string(token, "sub")
}

fn jwt_issuer_is_workos(token: &str) -> bool {
    jwt_claim_string(token, "iss").is_some_and(|issuer| {
        let issuer = issuer.to_ascii_lowercase();
        issuer.contains("workos") || issuer.contains("authkit")
    })
}

fn jwt_claim_string(token: &str, claim: &str) -> Option<String> {
    jwt_payload(token)?.get(claim)?.as_str().map(str::to_owned)
}

fn jwt_claim_u64(token: &str, claim: &str) -> Option<u64> {
    jwt_payload(token)?.get(claim)?.as_u64()
}

fn jwt_payload(token: &str) -> Option<serde_json::Value> {
    use base64::{
        engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
        Engine as _,
    };

    let encoded_payload = token.split('.').nth(1)?;
    let payload = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .or_else(|_| URL_SAFE.decode(encoded_payload))
        .ok()?;
    serde_json::from_slice(&payload).ok()
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
    let model_id = bundled_default_model_id(&source)?;
    let property = bundled_baseten_property(&source)?;
    let key = bundled_exported_string(&source, property)?;
    Ok(WisprGrpcBackend {
        host: format!("model-{model_id}.grpc.api.baseten.co"),
        model_id: format!("model-{model_id}"),
        environment: "production".into(),
        baseten_authorization: Some(format!("Api-Key {key}")),
    })
}

fn bundled_default_model_id(source: &str) -> Result<String, ProviderAttemptState> {
    let (_, value) = source
        .split_once("RT=\"")
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

    fn submit(
        &self,
        request: WisprFlowWireRequest,
    ) -> Result<WisprGrpcStreamResponse, ProviderAttemptState> {
        let user_id = self.identity.user_id()?;
        let identifiers = self.identity.fresh_request_identifiers()?;
        let backend = self.backend.backend()?;
        let mut metadata = backend.metadata();
        metadata.push(("authorization", format!("Bearer {}", request.session)));
        self.stream.stream(WisprGrpcStreamCall {
            host: backend.host,
            method: TRANSCRIBE_STREAM_PATH,
            metadata,
            messages: encode_observed_requests(&user_id, identifiers, &request)?,
        })
    }
}

impl WisprFlowWireClient for ObservedWisprGrpcClient {
    fn transcribe_stream(
        &self,
        request: WisprFlowWireRequest,
    ) -> Result<TranscriptText, ProviderAttemptState> {
        let responses = self.submit(request)?;
        responses.provider_state()?;
        decode_observed_responses(&responses.messages)
    }
}

/// Encodes the observed request queue: Init, optional cursor context, audio,
/// then Commit=True. Each entry is one independent gRPC message; the caller
/// alone half-closes the HTTP/2 stream after the commit message.
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
    let mut messages = vec![protobuf_message(1, &init_body)];
    if !request.request.preceding_transcript_tail().is_empty() {
        let textbox = protobuf_string(2, request.request.preceding_transcript_tail());
        let context = protobuf_message(2, &protobuf_message(2, &textbox));
        messages.push(context);
    }
    let payload = protobuf_bytes(1, &request.wav_pcm16);
    let audio_file = protobuf_message(2, &payload);
    messages.push(protobuf_message(3, &audio_file));
    messages.push(protobuf_integer(4, 1));
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
    Integer { number: u64 },
    Bytes { number: u64, value: &'a [u8] },
}

fn protobuf_fields(data: &[u8]) -> Result<Vec<ProtobufField<'_>>, ProviderAttemptState> {
    let mut fields = Vec::new();
    let mut position = 0;
    while position < data.len() {
        let tag = read_protobuf_varint(data, &mut position)?;
        let number = tag >> 3;
        match tag & 7 {
            0 => {
                read_protobuf_varint(data, &mut position)?;
                fields.push(ProtobufField::Integer { number });
            }
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

/// Runs one isolated, non-retrying Wispr gRPC witness against a supplied
/// synthetic recording. It does not create a Listener runtime, capture audio,
/// open Listener sockets, route a fallback, parse a transcript, or return one.
pub fn sandbox_wispr_witness(
    session_descriptor: i32,
    desktop_bundle_descriptor: i32,
    artifact_path: &Path,
) -> WisprWitnessDiagnostics {
    let checkpoint = WisprWitnessCheckpoint::new("witness");
    sandbox_wispr_witness_checkpointed(
        session_descriptor,
        desktop_bundle_descriptor,
        artifact_path,
        &checkpoint,
    )
}

/// Runs the isolated witness while advancing a caller-owned, static progress
/// checkpoint only after each private operation completes. The checkpoint is
/// suitable for redacted crash diagnostics; it never receives request,
/// session, bundle, or response values.
pub fn sandbox_wispr_witness_checkpointed(
    session_descriptor: i32,
    desktop_bundle_descriptor: i32,
    artifact_path: &Path,
    checkpoint: &WisprWitnessCheckpoint,
) -> WisprWitnessDiagnostics {
    let session = match InheritedFdDesktopWisprSession::new(session_descriptor) {
        Ok(session) => Arc::new(session),
        Err(_) => return WisprWitnessDiagnostics::at("session-descriptor"),
    };
    checkpoint.advance("session-descriptor");
    let backend = match DesktopWisprBackendSource::new(desktop_bundle_descriptor) {
        Ok(backend) => Arc::new(backend),
        Err(_) => return WisprWitnessDiagnostics::at("bundle-descriptor"),
    };
    checkpoint.advance("bundle-descriptor");
    let (access_token, _, bearer_state) = match session.session() {
        Ok(session) => session,
        Err(_) => return WisprWitnessDiagnostics::at("session"),
    };
    checkpoint.advance("session");
    let request = ProviderTranscriptRequest::for_test(artifact_path);
    let wav_pcm16 = match RecordingLogWisprMediaAdapter.wav_pcm16(&request) {
        Ok(wav_pcm16) => wav_pcm16,
        Err(_) => return WisprWitnessDiagnostics::at("recording"),
    };
    checkpoint.advance("request-recording");
    if !is_wispr_pcm16_wav(&wav_pcm16)
        || wav_pcm16.len() > 44 + (WISPR_MAXIMUM_SAMPLES as usize * 2)
    {
        return WisprWitnessDiagnostics::at("audio-validation");
    }
    checkpoint.advance("audio-validation");
    let user_id = match session.user_id() {
        Ok(user_id) => user_id,
        Err(_) => return WisprWitnessDiagnostics::at("session"),
    };
    let identifiers = match session.fresh_request_identifiers() {
        Ok(identifiers) => identifiers,
        Err(_) => return WisprWitnessDiagnostics::at("request-identifiers"),
    };
    checkpoint.advance("request-identifiers");
    let backend = match backend.backend() {
        Ok(backend) => backend,
        Err(_) => return WisprWitnessDiagnostics::at("bundle-descriptor"),
    };
    checkpoint.advance("backend-metadata");
    let mut metadata = backend.metadata();
    metadata.push(("authorization", format!("Bearer {access_token}")));
    let messages = match encode_observed_requests(
        &user_id,
        identifiers,
        &WisprFlowWireRequest::new(&access_token, request, wav_pcm16),
    ) {
        Ok(messages) => messages,
        Err(_) => return WisprWitnessDiagnostics::at("request-encoding"),
    };
    checkpoint.advance("request-encoding");
    let call = WisprGrpcStreamCall {
        host: backend.host,
        method: TRANSCRIBE_STREAM_PATH,
        metadata,
        messages,
    };
    match NativeWisprGrpcStreamingBoundary.stream_checkpointed(call, checkpoint) {
        Ok(response) => response.diagnostics(
            if response.provider_state().is_ok() {
                "complete"
            } else {
                "response"
            },
            bearer_state,
        ),
        Err(_) => WisprWitnessDiagnostics::at(checkpoint.stage()),
    }
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
        assert_eq!(calls[0].messages().len(), 4);
        assert!(calls[0]
            .metadata()
            .iter()
            .any(|(name, value)| *name == "authorization" && value == "Bearer synthetic-session"));
    }

    #[test]
    fn observed_wire_queues_init_context_audio_and_commit_as_distinct_messages() {
        let request = WisprFlowWireRequest::new(
            "synthetic-session",
            ProviderTranscriptRequest::new(
                "/durable/audio.wav".into(),
                "synthetic cursor context".into(),
                vec!["synthetic-vocabulary".into()],
            ),
            synthetic_wav(),
        );

        let messages = encode_observed_requests(
            "synthetic-user",
            WisprRequestIdentifiers::new(
                "00000000-0000-4000-8000-000000000001".into(),
                "00000000-0000-4000-8000-000000000002".into(),
            ),
            &request,
        )
        .expect("synthetic request should encode");

        assert_eq!(messages.len(), 4);
        assert!(matches!(
            protobuf_fields(&messages[0]).unwrap().as_slice(),
            [ProtobufField::Bytes { number: 1, .. }]
        ));
        assert!(matches!(
            protobuf_fields(&messages[1]).unwrap().as_slice(),
            [ProtobufField::Bytes { number: 2, .. }]
        ));
        assert!(matches!(
            protobuf_fields(&messages[2]).unwrap().as_slice(),
            [ProtobufField::Bytes { number: 3, .. }]
        ));
        assert_eq!(messages[3], protobuf_integer(4, 1));
    }

    #[test]
    fn witness_diagnostics_retain_only_the_allowed_response_structure() {
        let response = WisprGrpcStreamResponse {
            messages: vec![synthetic_result_response(
                "response text must not cross the witness boundary",
                "nor may alternate transcript text cross it",
            )],
            http_status: Some(200),
            grpc_status: Some(0),
            content_type: Some("application/grpc".into()),
            permission_category: PermissionCategory::Absent,
        };

        let value = serde_json::to_value(response.diagnostics("complete", BearerState::Unknown))
            .expect("diagnostics serialize");
        let object = value.as_object().expect("diagnostics are an object");
        assert_eq!(
            object.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "bearer_state",
                "content_type",
                "grpc_status",
                "http_status",
                "local_stage",
                "permission_category",
                "protobuf_top_level_tag_histograms",
                "response_frame_count",
                "response_frame_lengths",
            ],
        );
        let rendered = value.to_string();
        assert!(!rendered.contains("response text must not cross"));
        assert!(!rendered.contains("alternate transcript text"));
        assert_eq!(object["response_frame_count"], 1);
        assert_eq!(object["response_frame_lengths"], serde_json::json!([101]));
        assert_eq!(
            object["protobuf_top_level_tag_histograms"],
            serde_json::json!([[{"tag": 1, "count": 1}]]),
        );
    }

    #[test]
    fn sandbox_diagnostics_reduce_authorization_evidence_to_closed_categories() {
        let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000);
        let workos_session = serde_json::json!({
            "authProviderId": "workos",
            "expiresAt": 2_000_090_000_u64,
        });
        let supabase_session = serde_json::json!({
            "expires_at": 2_000_089_u64,
        });

        assert_eq!(
            desktop_bearer_state(&workos_session, "synthetic", observed_at),
            BearerState::NearExpiry,
        );
        assert_eq!(
            desktop_bearer_state(&supabase_session, "synthetic", observed_at),
            BearerState::NearExpiry,
        );
        assert_eq!(
            desktop_bearer_state(
                &serde_json::json!({"expires_at": 2_000_091_u64}),
                "synthetic",
                observed_at,
            ),
            BearerState::Fresh,
        );
        assert_eq!(
            desktop_bearer_state(
                &serde_json::json!({"authProviderId": "workos", "expiresAt": 2_000_000_000_u64}),
                "synthetic",
                observed_at,
            ),
            BearerState::Expired,
        );
        assert_eq!(
            desktop_bearer_state(&serde_json::json!({}), "synthetic", observed_at),
            BearerState::Unknown,
        );
        assert_eq!(
            desktop_auth_provider(
                &serde_json::json!({}),
                &synthetic_jwt_with_issuer("https://authkit.example.invalid"),
            ),
            DesktopAuthProvider::Workos,
        );
        assert_eq!(
            desktop_auth_provider(&serde_json::json!({}), "synthetic"),
            DesktopAuthProvider::Supabase,
        );
        assert_eq!(
            grpc_message_permission_category(Some("BASEten API-Key rejected")),
            PermissionCategory::BasetenApiKey,
        );
        assert_eq!(
            grpc_message_permission_category(Some("model entitlement denied")),
            PermissionCategory::ModelEntitlement,
        );
        assert_eq!(
            grpc_message_permission_category(Some("permission denied")),
            PermissionCategory::GenericPermission,
        );
        assert_eq!(
            grpc_message_permission_category(None),
            PermissionCategory::Absent,
        );

        let response = WisprGrpcStreamResponse {
            messages: Vec::new(),
            http_status: Some(403),
            grpc_status: Some(7),
            content_type: Some("application/grpc".into()),
            permission_category: grpc_message_permission_category(Some("Bearer rejected")),
        };
        let diagnostics = serde_json::to_value(response.diagnostics(
            "response",
            desktop_bearer_state(&workos_session, "synthetic", observed_at),
        ))
        .expect("diagnostics serialize");
        let object = diagnostics.as_object().expect("diagnostics are an object");
        assert_eq!(
            object.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "bearer_state",
                "content_type",
                "grpc_status",
                "http_status",
                "local_stage",
                "permission_category",
                "protobuf_top_level_tag_histograms",
                "response_frame_count",
                "response_frame_lengths",
            ],
        );
        assert_eq!(object["bearer_state"], "near-expiry");
        assert_eq!(object["permission_category"], "bearer-auth");
        assert!(diagnostics.get("grpc_message").is_none());
        assert!(!diagnostics.to_string().contains("Bearer rejected"));
    }

    fn synthetic_jwt_with_issuer(issuer: &str) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

        let payload = serde_json::json!({"iss": issuer});
        format!(
            "synthetic.{}.signature",
            URL_SAFE_NO_PAD.encode(payload.to_string())
        )
    }

    #[test]
    fn nested_runtime_uses_an_owned_runtime_thread() {
        let outer_thread = std::thread::current().id();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("synthetic outer runtime");
        let checkpoint = WisprWitnessCheckpoint::new("request-encoding");
        let outcome = runtime.block_on(async {
            run_wispr_runtime(&checkpoint, async {
                Ok::<_, ProviderAttemptState>(std::thread::current().id())
            })
        });
        assert_ne!(outcome.expect("owned runtime result"), outer_thread);
    }

    #[test]
    fn child_runtime_panic_preserves_its_shared_static_checkpoint() {
        let checkpoint = WisprWitnessCheckpoint::new("runtime");
        let child_checkpoint = checkpoint.clone();
        let outer = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("synthetic outer runtime");
        let runtime_checkpoint = checkpoint.clone();
        let outcome = outer.block_on(async move {
            run_wispr_runtime::<()>(&runtime_checkpoint, async move {
                child_checkpoint.advance("tcp-connect");
                panic!("synthetic child panic")
            })
        });

        assert_eq!(outcome, Err(ProviderAttemptState::Unavailable));
        assert_eq!(checkpoint.stage(), "tcp-connect");
    }

    #[test]
    fn owned_runtime_marks_first_future_poll_before_a_child_unwind() {
        let checkpoint = WisprWitnessCheckpoint::new("request-encoding");
        let child_checkpoint = checkpoint.clone();
        let outcome = run_wispr_runtime::<()>(&checkpoint, async move {
            child_checkpoint.advance("future-first-poll");
            panic!("synthetic first-poll panic")
        });

        assert_eq!(outcome, Err(ProviderAttemptState::Unavailable));
        assert_eq!(checkpoint.stage(), "future-first-poll");
    }

    #[test]
    fn loopback_resolution_reaches_the_static_dns_completed_checkpoint() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("synthetic resolver runtime");
        let checkpoint = WisprWitnessCheckpoint::new("future-first-poll");

        let addresses = runtime
            .block_on(resolve_wispr_addresses("127.0.0.1", &checkpoint))
            .expect("numeric loopback resolves without a network lookup");

        assert!(!addresses.is_empty());
        assert_eq!(checkpoint.stage(), "dns-resolution-completed");
    }

    #[test]
    fn refused_local_dial_retains_the_static_attempted_checkpoint() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("synthetic dial runtime");
        let checkpoint = WisprWitnessCheckpoint::new("dns-resolution-completed");
        let address = "127.0.0.1:0"
            .parse::<SocketAddr>()
            .expect("synthetic reserved port");

        let result = runtime.block_on(dial_wispr_addresses(&[address], &checkpoint));

        assert!(matches!(
            result,
            Err(ProviderAttemptState::TransientFailure)
        ));
        assert_eq!(checkpoint.stage(), "tcp-dial-attempted");
    }

    #[test]
    fn witness_tls_configuration_does_not_depend_on_process_default_provider() {
        let checkpoint = WisprWitnessCheckpoint::new("tcp-dial-connected");
        let result = std::panic::catch_unwind(|| wispr_tls_connector(&checkpoint));

        assert!(result.is_ok());
        assert!(result.expect("explicit provider configuration").is_ok());
        assert_eq!(checkpoint.stage(), "tls-connector-ready");
    }

    #[test]
    fn witness_tls_server_name_is_a_static_completed_checkpoint() {
        let checkpoint = WisprWitnessCheckpoint::new("tls-connector-ready");

        let server_name = wispr_server_name(WISPR_GRPC_HOST.to_owned(), &checkpoint);

        assert!(server_name.is_ok());
        assert_eq!(checkpoint.stage(), "tls-server-name-completed");
    }

    #[test]
    fn loopback_h2_accepts_the_real_four_message_witness_request() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("offline h2 runtime");
        runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("offline loopback listener");
            let address = listener.local_addr().expect("loopback listener address");
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.expect("loopback accept");
                let mut connection = h2::server::handshake(socket).await.expect("h2 server");
                let (_request, mut respond) = connection
                    .accept()
                    .await
                    .expect("h2 request")
                    .expect("one request");
                respond
                    .send_response(
                        http::Response::builder().status(200).body(()).unwrap(),
                        true,
                    )
                    .expect("h2 response");
                while connection.accept().await.is_some() {}
            });
            let socket = tokio::net::TcpStream::connect(address)
                .await
                .expect("loopback client connect");
            let (mut sender, connection) = h2::client::handshake(socket).await.expect("h2 client");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let request = WisprFlowWireRequest::new(
                "synthetic-session",
                ProviderTranscriptRequest::new(
                    "/synthetic.wav".into(),
                    "synthetic context".into(),
                    Vec::new(),
                ),
                synthetic_wav(),
            );
            let messages = encode_observed_requests(
                "synthetic-user",
                WisprRequestIdentifiers::new(
                    "synthetic-session-id".into(),
                    "synthetic-request-id".into(),
                ),
                &request,
            )
            .expect("four synthetic messages");
            assert_eq!(messages.len(), 4);
            let call = WisprGrpcStreamCall {
                host: "offline.wispr.test".into(),
                method: TRANSCRIBE_STREAM_PATH,
                metadata: vec![("x-wispr-synthetic", "offline".into())],
                messages: messages.clone(),
            };
            let checkpoint = WisprWitnessCheckpoint::new("http2-ready");
            let request = wispr_h2_request(&call, &checkpoint).expect("synthetic request");
            let (response, mut upload) = sender
                .send_request(request, false)
                .expect("h2 request has scheme and authority");
            for message in &messages {
                native_send_grpc_message(&mut upload, message)
                    .await
                    .expect("synthetic grpc frame");
                checkpoint.advance(wispr_outbound_frame_stage(message));
            }
            upload.send_data(Bytes::new(), true).expect("h2 half close");
            assert_eq!(response.await.expect("h2 response").status(), 200);
            server.abort();
            assert_eq!(checkpoint.stage(), "client-commit-frame-sent");
        });
    }

    #[test]
    fn desktop_backend_descriptor_selects_the_packaged_default_without_exposing_its_key() {
        let descriptor = desktop_backend_from_bundle(
            br#"const RT="v31pl413";const basetenApiKey="synthetic-client-key";const Rt={Fo:()=>basetenApiKey};class G{static ASR_VARIANT_BASETEN_MODEL_IDS={[xt.eW.QwenHttp]:"q049l843"};static getRpcOptions(){return {"baseten-authorization":`Api-Key ${Rt.Fo}`}}}"#,
        )
        .expect("synthetic desktop bundle descriptor");

        assert_eq!(descriptor.host, "model-v31pl413.grpc.api.baseten.co");
        assert_eq!(descriptor.model_id, "model-v31pl413");
        assert_eq!(descriptor.environment, "production");
        assert!(descriptor.baseten_authorization.is_some());
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
        fn stream(
            &self,
            call: WisprGrpcStreamCall,
        ) -> Result<WisprGrpcStreamResponse, ProviderAttemptState> {
            self.calls.lock().expect("synthetic call sink").push(call);
            Ok(WisprGrpcStreamResponse {
                messages: self.responses.clone(),
                http_status: Some(200),
                grpc_status: Some(0),
                content_type: Some("application/grpc".into()),
                permission_category: PermissionCategory::Absent,
            })
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
