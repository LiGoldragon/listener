use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc,
    thread,
    time::Duration,
};

use signal_listener::{DurableAudioArtifact, TranscriptText};

use serde::Deserialize;

use crate::{
    CompactAudioArtifact, Error, OpusWebmEncoder, RecordingAudioFormat, RecordingSampleFormat,
    Result, StatusPublisher,
};

const OPENAI_TRANSCRIPTION_URL: &str = "https://api.openai.com/v1/audio/transcriptions";
const OPENAI_TRANSCRIPTION_MODEL: &str = "gpt-4o-transcribe";
const OPENAI_TRANSCRIPTION_LANGUAGE: &str = "en";
const OPENAI_TRANSCRIPTION_GENERIC_INSTRUCTION: &str = "Transcribe spoken English as dictated text. Preserve technical names, product names, and acronyms exactly when spoken. Do not translate.";
pub const TRANSCRIPTION_CUSTOMIZATION_ARCHIVE_ENVIRONMENT_VARIABLE: &str =
    "LISTENER_TRANSCRIPTION_CUSTOMIZATION_ARCHIVE";
const OPENAI_MAXIMUM_UPLOAD_BYTES: usize = 25 * 1024 * 1024;
const TRANSCRIPTION_ACTOR_QUEUE_CAPACITY: usize = 1;
const TRANSCRIPTION_ACTOR_REPLY_TIMEOUT: Duration = Duration::from_secs(600);
const TRANSCRIPTION_CUSTOMIZATION_ARCHIVE_MAGIC: [u8; 8] = *b"LSTNVOC\0";
const TRANSCRIPTION_CUSTOMIZATION_ARCHIVE_VERSION: u32 = 1;

pub trait BatchTranscriber: Send {
    fn transcribe(&self, request: BatchTranscriptionRequest) -> Result<TranscriptText>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchTranscriptionRequest {
    artifact: DurableAudioArtifact,
    input: BatchTranscriptionInput,
}

impl BatchTranscriptionRequest {
    pub fn new(artifact: DurableAudioArtifact) -> Self {
        let input = BatchTranscriptionInput::listener_recording_log(PathBuf::from(
            artifact.path().as_str(),
        ));
        Self { artifact, input }
    }

    pub fn new_with_input(artifact: DurableAudioArtifact, input: BatchTranscriptionInput) -> Self {
        Self { artifact, input }
    }

    pub fn artifact(&self) -> &DurableAudioArtifact {
        &self.artifact
    }

    pub fn input(&self) -> &BatchTranscriptionInput {
        &self.input
    }

    pub fn artifact_path(&self) -> PathBuf {
        PathBuf::from(self.artifact.path().as_str())
    }

    pub fn input_path(&self) -> &PathBuf {
        self.input.path()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchTranscriptionInput {
    path: PathBuf,
    format: BatchTranscriptionInputFormat,
}

impl BatchTranscriptionInput {
    pub fn listener_recording_log(path: PathBuf) -> Self {
        Self {
            path,
            format: BatchTranscriptionInputFormat::ListenerRecordingLog,
        }
    }

    pub fn signed_sixteen_bit_little_endian_pcm(
        path: PathBuf,
        audio_format: RecordingAudioFormat,
    ) -> Self {
        Self {
            path,
            format: BatchTranscriptionInputFormat::SignedSixteenBitLittleEndianPcm { audio_format },
        }
    }

    pub fn webm_opus(path: PathBuf) -> Self {
        Self {
            path,
            format: BatchTranscriptionInputFormat::WebmOpus,
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn format(&self) -> &BatchTranscriptionInputFormat {
        &self.format
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchTranscriptionInputFormat {
    ListenerRecordingLog,
    SignedSixteenBitLittleEndianPcm { audio_format: RecordingAudioFormat },
    WebmOpus,
}

#[derive(Clone, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TranscriptionCustomization {
    vocabulary_terms: Vec<String>,
}

impl TranscriptionCustomization {
    pub fn new(vocabulary_terms: Vec<String>) -> Self {
        Self { vocabulary_terms }
    }

    pub fn vocabulary_terms(&self) -> &[String] {
        &self.vocabulary_terms
    }

    pub fn from_rkyv_bytes(bytes: &[u8]) -> Result<Self> {
        TranscriptionCustomizationArchive::from_bytes(bytes)?.into_customization()
    }

    pub fn to_rkyv_bytes(&self) -> Result<Vec<u8>> {
        TranscriptionCustomizationArchive::from_customization(self)
            .map(|archive| archive.into_bytes())
    }

    pub fn from_archive_path(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = fs::read(path)?;
        Self::from_rkyv_bytes(&bytes)
    }

    pub fn prompt(&self) -> TranscriptionPrompt {
        TranscriptionPrompt::with_customization(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TranscriptionCustomizationArchiveFormat {
    magic: [u8; 8],
    version: u32,
}

impl TranscriptionCustomizationArchiveFormat {
    fn current() -> Self {
        Self {
            magic: TRANSCRIPTION_CUSTOMIZATION_ARCHIVE_MAGIC,
            version: TRANSCRIPTION_CUSTOMIZATION_ARCHIVE_VERSION,
        }
    }

    fn header_length(&self) -> usize {
        self.magic.len() + std::mem::size_of::<u32>()
    }

    fn validate(&self) -> Result<()> {
        let current = Self::current();
        if self.magic != current.magic {
            return Err(Error::TranscriptionCustomizationArchiveMagic);
        }
        if self.version != current.version {
            return Err(Error::TranscriptionCustomizationArchiveVersion {
                version: self.version,
                expected: current.version,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TranscriptionCustomizationArchive {
    format: TranscriptionCustomizationArchiveFormat,
    payload: Vec<u8>,
}

impl TranscriptionCustomizationArchive {
    fn from_customization(customization: &TranscriptionCustomization) -> Result<Self> {
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(customization)
            .map(|bytes| bytes.to_vec())
            .map_err(|_| Error::TranscriptionCustomizationEncode)?;
        Ok(Self {
            format: TranscriptionCustomizationArchiveFormat::current(),
            payload,
        })
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let current = TranscriptionCustomizationArchiveFormat::current();
        if bytes.len() < current.header_length() {
            return Err(Error::TranscriptionCustomizationArchiveHeader);
        }
        let mut magic = [0_u8; 8];
        magic.copy_from_slice(&bytes[..current.magic.len()]);
        let version_start = current.magic.len();
        let version_end = version_start + std::mem::size_of::<u32>();
        let version = u32::from_le_bytes(
            bytes[version_start..version_end]
                .try_into()
                .map_err(|_| Error::TranscriptionCustomizationArchiveHeader)?,
        );
        Ok(Self {
            format: TranscriptionCustomizationArchiveFormat { magic, version },
            payload: bytes[version_end..].to_vec(),
        })
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.format.header_length() + self.payload.len());
        bytes.extend_from_slice(&self.format.magic);
        bytes.extend_from_slice(&self.format.version.to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn into_customization(self) -> Result<TranscriptionCustomization> {
        self.format.validate()?;
        rkyv::from_bytes::<TranscriptionCustomization, rkyv::rancor::Error>(&self.payload)
            .map_err(|_| Error::TranscriptionCustomizationDecode)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranscriptionCustomizationTextSource {
    text: String,
}

impl TranscriptionCustomizationTextSource {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    pub fn into_customization(self) -> TranscriptionCustomization {
        let vocabulary_terms = self
            .text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect();
        TranscriptionCustomization::new(vocabulary_terms)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranscriptionCustomizationEnvironment {
    archive_path: Option<PathBuf>,
}

impl TranscriptionCustomizationEnvironment {
    pub fn from_process() -> Self {
        Self {
            archive_path: std::env::var_os(
                TRANSCRIPTION_CUSTOMIZATION_ARCHIVE_ENVIRONMENT_VARIABLE,
            )
            .map(PathBuf::from),
        }
    }

    pub fn new(archive_path: Option<PathBuf>) -> Self {
        Self { archive_path }
    }

    pub fn archive_path(&self) -> Option<&Path> {
        self.archive_path.as_deref()
    }

    pub fn prompt(&self) -> Result<TranscriptionPrompt> {
        match self.archive_path() {
            Some(path) => TranscriptionCustomization::from_archive_path(path)
                .map(|customization| customization.prompt()),
            None => Ok(TranscriptionPrompt::generic()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranscriptionPrompt {
    text: String,
}

impl TranscriptionPrompt {
    pub fn generic() -> Self {
        Self {
            text: OPENAI_TRANSCRIPTION_GENERIC_INSTRUCTION.to_owned(),
        }
    }

    pub fn with_customization(customization: &TranscriptionCustomization) -> Self {
        let mut prompt = Self::generic();
        if !customization.vocabulary_terms().is_empty() {
            prompt
                .text
                .push_str("\nVocabulary terms to preserve exactly when spoken: ");
            prompt
                .text
                .push_str(&customization.vocabulary_terms().join(", "));
            prompt.text.push('.');
        }
        prompt
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn into_string(self) -> String {
        self.text
    }
}

pub struct OpenAiBatchTranscriptionActor {
    sender: mpsc::SyncSender<TranscriptionActorMessage>,
    reply_timeout: Duration,
}

impl OpenAiBatchTranscriptionActor {
    pub fn from_environment(status_publisher: StatusPublisher) -> Result<Self> {
        Ok(Self::new(
            OpenAiRestTranscriber::from_environment()?,
            status_publisher,
            TRANSCRIPTION_ACTOR_REPLY_TIMEOUT,
        ))
    }

    pub fn new(
        transcriber: OpenAiRestTranscriber,
        status_publisher: StatusPublisher,
        reply_timeout: Duration,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel(TRANSCRIPTION_ACTOR_QUEUE_CAPACITY);
        TranscriptionActorWorker::new(transcriber, status_publisher, receiver).spawn();
        Self {
            sender,
            reply_timeout,
        }
    }
}

impl BatchTranscriber for OpenAiBatchTranscriptionActor {
    fn transcribe(&self, request: BatchTranscriptionRequest) -> Result<TranscriptText> {
        let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
        self.sender
            .send(TranscriptionActorMessage::Transcribe(
                TranscriptionActorRequest::new(request, reply_sender),
            ))
            .map_err(|_| Error::TranscriptionActorUnavailable {
                message: "transcription actor is not running".to_owned(),
            })?;
        reply_receiver
            .recv_timeout(self.reply_timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => Error::TranscriptionActorUnavailable {
                    message: "OpenAI transcription actor timed out".to_owned(),
                },
                mpsc::RecvTimeoutError::Disconnected => Error::TranscriptionActorUnavailable {
                    message: "OpenAI transcription actor disconnected".to_owned(),
                },
            })?
    }
}

struct TranscriptionActorWorker {
    transcriber: OpenAiRestTranscriber,
    status_publisher: StatusPublisher,
    receiver: mpsc::Receiver<TranscriptionActorMessage>,
    in_flight: Option<DurableAudioArtifact>,
}

impl TranscriptionActorWorker {
    fn new(
        transcriber: OpenAiRestTranscriber,
        status_publisher: StatusPublisher,
        receiver: mpsc::Receiver<TranscriptionActorMessage>,
    ) -> Self {
        Self {
            transcriber,
            status_publisher,
            receiver,
            in_flight: None,
        }
    }

    fn spawn(mut self) {
        thread::spawn(move || self.run());
    }

    fn run(&mut self) {
        while let Ok(message) = self.receiver.recv() {
            match message {
                TranscriptionActorMessage::Transcribe(request) => {
                    self.handle_transcription(request)
                }
            }
        }
    }

    fn handle_transcription(&mut self, request: TranscriptionActorRequest) {
        self.in_flight = Some(request.request().artifact().clone());
        self.status_publisher.publish_transcribing();
        let result = self.transcriber.transcribe(request.request().clone());
        if result.is_err() {
            self.status_publisher.publish_error();
        }
        let _ = request.reply(result);
        self.in_flight = None;
    }
}

enum TranscriptionActorMessage {
    Transcribe(TranscriptionActorRequest),
}

struct TranscriptionActorRequest {
    request: BatchTranscriptionRequest,
    reply_sender: mpsc::SyncSender<Result<TranscriptText>>,
}

impl TranscriptionActorRequest {
    fn new(
        request: BatchTranscriptionRequest,
        reply_sender: mpsc::SyncSender<Result<TranscriptText>>,
    ) -> Self {
        Self {
            request,
            reply_sender,
        }
    }

    fn request(&self) -> &BatchTranscriptionRequest {
        &self.request
    }

    fn reply(
        self,
        result: Result<TranscriptText>,
    ) -> std::result::Result<(), mpsc::SendError<Result<TranscriptText>>> {
        self.reply_sender.send(result)
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiRestTranscriber {
    client: reqwest::blocking::Client,
    credentials: OpenAiCredentialSource,
    request_configuration: OpenAiTranscriptionRequestConfiguration,
}

impl OpenAiRestTranscriber {
    pub fn from_environment() -> Result<Self> {
        Ok(Self::new(
            reqwest::blocking::Client::builder()
                .timeout(TRANSCRIPTION_ACTOR_REPLY_TIMEOUT)
                .build()
                .unwrap_or_else(|_| reqwest::blocking::Client::new()),
            OpenAiCredentialSource::gopass("openai/api-key"),
            OpenAiTranscriptionRequestConfiguration::from_environment()?,
        ))
    }

    pub fn from_customization_archive_path(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self::new(
            reqwest::blocking::Client::builder()
                .timeout(TRANSCRIPTION_ACTOR_REPLY_TIMEOUT)
                .build()
                .unwrap_or_else(|_| reqwest::blocking::Client::new()),
            OpenAiCredentialSource::gopass("openai/api-key"),
            OpenAiTranscriptionRequestConfiguration::from_customization_archive_path(path)?,
        ))
    }

    pub fn new(
        client: reqwest::blocking::Client,
        credentials: OpenAiCredentialSource,
        request_configuration: OpenAiTranscriptionRequestConfiguration,
    ) -> Self {
        Self {
            client,
            credentials,
            request_configuration,
        }
    }

    pub fn transcribe(&self, request: BatchTranscriptionRequest) -> Result<TranscriptText> {
        match request.input().format() {
            BatchTranscriptionInputFormat::WebmOpus => {
                self.transcribe_webm_opus(request.input_path())
            }
            _ => self.transcribe_upload(
                WavAudioUpload::from_batch_input(request.input())?.into_upload(),
            ),
        }
    }

    fn transcribe_webm_opus(&self, path: &Path) -> Result<TranscriptText> {
        let artifact = CompactAudioArtifact::new(path);
        artifact.validate()?;
        let encoder = OpusWebmEncoder::from_environment();
        let duration_seconds = encoder.duration_seconds(path)?;
        let mut transcripts = Vec::new();
        for start_seconds in (0..duration_seconds).step_by(600) {
            let bytes = if duration_seconds <= 600 {
                fs::read(path)?
            } else {
                encoder.chunk_webm(path, start_seconds, 600)?
            };
            transcripts.push(self.transcribe_upload(OpenAiAudioUpload::webm(bytes))?);
        }
        let text = transcripts
            .iter()
            .map(TranscriptText::as_str)
            .collect::<Vec<_>>()
            .join(" ");
        Ok(TranscriptText::new(text))
    }

    fn transcribe_upload(&self, upload: OpenAiAudioUpload) -> Result<TranscriptText> {
        if upload.bytes.len() > OPENAI_MAXIMUM_UPLOAD_BYTES {
            return Err(Error::TranscriptionBackendUnavailable {
                message: format!(
                    "OpenAI upload is {} bytes, above the 25 MiB limit",
                    upload.bytes.len()
                ),
            });
        }
        let api_key = self.credentials.resolve()?;
        let file_part = reqwest::blocking::multipart::Part::bytes(upload.bytes)
            .file_name(upload.file_name)
            .mime_str(&upload.mime)
            .map_err(|error| Error::TranscriptionBackendUnavailable {
                message: format!("failed to prepare OpenAI audio upload: {error}"),
            })?;
        let form = reqwest::blocking::multipart::Form::new()
            .part("file", file_part)
            .text("model", self.request_configuration.model().to_owned())
            .text("language", self.request_configuration.language().to_owned())
            .text("prompt", self.request_configuration.prompt().to_owned());
        let response = self
            .client
            .post(self.request_configuration.endpoint())
            .bearer_auth(api_key)
            .multipart(form)
            .send()
            .map_err(|error| Error::TranscriptionBackendUnavailable {
                message: format!("OpenAI transcription request failed: {error}"),
            })?;
        OpenAiTranscriptionResponseBody::from_response(response)?.into_transcript_text()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenAiCredentialSource {
    kind: OpenAiCredentialKind,
}

impl OpenAiCredentialSource {
    pub fn gopass(secret_name: impl Into<String>) -> Self {
        Self {
            kind: OpenAiCredentialKind::Gopass {
                secret_name: secret_name.into(),
            },
        }
    }

    pub fn literal(api_key: impl Into<String>) -> Self {
        Self {
            kind: OpenAiCredentialKind::Literal {
                api_key: api_key.into(),
            },
        }
    }

    pub fn resolve(&self) -> Result<String> {
        match &self.kind {
            OpenAiCredentialKind::Gopass { secret_name } => self.resolve_gopass(secret_name),
            OpenAiCredentialKind::Literal { api_key } => Ok(api_key.clone()),
        }
    }

    fn resolve_gopass(&self, secret_name: &str) -> Result<String> {
        let output = Command::new("gopass")
            .args(["show", "-o", secret_name])
            .output()
            .map_err(|error| Error::TranscriptionBackendUnavailable {
                message: format!("failed to start gopass for OpenAI credential: {error}"),
            })?;
        if !output.status.success() {
            return Err(Error::TranscriptionBackendUnavailable {
                message: format!("gopass returned {} for OpenAI credential", output.status),
            });
        }
        let key = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if key.is_empty() {
            Err(Error::TranscriptionBackendUnavailable {
                message: "gopass returned an empty OpenAI credential".to_owned(),
            })
        } else {
            Ok(key)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum OpenAiCredentialKind {
    Gopass { secret_name: String },
    Literal { api_key: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenAiTranscriptionRequestConfiguration {
    endpoint: String,
    model: String,
    language: String,
    prompt: String,
}

impl OpenAiTranscriptionRequestConfiguration {
    pub fn new(
        model: impl Into<String>,
        language: impl Into<String>,
        prompt: impl Into<String>,
    ) -> Self {
        Self::new_with_endpoint(OPENAI_TRANSCRIPTION_URL, model, language, prompt)
    }

    pub fn new_with_endpoint(
        endpoint: impl Into<String>,
        model: impl Into<String>,
        language: impl Into<String>,
        prompt: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            model: model.into(),
            language: language.into(),
            prompt: prompt.into(),
        }
    }

    pub fn from_environment() -> Result<Self> {
        let prompt = TranscriptionCustomizationEnvironment::from_process().prompt()?;
        Ok(Self::new(
            OPENAI_TRANSCRIPTION_MODEL,
            OPENAI_TRANSCRIPTION_LANGUAGE,
            prompt.into_string(),
        ))
    }

    pub fn from_customization_archive_path(path: impl Into<PathBuf>) -> Result<Self> {
        let prompt = TranscriptionCustomizationEnvironment::new(Some(path.into())).prompt()?;
        Ok(Self::new(
            OPENAI_TRANSCRIPTION_MODEL,
            OPENAI_TRANSCRIPTION_LANGUAGE,
            prompt.into_string(),
        ))
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn language(&self) -> &str {
        &self.language
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }
}

impl Default for OpenAiTranscriptionRequestConfiguration {
    fn default() -> Self {
        Self::new(
            OPENAI_TRANSCRIPTION_MODEL,
            OPENAI_TRANSCRIPTION_LANGUAGE,
            TranscriptionPrompt::generic().into_string(),
        )
    }
}

#[derive(Deserialize)]
struct OpenAiTranscriptionResponse {
    text: String,
}

struct OpenAiTranscriptionResponseBody {
    status: reqwest::StatusCode,
    body: String,
}

impl OpenAiTranscriptionResponseBody {
    fn from_response(response: reqwest::blocking::Response) -> Result<Self> {
        let status = response.status();
        let body = response
            .text()
            .map_err(|error| Error::TranscriptionBackendUnavailable {
                message: format!("failed to read OpenAI transcription response: {error}"),
            })?;
        Ok(Self { status, body })
    }

    fn into_transcript_text(self) -> Result<TranscriptText> {
        if !self.status.is_success() {
            return Err(Error::TranscriptionBackendUnavailable {
                message: format!(
                    "OpenAI transcription returned HTTP {}",
                    self.status.as_u16()
                ),
            });
        }
        let parsed: OpenAiTranscriptionResponse =
            serde_json::from_str(&self.body).map_err(|error| {
                Error::TranscriptionBackendUnavailable {
                    message: format!("failed to decode OpenAI transcription response: {error}"),
                }
            })?;
        let transcript = parsed.text.trim().to_owned();
        if transcript.is_empty() {
            Err(Error::TranscriptionBackendUnavailable {
                message: "OpenAI transcription response did not contain text".to_owned(),
            })
        } else {
            Ok(TranscriptText::new(transcript))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OpenAiAudioUpload {
    bytes: Vec<u8>,
    file_name: String,
    mime: String,
}

impl OpenAiAudioUpload {
    fn webm(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            file_name: "listener-input.webm".to_owned(),
            mime: "audio/webm".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WavAudioUpload {
    bytes: Vec<u8>,
}

impl WavAudioUpload {
    fn from_batch_input(input: &BatchTranscriptionInput) -> Result<Self> {
        match input.format() {
            BatchTranscriptionInputFormat::SignedSixteenBitLittleEndianPcm { audio_format } => {
                Self::from_raw_pcm(input.path(), *audio_format)
            }
            BatchTranscriptionInputFormat::ListenerRecordingLog
            | BatchTranscriptionInputFormat::WebmOpus => {
                Err(Error::TranscriptionBackendUnavailable {
                    message: "OpenAI WAV upload requires raw PCM input".to_owned(),
                })
            }
        }
    }

    fn from_raw_pcm(path: &PathBuf, audio_format: RecordingAudioFormat) -> Result<Self> {
        if audio_format.sample_format() != RecordingSampleFormat::SignedSixteenBitLittleEndian {
            return Err(Error::TranscriptionBackendUnavailable {
                message: "OpenAI transcription only supports Listener s16le PCM exports".to_owned(),
            });
        }
        let pcm = fs::read(path)?;
        if pcm.is_empty() {
            return Err(Error::TranscriptionBackendUnavailable {
                message: "OpenAI transcription input is empty".to_owned(),
            });
        }
        if !pcm
            .len()
            .is_multiple_of(usize::from(audio_format.bytes_per_frame()))
        {
            return Err(Error::IncompletePcmFrame {
                remaining_bytes: pcm.len() % usize::from(audio_format.bytes_per_frame()),
                bytes_per_frame: audio_format.bytes_per_frame(),
            });
        }
        Self::new(pcm, audio_format)
    }

    fn new(pcm: Vec<u8>, audio_format: RecordingAudioFormat) -> Result<Self> {
        let data_length =
            u32::try_from(pcm.len()).map_err(|_| Error::TranscriptionBackendUnavailable {
                message: "OpenAI transcription input is too large for WAV".to_owned(),
            })?;
        let chunk_length =
            data_length
                .checked_add(36)
                .ok_or_else(|| Error::TranscriptionBackendUnavailable {
                    message: "OpenAI transcription input is too large for WAV".to_owned(),
                })?;
        let byte_rate = audio_format
            .sample_rate()
            .checked_mul(u32::from(audio_format.bytes_per_frame()))
            .ok_or_else(|| Error::TranscriptionBackendUnavailable {
                message: "OpenAI transcription WAV byte rate overflowed".to_owned(),
            })?;
        let mut bytes = Vec::with_capacity(44 + pcm.len());
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&chunk_length.to_le_bytes());
        bytes.extend_from_slice(b"WAVEfmt ");
        bytes.extend_from_slice(&16_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&audio_format.channel_count().to_le_bytes());
        bytes.extend_from_slice(&audio_format.sample_rate().to_le_bytes());
        bytes.extend_from_slice(&byte_rate.to_le_bytes());
        bytes.extend_from_slice(&audio_format.bytes_per_frame().to_le_bytes());
        bytes.extend_from_slice(&16_u16.to_le_bytes());
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&data_length.to_le_bytes());
        bytes.extend_from_slice(&pcm);
        Ok(Self { bytes })
    }

    fn into_upload(self) -> OpenAiAudioUpload {
        OpenAiAudioUpload {
            bytes: self.bytes,
            file_name: "listener-input.wav".to_owned(),
            mime: "audio/wav".to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfiguredBatchTranscriber {
    command: Option<BatchTranscriptionCommand>,
    stub: HonestStubTranscriber,
}

impl ConfiguredBatchTranscriber {
    pub fn from_environment() -> Self {
        Self {
            command: std::env::var("LISTENER_DEVELOPMENT_TRANSCRIPTION_PROGRAM")
                .ok()
                .map(BatchTranscriptionCommand::new),
            stub: HonestStubTranscriber::from_environment(),
        }
    }

    pub fn new(command: Option<BatchTranscriptionCommand>, stub: HonestStubTranscriber) -> Self {
        Self { command, stub }
    }
}

impl BatchTranscriber for ConfiguredBatchTranscriber {
    fn transcribe(&self, request: BatchTranscriptionRequest) -> Result<TranscriptText> {
        match &self.command {
            Some(command) => command.transcribe(request),
            None => self.stub.transcribe(request),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchTranscriptionCommand {
    program: String,
}

impl BatchTranscriptionCommand {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
        }
    }

    pub fn transcribe(&self, request: BatchTranscriptionRequest) -> Result<TranscriptText> {
        let output = Command::new(&self.program)
            .arg(request.input_path())
            .output()
            .map_err(|error| Error::TranscriptionBackendUnavailable {
                message: format!("failed to start {}: {error}", self.program),
            })?;

        if output.status.success() {
            let transcript = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            Ok(TranscriptText::new(transcript))
        } else {
            Err(Error::TranscriptionBackendUnavailable {
                message: format!("{} exited with {}", self.program, output.status),
            })
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HonestStubTranscriber {
    transcript_text: Option<String>,
}

impl HonestStubTranscriber {
    pub fn from_environment() -> Self {
        Self::new(std::env::var("LISTENER_STUB_TRANSCRIPT").ok())
    }

    pub fn new(transcript_text: Option<String>) -> Self {
        Self { transcript_text }
    }
}

impl BatchTranscriber for HonestStubTranscriber {
    fn transcribe(&self, request: BatchTranscriptionRequest) -> Result<TranscriptText> {
        let transcript = self.transcript_text.clone().unwrap_or_else(|| {
            format!(
                "[listener transcription backend not configured; audio artifact: {}]",
                request.artifact().path().as_str()
            )
        });
        Ok(TranscriptText::new(transcript))
    }
}
