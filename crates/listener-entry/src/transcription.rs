use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use listener_emission::{Transcribe_Data, Transcription_Data_Heard_Data as Heard};
use listener_library::{Backend, Failure, Protocol, ProviderId, Text};
use listener_place::{AudioLocating, random_bytes};
use serde_json::{Value, json};
use std::{io::Write, net::IpAddr, process::Stdio, time::Duration};
use tokio::{
    process::Command,
    time::{Instant, timeout_at},
};
use ureq::{
    Agent,
    http::{Response, Uri},
};

enum BackendFailure {
    Key,
    Audio,
    Timeout,
    Address,
    Model,
    Network,
    Status(u16),
    Malformed,
}
impl From<BackendFailure> for Failure {
    fn from(failure: BackendFailure) -> Self {
        Self(match failure {
            BackendFailure::Key => "The credential reader did not supply a key.".into(),
            BackendFailure::Audio => "The kept audio could not be read.".into(),
            BackendFailure::Timeout => "The transcription deadline was reached.".into(),
            BackendFailure::Address => {
                "The backend address must be HTTPS or a loopback HTTP address.".into()
            }
            BackendFailure::Model => "The Gemini model must be one path segment.".into(),
            BackendFailure::Network => "The transcription exchange did not complete.".into(),
            BackendFailure::Status(status) => {
                format!("The transcription service returned HTTP {status}.")
            }
            BackendFailure::Malformed => {
                "The transcription service did not return a whole text in its expected format."
                    .into()
            }
        })
    }
}
impl From<ureq::Error> for BackendFailure {
    fn from(error: ureq::Error) -> Self {
        match error {
            ureq::Error::Timeout(_) => Self::Timeout,
            _ => Self::Network,
        }
    }
}

struct Credential(String);
trait ReadingCredential {
    async fn key(&self) -> Result<Credential, BackendFailure>;
}
impl ReadingCredential for Transcribe_Data {
    async fn key(&self) -> Result<Credential, BackendFailure> {
        let result = Command::new(&self.program.0)
            .arg(&self.backend.secret_entry.0)
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|_| BackendFailure::Key)?;
        if !result.status.success() {
            return Err(BackendFailure::Key);
        }
        let key = String::from_utf8(result.stdout).map_err(|_| BackendFailure::Key)?;
        let key = key.trim_end_matches(['\n', '\r']);
        if key.is_empty() || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(BackendFailure::Key);
        }
        Ok(Credential(key.to_owned()))
    }
}

pub trait Transcribing {
    async fn transcribe(self) -> Result<Heard, Failure>;
}
impl Transcribing for Transcribe_Data {
    async fn transcribe(self) -> Result<Heard, Failure> {
        let protocol = match self.backend.protocol {
            Protocol::OpenAiRealtimeTranscription => {
                return self.realtime().await.map_err(Into::into);
            }
            Protocol::OpenAiTranscription => BatchProtocol::OpenAi,
            Protocol::Gemini => BatchProtocol::Gemini,
        };
        let duration = u64::try_from(self.backend.timeout.0.0)
            .ok()
            .filter(|seconds| *seconds > 0)
            .map(Duration::from_secs)
            .ok_or(BackendFailure::Timeout)?;
        let deadline = Instant::now()
            .checked_add(duration)
            .ok_or(BackendFailure::Timeout)?;
        let key = timeout_at(deadline, self.key())
            .await
            .map_err(|_| BackendFailure::Timeout)??;
        let path = self.audio_location.audio_path();
        let backend = self.backend;
        let mut prompt = backend.instruction.0.clone();
        if !self.term_vector.is_empty() {
            prompt.push_str("\nTerms: ");
            prompt.push_str(
                &self
                    .term_vector
                    .iter()
                    .map(|term| term.0.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        let work = Exchange {
            protocol,
            backend,
            key,
            prompt,
        };
        let result = tokio::task::spawn_blocking(move || {
            let audio = std::fs::read(path).map_err(|_| BackendFailure::Audio)?;
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(BackendFailure::Timeout)?;
            work.exchange(audio, remaining)
        });
        let heard = timeout_at(deadline, result)
            .await
            .map_err(|_| BackendFailure::Timeout)?
            .map_err(|_| BackendFailure::Network)??;
        Ok(heard)
    }
}

trait RealtimeTranscribing {
    async fn realtime(self) -> Result<Heard, BackendFailure>;
}

impl RealtimeTranscribing for Transcribe_Data {
    async fn realtime(self) -> Result<Heard, BackendFailure> {
        let helper = std::env::current_exe()
            .map_err(|_| BackendFailure::Network)?
            .with_file_name("listener-openai-realtime-transcription");
        let result = Command::new(helper)
            .args([
                "--endpoint",
                &self.backend.service_address.0,
                "--model",
                &self.backend.model_name.0,
                "--secret-program",
                &self.program.0,
                "--secret-entry",
                &self.backend.secret_entry.0,
                "--timeout",
                &self.backend.timeout.0.0.to_string(),
                "--audio",
            ])
            .arg(self.audio_location.audio_path())
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|_| BackendFailure::Network)?;
        if !result.status.success() {
            return Err(BackendFailure::Network);
        }
        let value: Value =
            serde_json::from_slice(&result.stdout).map_err(|_| BackendFailure::Malformed)?;
        let text = value
            .get("transcript")
            .and_then(Value::as_str)
            .ok_or(BackendFailure::Malformed)?;
        let id = value
            .get("item_id")
            .and_then(Value::as_str)
            .ok_or(BackendFailure::Malformed)?;
        Ok(Heard {
            text: Text(text.to_owned()),
            provider_id_option: Some(ProviderId(id.to_owned())),
        })
    }
}

enum BatchProtocol {
    OpenAi,
    Gemini,
}

struct Exchange {
    protocol: BatchProtocol,
    backend: Backend,
    key: Credential,
    prompt: String,
}

trait Exchanging {
    fn exchange(self, audio: Vec<u8>, remaining: Duration) -> Result<Heard, BackendFailure>;
    fn endpoint(&self) -> Result<String, BackendFailure>;
    fn open_ai(&self, agent: &Agent, endpoint: &str, audio: &[u8])
    -> Result<Heard, BackendFailure>;
    fn gemini(&self, agent: &Agent, endpoint: &str, audio: &[u8]) -> Result<Heard, BackendFailure>;
}
impl Exchanging for Exchange {
    fn exchange(self, audio: Vec<u8>, remaining: Duration) -> Result<Heard, BackendFailure> {
        let endpoint = self.endpoint()?;
        let agent = Agent::new_with_config(
            Agent::config_builder()
                .timeout_global(Some(remaining))
                .max_redirects(0)
                .http_status_as_error(false)
                .proxy(None)
                .build(),
        );
        match self.protocol {
            BatchProtocol::OpenAi => self.open_ai(&agent, &endpoint, &audio),
            BatchProtocol::Gemini => self.gemini(&agent, &endpoint, &audio),
        }
    }
    fn endpoint(&self) -> Result<String, BackendFailure> {
        let base = &self.backend.service_address.0;
        let uri: Uri = base.parse().map_err(|_| BackendFailure::Address)?;
        let host = uri.host().ok_or(BackendFailure::Address)?;
        let loopback = host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
        if !matches!(
            (uri.scheme_str(), loopback),
            (Some("https"), _) | (Some("http"), true)
        ) || uri.query().is_some()
            || uri
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
            || base.contains('#')
        {
            return Err(BackendFailure::Address);
        }
        let base = base.trim_end_matches('/');
        match self.protocol {
            BatchProtocol::OpenAi => Ok(format!("{base}/v1/audio/transcriptions")),
            BatchProtocol::Gemini => {
                let model = &self.backend.model_name.0;
                if model.is_empty()
                    || !model
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte))
                {
                    return Err(BackendFailure::Model);
                }
                Ok(format!("{base}/v1beta/models/{model}:generateContent"))
            }
        }
    }
    fn open_ai(
        &self,
        agent: &Agent,
        endpoint: &str,
        audio: &[u8],
    ) -> Result<Heard, BackendFailure> {
        let boundary =
            URL_SAFE_NO_PAD.encode(random_bytes::<24>().map_err(|_| BackendFailure::Network)?);
        let mut body = Vec::new();
        for (name, value) in [
            ("model", self.backend.model_name.0.as_str()),
            ("language", self.backend.language.0.as_str()),
            ("prompt", self.prompt.as_str()),
            ("response_format", "json"),
        ] {
            if !value.is_empty() {
                write!(body, "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n")
                    .map_err(|_| BackendFailure::Network)?;
            }
        }
        write!(body, "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"recording.ogg\"\r\nContent-Type: audio/ogg\r\n\r\n")
            .map_err(|_| BackendFailure::Network)?;
        body.extend_from_slice(audio);
        write!(body, "\r\n--{boundary}--\r\n").map_err(|_| BackendFailure::Network)?;
        let response = agent
            .post(endpoint)
            .header("authorization", format!("Bearer {}", self.key.0))
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .send(body.as_slice())?;
        let (value, request_id) = response.read_json()?;
        let text = value
            .get("text")
            .and_then(Value::as_str)
            .ok_or(BackendFailure::Malformed)?;
        let id = value
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or(request_id);
        Ok(Heard {
            text: Text(text.to_owned()),
            provider_id_option: id.map(ProviderId),
        })
    }
    fn gemini(&self, agent: &Agent, endpoint: &str, audio: &[u8]) -> Result<Heard, BackendFailure> {
        let language = &self.backend.language.0;
        let prompt = if language.is_empty() {
            self.prompt.clone()
        } else {
            format!("{}\nLanguage: {language}", self.prompt)
        };
        let body = json!({ "contents": [{ "role": "user", "parts": [
            { "text": prompt },
            { "inlineData": { "mimeType": "audio/ogg", "data": STANDARD.encode(audio) } }
        ] }] })
        .to_string();
        let response = agent
            .post(endpoint)
            .header("x-goog-api-key", self.key.0.as_str())
            .header("content-type", "application/json")
            .send(body.as_bytes())?;
        let (value, request_id) = response.read_json()?;
        let candidate = value
            .get("candidates")
            .and_then(|items| items.get(0))
            .ok_or(BackendFailure::Malformed)?;
        if candidate
            .get("finishReason")
            .and_then(Value::as_str)
            .is_some_and(|reason| reason != "STOP")
        {
            return Err(BackendFailure::Malformed);
        }
        let parts = candidate
            .pointer("/content/parts")
            .and_then(Value::as_array)
            .ok_or(BackendFailure::Malformed)?;
        let mut text = parts
            .iter()
            .filter(|part| part.get("thought").and_then(Value::as_bool) != Some(true))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .peekable();
        if text.peek().is_none() {
            return Err(BackendFailure::Malformed);
        }
        let id = value
            .get("responseId")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or(request_id);
        Ok(Heard {
            text: Text(text.collect()),
            provider_id_option: id.map(ProviderId),
        })
    }
}

trait ReadingResponse {
    fn read_json(self) -> Result<(Value, Option<String>), BackendFailure>;
}
impl ReadingResponse for Response<ureq::Body> {
    fn read_json(mut self) -> Result<(Value, Option<String>), BackendFailure> {
        if !self.status().is_success() {
            return Err(BackendFailure::Status(self.status().as_u16()));
        }
        let request = self
            .headers()
            .get("x-request-id")
            .and_then(|header| header.to_str().ok())
            .map(str::to_owned);
        let body = self
            .body_mut()
            .with_config()
            .limit(signal::MAXIMUM_SIGNAL_BYTES as u64)
            .read_to_vec()?;
        Ok((
            serde_json::from_slice(&body).map_err(|_| BackendFailure::Malformed)?,
            request,
        ))
    }
}
