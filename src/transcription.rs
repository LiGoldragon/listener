use std::{path::PathBuf, process::Command};

use signal_listener::{DurableAudioArtifact, TranscriptText};

use crate::{Error, RecordingAudioFormat, Result};

pub trait BatchTranscriber {
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfiguredBatchTranscriber {
    command: Option<BatchTranscriptionCommand>,
    stub: HonestStubTranscriber,
}

impl ConfiguredBatchTranscriber {
    pub fn from_environment() -> Self {
        Self {
            command: std::env::var("LISTENER_TRANSCRIPTION_PROGRAM")
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
