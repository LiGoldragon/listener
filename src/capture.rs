use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread::{self, JoinHandle},
};

use signal_listener::{
    AudioArtifactPath, CaptureSession, DurableAudioArtifact, InputSource, WirePath,
};

use crate::{Configuration, Error, Result};

pub trait AudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> Result<Box<dyn ActiveAudioCapture>>;
}

pub trait ActiveAudioCapture {
    fn artifact(&self) -> &DurableAudioArtifact;

    fn stop(self: Box<Self>) -> Result<DurableAudioArtifact>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AudioCaptureStart {
    session: CaptureSession,
    artifact: DurableAudioArtifact,
    input_source: InputSource,
}

impl AudioCaptureStart {
    pub fn new(
        session: CaptureSession,
        artifact: DurableAudioArtifact,
        input_source: InputSource,
    ) -> Self {
        Self {
            session,
            artifact,
            input_source,
        }
    }

    pub fn session(&self) -> &CaptureSession {
        &self.session
    }

    pub fn artifact(&self) -> &DurableAudioArtifact {
        &self.artifact
    }

    pub fn artifact_path(&self) -> PathBuf {
        PathBuf::from(self.artifact.path().as_str())
    }

    pub fn input_source(&self) -> InputSource {
        self.input_source
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureStore {
    directory: PathBuf,
}

impl CaptureStore {
    pub fn from_configuration(configuration: &Configuration) -> Self {
        Self::new(configuration.capture_store_directory())
    }

    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn prepare(&self) -> Result<()> {
        fs::create_dir_all(&self.directory)?;
        Ok(())
    }

    pub fn artifact_for_session(&self, session: &CaptureSession) -> DurableAudioArtifact {
        let file_name = format!("capture-{}.s16le", session.value());
        DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
            self.directory
                .join(file_name)
                .to_string_lossy()
                .into_owned(),
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessAudioCaptureBackend {
    command: AudioCaptureCommand,
}

impl ProcessAudioCaptureBackend {
    pub fn from_environment() -> Self {
        Self::new(AudioCaptureCommand::from_environment())
    }

    pub fn new(command: AudioCaptureCommand) -> Self {
        Self { command }
    }
}

impl AudioCaptureBackend for ProcessAudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> Result<Box<dyn ActiveAudioCapture>> {
        self.command.spawn(request)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AudioCaptureCommand {
    program: String,
    arguments: Vec<String>,
}

impl AudioCaptureCommand {
    pub fn from_environment() -> Self {
        let program =
            std::env::var("LISTENER_CAPTURE_PROGRAM").unwrap_or_else(|_| "parecord".to_owned());
        Self::new(
            program,
            vec![
                "--device=@DEFAULT_SOURCE@".to_owned(),
                "--raw".to_owned(),
                "--format=s16le".to_owned(),
                "--rate=16000".to_owned(),
                "--channels=1".to_owned(),
            ],
        )
    }

    pub fn new(program: impl Into<String>, arguments: Vec<String>) -> Self {
        Self {
            program: program.into(),
            arguments,
        }
    }

    pub fn spawn(&self, request: AudioCaptureStart) -> Result<Box<dyn ActiveAudioCapture>> {
        match request.input_source() {
            InputSource::SystemDefault => self.spawn_default_source(request),
        }
    }

    fn spawn_default_source(
        &self,
        request: AudioCaptureStart,
    ) -> Result<Box<dyn ActiveAudioCapture>> {
        let artifact_path = request.artifact_path();
        let parent = artifact_path
            .parent()
            .ok_or_else(|| Error::PathParentMissing {
                path: artifact_path.display().to_string(),
            })?;
        fs::create_dir_all(parent)?;

        let file = File::create(&artifact_path)?;
        let mut child = Command::new(&self.program)
            .args(&self.arguments)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| Error::AudioBackendUnavailable {
                message: format!("failed to start {}: {error}", self.program),
            })?;

        let stdout = child
            .stdout
            .take()
            .ok_or(Error::CaptureProcessStdoutUnavailable)?;
        let writer = CaptureWriter::new(stdout, file).spawn();

        Ok(Box::new(ProcessAudioCapture {
            artifact: request.artifact().clone(),
            child,
            writer,
        }))
    }
}

pub struct ProcessAudioCapture {
    artifact: DurableAudioArtifact,
    child: Child,
    writer: JoinHandle<Result<()>>,
}

impl ActiveAudioCapture for ProcessAudioCapture {
    fn artifact(&self) -> &DurableAudioArtifact {
        &self.artifact
    }

    fn stop(mut self: Box<Self>) -> Result<DurableAudioArtifact> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        self.child.wait()?;
        self.writer
            .join()
            .map_err(|_| Error::CaptureWriterThread)??;
        Ok(self.artifact)
    }
}

pub struct CaptureWriter {
    stdout: std::process::ChildStdout,
    file: File,
}

impl CaptureWriter {
    pub fn new(stdout: std::process::ChildStdout, file: File) -> Self {
        Self { stdout, file }
    }

    pub fn spawn(self) -> JoinHandle<Result<()>> {
        thread::spawn(move || self.write_until_capture_stops())
    }

    fn write_until_capture_stops(self) -> Result<()> {
        let mut reader = BufReader::new(self.stdout);
        let mut writer = BufWriter::new(self.file);
        std::io::copy(&mut reader, &mut writer)?;
        writer.flush()?;
        let file = writer.into_inner().map_err(|error| error.into_error())?;
        file.sync_all()?;
        Ok(())
    }
}
