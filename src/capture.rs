use std::{
    fs,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread::{self, JoinHandle},
    time::Duration,
};

use signal_listener::{
    AudioArtifactPath, CaptureSession, DurableAudioArtifact, InputSource, WirePath,
};

use crate::{
    Configuration, Error, RecordingAudioFormat, RecordingLog, RecordingLogHeader,
    RecordingLogWriter, RecoveredRecordingLog, Result, StatusPublisher,
};

const LIVE_LEVEL_SAMPLE_DURATION: Duration = Duration::from_millis(50);

pub trait AudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> Result<Box<dyn ActiveAudioCapture>>;
}

pub trait ActiveAudioCapture {
    fn artifact(&self) -> &DurableAudioArtifact;

    fn stop(self: Box<Self>) -> Result<DurableAudioArtifact>;
}

#[derive(Clone)]
pub struct AudioCaptureStart {
    session: CaptureSession,
    artifact: DurableAudioArtifact,
    input_source: InputSource,
    status_publisher: StatusPublisher,
}

impl AudioCaptureStart {
    pub fn new(
        session: CaptureSession,
        artifact: DurableAudioArtifact,
        input_source: InputSource,
        status_publisher: StatusPublisher,
    ) -> Self {
        Self {
            session,
            artifact,
            input_source,
            status_publisher,
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

    pub fn status_publisher(&self) -> StatusPublisher {
        self.status_publisher.clone()
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

    pub fn recover_recording_logs(&self) -> Result<RecoveredCaptureRecordings> {
        self.recording_logs()?.recover()
    }

    pub fn next_session_value_after_existing_artifacts(&self) -> Result<u64> {
        self.recording_logs()?.next_session_value()
    }

    pub fn artifact_for_session(&self, session: &CaptureSession) -> DurableAudioArtifact {
        let file_name = format!("capture-{}.listenerlog", session.value());
        DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
            self.directory
                .join(file_name)
                .to_string_lossy()
                .into_owned(),
        )))
    }

    pub fn raw_pcm_export_for_artifact(&self, artifact: &DurableAudioArtifact) -> PathBuf {
        let mut path = PathBuf::from(artifact.path().as_str());
        path.set_extension("raw.s16le");
        path
    }

    fn recording_logs(&self) -> Result<CaptureStoreRecordingLogs> {
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(CaptureStoreRecordingLogs::empty());
            }
            Err(error) => return Err(error.into()),
        };

        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if CaptureArtifactPathCandidate::new(&path).is_listener_log() {
                paths.push(path);
            }
        }
        paths.sort();
        Ok(CaptureStoreRecordingLogs::new(paths))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredCaptureRecordings {
    recordings: Vec<RecoveredRecordingLog>,
    next_session_value: u64,
}

impl RecoveredCaptureRecordings {
    pub fn empty() -> Self {
        Self {
            recordings: Vec::new(),
            next_session_value: 1,
        }
    }

    fn new(recordings: Vec<RecoveredRecordingLog>, next_session_value: u64) -> Self {
        Self {
            recordings,
            next_session_value,
        }
    }

    pub fn as_slice(&self) -> &[RecoveredRecordingLog] {
        self.recordings.as_slice()
    }

    pub fn next_session_value(&self) -> u64 {
        self.next_session_value
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CaptureStoreRecordingLogs {
    paths: Vec<PathBuf>,
}

impl CaptureStoreRecordingLogs {
    fn empty() -> Self {
        Self { paths: Vec::new() }
    }

    fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }

    fn recover(&self) -> Result<RecoveredCaptureRecordings> {
        let next_session_value = self.next_session_value()?;
        let mut recordings = Vec::new();
        for path in &self.paths {
            if path.is_file() {
                recordings.push(RecordingLog::new(path).recover()?);
            }
        }
        Ok(RecoveredCaptureRecordings::new(
            recordings,
            next_session_value,
        ))
    }

    fn next_session_value(&self) -> Result<u64> {
        let latest_session_value = self
            .paths
            .iter()
            .filter_map(|path| CaptureArtifactPathCandidate::new(path).session_value())
            .max();
        match latest_session_value {
            Some(value) => value
                .checked_add(1)
                .ok_or(Error::CaptureSessionSequenceExhausted {
                    last_session: value,
                }),
            None => Ok(1),
        }
    }
}

struct CaptureArtifactPathCandidate<'a> {
    path: &'a Path,
}

impl<'a> CaptureArtifactPathCandidate<'a> {
    fn new(path: &'a Path) -> Self {
        Self { path }
    }

    fn is_listener_log(&self) -> bool {
        self.path
            .extension()
            .is_some_and(|extension| extension == "listenerlog")
    }

    fn session_value(&self) -> Option<u64> {
        let file_name = self.path.file_name()?.to_str()?;
        file_name
            .strip_prefix("capture-")?
            .strip_suffix(".listenerlog")?
            .parse()
            .ok()
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
    audio_format: RecordingAudioFormat,
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
                "--latency-msec=50".to_owned(),
                "--process-time-msec=25".to_owned(),
            ],
        )
    }

    pub fn new(program: impl Into<String>, arguments: Vec<String>) -> Self {
        Self::new_with_audio_format(
            program,
            arguments,
            RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
        )
    }

    pub fn new_with_audio_format(
        program: impl Into<String>,
        arguments: Vec<String>,
        audio_format: RecordingAudioFormat,
    ) -> Self {
        Self {
            program: program.into(),
            arguments,
            audio_format,
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

        let header = RecordingLogHeader::from_capture_start(
            request.session(),
            request.input_source(),
            self.audio_format,
        )?;
        let recording_log = RecordingLogWriter::create(&artifact_path, header)?;
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
        let writer = CaptureWriter::new(stdout, recording_log, request.status_publisher()).spawn();

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

pub struct CaptureWriter<Input> {
    input: Input,
    recording_log: RecordingLogWriter,
    pending_pcm: CaptureWriterPendingPcm,
    status_publisher: StatusPublisher,
    read_buffer_bytes: usize,
}

impl<Input: Read> CaptureWriter<Input> {
    pub fn new(
        input: Input,
        recording_log: RecordingLogWriter,
        status_publisher: StatusPublisher,
    ) -> Self {
        let pending_pcm = CaptureWriterPendingPcm::new(recording_log.audio_format());
        let read_buffer_bytes = CaptureWriterReadWindow::new(
            recording_log.audio_format(),
            LIVE_LEVEL_SAMPLE_DURATION,
            recording_log.maximum_record_payload_bytes(),
        )
        .bytes();
        Self {
            input,
            recording_log,
            pending_pcm,
            status_publisher,
            read_buffer_bytes,
        }
    }

    pub fn write_until_capture_stops(mut self) -> Result<()> {
        let mut read_buffer = vec![0_u8; self.read_buffer_bytes];
        loop {
            let read_count = self.input.read(&mut read_buffer)?;
            if read_count == 0 {
                break;
            }
            self.pending_pcm.push_bytes(
                &read_buffer[..read_count],
                &mut self.recording_log,
                &self.status_publisher,
            )?;
        }
        self.pending_pcm.finish()?;
        self.recording_log.finish()
    }
}

impl<Input: Read + Send + 'static> CaptureWriter<Input> {
    pub fn spawn(self) -> JoinHandle<Result<()>> {
        thread::spawn(move || self.write_until_capture_stops())
    }
}

struct CaptureWriterReadWindow {
    audio_format: RecordingAudioFormat,
    duration: Duration,
    maximum_record_payload_bytes: u32,
}

impl CaptureWriterReadWindow {
    fn new(
        audio_format: RecordingAudioFormat,
        duration: Duration,
        maximum_record_payload_bytes: u32,
    ) -> Self {
        Self {
            audio_format,
            duration,
            maximum_record_payload_bytes,
        }
    }

    fn bytes(&self) -> usize {
        let sample_rate = u128::from(self.audio_format.sample_rate());
        let window_milliseconds = self.duration.as_millis().max(1);
        let frames = (sample_rate * window_milliseconds / 1_000).max(1);
        let window_bytes = frames * u128::from(self.audio_format.bytes_per_frame());
        let maximum_record_payload_bytes = u128::from(self.maximum_record_payload_bytes);
        window_bytes
            .min(maximum_record_payload_bytes)
            .max(u128::from(self.audio_format.bytes_per_frame())) as usize
    }
}

struct CaptureWriterPendingPcm {
    audio_format: RecordingAudioFormat,
    bytes: Vec<u8>,
}

impl CaptureWriterPendingPcm {
    fn new(audio_format: RecordingAudioFormat) -> Self {
        Self {
            audio_format,
            bytes: Vec::new(),
        }
    }

    fn push_bytes(
        &mut self,
        bytes: &[u8],
        recording_log: &mut RecordingLogWriter,
        status_publisher: &StatusPublisher,
    ) -> Result<()> {
        self.bytes.extend_from_slice(bytes);
        let bytes_per_frame = usize::from(self.audio_format.bytes_per_frame());
        let complete_length = self.bytes.len() - (self.bytes.len() % bytes_per_frame);
        if complete_length == 0 {
            return Ok(());
        }

        let complete_bytes: Vec<u8> = self.bytes.drain(..complete_length).collect();
        for payload in complete_bytes.chunks(recording_log.maximum_record_payload_bytes() as usize)
        {
            status_publisher.publish_recording_level(
                crate::MicrophoneLevel::from_recording_payload(
                    payload,
                    self.audio_format.sample_format(),
                ),
            );
            recording_log.append_record(payload)?;
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(Error::IncompletePcmFrame {
                remaining_bytes: self.bytes.len(),
                bytes_per_frame: self.audio_format.bytes_per_frame(),
            })
        }
    }
}
