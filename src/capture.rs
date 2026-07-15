use std::{
    env, fs,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime},
};

use signal_listener::{
    AudioArtifactPath, CaptureSession, DurableAudioArtifact, InputSource, WirePath,
};

use crate::{
    CompactAudioArtifact, Configuration, Error, LiveOpusWebmEncoder, OpusWebmEncoder,
    RecordingAudioFormat, RecordingLog, RecordingLogHeader, RecordingLogWriter,
    RecoveredRecordingLog, Result, StatusPublisher, artifact_privacy::OwnerPrivateDirectory,
};

const LIVE_LEVEL_SAMPLE_DURATION: Duration = Duration::from_millis(50);
const MILLISECONDS_PER_DAY: u64 = 24 * 60 * 60 * 1_000;

/// A finite age bound for retained capture media.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CaptureRetentionAge {
    milliseconds: u64,
}

impl CaptureRetentionAge {
    pub fn from_days(days: u64) -> Option<Self> {
        days.checked_mul(MILLISECONDS_PER_DAY)
            .map(|milliseconds| Self { milliseconds })
    }

    pub fn from_milliseconds(milliseconds: u64) -> Self {
        Self { milliseconds }
    }

    pub fn milliseconds(&self) -> u64 {
        self.milliseconds
    }

    fn expires(&self, modified_at: SystemTime, evaluated_at: SystemTime) -> bool {
        evaluated_at
            .duration_since(modified_at)
            .map(|elapsed| elapsed.as_millis() >= u128::from(self.milliseconds))
            .unwrap_or(false)
    }
}

/// A byte bound for all retained capture media.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CaptureRetentionByteLimit {
    bytes: u64,
}

impl CaptureRetentionByteLimit {
    pub fn new(bytes: u64) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Explicit policy for abandoned, cancelled, or failed capture media.
///
/// No media is deleted by this policy until an owner configures at least one
/// bound. `LISTENER_CAPTURE_RETENTION_DAYS` and
/// `LISTENER_CAPTURE_RETENTION_MAXIMUM_BYTES` may be set independently.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CaptureRetentionPolicy {
    maximum_age: Option<CaptureRetentionAge>,
    maximum_bytes: Option<CaptureRetentionByteLimit>,
}

impl CaptureRetentionPolicy {
    pub fn new(
        maximum_age: Option<CaptureRetentionAge>,
        maximum_bytes: Option<CaptureRetentionByteLimit>,
    ) -> Self {
        Self {
            maximum_age,
            maximum_bytes,
        }
    }

    pub fn from_environment() -> Result<Self> {
        let maximum_age = match Self::environment_u64("LISTENER_CAPTURE_RETENTION_DAYS")? {
            Some(days) => Some(CaptureRetentionAge::from_days(days).ok_or_else(|| {
                Error::InvalidCaptureRetentionPolicy {
                    variable: "LISTENER_CAPTURE_RETENTION_DAYS".to_owned(),
                    value: days.to_string(),
                }
            })?),
            None => None,
        };
        let maximum_bytes = Self::environment_u64("LISTENER_CAPTURE_RETENTION_MAXIMUM_BYTES")?
            .map(CaptureRetentionByteLimit::new);
        Ok(Self::new(maximum_age, maximum_bytes))
    }

    pub fn maximum_age(&self) -> Option<CaptureRetentionAge> {
        self.maximum_age
    }

    pub fn maximum_bytes(&self) -> Option<CaptureRetentionByteLimit> {
        self.maximum_bytes
    }

    fn environment_u64(variable: &str) -> Result<Option<u64>> {
        let Some(value) = env::var_os(variable) else {
            return Ok(None);
        };
        let value = value.to_string_lossy().into_owned();
        value
            .parse()
            .map(Some)
            .map_err(|_| Error::InvalidCaptureRetentionPolicy {
                variable: variable.to_owned(),
                value,
            })
    }
}

pub trait AudioCaptureBackend {
    fn start(&self, request: AudioCaptureStart) -> Result<Box<dyn ActiveAudioCapture>>;
}

pub trait ActiveAudioCapture {
    fn artifact(&self) -> &DurableAudioArtifact;

    fn stop(self: Box<Self>) -> Result<DurableAudioArtifact>;

    fn cancel(self: Box<Self>) -> Result<DurableAudioArtifact> {
        self.stop()
    }
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
        OwnerPrivateDirectory::new(&self.directory).ensure()?;
        Ok(())
    }

    pub fn recover_recording_logs(&self) -> Result<RecoveredCaptureRecordings> {
        self.cleanup_abandoned_intermediates()?;
        let recovered = self.recording_logs()?.recover()?;
        Ok(RecoveredCaptureRecordings::new(
            recovered.recordings,
            self.next_session_value_after_existing_artifacts()?,
        ))
    }

    /// Remove capture media whose terminal transcript is already durable.
    ///
    /// Call only while no capture is active: the known-session scan includes
    /// every canonical artifact name, including an active recording log.
    pub fn reclaim_completed_captures<F>(&self, is_completed: F) -> Result<()>
    where
        F: Fn(&CaptureSession) -> Result<bool>,
    {
        for session in self.known_sessions()? {
            if is_completed(&session)? {
                self.remove_terminal_capture_artifacts(&session)?;
            }
        }
        Ok(())
    }

    /// Enforce an explicitly configured retention policy over capture media.
    /// Older sessions are reclaimed first, preserving the most recent capture
    /// that can fit in a configured byte budget.
    pub fn enforce_retention(&self, retention: CaptureRetentionPolicy) -> Result<()> {
        self.enforce_retention_at(retention, SystemTime::now())
    }

    /// Enforce retention against a supplied clock for deterministic maintenance
    /// tests and future daemon-owned scheduling.
    pub fn enforce_retention_at(
        &self,
        retention: CaptureRetentionPolicy,
        evaluated_at: SystemTime,
    ) -> Result<()> {
        let mut retained = self.retained_captures()?;
        if let Some(maximum_age) = retention.maximum_age() {
            let mut still_retained = Vec::new();
            for capture in retained {
                if maximum_age.expires(capture.latest_modification, evaluated_at) {
                    self.remove_terminal_capture_artifacts(&capture.session)?;
                } else {
                    still_retained.push(capture);
                }
            }
            retained = still_retained;
        }
        if let Some(maximum_bytes) = retention.maximum_bytes() {
            retained.sort_by_key(|capture| capture.session.value());
            let mut retained_bytes = retained
                .iter()
                .fold(0_u64, |total, capture| total.saturating_add(capture.bytes));
            for capture in retained {
                if retained_bytes <= maximum_bytes.bytes() {
                    break;
                }
                self.remove_terminal_capture_artifacts(&capture.session)?;
                retained_bytes = retained_bytes.saturating_sub(capture.bytes);
            }
        }
        Ok(())
    }

    pub fn enforce_environment_retention(&self) -> Result<()> {
        self.enforce_retention(CaptureRetentionPolicy::from_environment()?)
    }

    pub fn next_session_value_after_existing_artifacts(&self) -> Result<u64> {
        match self.known_sessions()?.last() {
            Some(session) => {
                session
                    .value()
                    .checked_add(1)
                    .ok_or(Error::CaptureSessionSequenceExhausted {
                        last_session: session.value(),
                    })
            }
            None => Ok(1),
        }
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

    pub fn compact_audio_path_for_session(&self, session: &CaptureSession) -> PathBuf {
        self.directory
            .join(format!("capture-{}.webm", session.value()))
    }

    pub fn compact_artifact_for_session(&self, session: &CaptureSession) -> DurableAudioArtifact {
        DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
            self.compact_audio_path_for_session(session)
                .to_string_lossy()
                .into_owned(),
        )))
    }

    pub fn failed_marker_path_for_session(&self, session: &CaptureSession) -> PathBuf {
        self.directory
            .join(format!("capture-{}.transcription-failed", session.value()))
    }

    pub fn mark_transcription_failed(&self, session: &CaptureSession) -> Result<()> {
        self.prepare()?;
        fs::write(self.failed_marker_path_for_session(session), [])?;
        Ok(())
    }

    pub fn clear_transcription_failure(&self, session: &CaptureSession) -> Result<()> {
        match fs::remove_file(self.failed_marker_path_for_session(session)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn known_sessions(&self) -> Result<Vec<CaptureSession>> {
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut sessions = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if let Some(session) = CaptureArtifactPathCandidate::new(&path).any_session_value() {
                sessions.push(CaptureSession::new(session));
            }
        }
        sessions.sort_by_key(CaptureSession::value);
        sessions.dedup_by_key(|session| session.value());
        Ok(sessions)
    }

    /// Returns the compact artifact after the live encoder has atomically
    /// finalized it. The recovery log remains authoritative until this
    /// validation succeeds.
    pub fn finalize_live_compact_for_session(
        &self,
        session: &CaptureSession,
    ) -> Result<DurableAudioArtifact> {
        self.prepare()?;
        let compact = CompactAudioArtifact::new(self.compact_audio_path_for_session(session));
        compact.validate()?;
        self.remove_recovery_material(session)?;
        Ok(self.compact_artifact_for_session(session))
    }

    pub fn compact_audio_for_session(
        &self,
        session: &CaptureSession,
    ) -> Result<DurableAudioArtifact> {
        self.prepare()?;
        let compact_path = self.compact_audio_path_for_session(session);
        let compact = CompactAudioArtifact::new(&compact_path);
        let recording_log_path = PathBuf::from(self.artifact_for_session(session).path().as_str());
        if compact_path.exists() {
            match compact.validate() {
                Ok(()) => return self.finalize_live_compact_for_session(session),
                Err(Error::CompactAudioInvalid { .. }) => self.remove_if_exists(&compact_path)?,
                Err(error) => return Err(error),
            }
        }
        compact.discard_partial()?;
        if !recording_log_path.exists() {
            return Err(Error::CaptureNotFound {
                session: session.value(),
            });
        }
        let recovered = RecordingLog::new(&recording_log_path).recover()?;
        let temporary_pcm = self.recovery_pcm_path_for_session(session);
        let export = match recovered.export_raw_pcm(&temporary_pcm) {
            Ok(export) => export,
            Err(error) => {
                self.remove_if_exists(&temporary_pcm)?;
                return Err(error);
            }
        };
        let encoding = OpusWebmEncoder::from_environment().encode_pcm(
            export.path(),
            export.audio_format(),
            compact,
        );
        self.remove_if_exists(&temporary_pcm)?;
        encoding?;
        self.finalize_live_compact_for_session(session)
    }

    fn remove_recovery_material(&self, session: &CaptureSession) -> Result<()> {
        self.remove_if_exists(Path::new(
            self.artifact_for_session(session).path().as_str(),
        ))?;
        self.remove_if_exists(&self.raw_pcm_export_path_for_session(session))
    }

    fn remove_terminal_capture_artifacts(&self, session: &CaptureSession) -> Result<()> {
        for path in self.terminal_artifact_paths(session) {
            self.remove_if_exists(&path)?;
        }
        Ok(())
    }

    fn cleanup_abandoned_intermediates(&self) -> Result<()> {
        for session in self.known_sessions()? {
            for path in self.intermediate_artifact_paths(&session) {
                self.remove_if_exists(&path)?;
            }
            self.remove_unusable_compact_artifact(&session)?;
        }
        Ok(())
    }

    fn remove_unusable_compact_artifact(&self, session: &CaptureSession) -> Result<()> {
        let compact_path = self.compact_audio_path_for_session(session);
        if !compact_path.exists() {
            return Ok(());
        }
        match CompactAudioArtifact::new(&compact_path).validate() {
            Ok(()) => Ok(()),
            Err(Error::CompactAudioInvalid { .. }) => {
                self.remove_if_exists(&compact_path)?;
                let recording_log = self.artifact_for_session(session);
                if !Path::new(recording_log.path().as_str()).exists() {
                    self.remove_if_exists(&self.failed_marker_path_for_session(session))?;
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn retained_captures(&self) -> Result<Vec<RetainedCapture>> {
        let mut retained = Vec::new();
        for session in self.known_sessions()? {
            let mut bytes = 0_u64;
            let mut latest_modification = SystemTime::UNIX_EPOCH;
            let mut exists = false;
            for path in self.retained_artifact_paths(&session) {
                match fs::metadata(path) {
                    Ok(metadata) => {
                        exists = true;
                        bytes = bytes.saturating_add(metadata.len());
                        latest_modification = latest_modification.max(metadata.modified()?);
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            if exists {
                retained.push(RetainedCapture::new(session, bytes, latest_modification));
            }
        }
        Ok(retained)
    }

    fn retained_artifact_paths(&self, session: &CaptureSession) -> [PathBuf; 3] {
        [
            PathBuf::from(self.artifact_for_session(session).path().as_str()),
            self.compact_audio_path_for_session(session),
            self.failed_marker_path_for_session(session),
        ]
    }

    fn intermediate_artifact_paths(&self, session: &CaptureSession) -> [PathBuf; 4] {
        [
            self.raw_pcm_export_path_for_session(session),
            self.recovery_pcm_path_for_session(session),
            self.compact_partial_path_for_session(session),
            self.compact_encoding_path_for_session(session),
        ]
    }

    fn terminal_artifact_paths(&self, session: &CaptureSession) -> [PathBuf; 7] {
        let [recording_log, compact, failed_marker] = self.retained_artifact_paths(session);
        let [raw_pcm, recovery_pcm, compact_partial, compact_encoding] =
            self.intermediate_artifact_paths(session);
        [
            recording_log,
            compact,
            failed_marker,
            raw_pcm,
            recovery_pcm,
            compact_partial,
            compact_encoding,
        ]
    }

    fn raw_pcm_export_path_for_session(&self, session: &CaptureSession) -> PathBuf {
        self.raw_pcm_export_for_artifact(&self.artifact_for_session(session))
    }

    fn recovery_pcm_path_for_session(&self, session: &CaptureSession) -> PathBuf {
        self.directory
            .join(format!("capture-{}.encoding.s16le", session.value()))
    }

    fn compact_partial_path_for_session(&self, session: &CaptureSession) -> PathBuf {
        PathBuf::from(format!(
            "{}.part",
            self.compact_audio_path_for_session(session).display()
        ))
    }

    fn compact_encoding_path_for_session(&self, session: &CaptureSession) -> PathBuf {
        self.compact_audio_path_for_session(session)
            .with_extension("webm.encoding")
    }

    fn remove_if_exists(&self, path: &Path) -> Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
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

#[derive(Clone, Debug)]
struct RetainedCapture {
    session: CaptureSession,
    bytes: u64,
    latest_modification: SystemTime,
}

impl RetainedCapture {
    fn new(session: CaptureSession, bytes: u64, latest_modification: SystemTime) -> Self {
        Self {
            session,
            bytes,
            latest_modification,
        }
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
        let mut recordings = Vec::new();
        for path in &self.paths {
            if path.is_file() {
                recordings.push(RecordingLog::new(path).recover()?);
            }
        }
        Ok(RecoveredCaptureRecordings::new(recordings, 1))
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

    fn any_session_value(&self) -> Option<u64> {
        let file_name = self.path.file_name()?.to_str()?;
        let session = file_name.strip_prefix("capture-")?.split('.').next()?;
        session.parse().ok()
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
        OwnerPrivateDirectory::new(parent).ensure()?;

        let header = RecordingLogHeader::from_capture_start(
            request.session(),
            request.input_source(),
            self.audio_format,
        )?;
        let recording_log = RecordingLogWriter::create(&artifact_path, header)?;
        let compact_path = artifact_path.with_extension("webm");
        let live_encoder = LiveOpusWebmEncoder::start(
            OpusWebmEncoder::from_environment(),
            self.audio_format,
            CompactAudioArtifact::new(&compact_path),
        )?;
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
        let writer = CaptureWriter::new(
            stdout,
            recording_log,
            request.status_publisher(),
            Some(live_encoder.sender()),
        )
        .spawn();

        Ok(Box::new(ProcessAudioCapture {
            recovery_artifact: request.artifact().clone(),
            compact_artifact: DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
                compact_path.to_string_lossy().into_owned(),
            ))),
            child,
            writer,
            live_encoder,
        }))
    }
}

pub struct ProcessAudioCapture {
    recovery_artifact: DurableAudioArtifact,
    compact_artifact: DurableAudioArtifact,
    child: Child,
    writer: JoinHandle<Result<()>>,
    live_encoder: LiveOpusWebmEncoder,
}

impl ActiveAudioCapture for ProcessAudioCapture {
    fn artifact(&self) -> &DurableAudioArtifact {
        &self.recovery_artifact
    }

    fn stop(mut self: Box<Self>) -> Result<DurableAudioArtifact> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        self.child.wait()?;
        self.writer
            .join()
            .map_err(|_| Error::CaptureWriterThread)??;
        self.live_encoder.finish()?;
        Ok(self.compact_artifact.clone())
    }

    fn cancel(mut self: Box<Self>) -> Result<DurableAudioArtifact> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        self.child.wait()?;
        self.writer
            .join()
            .map_err(|_| Error::CaptureWriterThread)??;
        let _ = self.live_encoder.finish();
        let _ = fs::remove_file(self.compact_artifact.path().as_str());
        let _ = CompactAudioArtifact::new(self.compact_artifact.path().as_str()).discard_partial();
        Ok(self.recovery_artifact.clone())
    }
}

pub struct CaptureWriter<Input> {
    input: Input,
    recording_log: RecordingLogWriter,
    pending_pcm: CaptureWriterPendingPcm,
    status_publisher: StatusPublisher,
    live_encoder: Option<std::sync::mpsc::Sender<Vec<u8>>>,
    read_buffer_bytes: usize,
}

impl<Input: Read> CaptureWriter<Input> {
    pub fn new(
        input: Input,
        recording_log: RecordingLogWriter,
        status_publisher: StatusPublisher,
        live_encoder: Option<std::sync::mpsc::Sender<Vec<u8>>>,
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
            live_encoder,
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
                self.live_encoder.as_ref(),
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
        live_encoder: Option<&std::sync::mpsc::Sender<Vec<u8>>>,
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
            if let Some(live_encoder) = live_encoder {
                let _ = live_encoder.send(payload.to_vec());
            }
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
