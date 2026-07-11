use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::mpsc,
    thread::{self, JoinHandle},
};

use crate::{
    Error, RecordingAudioFormat, Result,
    artifact_privacy::{OWNER_PRIVATE_FILE_MODE, OwnerPrivateDirectory},
};

/// A durable, OpenAI-compatible speech artifact encoded as Opus in WebM.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactAudioArtifact {
    path: PathBuf,
}

impl CompactAudioArtifact {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn bytes(&self) -> Result<u64> {
        Ok(fs::metadata(&self.path)?.len())
    }

    pub fn validate(&self) -> Result<()> {
        if self.bytes()? == 0 {
            return Err(Error::CompactAudioInvalid {
                path: self.path.display().to_string(),
            });
        }
        Ok(())
    }
}

/// The live FFmpeg process that turns durable capture records into a compact
/// WebM/Opus artifact while capture continues.
///
/// The recording writer commits every PCM record before enqueueing it here. The
/// queue isolates capture from encoder I/O; if the process exits, capture keeps
/// its recoverable recording log and stop reports the encoder failure.
pub struct LiveOpusWebmEncoder {
    encoder: OpusWebmEncoder,
    destination: CompactAudioArtifact,
    partial: CompactAudioArtifact,
    sender: Option<mpsc::Sender<Vec<u8>>>,
    worker: Option<JoinHandle<Result<()>>>,
}

impl LiveOpusWebmEncoder {
    pub fn start(
        encoder: OpusWebmEncoder,
        audio_format: RecordingAudioFormat,
        destination: CompactAudioArtifact,
    ) -> Result<Self> {
        let parent = destination
            .path()
            .parent()
            .ok_or_else(|| Error::PathParentMissing {
                path: destination.path().display().to_string(),
            })?;
        OwnerPrivateDirectory::new(parent).ensure()?;
        let partial = CompactAudioArtifact::new(format!("{}.part", destination.path().display()));
        let _ = fs::remove_file(partial.path());
        let mut child = Command::new(&encoder.program)
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "s16le",
                "-ar",
                &audio_format.sample_rate().to_string(),
                "-ac",
                &audio_format.channel_count().to_string(),
                "-i",
                "pipe:0",
                "-vn",
                "-c:a",
                "libopus",
                "-application",
                "voip",
                "-b:a",
                "24k",
                "-cluster_time_limit",
                "1000",
                "-flush_packets",
                "1",
                "-f",
                "webm",
            ])
            .arg(partial.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| Error::CompactAudioEncode {
                message: format!("failed to start {}: {error}", encoder.program),
            })?;
        let stdin = child
            .stdin
            .take()
            .ok_or(Error::CaptureProcessStdoutUnavailable)?;
        let (sender, receiver) = mpsc::channel();
        let worker = LiveOpusWebmWorker::new(child, stdin, receiver).spawn();
        Ok(Self {
            encoder,
            destination,
            partial,
            sender: Some(sender),
            worker: Some(worker),
        })
    }

    pub fn sender(&self) -> mpsc::Sender<Vec<u8>> {
        self.sender
            .as_ref()
            .expect("live encoder sender exists before finalization")
            .clone()
    }

    pub fn finish(mut self) -> Result<CompactAudioArtifact> {
        self.sender.take();
        self.worker
            .take()
            .expect("live encoder has a worker")
            .join()
            .map_err(|_| Error::CaptureWriterThread)??;
        fs::set_permissions(
            self.partial.path(),
            fs::Permissions::from_mode(OWNER_PRIVATE_FILE_MODE),
        )?;
        self.partial.validate()?;
        self.partial.sync_file()?;
        fs::rename(self.partial.path(), self.destination.path())?;
        self.destination.sync_directory()?;
        self.encoder.validate_webm(self.destination.path())?;
        self.destination.validate()?;
        Ok(self.destination)
    }
}

impl CompactAudioArtifact {
    pub fn discard_partial(&self) -> Result<()> {
        let partial = CompactAudioArtifact::new(format!("{}.part", self.path().display()));
        match fs::remove_file(partial.path()) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn sync_file(&self) -> Result<()> {
        fs::File::open(self.path())?.sync_all()?;
        Ok(())
    }

    fn sync_directory(&self) -> Result<()> {
        let parent = self
            .path()
            .parent()
            .ok_or_else(|| Error::PathParentMissing {
                path: self.path().display().to_string(),
            })?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
}

struct LiveOpusWebmWorker {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    receiver: mpsc::Receiver<Vec<u8>>,
}

impl LiveOpusWebmWorker {
    fn new(
        child: std::process::Child,
        stdin: std::process::ChildStdin,
        receiver: mpsc::Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            child,
            stdin,
            receiver,
        }
    }

    fn spawn(self) -> JoinHandle<Result<()>> {
        thread::spawn(move || self.write_until_finished())
    }

    fn write_until_finished(mut self) -> Result<()> {
        while let Ok(pcm) = self.receiver.recv() {
            self.stdin.write_all(&pcm)?;
        }
        self.stdin.flush()?;
        drop(self.stdin);
        let status = self.child.wait()?;
        if status.success() {
            Ok(())
        } else {
            Err(Error::CompactAudioEncode {
                message: format!("live encoder exited with {status}"),
            })
        }
    }
}

/// The local FFmpeg adapter used at the provider boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpusWebmEncoder {
    program: String,
}

impl OpusWebmEncoder {
    pub fn from_environment() -> Self {
        Self::new(std::env::var("LISTENER_FFMPEG_PROGRAM").unwrap_or_else(|_| "ffmpeg".to_owned()))
    }

    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
        }
    }

    pub fn encode_pcm(
        &self,
        input: &Path,
        audio_format: RecordingAudioFormat,
        output: CompactAudioArtifact,
    ) -> Result<CompactAudioArtifact> {
        let parent = output
            .path()
            .parent()
            .ok_or_else(|| Error::PathParentMissing {
                path: output.path().display().to_string(),
            })?;
        OwnerPrivateDirectory::new(parent).ensure()?;
        let temporary = output.path().with_extension("webm.encoding");
        let _ = fs::remove_file(&temporary);
        let result = Command::new(&self.program)
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "s16le",
                "-ar",
                &audio_format.sample_rate().to_string(),
                "-ac",
                &audio_format.channel_count().to_string(),
                "-i",
            ])
            .arg(input)
            .args([
                "-vn",
                "-c:a",
                "libopus",
                "-application",
                "voip",
                "-b:a",
                "24k",
                "-f",
                "webm",
            ])
            .arg(&temporary)
            .output()
            .map_err(|error| Error::CompactAudioEncode {
                message: format!("failed to start {}: {error}", self.program),
            })?;
        if !result.status.success() {
            let _ = fs::remove_file(&temporary);
            return Err(Error::CompactAudioEncode {
                message: String::from_utf8_lossy(&result.stderr).trim().to_owned(),
            });
        }
        fs::set_permissions(
            &temporary,
            fs::Permissions::from_mode(OWNER_PRIVATE_FILE_MODE),
        )?;
        let temporary_artifact = CompactAudioArtifact::new(&temporary);
        temporary_artifact.validate()?;
        fs::rename(&temporary, output.path())?;
        self.validate_webm(output.path())?;
        output.validate()?;
        Ok(output)
    }

    pub fn validate_webm(&self, input: &Path) -> Result<()> {
        let output = Command::new(&self.program)
            .args(["-hide_banner", "-loglevel", "error", "-i"])
            .arg(input)
            .args(["-f", "null", "-"])
            .output()
            .map_err(|error| Error::CompactAudioEncode {
                message: format!("failed to start {}: {error}", self.program),
            })?;
        if output.status.success() {
            Ok(())
        } else {
            Err(Error::CompactAudioInvalid {
                path: input.display().to_string(),
            })
        }
    }

    pub fn chunk_webm(
        &self,
        input: &Path,
        start_seconds: u64,
        duration_seconds: u64,
    ) -> Result<Vec<u8>> {
        let output = Command::new(&self.program)
            .args(["-hide_banner", "-loglevel", "error", "-ss"])
            .arg(start_seconds.to_string())
            .args(["-t"])
            .arg(duration_seconds.to_string())
            .args(["-i"])
            .arg(input)
            .args([
                "-vn",
                "-c:a",
                "libopus",
                "-application",
                "voip",
                "-b:a",
                "24k",
                "-f",
                "webm",
                "pipe:1",
            ])
            .output()
            .map_err(|error| Error::CompactAudioEncode {
                message: format!("failed to start {}: {error}", self.program),
            })?;
        if !output.status.success() || output.stdout.is_empty() {
            return Err(Error::CompactAudioEncode {
                message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        Ok(output.stdout)
    }

    pub fn duration_seconds(&self, input: &Path) -> Result<u64> {
        let output = Command::new(&self.program)
            .args(["-hide_banner", "-i"])
            .arg(input)
            .output()
            .map_err(|error| Error::CompactAudioEncode {
                message: format!("failed to start {}: {error}", self.program),
            })?;
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        let duration = diagnostic
            .lines()
            .find_map(|line| line.trim().strip_prefix("Duration: "))
            .and_then(|value| value.split(',').next())
            .and_then(|value| {
                let fields: Vec<_> = value.trim().split(':').collect();
                if fields.len() != 3 {
                    return None;
                }
                Some(
                    fields[0].parse::<f64>().ok()? * 3600.0
                        + fields[1].parse::<f64>().ok()? * 60.0
                        + fields[2].parse::<f64>().ok()?,
                )
            })
            .ok_or_else(|| Error::CompactAudioInvalid {
                path: input.display().to_string(),
            })?;
        Ok(duration.ceil() as u64)
    }
}
