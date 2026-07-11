use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
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
        output.validate()?;
        Ok(output)
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
