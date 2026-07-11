//! Local transcript history — an append-only JSONL projection.
//!
//! Each capture that stops with a successful transcript appends one entry to a
//! private history file under Listener's XDG data directory
//! (`$XDG_DATA_HOME/listener/history.jsonl`, typically
//! `~/.local/share/listener/history.jsonl`). `TranscriptHistoryEntry` is the
//! typed in-memory record; the JSON line is its human/interchange projection.
//! The store holds spoken content, so it stays local-only and is created with
//! owner-only directory and file permissions.

use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use signal_listener::{CaptureSession, TranscriptText};

use crate::{Error, Result};

const HISTORY_DIRECTORY_MODE: u32 = 0o700;
const HISTORY_FILE_MODE: u32 = 0o600;

/// A cap on how many of the most recent transcripts a read returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryLimit {
    value: usize,
}

impl HistoryLimit {
    pub fn new(value: usize) -> Self {
        Self { value }
    }

    pub fn value(&self) -> usize {
        self.value
    }
}

/// When a transcript was recorded, as Unix milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryTimestamp {
    unix_milliseconds: i64,
}

impl HistoryTimestamp {
    pub fn now() -> Result<Self> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| Error::SystemClockBeforeUnixEpoch {
                message: error.to_string(),
            })?;
        Ok(Self {
            unix_milliseconds: duration.as_millis() as i64,
        })
    }

    pub fn from_unix_milliseconds(unix_milliseconds: i64) -> Self {
        Self { unix_milliseconds }
    }

    pub fn unix_milliseconds(&self) -> i64 {
        self.unix_milliseconds
    }
}

/// One recorded transcript: when it happened, its capture session, and its text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranscriptHistoryEntry {
    recorded_at: HistoryTimestamp,
    session: CaptureSession,
    transcript_text: TranscriptText,
}

impl TranscriptHistoryEntry {
    pub fn new(
        recorded_at: HistoryTimestamp,
        session: CaptureSession,
        transcript_text: TranscriptText,
    ) -> Self {
        Self {
            recorded_at,
            session,
            transcript_text,
        }
    }

    pub fn recorded_now(session: CaptureSession, transcript_text: TranscriptText) -> Result<Self> {
        Ok(Self::new(
            HistoryTimestamp::now()?,
            session,
            transcript_text,
        ))
    }

    pub fn recorded_at(&self) -> HistoryTimestamp {
        self.recorded_at
    }

    pub fn session(&self) -> &CaptureSession {
        &self.session
    }

    pub fn transcript_text(&self) -> &TranscriptText {
        &self.transcript_text
    }

    /// A single-line preview of the transcript, capped at `maximum_characters`,
    /// for display in a recall picker.
    pub fn preview(&self, maximum_characters: usize) -> String {
        let flattened = self
            .transcript_text
            .as_str()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        flattened.chars().take(maximum_characters).collect()
    }

    fn to_json_line(&self) -> Result<String> {
        let projection = TranscriptHistoryLine {
            unix_millis: self.recorded_at.unix_milliseconds(),
            session: self.session.value(),
            text: self.transcript_text.as_str().to_owned(),
        };
        serde_json::to_string(&projection).map_err(|error| Error::HistoryEntryEncode {
            message: error.to_string(),
        })
    }

    fn from_json_line(line: &str) -> Result<Self> {
        let projection: TranscriptHistoryLine =
            serde_json::from_str(line).map_err(|error| Error::HistoryEntryDecode {
                message: error.to_string(),
            })?;
        Ok(Self::new(
            HistoryTimestamp::from_unix_milliseconds(projection.unix_millis),
            CaptureSession::new(projection.session),
            TranscriptText::new(projection.text),
        ))
    }
}

/// The JSON projection of a history entry, one per line in the store file.
#[derive(Serialize, Deserialize)]
struct TranscriptHistoryLine {
    unix_millis: i64,
    session: u64,
    text: String,
}

/// The append-only JSONL transcript history file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranscriptHistoryStore {
    path: PathBuf,
}

impl TranscriptHistoryStore {
    pub fn from_environment() -> Self {
        Self::new(Self::default_path_from_environment())
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one transcript entry, creating the private directory and file if
    /// they do not exist yet.
    pub fn append(&self, entry: &TranscriptHistoryEntry) -> Result<()> {
        self.prepare_directory()?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(HISTORY_FILE_MODE)
            .open(&self.path)?;
        // Enforce owner-only permissions even if the file pre-existed looser.
        file.set_permissions(fs::Permissions::from_mode(HISTORY_FILE_MODE))?;
        let line = entry.to_json_line()?;
        writeln!(file, "{line}")?;
        Ok(())
    }

    /// Read the most recent transcripts, newest first, up to `limit`. A missing
    /// store reads as empty; a malformed line is skipped rather than failing the
    /// whole read.
    pub fn contains_session(&self, session: &CaptureSession) -> Result<bool> {
        Ok(self
            .read_recent(HistoryLimit::new(usize::MAX))?
            .iter()
            .any(|entry| entry.session() == session))
    }

    pub fn read_recent(&self, limit: HistoryLimit) -> Result<Vec<TranscriptHistoryEntry>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let reader = BufReader::new(File::open(&self.path)?);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(entry) = TranscriptHistoryEntry::from_json_line(&line) {
                entries.push(entry);
            }
        }

        entries.reverse();
        entries.truncate(limit.value());
        Ok(entries)
    }

    fn prepare_directory(&self) -> Result<()> {
        let parent = self.path.parent().ok_or_else(|| Error::PathParentMissing {
            path: self.path.display().to_string(),
        })?;
        if parent.as_os_str().is_empty() || parent.exists() {
            return Ok(());
        }
        fs::DirBuilder::new()
            .recursive(true)
            .mode(HISTORY_DIRECTORY_MODE)
            .create(parent)?;
        Ok(())
    }

    fn default_path_from_environment() -> PathBuf {
        if let Some(path) = env::var_os("LISTENER_HISTORY_STORE") {
            return PathBuf::from(path);
        }
        if let Some(data_home) = env::var_os("XDG_DATA_HOME") {
            return PathBuf::from(data_home)
                .join("listener")
                .join("history.jsonl");
        }
        if let Some(home) = env::var_os("HOME") {
            return PathBuf::from(home)
                .join(".local/share/listener")
                .join("history.jsonl");
        }
        env::temp_dir().join("listener").join("history.jsonl")
    }
}
