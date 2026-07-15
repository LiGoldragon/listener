//! Private transcript history with bounded, crash-safe retention.
//!
//! Each successful transcript is retained in a local JSONL projection under
//! Listener's XDG data directory. The store is private, bounded by an explicit
//! age-and-byte policy, and atomically compacted before appends and reads so an
//! interrupted rewrite leaves either the prior complete history or the new one.

use std::{
    collections::VecDeque,
    env,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use signal_listener::{CaptureSession, TranscriptText};

use crate::{Error, Result};

const HISTORY_DIRECTORY_MODE: u32 = 0o700;
const HISTORY_FILE_MODE: u32 = 0o600;
const DEFAULT_HISTORY_RETENTION_DAYS: u64 = 90;
const DEFAULT_HISTORY_MAXIMUM_BYTES: u64 = 16 * 1024 * 1024;
const MILLISECONDS_PER_DAY: u64 = 24 * 60 * 60 * 1000;
const HISTORY_READ_BUFFER_BYTES: usize = 8 * 1024;
const HISTORY_MAXIMUM_LINE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
static HISTORY_TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

/// A finite age for retaining private transcript records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryRetentionAge {
    milliseconds: u64,
}

impl HistoryRetentionAge {
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
}

/// A finite byte cap for the complete JSONL history projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryByteLimit {
    bytes: u64,
}

impl HistoryByteLimit {
    pub fn new(bytes: u64) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// The retention contract for transcript history.
///
/// The default is provisional operational policy: retain records for 90 days
/// and at most 16 MiB. Both values are configurable through
/// `LISTENER_HISTORY_RETENTION_DAYS` and `LISTENER_HISTORY_MAXIMUM_BYTES`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryRetentionPolicy {
    maximum_age: HistoryRetentionAge,
    maximum_bytes: HistoryByteLimit,
}

impl HistoryRetentionPolicy {
    pub fn new(maximum_age: HistoryRetentionAge, maximum_bytes: HistoryByteLimit) -> Self {
        Self {
            maximum_age,
            maximum_bytes,
        }
    }

    pub fn from_environment() -> Result<Self> {
        let maximum_age = match Self::environment_u64("LISTENER_HISTORY_RETENTION_DAYS")? {
            Some(days) => HistoryRetentionAge::from_days(days).ok_or_else(|| {
                Error::InvalidHistoryRetentionPolicy {
                    variable: "LISTENER_HISTORY_RETENTION_DAYS".to_owned(),
                    value: days.to_string(),
                }
            })?,
            None => Self::default().maximum_age,
        };
        let maximum_bytes = match Self::environment_u64("LISTENER_HISTORY_MAXIMUM_BYTES")? {
            Some(bytes) => HistoryByteLimit::new(bytes),
            None => Self::default().maximum_bytes,
        };
        Ok(Self::new(maximum_age, maximum_bytes))
    }

    pub fn maximum_age(&self) -> HistoryRetentionAge {
        self.maximum_age
    }

    pub fn maximum_bytes(&self) -> HistoryByteLimit {
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
            .map_err(|_| Error::InvalidHistoryRetentionPolicy {
                variable: variable.to_owned(),
                value,
            })
    }
}

impl Default for HistoryRetentionPolicy {
    fn default() -> Self {
        Self::new(
            HistoryRetentionAge::from_days(DEFAULT_HISTORY_RETENTION_DAYS)
                .expect("default history retention age fits u64"),
            HistoryByteLimit::new(DEFAULT_HISTORY_MAXIMUM_BYTES),
        )
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

    fn is_expired_at(&self, evaluated_at: Self, retention: HistoryRetentionPolicy) -> bool {
        let maximum_age = i64::try_from(retention.maximum_age().milliseconds()).unwrap_or(i64::MAX);
        self.unix_milliseconds < evaluated_at.unix_milliseconds.saturating_sub(maximum_age)
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

/// An encoded entry retained during one bounded compaction pass.
struct RetainedHistoryEntry {
    entry: TranscriptHistoryEntry,
    line: String,
}

impl RetainedHistoryEntry {
    fn from_entry(entry: TranscriptHistoryEntry) -> Result<Self> {
        let line = entry.to_json_line()?;
        Ok(Self { entry, line })
    }

    fn byte_length(&self) -> u64 {
        u64::try_from(self.line.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1)
    }
}

/// The append-only JSONL transcript history file, maintained as a bounded
/// projection by the sole daemon writer and serialized with an advisory lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranscriptHistoryStore {
    path: PathBuf,
    retention: HistoryRetentionPolicy,
}

impl TranscriptHistoryStore {
    pub fn from_environment() -> Result<Self> {
        Ok(Self::new_with_retention(
            Self::default_path_from_environment(),
            HistoryRetentionPolicy::from_environment()?,
        ))
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::new_with_retention(path, HistoryRetentionPolicy::default())
    }

    pub fn new_with_retention(path: impl Into<PathBuf>, retention: HistoryRetentionPolicy) -> Self {
        Self {
            path: path.into(),
            retention,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn retention(&self) -> HistoryRetentionPolicy {
        self.retention
    }

    /// Append one transcript and atomically hard-delete expired or over-budget
    /// records. A record larger than the complete byte budget is not retained.
    pub fn append(&self, entry: &TranscriptHistoryEntry) -> Result<()> {
        self.prepare_directory()?;
        let _lock = self.lock()?;
        self.cleanup_interrupted_rewrites()?;
        let now = HistoryTimestamp::now()?;
        let (mut retained, mut retained_bytes) = self.retained_entries(now)?;
        self.retain_entry(&mut retained, &mut retained_bytes, entry.clone())?;
        self.replace_with(&retained)
    }

    /// Compact the store against a caller-provided clock. This makes retention
    /// maintenance testable without depending on the system clock.
    pub fn compact_at(&self, evaluated_at: HistoryTimestamp) -> Result<()> {
        if !self.path.exists() {
            return Ok(());
        }
        self.prepare_directory()?;
        let _lock = self.lock()?;
        self.cleanup_interrupted_rewrites()?;
        let (retained, _) = self.retained_entries(evaluated_at)?;
        self.replace_with(&retained)
    }

    /// Read the most recent transcripts, newest first, up to `limit`.
    ///
    /// Reads compact first. Therefore a legacy unbounded history is streamed
    /// once with a bounded line buffer, atomically replaced by the retained
    /// projection, and never loaded wholesale into memory.
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
        self.prepare_directory()?;
        let _lock = self.lock()?;
        self.cleanup_interrupted_rewrites()?;
        let (retained, _) = self.retained_entries(HistoryTimestamp::now()?)?;
        self.replace_with(&retained)?;
        Ok(retained
            .iter()
            .rev()
            .take(limit.value())
            .map(|retained| retained.entry.clone())
            .collect())
    }

    fn retain_entry(
        &self,
        retained: &mut VecDeque<RetainedHistoryEntry>,
        retained_bytes: &mut u64,
        entry: TranscriptHistoryEntry,
    ) -> Result<()> {
        let retained_entry = RetainedHistoryEntry::from_entry(entry)?;
        if retained_entry.byte_length() > self.retention.maximum_bytes().bytes() {
            return Ok(());
        }
        *retained_bytes = retained_bytes.saturating_add(retained_entry.byte_length());
        retained.push_back(retained_entry);
        while *retained_bytes > self.retention.maximum_bytes().bytes() {
            let Some(expired_by_size) = retained.pop_front() else {
                return Ok(());
            };
            *retained_bytes = retained_bytes.saturating_sub(expired_by_size.byte_length());
        }
        Ok(())
    }

    fn retained_entries(
        &self,
        evaluated_at: HistoryTimestamp,
    ) -> Result<(VecDeque<RetainedHistoryEntry>, u64)> {
        if !self.path.exists() {
            return Ok((VecDeque::new(), 0));
        }
        let file = File::open(&self.path)?;
        let maximum_line_bytes = self.maximum_line_bytes();
        let mut reader = HistoryLineReader::new(file, maximum_line_bytes);
        let mut retained = VecDeque::new();
        let mut retained_bytes = 0;
        while let Some(line) = reader.next_line()? {
            if line.trim().is_empty() {
                continue;
            }
            let Ok(entry) = TranscriptHistoryEntry::from_json_line(&line) else {
                continue;
            };
            if entry
                .recorded_at()
                .is_expired_at(evaluated_at, self.retention)
            {
                continue;
            }
            self.retain_entry(&mut retained, &mut retained_bytes, entry)?;
        }
        Ok((retained, retained_bytes))
    }

    fn replace_with(&self, retained: &VecDeque<RetainedHistoryEntry>) -> Result<()> {
        let (temporary_path, mut temporary_file) = self.temporary_file()?;
        for entry in retained {
            writeln!(temporary_file, "{}", entry.line)?;
        }
        temporary_file.sync_all()?;
        drop(temporary_file);
        fs::rename(&temporary_path, &self.path)?;
        self.sync_parent_directory()
    }

    fn temporary_file(&self) -> Result<(PathBuf, File)> {
        for _ in 0..64 {
            let temporary_path = self.temporary_path();
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(HISTORY_FILE_MODE)
                .open(&temporary_path)
            {
                Ok(file) => return Ok((temporary_path, file)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a transcript history temporary file",
        )))
    }

    fn temporary_path(&self) -> PathBuf {
        let sequence = HISTORY_TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let file_name = self
            .path
            .file_name()
            .map(|file_name| file_name.to_string_lossy())
            .unwrap_or_else(|| "history.jsonl".into());
        self.path
            .with_file_name(format!("{file_name}.tmp-{}-{sequence}", std::process::id()))
    }

    fn cleanup_interrupted_rewrites(&self) -> Result<()> {
        let Some(parent) = self.path.parent() else {
            return Ok(());
        };
        let prefix = self.temporary_prefix();
        for entry in fs::read_dir(parent)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                let path = entry.path();
                if path.is_file() {
                    fs::remove_file(path)?;
                }
            }
        }
        Ok(())
    }

    fn temporary_prefix(&self) -> String {
        let file_name = self
            .path
            .file_name()
            .map(|file_name| file_name.to_string_lossy())
            .unwrap_or_else(|| "history.jsonl".into());
        format!("{file_name}.tmp-")
    }

    fn lock(&self) -> Result<File> {
        let lock_path = self.path.with_file_name(format!(
            "{}.lock",
            self.path
                .file_name()
                .map(|file_name| file_name.to_string_lossy())
                .unwrap_or_else(|| "history.jsonl".into())
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(HISTORY_FILE_MODE)
            .open(lock_path)?;
        file.set_permissions(fs::Permissions::from_mode(HISTORY_FILE_MODE))?;
        file.lock_exclusive()?;
        Ok(file)
    }

    fn maximum_line_bytes(&self) -> usize {
        usize::try_from(self.retention.maximum_bytes().bytes())
            .unwrap_or(usize::MAX)
            .min(HISTORY_MAXIMUM_LINE_BUFFER_BYTES)
    }

    fn sync_parent_directory(&self) -> Result<()> {
        let parent = self.path.parent().ok_or_else(|| Error::PathParentMissing {
            path: self.path.display().to_string(),
        })?;
        File::open(parent)?.sync_all()?;
        Ok(())
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

/// A bounded reader that skips oversized or torn JSONL lines.
struct HistoryLineReader {
    reader: BufReader<File>,
    buffer: [u8; HISTORY_READ_BUFFER_BYTES],
    buffered_length: usize,
    buffered_position: usize,
    state: HistoryLineState,
    maximum_line_bytes: usize,
}

impl HistoryLineReader {
    fn new(file: File, maximum_line_bytes: usize) -> Self {
        Self {
            reader: BufReader::new(file),
            buffer: [0; HISTORY_READ_BUFFER_BYTES],
            buffered_length: 0,
            buffered_position: 0,
            state: HistoryLineState::Collecting(Vec::new()),
            maximum_line_bytes,
        }
    }

    fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            if self.buffered_position == self.buffered_length {
                self.buffered_length = self.reader.read(&mut self.buffer)?;
                self.buffered_position = 0;
                if self.buffered_length == 0 {
                    return Ok(None);
                }
            }
            let byte = self.buffer[self.buffered_position];
            self.buffered_position += 1;
            if let Some(line) = self.state.accept(byte, self.maximum_line_bytes) {
                return Ok(String::from_utf8(line).ok());
            }
        }
    }
}

/// The current state of one bounded JSONL line.
enum HistoryLineState {
    Collecting(Vec<u8>),
    Discarding,
}

impl HistoryLineState {
    fn accept(&mut self, byte: u8, maximum_line_bytes: usize) -> Option<Vec<u8>> {
        match self {
            Self::Collecting(line) if byte == b'\n' => {
                let complete = std::mem::take(line);
                Some(complete)
            }
            Self::Collecting(line) if line.len() < maximum_line_bytes => {
                line.push(byte);
                None
            }
            Self::Collecting(_) => {
                *self = Self::Discarding;
                None
            }
            Self::Discarding if byte == b'\n' => {
                *self = Self::Collecting(Vec::new());
                None
            }
            Self::Discarding => None,
        }
    }
}
