use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use signal_listener::{CaptureSession, InputSource};

use crate::{Error, Result};

const FILE_MAGIC: [u8; 8] = *b"LSTNLOG1";
const RECORD_MAGIC: [u8; 8] = *b"LSTNREC1";
const COMMIT_MAGIC: [u8; 8] = *b"LSTNCMT1";
const FILE_VERSION: u16 = 1;
const RECORD_VERSION: u16 = 1;
const FILE_HEADER_LENGTH: usize = 128;
const RECORD_HEADER_LENGTH: usize = 48;
const RECORD_TRAILER_LENGTH: usize = 32;
const FILE_HEADER_CHECKSUM_OFFSET: usize = 92;
const RECORD_HEADER_CHECKSUM_OFFSET: usize = 44;
const DEFAULT_MAXIMUM_RECORD_PAYLOAD_BYTES: u32 = 8192;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordingSampleFormat {
    SignedSixteenBitLittleEndian,
}

impl RecordingSampleFormat {
    pub fn code(&self) -> u16 {
        match self {
            Self::SignedSixteenBitLittleEndian => 1,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::SignedSixteenBitLittleEndian => "s16le",
        }
    }

    pub fn bytes_per_sample(&self) -> u16 {
        match self {
            Self::SignedSixteenBitLittleEndian => 2,
        }
    }

    fn from_code(path: &Path, code: u16) -> Result<Self> {
        match code {
            1 => Ok(Self::SignedSixteenBitLittleEndian),
            other => Err(Error::invalid_recording_log(
                path,
                format!("unsupported sample format code {other}"),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordingInputSource {
    SystemDefault,
}

impl RecordingInputSource {
    pub fn from_input_source(input_source: InputSource) -> Self {
        match input_source {
            InputSource::SystemDefault => Self::SystemDefault,
        }
    }

    pub fn code(&self) -> u16 {
        match self {
            Self::SystemDefault => 1,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::SystemDefault => "system-default",
        }
    }

    fn from_code(path: &Path, code: u16) -> Result<Self> {
        match code {
            1 => Ok(Self::SystemDefault),
            other => Err(Error::invalid_recording_log(
                path,
                format!("unsupported input source code {other}"),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordingAudioFormat {
    sample_format: RecordingSampleFormat,
    sample_rate: u32,
    channel_count: u16,
    bytes_per_frame: u16,
}

impl RecordingAudioFormat {
    pub fn signed_sixteen_bit_little_endian_mono_16khz() -> Self {
        Self {
            sample_format: RecordingSampleFormat::SignedSixteenBitLittleEndian,
            sample_rate: 16_000,
            channel_count: 1,
            bytes_per_frame: 2,
        }
    }

    pub fn new(
        sample_format: RecordingSampleFormat,
        sample_rate: u32,
        channel_count: u16,
    ) -> Result<Self> {
        if sample_rate == 0 {
            return Err(Error::InvalidAudioFormat {
                message: "sample rate must be greater than zero".to_owned(),
            });
        }
        if channel_count == 0 {
            return Err(Error::InvalidAudioFormat {
                message: "channel count must be greater than zero".to_owned(),
            });
        }

        let bytes_per_frame =
            u32::from(channel_count) * u32::from(sample_format.bytes_per_sample());
        if bytes_per_frame > u32::from(u16::MAX) {
            return Err(Error::InvalidAudioFormat {
                message: format!("bytes per frame {bytes_per_frame} exceeds u16"),
            });
        }

        Ok(Self {
            sample_format,
            sample_rate,
            channel_count,
            bytes_per_frame: bytes_per_frame as u16,
        })
    }

    pub fn sample_format(&self) -> RecordingSampleFormat {
        self.sample_format
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub fn channel_count(&self) -> u16 {
        self.channel_count
    }

    pub fn bytes_per_frame(&self) -> u16 {
        self.bytes_per_frame
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordingStartTime {
    unix_seconds: i64,
    unix_nanoseconds: u32,
}

impl RecordingStartTime {
    pub fn now() -> Result<Self> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| Error::SystemClockBeforeUnixEpoch {
                message: error.to_string(),
            })?;
        Ok(Self {
            unix_seconds: duration.as_secs() as i64,
            unix_nanoseconds: duration.subsec_nanos(),
        })
    }

    pub fn from_unix_parts(unix_seconds: i64, unix_nanoseconds: u32) -> Self {
        Self {
            unix_seconds,
            unix_nanoseconds,
        }
    }

    pub fn unix_seconds(&self) -> i64 {
        self.unix_seconds
    }

    pub fn unix_nanoseconds(&self) -> u32 {
        self.unix_nanoseconds
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingLogHeader {
    session: CaptureSession,
    audio_format: RecordingAudioFormat,
    input_source: RecordingInputSource,
    start_time: RecordingStartTime,
    maximum_record_payload_bytes: u32,
}

impl RecordingLogHeader {
    pub fn from_capture_start(
        session: &CaptureSession,
        input_source: InputSource,
        audio_format: RecordingAudioFormat,
    ) -> Result<Self> {
        Self::new(
            session.clone(),
            audio_format,
            RecordingInputSource::from_input_source(input_source),
            RecordingStartTime::now()?,
            DEFAULT_MAXIMUM_RECORD_PAYLOAD_BYTES,
        )
    }

    pub fn new(
        session: CaptureSession,
        audio_format: RecordingAudioFormat,
        input_source: RecordingInputSource,
        start_time: RecordingStartTime,
        maximum_record_payload_bytes: u32,
    ) -> Result<Self> {
        if maximum_record_payload_bytes == 0 {
            return Err(Error::InvalidAudioFormat {
                message: "maximum record payload bytes must be greater than zero".to_owned(),
            });
        }

        let bytes_per_frame = u32::from(audio_format.bytes_per_frame());
        if maximum_record_payload_bytes < bytes_per_frame {
            return Err(Error::InvalidAudioFormat {
                message: format!(
                    "maximum record payload bytes {maximum_record_payload_bytes} is smaller than frame size {bytes_per_frame}"
                ),
            });
        }
        if !maximum_record_payload_bytes.is_multiple_of(bytes_per_frame) {
            return Err(Error::InvalidAudioFormat {
                message: format!(
                    "maximum record payload bytes {maximum_record_payload_bytes} is not frame-aligned for {bytes_per_frame}-byte frames"
                ),
            });
        }

        Ok(Self {
            session,
            audio_format,
            input_source,
            start_time,
            maximum_record_payload_bytes,
        })
    }

    pub fn session(&self) -> &CaptureSession {
        &self.session
    }

    pub fn audio_format(&self) -> RecordingAudioFormat {
        self.audio_format
    }

    pub fn input_source(&self) -> RecordingInputSource {
        self.input_source
    }

    pub fn start_time(&self) -> RecordingStartTime {
        self.start_time
    }

    pub fn maximum_record_payload_bytes(&self) -> u32 {
        self.maximum_record_payload_bytes
    }

    fn to_bytes(&self) -> [u8; FILE_HEADER_LENGTH] {
        let mut bytes = RecordingLogBytes::new();
        bytes.push_slice(&FILE_MAGIC);
        bytes.push_u16(FILE_VERSION);
        bytes.push_u16(FILE_HEADER_LENGTH as u16);
        bytes.push_u32(self.audio_format.sample_rate());
        bytes.push_u16(self.audio_format.channel_count());
        bytes.push_u16(self.audio_format.bytes_per_frame());
        bytes.push_u16(self.audio_format.sample_format().code());
        bytes.push_u16(self.input_source.code());
        bytes.push_u64(self.session.value());
        bytes.push_i64(self.start_time.unix_seconds());
        bytes.push_u32(self.start_time.unix_nanoseconds());
        bytes.push_u16(RECORD_HEADER_LENGTH as u16);
        bytes.push_u16(RECORD_TRAILER_LENGTH as u16);
        bytes.push_u32(self.maximum_record_payload_bytes);
        bytes.push_slice(
            RecordingLogLabel::<8>::from_text(self.audio_format.sample_format().label()).bytes(),
        );
        bytes.push_slice(RecordingLogLabel::<32>::from_text(self.input_source.label()).bytes());
        bytes.push_u32(bytes.checksum());
        bytes.pad_to(FILE_HEADER_LENGTH);
        bytes.into_fixed()
    }

    fn read_from(file: &mut File, path: &Path) -> Result<Self> {
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = [0_u8; FILE_HEADER_LENGTH];
        match file.read_exact(&mut bytes) {
            Ok(()) => Self::from_bytes(path, &bytes),
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => Err(
                Error::invalid_recording_log(path, "file ended before complete header"),
            ),
            Err(error) => Err(error.into()),
        }
    }

    fn from_bytes(path: &Path, bytes: &[u8; FILE_HEADER_LENGTH]) -> Result<Self> {
        let expected_checksum = crc32fast::hash(&bytes[..FILE_HEADER_CHECKSUM_OFFSET]);
        let mut cursor = RecordingLogByteCursor::new(path, bytes);

        let magic = cursor.read_exact::<8>()?;
        if magic != FILE_MAGIC {
            return Err(Error::invalid_recording_log(path, "file magic mismatch"));
        }

        let version = cursor.read_u16()?;
        if version != FILE_VERSION {
            return Err(Error::invalid_recording_log(
                path,
                format!("unsupported file version {version}"),
            ));
        }

        let header_length = cursor.read_u16()?;
        if usize::from(header_length) != FILE_HEADER_LENGTH {
            return Err(Error::invalid_recording_log(
                path,
                format!("header length {header_length} does not match {FILE_HEADER_LENGTH}"),
            ));
        }

        let sample_rate = cursor.read_u32()?;
        let channel_count = cursor.read_u16()?;
        let bytes_per_frame = cursor.read_u16()?;
        let sample_format = RecordingSampleFormat::from_code(path, cursor.read_u16()?)?;
        let input_source = RecordingInputSource::from_code(path, cursor.read_u16()?)?;
        let session = CaptureSession::new(cursor.read_u64()?);
        let start_time =
            RecordingStartTime::from_unix_parts(cursor.read_i64()?, cursor.read_u32()?);
        let record_header_length = cursor.read_u16()?;
        let record_trailer_length = cursor.read_u16()?;
        let maximum_record_payload_bytes = cursor.read_u32()?;
        let sample_format_label = cursor.read_exact::<8>()?;
        let input_source_label = cursor.read_exact::<32>()?;
        let actual_checksum = cursor.read_u32()?;

        if actual_checksum != expected_checksum {
            return Err(Error::invalid_recording_log(
                path,
                "header checksum mismatch",
            ));
        }
        if usize::from(record_header_length) != RECORD_HEADER_LENGTH {
            return Err(Error::invalid_recording_log(
                path,
                format!(
                    "record header length {record_header_length} does not match {RECORD_HEADER_LENGTH}"
                ),
            ));
        }
        if usize::from(record_trailer_length) != RECORD_TRAILER_LENGTH {
            return Err(Error::invalid_recording_log(
                path,
                format!(
                    "record trailer length {record_trailer_length} does not match {RECORD_TRAILER_LENGTH}"
                ),
            ));
        }
        if sample_format_label != *RecordingLogLabel::<8>::from_text(sample_format.label()).bytes()
        {
            return Err(Error::invalid_recording_log(
                path,
                "sample format label does not match sample format code",
            ));
        }
        if input_source_label != *RecordingLogLabel::<32>::from_text(input_source.label()).bytes() {
            return Err(Error::invalid_recording_log(
                path,
                "input source label does not match input source code",
            ));
        }

        let audio_format = RecordingAudioFormat::new(sample_format, sample_rate, channel_count)?;
        if bytes_per_frame != audio_format.bytes_per_frame() {
            return Err(Error::invalid_recording_log(
                path,
                format!(
                    "bytes per frame {bytes_per_frame} does not match decoded audio format {}",
                    audio_format.bytes_per_frame()
                ),
            ));
        }

        Self::new(
            session,
            audio_format,
            input_source,
            start_time,
            maximum_record_payload_bytes,
        )
    }
}

pub trait RecordingLogDurability: Send {
    fn commit(&mut self, file: &mut File) -> Result<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordingLogSyncMode {
    Data,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingLogDurabilityPolicy {
    sync_mode: RecordingLogSyncMode,
}

impl RecordingLogDurabilityPolicy {
    pub fn data() -> Self {
        Self {
            sync_mode: RecordingLogSyncMode::Data,
        }
    }
}

impl RecordingLogDurability for RecordingLogDurabilityPolicy {
    fn commit(&mut self, file: &mut File) -> Result<()> {
        match self.sync_mode {
            RecordingLogSyncMode::Data => {
                file.flush()?;
                file.sync_data()?;
            }
        }
        Ok(())
    }
}

pub struct RecordingLogWriter {
    path: PathBuf,
    header: RecordingLogHeader,
    file: File,
    durability: Box<dyn RecordingLogDurability>,
    next_sequence: u64,
    next_byte_offset: u64,
}

impl RecordingLogWriter {
    pub fn create(path: impl AsRef<Path>, header: RecordingLogHeader) -> Result<Self> {
        Self::create_with_durability(path, header, Box::new(RecordingLogDurabilityPolicy::data()))
    }

    pub fn create_with_durability(
        path: impl AsRef<Path>,
        header: RecordingLogHeader,
        durability: Box<dyn RecordingLogDurability>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let parent = path
            .parent()
            .ok_or_else(|| Error::PathParentMissing {
                path: path.display().to_string(),
            })?
            .to_path_buf();
        fs::create_dir_all(&parent)?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| match error.kind() {
                ErrorKind::AlreadyExists => Error::recording_log_already_exists(&path),
                _ => Error::Io(error),
            })?;
        file.write_all(&header.to_bytes())?;

        let mut writer = Self {
            path,
            header,
            file,
            durability,
            next_sequence: 0,
            next_byte_offset: 0,
        };
        writer.durability.commit(&mut writer.file)?;
        RecordingLogParentDirectory::new(parent).sync()?;
        Ok(writer)
    }

    pub fn append_record(&mut self, payload: &[u8]) -> Result<RecordingLogRecordCommit> {
        if payload.is_empty() {
            return Err(Error::invalid_recording_log(
                &self.path,
                "record payload must not be empty",
            ));
        }
        if payload.len() > self.header.maximum_record_payload_bytes() as usize {
            return Err(Error::invalid_recording_log(
                &self.path,
                format!(
                    "record payload has {} bytes, maximum is {}",
                    payload.len(),
                    self.header.maximum_record_payload_bytes()
                ),
            ));
        }

        let bytes_per_frame = usize::from(self.header.audio_format().bytes_per_frame());
        if !payload.len().is_multiple_of(bytes_per_frame) {
            return Err(Error::IncompletePcmFrame {
                remaining_bytes: payload.len() % bytes_per_frame,
                bytes_per_frame: self.header.audio_format().bytes_per_frame(),
            });
        }

        let payload_checksum = crc32fast::hash(payload);
        let payload_length = payload.len() as u32;
        let sequence = self.next_sequence;
        let byte_offset = self.next_byte_offset;
        let frame_offset = byte_offset / u64::from(self.header.audio_format().bytes_per_frame());
        let next_byte_offset = self
            .next_byte_offset
            .checked_add(u64::from(payload_length))
            .ok_or_else(|| {
                Error::invalid_recording_log(&self.path, "record byte offset overflow")
            })?;
        let record_header = RecordingLogRecordHeader::new(
            sequence,
            frame_offset,
            byte_offset,
            payload_length,
            payload_checksum,
        );
        let record_trailer = RecordingLogRecordTrailer::new(
            sequence,
            next_byte_offset,
            payload_length,
            payload_checksum,
        );

        let record_start_position = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&record_header.to_bytes())?;
        self.file.write_all(payload)?;
        self.file.write_all(&record_trailer.to_bytes())?;
        self.durability.commit(&mut self.file)?;
        let record_end_position = self.file.stream_position()?;

        self.next_sequence += 1;
        self.next_byte_offset = next_byte_offset;

        Ok(RecordingLogRecordCommit {
            sequence,
            byte_offset,
            frame_offset,
            payload_length,
            record_start_position,
            record_end_position,
        })
    }

    pub fn audio_format(&self) -> RecordingAudioFormat {
        self.header.audio_format()
    }

    pub fn maximum_record_payload_bytes(&self) -> u32 {
        self.header.maximum_record_payload_bytes()
    }

    pub fn finish(mut self) -> Result<()> {
        self.durability.commit(&mut self.file)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingLogRecordCommit {
    sequence: u64,
    byte_offset: u64,
    frame_offset: u64,
    payload_length: u32,
    record_start_position: u64,
    record_end_position: u64,
}

impl RecordingLogRecordCommit {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    pub fn frame_offset(&self) -> u64 {
        self.frame_offset
    }

    pub fn payload_length(&self) -> u32 {
        self.payload_length
    }

    pub fn record_start_position(&self) -> u64 {
        self.record_start_position
    }

    pub fn record_end_position(&self) -> u64 {
        self.record_end_position
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordingLog {
    path: PathBuf,
}

impl RecordingLog {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn recover(&self) -> Result<RecoveredRecordingLog> {
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let header = RecordingLogHeader::read_from(&mut file, &self.path)?;
        let scan = {
            let mut scanner =
                RecordingLogRecoveryScanner::new(&mut file, self.path.clone(), header.clone());
            scanner.scan()?
        };
        let original_length = file.metadata()?.len();
        let truncated_from = if original_length > scan.valid_end_position {
            file.set_len(scan.valid_end_position)?;
            file.sync_all()?;
            Some(original_length)
        } else {
            None
        };

        Ok(RecoveredRecordingLog {
            path: self.path.clone(),
            header,
            records: scan.records,
            valid_end_position: scan.valid_end_position,
            truncated_from,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredRecordingLog {
    path: PathBuf,
    header: RecordingLogHeader,
    records: Vec<RecoveredRecordingLogRecord>,
    valid_end_position: u64,
    truncated_from: Option<u64>,
}

impl RecoveredRecordingLog {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &RecordingLogHeader {
        &self.header
    }

    pub fn records(&self) -> &[RecoveredRecordingLogRecord] {
        &self.records
    }

    pub fn valid_end_position(&self) -> u64 {
        self.valid_end_position
    }

    pub fn truncated_from(&self) -> Option<u64> {
        self.truncated_from
    }

    pub fn total_payload_bytes(&self) -> u64 {
        self.records
            .iter()
            .map(|record| u64::from(record.payload_length()))
            .sum()
    }

    pub fn export_raw_pcm(&self, destination: impl AsRef<Path>) -> Result<RawPcmExport> {
        let destination = destination.as_ref().to_path_buf();
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut source = File::open(&self.path)?;
        let mut output = File::create(&destination)?;
        let mut payload = Vec::new();
        for record in &self.records {
            source.seek(SeekFrom::Start(record.payload_position()))?;
            payload.resize(record.payload_length() as usize, 0);
            source.read_exact(&mut payload)?;
            output.write_all(&payload)?;
        }
        output.flush()?;
        output.sync_data()?;

        Ok(RawPcmExport {
            path: destination,
            audio_format: self.header.audio_format(),
            byte_length: self.total_payload_bytes(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredRecordingLogRecord {
    sequence: u64,
    byte_offset: u64,
    frame_offset: u64,
    payload_position: u64,
    payload_length: u32,
    payload_checksum: u32,
    record_end_position: u64,
}

impl RecoveredRecordingLogRecord {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    pub fn frame_offset(&self) -> u64 {
        self.frame_offset
    }

    pub fn payload_position(&self) -> u64 {
        self.payload_position
    }

    pub fn payload_length(&self) -> u32 {
        self.payload_length
    }

    pub fn payload_checksum(&self) -> u32 {
        self.payload_checksum
    }

    pub fn record_end_position(&self) -> u64 {
        self.record_end_position
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawPcmExport {
    path: PathBuf,
    audio_format: RecordingAudioFormat,
    byte_length: u64,
}

impl RawPcmExport {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn audio_format(&self) -> RecordingAudioFormat {
        self.audio_format
    }

    pub fn byte_length(&self) -> u64 {
        self.byte_length
    }
}

struct RecordingLogRecoveryScanner<'a> {
    file: &'a mut File,
    path: PathBuf,
    header: RecordingLogHeader,
    expected_sequence: u64,
    expected_byte_offset: u64,
}

impl<'a> RecordingLogRecoveryScanner<'a> {
    fn new(file: &'a mut File, path: PathBuf, header: RecordingLogHeader) -> Self {
        Self {
            file,
            path,
            header,
            expected_sequence: 0,
            expected_byte_offset: 0,
        }
    }

    fn scan(&mut self) -> Result<RecordingLogScan> {
        self.file.seek(SeekFrom::Start(FILE_HEADER_LENGTH as u64))?;
        let mut records = Vec::new();
        let mut valid_end_position = FILE_HEADER_LENGTH as u64;

        loop {
            let record_start_position = self.file.stream_position()?;
            let Some(record_header_bytes) =
                self.read_fixed_or_incomplete::<RECORD_HEADER_LENGTH>()?
            else {
                break;
            };
            let Ok(record_header) =
                RecordingLogRecordHeader::from_bytes(&self.path, &record_header_bytes)
            else {
                break;
            };
            if !self.record_header_fits(&record_header) {
                break;
            }

            let payload_position = self.file.stream_position()?;
            let Some(payload) =
                self.read_payload_or_incomplete(record_header.payload_length as usize)?
            else {
                break;
            };
            let payload_checksum = crc32fast::hash(&payload);
            if payload_checksum != record_header.payload_checksum {
                break;
            }

            let Some(record_trailer_bytes) =
                self.read_fixed_or_incomplete::<RECORD_TRAILER_LENGTH>()?
            else {
                break;
            };
            let Ok(record_trailer) =
                RecordingLogRecordTrailer::from_bytes(&self.path, &record_trailer_bytes)
            else {
                break;
            };
            if !self.record_trailer_matches(&record_header, &record_trailer) {
                break;
            }

            let record_end_position = self.file.stream_position()?;
            records.push(RecoveredRecordingLogRecord {
                sequence: record_header.sequence,
                byte_offset: record_header.byte_offset,
                frame_offset: record_header.frame_offset,
                payload_position,
                payload_length: record_header.payload_length,
                payload_checksum: record_header.payload_checksum,
                record_end_position,
            });
            self.expected_sequence += 1;
            self.expected_byte_offset += u64::from(record_header.payload_length);
            valid_end_position = record_end_position;

            if record_start_position == record_end_position {
                break;
            }
        }

        Ok(RecordingLogScan {
            records,
            valid_end_position,
        })
    }

    fn record_header_fits(&self, record_header: &RecordingLogRecordHeader) -> bool {
        let bytes_per_frame = u64::from(self.header.audio_format().bytes_per_frame());
        let expected_frame_offset = self.expected_byte_offset / bytes_per_frame;
        record_header.sequence == self.expected_sequence
            && record_header.byte_offset == self.expected_byte_offset
            && record_header.frame_offset == expected_frame_offset
            && record_header.payload_length > 0
            && record_header.payload_length <= self.header.maximum_record_payload_bytes()
            && record_header
                .payload_length
                .is_multiple_of(self.header.audio_format().bytes_per_frame() as u32)
    }

    fn record_trailer_matches(
        &self,
        record_header: &RecordingLogRecordHeader,
        record_trailer: &RecordingLogRecordTrailer,
    ) -> bool {
        record_trailer.sequence == record_header.sequence
            && record_trailer.payload_length == record_header.payload_length
            && record_trailer.payload_checksum == record_header.payload_checksum
            && record_trailer.next_byte_offset
                == record_header.byte_offset + u64::from(record_header.payload_length)
    }

    fn read_fixed_or_incomplete<const LENGTH: usize>(&mut self) -> Result<Option<[u8; LENGTH]>> {
        let mut bytes = [0_u8; LENGTH];
        let mut total_read = 0;
        while total_read < LENGTH {
            match self.file.read(&mut bytes[total_read..]) {
                Ok(0) => return Ok(None),
                Ok(read_count) => total_read += read_count,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(Some(bytes))
    }

    fn read_payload_or_incomplete(&mut self, length: usize) -> Result<Option<Vec<u8>>> {
        let mut bytes = vec![0_u8; length];
        let mut total_read = 0;
        while total_read < length {
            match self.file.read(&mut bytes[total_read..]) {
                Ok(0) => return Ok(None),
                Ok(read_count) => total_read += read_count,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(Some(bytes))
    }
}

struct RecordingLogScan {
    records: Vec<RecoveredRecordingLogRecord>,
    valid_end_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordingLogRecordHeader {
    sequence: u64,
    frame_offset: u64,
    byte_offset: u64,
    payload_length: u32,
    payload_checksum: u32,
}

impl RecordingLogRecordHeader {
    fn new(
        sequence: u64,
        frame_offset: u64,
        byte_offset: u64,
        payload_length: u32,
        payload_checksum: u32,
    ) -> Self {
        Self {
            sequence,
            frame_offset,
            byte_offset,
            payload_length,
            payload_checksum,
        }
    }

    fn to_bytes(&self) -> [u8; RECORD_HEADER_LENGTH] {
        let mut bytes = RecordingLogBytes::new();
        bytes.push_slice(&RECORD_MAGIC);
        bytes.push_u16(RECORD_VERSION);
        bytes.push_u16(RECORD_HEADER_LENGTH as u16);
        bytes.push_u64(self.sequence);
        bytes.push_u64(self.frame_offset);
        bytes.push_u64(self.byte_offset);
        bytes.push_u32(self.payload_length);
        bytes.push_u32(self.payload_checksum);
        bytes.push_u32(bytes.checksum());
        bytes.into_fixed()
    }

    fn from_bytes(path: &Path, bytes: &[u8; RECORD_HEADER_LENGTH]) -> Result<Self> {
        let expected_checksum = crc32fast::hash(&bytes[..RECORD_HEADER_CHECKSUM_OFFSET]);
        let mut cursor = RecordingLogByteCursor::new(path, bytes);
        let magic = cursor.read_exact::<8>()?;
        if magic != RECORD_MAGIC {
            return Err(Error::invalid_recording_log(path, "record magic mismatch"));
        }
        let version = cursor.read_u16()?;
        if version != RECORD_VERSION {
            return Err(Error::invalid_recording_log(
                path,
                format!("unsupported record version {version}"),
            ));
        }
        let header_length = cursor.read_u16()?;
        if usize::from(header_length) != RECORD_HEADER_LENGTH {
            return Err(Error::invalid_recording_log(
                path,
                format!(
                    "record header length {header_length} does not match {RECORD_HEADER_LENGTH}"
                ),
            ));
        }
        let header = Self {
            sequence: cursor.read_u64()?,
            frame_offset: cursor.read_u64()?,
            byte_offset: cursor.read_u64()?,
            payload_length: cursor.read_u32()?,
            payload_checksum: cursor.read_u32()?,
        };
        let actual_checksum = cursor.read_u32()?;
        if actual_checksum != expected_checksum {
            return Err(Error::invalid_recording_log(
                path,
                "record header checksum mismatch",
            ));
        }
        Ok(header)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordingLogRecordTrailer {
    sequence: u64,
    next_byte_offset: u64,
    payload_length: u32,
    payload_checksum: u32,
}

impl RecordingLogRecordTrailer {
    fn new(
        sequence: u64,
        next_byte_offset: u64,
        payload_length: u32,
        payload_checksum: u32,
    ) -> Self {
        Self {
            sequence,
            next_byte_offset,
            payload_length,
            payload_checksum,
        }
    }

    fn to_bytes(&self) -> [u8; RECORD_TRAILER_LENGTH] {
        let mut bytes = RecordingLogBytes::new();
        bytes.push_slice(&COMMIT_MAGIC);
        bytes.push_u64(self.sequence);
        bytes.push_u64(self.next_byte_offset);
        bytes.push_u32(self.payload_length);
        bytes.push_u32(self.payload_checksum);
        bytes.into_fixed()
    }

    fn from_bytes(path: &Path, bytes: &[u8; RECORD_TRAILER_LENGTH]) -> Result<Self> {
        let mut cursor = RecordingLogByteCursor::new(path, bytes);
        let magic = cursor.read_exact::<8>()?;
        if magic != COMMIT_MAGIC {
            return Err(Error::invalid_recording_log(path, "commit magic mismatch"));
        }
        Ok(Self {
            sequence: cursor.read_u64()?,
            next_byte_offset: cursor.read_u64()?,
            payload_length: cursor.read_u32()?,
            payload_checksum: cursor.read_u32()?,
        })
    }
}

struct RecordingLogParentDirectory {
    path: PathBuf,
}

impl RecordingLogParentDirectory {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn sync(&self) -> Result<()> {
        let directory = File::open(&self.path)?;
        directory.sync_all()?;
        Ok(())
    }
}

struct RecordingLogLabel<const LENGTH: usize> {
    bytes: [u8; LENGTH],
}

impl<const LENGTH: usize> RecordingLogLabel<LENGTH> {
    fn from_text(text: &str) -> Self {
        let mut bytes = [0_u8; LENGTH];
        let source = text.as_bytes();
        let count = source.len().min(LENGTH);
        bytes[..count].copy_from_slice(&source[..count]);
        Self { bytes }
    }

    fn bytes(&self) -> &[u8; LENGTH] {
        &self.bytes
    }
}

struct RecordingLogBytes {
    bytes: Vec<u8>,
}

impl RecordingLogBytes {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn push_slice(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn push_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn checksum(&self) -> u32 {
        crc32fast::hash(&self.bytes)
    }

    fn pad_to(&mut self, length: usize) {
        self.bytes.resize(length, 0);
    }

    fn into_fixed<const LENGTH: usize>(self) -> [u8; LENGTH] {
        let mut fixed = [0_u8; LENGTH];
        fixed.copy_from_slice(&self.bytes);
        fixed
    }
}

struct RecordingLogByteCursor<'a> {
    path: &'a Path,
    bytes: &'a [u8],
    position: usize,
}

impl<'a> RecordingLogByteCursor<'a> {
    fn new(path: &'a Path, bytes: &'a [u8]) -> Self {
        Self {
            path,
            bytes,
            position: 0,
        }
    }

    fn read_exact<const LENGTH: usize>(&mut self) -> Result<[u8; LENGTH]> {
        if self.position + LENGTH > self.bytes.len() {
            return Err(Error::invalid_recording_log(
                self.path,
                "binary structure ended early",
            ));
        }
        let mut output = [0_u8; LENGTH];
        output.copy_from_slice(&self.bytes[self.position..self.position + LENGTH]);
        self.position += LENGTH;
        Ok(output)
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_exact::<2>()?))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_exact::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_exact::<8>()?))
    }

    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.read_exact::<8>()?))
    }
}
