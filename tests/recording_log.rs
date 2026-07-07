use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
};

use listener::{
    Error, RecordingAudioFormat, RecordingInputSource, RecordingLog, RecordingLogHeader,
    RecordingLogWriter, RecordingSampleFormat, RecordingStartTime,
};
use signal_listener::CaptureSession;
use tempfile::TempDir;

struct RecordingLogFixture {
    directory: TempDir,
}

impl RecordingLogFixture {
    fn new() -> Self {
        Self {
            directory: TempDir::new().expect("temp directory"),
        }
    }

    fn path(&self) -> PathBuf {
        self.directory.path().join("capture.listenerlog")
    }

    fn header(&self) -> RecordingLogHeader {
        RecordingLogHeader::new(
            CaptureSession::new(77),
            RecordingAudioFormat::new(
                RecordingSampleFormat::SignedSixteenBitLittleEndian,
                16_000,
                1,
            )
            .expect("audio format"),
            RecordingInputSource::SystemDefault,
            RecordingStartTime::from_unix_parts(1_700_000_000, 123_000_000),
            8192,
        )
        .expect("recording log header")
    }
}

#[test]
fn header_and_complete_records_recover() {
    let fixture = RecordingLogFixture::new();
    let path = fixture.path();
    let mut writer = RecordingLogWriter::create(&path, fixture.header()).expect("create log");

    writer
        .append_record(&[0, 1, 2, 3])
        .expect("append first record");
    writer
        .append_record(&[4, 5, 6, 7])
        .expect("append second record");
    writer.finish().expect("finish log");

    let recovered = RecordingLog::new(&path).recover().expect("recover log");

    assert_eq!(recovered.truncated_from(), None);
    assert_eq!(recovered.header().session().value(), 77);
    assert_eq!(recovered.header().audio_format().sample_rate(), 16_000);
    assert_eq!(recovered.header().audio_format().channel_count(), 1);
    assert_eq!(recovered.header().audio_format().bytes_per_frame(), 2);
    assert_eq!(recovered.records().len(), 2);
    assert_eq!(recovered.records()[0].sequence(), 0);
    assert_eq!(recovered.records()[0].byte_offset(), 0);
    assert_eq!(recovered.records()[1].sequence(), 1);
    assert_eq!(recovered.records()[1].byte_offset(), 4);
    assert_eq!(recovered.total_payload_bytes(), 8);

    let export = recovered
        .export_raw_pcm(fixture.directory.path().join("capture.raw.s16le"))
        .expect("export raw pcm");
    assert_eq!(
        fs::read(export.path()).expect("raw bytes"),
        vec![0, 1, 2, 3, 4, 5, 6, 7]
    );
}

#[test]
fn recording_log_and_raw_export_use_owner_only_permissions() {
    let fixture = RecordingLogFixture::new();
    let private_parent = fixture.directory.path().join("private");
    let path = private_parent.join("capture.listenerlog");
    let mut writer = RecordingLogWriter::create(&path, fixture.header()).expect("create log");
    writer
        .append_record(&[0, 1, 2, 3])
        .expect("append complete record");
    writer.finish().expect("finish log");

    let recovered = RecordingLog::new(&path).recover().expect("recover log");
    let export = recovered
        .export_raw_pcm(private_parent.join("capture.raw.s16le"))
        .expect("export raw pcm");

    let parent_mode = fs::metadata(&private_parent)
        .expect("private parent metadata")
        .permissions()
        .mode()
        & 0o777;
    let log_mode = fs::metadata(&path)
        .expect("listenerlog metadata")
        .permissions()
        .mode()
        & 0o777;
    let raw_mode = fs::metadata(export.path())
        .expect("raw export metadata")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(parent_mode, 0o700, "artifact parent must be owner-only");
    assert_eq!(log_mode, 0o600, "listenerlog must be owner-only");
    assert_eq!(raw_mode, 0o600, "raw PCM export must be owner-only");
}

#[test]
fn create_refuses_existing_listenerlog_without_truncating() {
    let fixture = RecordingLogFixture::new();
    let path = fixture.path();
    let mut writer = RecordingLogWriter::create(&path, fixture.header()).expect("create log");
    writer
        .append_record(&[0, 1, 2, 3])
        .expect("append complete record");
    writer.finish().expect("finish log");
    let original_bytes = fs::read(&path).expect("original log bytes");

    match RecordingLogWriter::create(&path, fixture.header()) {
        Err(Error::RecordingLogAlreadyExists { path: error_path }) => {
            assert_eq!(error_path, path.display().to_string());
        }
        Err(error) => panic!("expected existing recording log error, got {error}"),
        Ok(_) => panic!("expected existing recording log creation to fail"),
    }

    assert_eq!(
        fs::read(&path).expect("log bytes after refused create"),
        original_bytes
    );
}

#[test]
fn incomplete_record_recovery_stops_at_last_valid_boundary() {
    let fixture = RecordingLogFixture::new();
    let path = fixture.path();
    let mut writer = RecordingLogWriter::create(&path, fixture.header()).expect("create log");
    writer
        .append_record(&[10, 11, 12, 13])
        .expect("append complete record");
    writer.finish().expect("finish log");
    let valid_length = fs::metadata(&path).expect("valid metadata").len();

    OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open log for torn append")
        .write_all(b"LSTNREC1torn")
        .expect("append torn record");
    let torn_length = fs::metadata(&path).expect("torn metadata").len();

    let recovered = RecordingLog::new(&path)
        .recover()
        .expect("recover torn log");

    assert_eq!(recovered.records().len(), 1);
    assert_eq!(recovered.truncated_from(), Some(torn_length));
    assert_eq!(
        fs::metadata(&path).expect("recovered metadata").len(),
        valid_length
    );
}

#[test]
fn recovery_truncation_is_idempotent() {
    let fixture = RecordingLogFixture::new();
    let path = fixture.path();
    let mut writer = RecordingLogWriter::create(&path, fixture.header()).expect("create log");
    writer
        .append_record(&[20, 21, 22, 23])
        .expect("append complete record");
    writer.finish().expect("finish log");

    OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open log for corrupt append")
        .write_all(b"corrupt trailing bytes")
        .expect("append corrupt bytes");

    let recovered_once = RecordingLog::new(&path)
        .recover()
        .expect("recover corrupt log once");
    let length_after_first_recovery = fs::metadata(&path)
        .expect("metadata after first recovery")
        .len();
    let recovered_twice = RecordingLog::new(&path)
        .recover()
        .expect("recover corrupt log twice");

    assert!(recovered_once.truncated_from().is_some());
    assert_eq!(recovered_twice.truncated_from(), None);
    assert_eq!(
        fs::metadata(&path)
            .expect("metadata after second recovery")
            .len(),
        length_after_first_recovery
    );
    assert_eq!(recovered_twice.records().len(), 1);
}
