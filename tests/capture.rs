use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    sync::{Arc, Mutex},
};

use listener::{
    CaptureStore, RecordingAudioFormat, RecordingInputSource, RecordingLog, RecordingLogDurability,
    RecordingLogHeader, RecordingLogWriter, RecordingStartTime, capture::CaptureWriter,
};
use signal_listener::CaptureSession;
use tempfile::TempDir;

struct CaptureWriterFixture {
    directory: TempDir,
    committed_lengths: Arc<Mutex<Vec<u64>>>,
}

impl CaptureWriterFixture {
    fn new() -> Self {
        Self {
            directory: TempDir::new().expect("temp directory"),
            committed_lengths: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn path(&self) -> std::path::PathBuf {
        self.directory.path().join("capture.listenerlog")
    }

    fn header(&self) -> RecordingLogHeader {
        RecordingLogHeader::new(
            CaptureSession::new(91),
            RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
            RecordingInputSource::SystemDefault,
            RecordingStartTime::from_unix_parts(1_700_000_001, 0),
            8192,
        )
        .expect("recording header")
    }
}

#[test]
fn capture_store_prepare_uses_owner_only_directory_permissions() {
    let directory = TempDir::new().expect("temp directory");
    let capture_store_path = directory.path().join("captures");
    let capture_store = CaptureStore::new(&capture_store_path);

    capture_store.prepare().expect("prepare capture store");

    let directory_mode = fs::metadata(&capture_store_path)
        .expect("capture store metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        directory_mode, 0o700,
        "capture store directory must be owner-only"
    );
}

#[test]
fn capture_writer_commits_record_before_input_end() {
    let fixture = CaptureWriterFixture::new();
    let path = fixture.path();
    let recording_log = RecordingLogWriter::create_with_durability(
        &path,
        fixture.header(),
        Box::new(CommitProbe::new(Arc::clone(&fixture.committed_lengths))),
    )
    .expect("create recording log");
    let baseline_commit_count = fixture
        .committed_lengths
        .lock()
        .expect("commit lengths")
        .len();
    let input = CommitAwareInput::new(
        vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        Arc::clone(&fixture.committed_lengths),
        baseline_commit_count,
    );

    CaptureWriter::new(input, recording_log, listener::StatusPublisher::silent())
        .write_until_capture_stops()
        .expect("write capture stream");

    let commit_count = fixture
        .committed_lengths
        .lock()
        .expect("commit lengths")
        .len();
    assert!(
        commit_count >= baseline_commit_count + 2,
        "expected header plus one commit per payload record, got {commit_count} commits"
    );

    let export = RecordingLog::new(&path)
        .recover()
        .expect("recover log")
        .export_raw_pcm(fixture.directory.path().join("capture.raw.s16le"))
        .expect("export raw pcm");
    assert_eq!(
        fs::read(export.path()).expect("raw bytes"),
        vec![0, 1, 2, 3, 4, 5, 6, 7]
    );
}

#[test]
fn capture_writer_samples_live_level_at_fifty_millisecond_pcm_window() {
    let fixture = CaptureWriterFixture::new();
    let path = fixture.path();
    let recording_log = RecordingLogWriter::create_with_durability(
        &path,
        fixture.header(),
        Box::new(CommitProbe::new(Arc::clone(&fixture.committed_lengths))),
    )
    .expect("create recording log");
    let (status_publisher, status_events) = listener::StatusPublisher::recorder();
    let sample_window_bytes = 1_600;
    let input = ReadWindowProbeInput::new(vec![
        vec![1; sample_window_bytes],
        vec![2; sample_window_bytes],
    ]);

    CaptureWriter::new(input, recording_log, status_publisher)
        .write_until_capture_stops()
        .expect("write capture stream");

    let events = status_events.events();
    let recording_levels: Vec<f32> = events
        .iter()
        .filter(|event| event.state() == listener::ListenerStatusState::Recording)
        .map(|event| event.level().value())
        .collect();
    assert_eq!(
        recording_levels.len(),
        2,
        "expected one live level event per 50 ms PCM window, got {events:?}"
    );
    assert!(
        recording_levels.iter().all(|level| *level > 0.0),
        "expected nonzero live levels, got {recording_levels:?}"
    );

    let export = RecordingLog::new(&path)
        .recover()
        .expect("recover log")
        .export_raw_pcm(fixture.directory.path().join("capture.raw.s16le"))
        .expect("export raw pcm");
    assert_eq!(
        fs::read(export.path()).expect("raw bytes").len(),
        sample_window_bytes * 2
    );
}

struct CommitProbe {
    committed_lengths: Arc<Mutex<Vec<u64>>>,
}

impl CommitProbe {
    fn new(committed_lengths: Arc<Mutex<Vec<u64>>>) -> Self {
        Self { committed_lengths }
    }
}

impl RecordingLogDurability for CommitProbe {
    fn commit(&mut self, file: &mut File) -> listener::Result<()> {
        file.flush()?;
        file.sync_data()?;
        self.committed_lengths
            .lock()
            .expect("commit lengths")
            .push(file.metadata()?.len());
        Ok(())
    }
}

struct CommitAwareInput {
    chunks: Vec<Vec<u8>>,
    next_chunk: usize,
    committed_lengths: Arc<Mutex<Vec<u64>>>,
    baseline_commit_count: usize,
}

impl CommitAwareInput {
    fn new(
        chunks: Vec<Vec<u8>>,
        committed_lengths: Arc<Mutex<Vec<u64>>>,
        baseline_commit_count: usize,
    ) -> Self {
        Self {
            chunks,
            next_chunk: 0,
            committed_lengths,
            baseline_commit_count,
        }
    }

    fn commit_count(&self) -> usize {
        self.committed_lengths.lock().expect("commit lengths").len()
    }
}

impl Read for CommitAwareInput {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.next_chunk == 1 {
            assert!(
                self.commit_count() > self.baseline_commit_count,
                "first payload record was not committed before the next capture read"
            );
        }
        if self.next_chunk >= self.chunks.len() {
            return Ok(0);
        }

        let chunk = &self.chunks[self.next_chunk];
        output[..chunk.len()].copy_from_slice(chunk);
        self.next_chunk += 1;
        Ok(chunk.len())
    }
}

struct ReadWindowProbeInput {
    chunks: Vec<Vec<u8>>,
    next_chunk: usize,
}

impl ReadWindowProbeInput {
    fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            chunks,
            next_chunk: 0,
        }
    }
}

impl Read for ReadWindowProbeInput {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        assert_eq!(
            output.len(),
            1_600,
            "capture writer should request about 50 ms of 16 kHz mono s16le PCM, not the 8192-byte durable record ceiling"
        );
        if self.next_chunk >= self.chunks.len() {
            return Ok(0);
        }

        let chunk = &self.chunks[self.next_chunk];
        output[..chunk.len()].copy_from_slice(chunk);
        self.next_chunk += 1;
        Ok(chunk.len())
    }
}
