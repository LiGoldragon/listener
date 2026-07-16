use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use listener::{
    CaptureRetentionAge, CaptureRetentionByteLimit, CaptureRetentionPolicy, CaptureStore,
    CompactAudioArtifact, LiveOpusWebmEncoder, OpusWebmEncoder, RecordingAudioFormat,
    RecordingInputSource, RecordingLog, RecordingLogDurability, RecordingLogHeader,
    RecordingLogWriter, RecordingStartTime, TerminalCaptureState, capture::CaptureWriter,
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
fn compact_retry_artifact_advances_next_capture_session() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    fs::write(
        store.compact_audio_path_for_session(&CaptureSession::new(91)),
        b"compact",
    )
    .expect("write retained retry artifact");

    assert_eq!(
        store
            .next_session_value_after_existing_artifacts()
            .expect("next session"),
        92,
        "a retained compact artifact must never be overwritten after daemon restart"
    );
}

#[test]
fn terminal_capture_media_reclaims_oldest_session_when_byte_bound_is_exceeded() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let first = CaptureSession::new(1);
    let second = CaptureSession::new(2);
    fs::write(store.artifact_for_session(&first).path().as_str(), b"first")
        .expect("write first retained capture");
    fs::write(store.compact_audio_path_for_session(&second), b"second")
        .expect("write second retained capture");
    store
        .mark_terminal_capture(&first, TerminalCaptureState::Ready)
        .expect("mark first terminal capture");
    store
        .mark_terminal_capture(&second, TerminalCaptureState::Ready)
        .expect("mark second terminal capture");

    store
        .enforce_retention(CaptureRetentionPolicy::default())
        .expect("default three-day retention leaves fresh captures intact");
    assert!(Path::new(store.artifact_for_session(&first).path().as_str()).exists());
    assert!(store.compact_audio_path_for_session(&second).exists());

    store
        .enforce_retention(CaptureRetentionPolicy::new(
            None,
            Some(CaptureRetentionByteLimit::new(30)),
        ))
        .expect("enforce explicit byte bound");
    assert!(
        !Path::new(store.artifact_for_session(&first).path().as_str()).exists(),
        "the oldest session must be reclaimed first when the byte cap is exceeded"
    );
    assert!(
        store.compact_audio_path_for_session(&second).exists(),
        "the newest session remains within the configured byte cap"
    );
}

#[test]
fn default_three_day_terminal_retention_reclaims_terminal_media() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(1);
    fs::write(
        store.artifact_for_session(&session).path().as_str(),
        b"terminal media",
    )
    .expect("write terminal capture");
    store
        .mark_terminal_capture(&session, TerminalCaptureState::Cancelled)
        .expect("mark terminal capture");

    store
        .enforce_retention_at(
            CaptureRetentionPolicy::default(),
            SystemTime::now() + Duration::from_secs(4 * 24 * 60 * 60),
        )
        .expect("enforce default three-day retention");
    assert!(
        !Path::new(store.artifact_for_session(&session).path().as_str()).exists(),
        "default policy must reap terminal media after its three-day horizon"
    );
}

#[test]
fn retention_never_reaps_an_active_capture_without_terminal_metadata() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(1);
    let active_path = store.artifact_for_session(&session);
    fs::write(active_path.path().as_str(), b"active capture media").expect("write active capture");

    store
        .enforce_retention_at(
            CaptureRetentionPolicy::default(),
            SystemTime::now() + Duration::from_secs(4 * 24 * 60 * 60),
        )
        .expect("enforce retention around active capture");

    assert!(
        Path::new(active_path.path().as_str()).exists(),
        "only terminal metadata admits a capture to retention"
    );
}

#[test]
fn retention_does_not_treat_non_audio_capture_history_as_legacy_audio() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(2);
    let history_path = fixture
        .directory
        .path()
        .join("captures/capture-2.history.json");
    fs::write(&history_path, b"durable non-audio history").expect("write history");
    store
        .mark_terminal_capture(&session, TerminalCaptureState::Completed)
        .expect("mark terminal capture");

    store
        .migrate_terminal_captures()
        .expect("migrate terminal capture directory");
    store
        .enforce_retention_at(
            CaptureRetentionPolicy::default(),
            SystemTime::now() + Duration::from_secs(4 * 24 * 60 * 60),
        )
        .expect("reap terminal capture media");

    assert!(
        history_path.exists(),
        "non-audio durable history is outside Listener's audio retention set"
    );
}

#[test]
fn failed_legacy_audio_migration_preserves_source_until_terminal_reaping() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(3);
    let legacy_path = fixture.directory.path().join("captures/capture-3.wav");
    fs::write(&legacy_path, b"not decodable audio").expect("write corrupt legacy audio");
    store
        .mark_terminal_capture(&session, TerminalCaptureState::Ready)
        .expect("mark terminal capture");

    store
        .migrate_terminal_captures()
        .expect("attempt legacy migration");

    assert!(
        legacy_path.exists(),
        "a legacy source survives unless a validated canonical WebM/Opus artifact replaces it"
    );
    assert!(
        !store.compact_audio_path_for_session(&session).exists(),
        "failed migration cannot publish a canonical artifact"
    );

    store
        .enforce_retention_at(
            CaptureRetentionPolicy::default(),
            SystemTime::now() + Duration::from_secs(4 * 24 * 60 * 60),
        )
        .expect("reap expired terminal legacy audio");
    assert!(
        !legacy_path.exists(),
        "expired terminal legacy audio remains within the authorized reaper scope"
    );
}

#[test]
fn retained_capture_age_bound_reclaims_abandoned_media_at_maintenance_time() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(1);
    fs::write(
        store.artifact_for_session(&session).path().as_str(),
        b"abandoned",
    )
    .expect("write abandoned capture");
    store
        .mark_terminal_capture(&session, TerminalCaptureState::Ready)
        .expect("mark abandoned capture terminal");

    store
        .enforce_retention_at(
            CaptureRetentionPolicy::new(Some(CaptureRetentionAge::from_milliseconds(1)), None),
            SystemTime::now() + Duration::from_secs(1),
        )
        .expect("enforce explicit age bound");
    assert!(
        !Path::new(store.artifact_for_session(&session).path().as_str()).exists(),
        "capture older than the configured age bound must be reclaimed"
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

    CaptureWriter::new(
        input,
        recording_log,
        listener::StatusPublisher::silent(),
        None,
    )
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
fn live_opus_encoder_writes_compact_audio_before_finalization() {
    let fixture = CaptureWriterFixture::new();
    let destination = CompactAudioArtifact::new(fixture.directory.path().join("capture-91.webm"));
    let encoder = LiveOpusWebmEncoder::start(
        OpusWebmEncoder::from_environment(),
        RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
        destination.clone(),
    )
    .expect("start live encoder");
    let sender = encoder.sender();
    sender
        .send(vec![0_u8; 192_000])
        .expect("queue six seconds of PCM without blocking capture");
    let partial = fixture.directory.path().join("capture-91.webm.part");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while fs::metadata(&partial)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
        == 0
    {
        assert!(
            std::time::Instant::now() < deadline,
            "live encoder did not write the in-progress container before stop"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    drop(sender);

    let completed = encoder.finish().expect("finalize live encoder");
    assert_eq!(completed.path(), destination.path());
    assert!(completed.bytes().expect("compact bytes") > 0);
    assert!(
        !fixture
            .directory
            .path()
            .join("capture-91.webm.part")
            .exists(),
        "finalization atomically removes the unfinished container name"
    );
}

#[test]
fn interrupted_live_partial_is_discarded_and_recovered_from_durable_log() {
    let fixture = CaptureWriterFixture::new();
    let capture_directory = fixture.directory.path().join("captures");
    let store = CaptureStore::new(&capture_directory);
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(91);
    let recording_log = RecordingLogWriter::create(
        store.artifact_for_session(&session).path().as_str(),
        fixture.header(),
    )
    .expect("create durable recovery log");
    let mut recording_log = recording_log;
    for payload in vec![0_u8; 32_000].chunks(8_000) {
        recording_log
            .append_record(payload)
            .expect("commit one second of recovery audio");
    }
    recording_log.finish().expect("finish recovery log");
    fs::write(
        capture_directory.join("capture-91.webm.part"),
        b"interrupted container",
    )
    .expect("write interrupted partial");
    fs::write(capture_directory.join("capture-91.webm"), [])
        .expect("write invalid interrupted compact output");
    fs::write(
        capture_directory.join("capture-91.raw.s16le"),
        b"interrupted raw export",
    )
    .expect("write interrupted raw export");
    fs::write(
        capture_directory.join("capture-91.encoding.s16le"),
        b"interrupted recovery pcm",
    )
    .expect("write interrupted recovery pcm");
    fs::write(
        capture_directory.join("capture-91.webm.encoding"),
        b"interrupted encoding output",
    )
    .expect("write interrupted encoding output");

    let compact = store
        .compact_audio_for_session(&session)
        .expect("recover compact artifact from durable log");
    assert!(compact.path().as_str().ends_with(".webm"));
    assert!(Path::new(compact.path().as_str()).exists());
    assert!(
        fs::metadata(compact.path().as_str())
            .expect("recovered compact metadata")
            .len()
            > 0,
        "an invalid terminal WebM is replaced from its durable recording log"
    );
    assert!(
        !Path::new(store.artifact_for_session(&session).path().as_str()).exists(),
        "only validated compact output permits removal of recovery source"
    );
    assert!(
        !capture_directory.join("capture-91.webm.part").exists(),
        "an interrupted live container is never treated as retryable"
    );
    assert!(
        !capture_directory.join("capture-91.raw.s16le").exists(),
        "an abandoned raw export is removed before crash recovery"
    );
    assert!(
        !capture_directory.join("capture-91.encoding.s16le").exists(),
        "an abandoned recovery PCM export is removed before crash recovery"
    );
    assert!(
        !capture_directory.join("capture-91.webm.encoding").exists(),
        "an abandoned encoder output is removed before crash recovery"
    );
}

#[test]
fn recovery_discards_unusable_terminal_compact_artifact_without_a_recovery_log() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(91);
    fs::write(store.compact_audio_path_for_session(&session), [])
        .expect("write unusable compact artifact");
    store
        .mark_transcription_failed(&session)
        .expect("mark failed terminal compact artifact");

    store
        .migrate_terminal_captures()
        .expect("migrate abandoned capture directory");

    assert!(
        !store.compact_audio_path_for_session(&session).exists(),
        "a zero-byte terminal compact artifact cannot be retained"
    );
    assert_eq!(
        store
            .terminal_capture_state(&session)
            .expect("read terminal state"),
        Some(TerminalCaptureState::Failed),
        "failed conversion remains observable until terminal retention reaps its metadata"
    );
}

#[test]
fn idle_migration_reencodes_terminal_legacy_log_once_and_leaves_only_webm_opus() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(91);
    let mut writer = RecordingLogWriter::create(
        store.artifact_for_session(&session).path().as_str(),
        fixture.header(),
    )
    .expect("create terminal legacy log");
    for payload in vec![0_u8; 32_000].chunks(8_000) {
        writer
            .append_record(payload)
            .expect("write one second of terminal audio");
    }
    writer.finish().expect("finish terminal log");
    store
        .mark_terminal_capture(&session, TerminalCaptureState::Cancelled)
        .expect("mark cancelled terminal capture");

    store
        .migrate_terminal_captures()
        .expect("migrate terminal legacy log");
    store
        .migrate_terminal_captures()
        .expect("repeat idle migration idempotently");

    assert!(store.compact_audio_path_for_session(&session).exists());
    assert!(
        !Path::new(store.artifact_for_session(&session).path().as_str()).exists(),
        "a verified canonical WebM/Opus artifact replaces the legacy recovery log"
    );
    assert_eq!(
        store
            .terminal_capture_state(&session)
            .expect("read terminal state"),
        Some(TerminalCaptureState::Cancelled),
        "migration must not rewrite the terminal outcome"
    );
}

#[test]
fn idle_migration_reencodes_decodable_legacy_container_before_deleting_source() {
    let fixture = CaptureWriterFixture::new();
    let store = CaptureStore::new(fixture.directory.path().join("captures"));
    store.prepare().expect("prepare capture store");
    let session = CaptureSession::new(92);
    let raw_path = fixture.directory.path().join("legacy.s16le");
    let legacy_path = fixture.directory.path().join("captures/capture-92.wav");
    fs::write(&raw_path, vec![0_u8; 32_000]).expect("write legacy PCM fixture");
    let status = std::process::Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "s16le",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-i",
        ])
        .arg(&raw_path)
        .arg(&legacy_path)
        .status()
        .expect("start ffmpeg for legacy WAV fixture");
    assert!(status.success(), "create decodable legacy WAV fixture");

    store
        .migrate_terminal_captures()
        .expect("migrate decodable legacy container");

    assert!(
        store.compact_audio_path_for_session(&session).exists(),
        "a decodable legacy container is atomically replaced with WebM/Opus"
    );
    assert!(
        !legacy_path.exists(),
        "the legacy source is deleted only after canonical validation"
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

    CaptureWriter::new(input, recording_log, status_publisher, None)
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
