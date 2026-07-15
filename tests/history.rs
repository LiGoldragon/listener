use std::{
    fs,
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    sync::{Arc, Barrier},
    thread,
};

use listener::{
    HistoryByteLimit, HistoryLimit, HistoryRetentionAge, HistoryRetentionPolicy, HistoryTimestamp,
    TranscriptHistoryEntry, TranscriptHistoryStore,
};
use signal_listener::{CaptureSession, TranscriptText};
use tempfile::TempDir;

fn entry(session: u64, text: &str) -> TranscriptHistoryEntry {
    TranscriptHistoryEntry::new(
        HistoryTimestamp::from_unix_milliseconds(1_700_000_000_000 + session as i64),
        CaptureSession::new(session),
        TranscriptText::new(text),
    )
}

fn long_lived_policy() -> HistoryRetentionPolicy {
    HistoryRetentionPolicy::new(
        HistoryRetentionAge::from_days(36_500).expect("long-lived test age"),
        HistoryByteLimit::new(1024 * 1024),
    )
}

fn store(path: impl Into<std::path::PathBuf>) -> TranscriptHistoryStore {
    TranscriptHistoryStore::new_with_retention(path, long_lived_policy())
}

#[test]
fn append_and_read_back_transcript_history_newest_first() {
    let directory = TempDir::new().expect("temp directory");
    let store = store(directory.path().join("data").join("history.jsonl"));

    store
        .append(&entry(1, "entry-one"))
        .expect("append first entry");
    store
        .append(&entry(2, "entry-two"))
        .expect("append second entry");

    let entries = store
        .read_recent(HistoryLimit::new(10))
        .expect("read transcript history");

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].transcript_text().as_str(), "entry-two");
    assert_eq!(entries[0].session().value(), 2);
    assert_eq!(entries[1].transcript_text().as_str(), "entry-one");
    assert_eq!(entries[1].session().value(), 1);
    assert_eq!(
        entries[1].recorded_at(),
        HistoryTimestamp::from_unix_milliseconds(1_700_000_000_001)
    );
}

#[test]
fn read_recent_truncates_to_the_newest_entries() {
    let directory = TempDir::new().expect("temp directory");
    let store = store(directory.path().join("history.jsonl"));

    for index in 0..5 {
        store
            .append(&entry(index, &format!("entry-{index}")))
            .expect("append entry");
    }

    let entries = store
        .read_recent(HistoryLimit::new(3))
        .expect("read transcript history");

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].transcript_text().as_str(), "entry-4");
    assert_eq!(entries[2].transcript_text().as_str(), "entry-2");
}

#[test]
fn multiline_transcript_survives_the_jsonl_round_trip() {
    let directory = TempDir::new().expect("temp directory");
    let store = store(directory.path().join("history.jsonl"));
    let spoken = "line one\nline two\twith a tab";

    store
        .append(&entry(7, spoken))
        .expect("append multiline entry");

    let entries = store
        .read_recent(HistoryLimit::new(10))
        .expect("read transcript history");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].transcript_text().as_str(), spoken);
    assert_eq!(entries[0].preview(160), "line one line two with a tab");
}

#[test]
fn history_store_uses_owner_only_permissions() {
    let directory = TempDir::new().expect("temp directory");
    let store_directory = directory.path().join("private");
    let store = store(store_directory.join("history.jsonl"));

    store.append(&entry(1, "entry")).expect("append entry");

    let file_mode = fs::metadata(store.path())
        .expect("history file metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(file_mode, 0o600, "history file must be owner-only");

    let directory_mode = fs::metadata(&store_directory)
        .expect("history directory metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        directory_mode, 0o700,
        "history directory must be owner-only"
    );
}

#[test]
fn missing_history_store_reads_empty() {
    let directory = TempDir::new().expect("temp directory");
    let store = store(directory.path().join("absent.jsonl"));

    assert!(
        store
            .read_recent(HistoryLimit::new(10))
            .expect("read empty history")
            .is_empty()
    );
}

#[test]
fn retention_keeps_the_exact_expiry_boundary_then_hard_deletes() {
    let directory = TempDir::new().expect("temp directory");
    let retention = HistoryRetentionPolicy::new(
        HistoryRetentionAge::from_milliseconds(1_000),
        HistoryByteLimit::new(1024),
    );
    let store = TranscriptHistoryStore::new_with_retention(
        directory.path().join("history.jsonl"),
        retention,
    );
    let recorded_at = HistoryTimestamp::now().expect("current timestamp");
    let recorded = TranscriptHistoryEntry::new(
        recorded_at,
        CaptureSession::new(1),
        TranscriptText::new("boundary-entry"),
    );

    store.append(&recorded).expect("append entry");
    store
        .compact_at(HistoryTimestamp::from_unix_milliseconds(
            recorded_at.unix_milliseconds() + 1_000,
        ))
        .expect("compact at expiry boundary");
    assert_eq!(
        store
            .read_recent(HistoryLimit::new(1))
            .expect("read boundary entry")
            .len(),
        1,
        "a record exactly at the retention boundary remains"
    );

    store
        .compact_at(HistoryTimestamp::from_unix_milliseconds(
            recorded_at.unix_milliseconds() + 1_001,
        ))
        .expect("compact after expiry boundary");
    assert!(
        store
            .read_recent(HistoryLimit::new(1))
            .expect("read expired history")
            .is_empty(),
        "expired records are hard-deleted rather than moved aside"
    );
    assert_eq!(fs::metadata(store.path()).expect("compacted file").len(), 0);
}

#[test]
fn repeated_appends_hard_cap_history_bytes_and_keep_newest_records() {
    let directory = TempDir::new().expect("temp directory");
    let maximum_bytes = 180;
    let store = TranscriptHistoryStore::new_with_retention(
        directory.path().join("history.jsonl"),
        HistoryRetentionPolicy::new(
            HistoryRetentionAge::from_days(1).expect("one day"),
            HistoryByteLimit::new(maximum_bytes),
        ),
    );
    let now = HistoryTimestamp::now().expect("current timestamp");

    for session in 0..12 {
        store
            .append(&TranscriptHistoryEntry::new(
                now,
                CaptureSession::new(session),
                TranscriptText::new(format!("bounded-{session}")),
            ))
            .expect("append bounded entry");
        assert!(
            fs::metadata(store.path()).expect("history metadata").len() <= maximum_bytes,
            "each append compacts to the configured byte budget"
        );
    }

    let entries = store
        .read_recent(HistoryLimit::new(12))
        .expect("read bounded history");
    assert!(
        entries.len() < 12,
        "oldest records were deleted by byte cap"
    );
    assert_eq!(entries[0].session().value(), 11);
}

#[test]
fn read_compacts_legacy_history_before_serving_a_limited_recall() {
    let directory = TempDir::new().expect("temp directory");
    let path = directory.path().join("history.jsonl");
    let maximum_bytes = 180;
    let store = TranscriptHistoryStore::new_with_retention(
        &path,
        HistoryRetentionPolicy::new(
            HistoryRetentionAge::from_days(1).expect("one day"),
            HistoryByteLimit::new(maximum_bytes),
        ),
    );
    let now = HistoryTimestamp::now().expect("current timestamp");
    let mut legacy = fs::File::create(&path).expect("legacy history file");
    for session in 0..32 {
        writeln!(
            legacy,
            r#"{{"unix_millis":{},"session":{},"text":"legacy-{}"}}"#,
            now.unix_milliseconds(),
            session,
            session
        )
        .expect("write legacy entry");
    }
    legacy.sync_all().expect("sync legacy history");

    let entries = store
        .read_recent(HistoryLimit::new(1))
        .expect("read compacted history");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].session().value(), 31);
    assert!(
        fs::metadata(&path).expect("compacted metadata").len() <= maximum_bytes,
        "the one-time legacy scan leaves bounded work for all later recalls"
    );
}

#[test]
fn malformed_tail_and_interrupted_rewrite_are_discarded_safely() {
    let directory = TempDir::new().expect("temp directory");
    let path = directory.path().join("history.jsonl");
    let store = store(&path);
    store
        .append(&entry(1, "valid"))
        .expect("append valid entry");

    let temporary = directory.path().join("history.jsonl.tmp-interrupted");
    fs::write(&temporary, b"incomplete replacement").expect("write interrupted replacement");
    let mut history = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open history tail");
    history
        .write_all(b"{\"unix_millis\":")
        .expect("write malformed tail");
    history.sync_all().expect("sync malformed tail");

    let entries = store
        .read_recent(HistoryLimit::new(10))
        .expect("read recovered history");
    assert_eq!(entries.len(), 1);
    assert!(!temporary.exists(), "stale rewrite files are hard-deleted");
    assert!(
        fs::read_to_string(&path)
            .expect("read compacted history")
            .ends_with('\n'),
        "the recovered replacement has only complete JSONL records"
    );
}

#[test]
fn atomic_replacement_preserves_existing_reader_view() {
    let directory = TempDir::new().expect("temp directory");
    let store = store(directory.path().join("history.jsonl"));
    store
        .append(&entry(1, "first"))
        .expect("append first entry");
    let mut prior_reader = fs::File::open(store.path()).expect("open prior history inode");

    store
        .append(&entry(2, "second"))
        .expect("atomically append second entry");

    let mut prior_contents = String::new();
    prior_reader
        .read_to_string(&mut prior_contents)
        .expect("read prior inode");
    assert!(prior_contents.contains("\"session\":1"));
    assert!(!prior_contents.contains("\"session\":2"));
    assert_eq!(
        store
            .read_recent(HistoryLimit::new(10))
            .expect("read replacement")
            .len(),
        2
    );
}

#[test]
fn concurrent_appends_serialize_without_lost_records() {
    let directory = TempDir::new().expect("temp directory");
    let store = Arc::new(store(directory.path().join("history.jsonl")));
    let barrier = Arc::new(Barrier::new(3));
    let mut writers = Vec::new();

    for session in 1..=2 {
        let writer_store = Arc::clone(&store);
        let writer_barrier = Arc::clone(&barrier);
        writers.push(thread::spawn(move || {
            writer_barrier.wait();
            writer_store.append(&entry(session, "concurrent-entry"))
        }));
    }
    barrier.wait();
    for writer in writers {
        writer
            .join()
            .expect("writer thread")
            .expect("serialized append");
    }

    let entries = store
        .read_recent(HistoryLimit::new(10))
        .expect("read serialized history");
    assert_eq!(entries.len(), 2);
}
