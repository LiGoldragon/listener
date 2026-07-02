use std::{fs, os::unix::fs::PermissionsExt};

use listener::{HistoryLimit, HistoryTimestamp, TranscriptHistoryEntry, TranscriptHistoryStore};
use signal_listener::{CaptureSession, TranscriptText};
use tempfile::TempDir;

fn entry(session: u64, text: &str) -> TranscriptHistoryEntry {
    TranscriptHistoryEntry::new(
        HistoryTimestamp::from_unix_milliseconds(1_700_000_000_000 + session as i64),
        CaptureSession::new(session),
        TranscriptText::new(text),
    )
}

#[test]
fn append_and_read_back_transcript_history_newest_first() {
    let directory = TempDir::new().expect("temp directory");
    let store = TranscriptHistoryStore::new(directory.path().join("data").join("history.jsonl"));

    store
        .append(&entry(1, "first transcript"))
        .expect("append first entry");
    store
        .append(&entry(2, "second transcript"))
        .expect("append second entry");

    let entries = store
        .read_recent(HistoryLimit::new(10))
        .expect("read transcript history");

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].transcript_text().as_str(), "second transcript");
    assert_eq!(entries[0].session().value(), 2);
    assert_eq!(entries[1].transcript_text().as_str(), "first transcript");
    assert_eq!(entries[1].session().value(), 1);
    assert_eq!(
        entries[1].recorded_at(),
        HistoryTimestamp::from_unix_milliseconds(1_700_000_000_001)
    );
}

#[test]
fn read_recent_truncates_to_the_newest_entries() {
    let directory = TempDir::new().expect("temp directory");
    let store = TranscriptHistoryStore::new(directory.path().join("history.jsonl"));

    for index in 0..5 {
        store
            .append(&entry(index, &format!("transcript {index}")))
            .expect("append entry");
    }

    let entries = store
        .read_recent(HistoryLimit::new(3))
        .expect("read transcript history");

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].transcript_text().as_str(), "transcript 4");
    assert_eq!(entries[2].transcript_text().as_str(), "transcript 2");
}

#[test]
fn multiline_transcript_survives_the_jsonl_round_trip() {
    let directory = TempDir::new().expect("temp directory");
    let store = TranscriptHistoryStore::new(directory.path().join("history.jsonl"));
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
    let store = TranscriptHistoryStore::new(store_directory.join("history.jsonl"));

    store
        .append(&entry(1, "spoken content"))
        .expect("append entry");

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
    let store = TranscriptHistoryStore::new(directory.path().join("absent.jsonl"));

    assert!(
        store
            .read_recent(HistoryLimit::new(10))
            .expect("read empty history")
            .is_empty()
    );
}
