use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use listener::{
    ClipboardCommand, HistoryByteLimit, HistoryLimit, HistoryRetentionAge, HistoryRetentionPolicy,
    HistoryTimestamp, RecallOutcome, RecallSelector, TranscriptHistoryEntry,
    TranscriptHistoryStore, TranscriptRecall,
};
use signal_listener::{CaptureSession, TranscriptText};
use tempfile::TempDir;

struct RecallFixture {
    directory: TempDir,
}

impl RecallFixture {
    fn new() -> Self {
        Self {
            directory: TempDir::new().expect("temp directory"),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    fn history_store(&self) -> TranscriptHistoryStore {
        TranscriptHistoryStore::new_with_retention(
            self.path("history.jsonl"),
            HistoryRetentionPolicy::new(
                HistoryRetentionAge::from_days(36_500).expect("long-lived test age"),
                HistoryByteLimit::new(1024 * 1024),
            ),
        )
    }

    fn seed(&self, session: u64, text: &str) {
        self.history_store()
            .append(&TranscriptHistoryEntry::new(
                HistoryTimestamp::from_unix_milliseconds(1_700_000_000_000 + session as i64),
                CaptureSession::new(session),
                TranscriptText::new(text),
            ))
            .expect("seed transcript history");
    }

    /// A selector stub that consumes the piped rows and echoes a fixed accepted
    /// index column, standing in for the interactive fuzzel picker.
    fn selector_choosing_index(&self, index: usize) -> RecallSelector {
        let script = self.path("selector.sh");
        write_executable(
            &script,
            &format!("#!/bin/sh\ncat > /dev/null\nprintf '{index}\\n'\n"),
        );
        RecallSelector::new(script.to_string_lossy().into_owned(), Vec::new())
    }

    fn selector_cancelling(&self) -> RecallSelector {
        let script = self.path("selector-cancel.sh");
        write_executable(&script, "#!/bin/sh\ncat > /dev/null\nexit 1\n");
        RecallSelector::new(script.to_string_lossy().into_owned(), Vec::new())
    }

    fn selector_recording_rows(&self) -> (RecallSelector, PathBuf) {
        let output = self.path("selector-rows.txt");
        let script = self.path("selector-recording.sh");
        write_executable(
            &script,
            &format!("#!/bin/sh\ncat > '{}'\nexit 0\n", output.display()),
        );
        (
            RecallSelector::new(script.to_string_lossy().into_owned(), Vec::new()),
            output,
        )
    }

    /// A clipboard stub that records the copied text to a file for inspection.
    fn recording_clipboard(&self) -> (ClipboardCommand, PathBuf) {
        let output = self.path("clipboard.txt");
        let script = self.path("clipboard.sh");
        write_executable(
            &script,
            &format!("#!/bin/sh\ncat > '{}'\n", output.display()),
        );
        (
            ClipboardCommand::new(script.to_string_lossy().into_owned()),
            output,
        )
    }
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write stub script");
    let mut permissions = fs::metadata(path).expect("stub metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("mark stub executable");
}

#[test]
fn recall_copies_the_selected_transcript_to_the_clipboard() {
    let fixture = RecallFixture::new();
    fixture.seed(1, "first transcript");
    fixture.seed(2, "second transcript");

    // Newest first is [second, first]; index 1 selects the older transcript.
    let (clipboard, clipboard_output) = fixture.recording_clipboard();
    let recall = TranscriptRecall::new(
        fixture.history_store(),
        fixture.selector_choosing_index(1),
        clipboard,
        HistoryLimit::new(20),
    );

    match recall.run().expect("run recall") {
        RecallOutcome::Copied(text) => assert_eq!(text.as_str(), "first transcript"),
        other => panic!("expected copied outcome, got {other:?}"),
    }

    let copied = fs::read_to_string(&clipboard_output).expect("clipboard output");
    assert_eq!(copied, "first transcript");
}

#[test]
fn recall_over_empty_history_shows_an_empty_picker_and_leaves_the_clipboard_untouched() {
    let fixture = RecallFixture::new();
    let (clipboard, clipboard_output) = fixture.recording_clipboard();
    let (selector, selector_rows) = fixture.selector_recording_rows();
    let recall = TranscriptRecall::new(
        fixture.history_store(),
        selector,
        clipboard,
        HistoryLimit::new(20),
    );

    assert_eq!(
        recall.run().expect("run recall"),
        RecallOutcome::EmptyHistory
    );
    assert_eq!(
        fs::read_to_string(&selector_rows).expect("selector rows"),
        "-\tNo transcript history yet"
    );
    assert!(
        !clipboard_output.exists(),
        "empty history must not touch the clipboard"
    );
}

#[test]
fn recall_cancelled_selection_leaves_the_clipboard_untouched() {
    let fixture = RecallFixture::new();
    fixture.seed(1, "only transcript");
    let (clipboard, clipboard_output) = fixture.recording_clipboard();
    let recall = TranscriptRecall::new(
        fixture.history_store(),
        fixture.selector_cancelling(),
        clipboard,
        HistoryLimit::new(20),
    );

    assert_eq!(
        recall.run().expect("run recall"),
        RecallOutcome::NoSelection
    );
    assert!(
        !clipboard_output.exists(),
        "a cancelled selection must not touch the clipboard"
    );
}
