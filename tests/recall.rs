use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use listener::{
    ClipboardCommand, HistoryLimit, HistoryTimestamp, RecallOutcome, RecallSelector,
    TranscriptHistoryEntry, TranscriptHistoryStore, TranscriptRecall,
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
        TranscriptHistoryStore::new(self.path("history.jsonl"))
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
fn recall_over_empty_history_reports_empty_and_never_invokes_the_selector() {
    let fixture = RecallFixture::new();
    let (clipboard, clipboard_output) = fixture.recording_clipboard();
    // A selector pointed at a missing program would error if it were spawned.
    let missing_selector = RecallSelector::new(
        fixture
            .path("does-not-exist")
            .to_string_lossy()
            .into_owned(),
        Vec::new(),
    );
    let recall = TranscriptRecall::new(
        fixture.history_store(),
        missing_selector,
        clipboard,
        HistoryLimit::new(20),
    );

    assert_eq!(
        recall.run().expect("run recall"),
        RecallOutcome::EmptyHistory
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
