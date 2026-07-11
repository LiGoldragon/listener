//! Transcript recall — browse recent transcripts and copy one to the clipboard.
//!
//! Recall mirrors the Whisrs `whisrs-recall` pattern: read the local transcript
//! history newest first, present a `fuzzel --dmenu` picker over one-line
//! previews, and copy the full chosen transcript to the system clipboard. It is
//! a read-only reader over the JSONL history that the daemon writes; it does not
//! talk to the daemon and works even while the daemon is stopped.

use std::{
    env,
    io::Write,
    process::{Command, Stdio},
};

use signal_listener::{DeliveryOutcome, OutputTarget, TranscriptText};

use crate::{
    ClipboardCommand, Error, HistoryLimit, Result, TranscriptDeliveryRequest,
    TranscriptHistoryEntry, TranscriptHistoryStore,
};

const DEFAULT_RECALL_LIMIT: usize = 20;
const MAXIMUM_PREVIEW_CHARACTERS: usize = 160;
const EMPTY_HISTORY_PICKER_ROW: &str = "-\tNo transcript history yet";

/// The recall flow: read history, pick one transcript, copy it to the clipboard.
pub struct TranscriptRecall {
    history_store: TranscriptHistoryStore,
    selector: RecallSelector,
    clipboard: ClipboardCommand,
    limit: HistoryLimit,
}

impl TranscriptRecall {
    pub fn from_environment() -> Self {
        Self::new(
            TranscriptHistoryStore::from_environment(),
            RecallSelector::from_environment(),
            ClipboardCommand::from_environment(),
            HistoryLimit::new(DEFAULT_RECALL_LIMIT),
        )
    }

    pub fn new(
        history_store: TranscriptHistoryStore,
        selector: RecallSelector,
        clipboard: ClipboardCommand,
        limit: HistoryLimit,
    ) -> Self {
        Self {
            history_store,
            selector,
            clipboard,
            limit,
        }
    }

    pub fn run(&self) -> Result<RecallOutcome> {
        let entries = self.history_store.read_recent(self.limit)?;
        if entries.is_empty() {
            self.selector.show_empty_history()?;
            return Ok(RecallOutcome::EmptyHistory);
        }

        let rows = self.selection_rows(&entries);
        let Some(selection) = self.selector.select(&rows)? else {
            return Ok(RecallOutcome::NoSelection);
        };
        let Some(entry) = selection.resolve(&entries) else {
            return Ok(RecallOutcome::NoSelection);
        };

        let transcript_text = entry.transcript_text().clone();
        match self.clipboard.deliver(TranscriptDeliveryRequest::new(
            OutputTarget::SystemClipboard,
            transcript_text.clone(),
        )) {
            DeliveryOutcome::Delivered(_) => Ok(RecallOutcome::Copied(transcript_text)),
            DeliveryOutcome::Failed(failure) => Err(Error::OutputTargetRejected {
                message: format!(
                    "clipboard rejected recalled transcript: {:?}",
                    failure.delivery_failure_reason
                ),
            }),
        }
    }

    fn selection_rows(&self, entries: &[TranscriptHistoryEntry]) -> String {
        entries
            .iter()
            .enumerate()
            .map(|(index, entry)| format!("{index}\t{}", entry.preview(MAXIMUM_PREVIEW_CHARACTERS)))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// The result of one recall invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecallOutcome {
    /// No transcripts have been recorded yet.
    EmptyHistory,
    /// The picker closed without a selection (the user cancelled).
    NoSelection,
    /// The chosen transcript was copied to the clipboard.
    Copied(TranscriptText),
}

impl RecallOutcome {
    pub fn report(&self) {
        match self {
            Self::EmptyHistory => eprintln!("listener-recall: no transcript history yet"),
            Self::NoSelection => {}
            Self::Copied(_) => {
                eprintln!("listener-recall: copied selected transcript to the clipboard")
            }
        }
    }
}

/// A dmenu-style selector program driven over stdin/stdout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecallSelector {
    program: String,
    arguments: Vec<String>,
}

impl RecallSelector {
    pub fn fuzzel_default() -> Self {
        Self::new(
            "fuzzel",
            vec![
                "--dmenu".to_owned(),
                "--prompt".to_owned(),
                "listener> ".to_owned(),
                "--width=120".to_owned(),
                "--with-nth=2".to_owned(),
                "--accept-nth=1".to_owned(),
                "--match-nth=2".to_owned(),
            ],
        )
    }

    pub fn from_environment() -> Self {
        let mut selector = Self::fuzzel_default();
        if let Some(program) = env::var_os("LISTENER_RECALL_SELECTOR") {
            selector.program = program.to_string_lossy().into_owned();
        }
        selector
    }

    pub fn new(program: impl Into<String>, arguments: Vec<String>) -> Self {
        Self {
            program: program.into(),
            arguments,
        }
    }

    /// Show a visible empty-history picker row and ignore any accepted token.
    fn show_empty_history(&self) -> Result<()> {
        self.select(EMPTY_HISTORY_PICKER_ROW).map(|_| ())
    }

    /// Feed the tab-separated `index<TAB>preview` rows to the selector and read
    /// back the accepted index column. A nonzero exit or empty output means the
    /// selection was cancelled.
    fn select(&self, rows: &str) -> Result<Option<RecallSelection>> {
        let mut child = Command::new(&self.program)
            .args(&self.arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| Error::RecallSelectorUnavailable {
                program: self.program.clone(),
                message: error.to_string(),
            })?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::RecallSelectorUnavailable {
                program: self.program.clone(),
                message: "selector did not expose stdin".to_owned(),
            })?;
        stdin.write_all(rows.as_bytes())?;
        drop(stdin);

        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Ok(None);
        }
        let token = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if token.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecallSelection::new(token)))
        }
    }
}

/// The index column the selector accepted, resolved against the entry list.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RecallSelection {
    token: String,
}

impl RecallSelection {
    fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    fn resolve<'entries>(
        &self,
        entries: &'entries [TranscriptHistoryEntry],
    ) -> Option<&'entries TranscriptHistoryEntry> {
        let index: usize = self.token.parse().ok()?;
        entries.get(index)
    }
}
