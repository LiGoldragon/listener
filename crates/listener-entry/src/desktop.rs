use listener_emission::{InterruptionCause, Notice};
use listener_library::{Delivery, Failing, Failure, Program, Text};
use std::{collections::HashMap, process::Stdio};
use tokio::{io::AsyncWriteExt, process::Command};

pub trait Delivering {
    async fn deliver(self, text: Text) -> Result<(), Failure>;
}
impl Delivering for Program {
    async fn deliver(self, text: Text) -> Result<(), Failure> {
        let mut child = Command::new(self.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| Failure(format!("Could not start the desktop program: {error}")))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| Failure("No desktop program input.".into()))?;
        let written = stdin.write_all(text.0.as_bytes()).await;
        drop(stdin);
        let status = child
            .wait()
            .await
            .map_err(|error| Failure(format!("Could not observe the desktop program: {error}")))?;
        if status.success() {
            written.map_err(|error| {
                Failure(format!(
                    "Could not supply the whole text to the desktop program: {error}"
                ))
            })?;
            Ok(())
        } else {
            Err(Failure(format!("The desktop program ended with {status}.")))
        }
    }
}

trait DescribingFailure {
    fn description(&self) -> String;
}
impl DescribingFailure for Failing {
    fn description(&self) -> String {
        match self {
            Self::Failure(failure) => failure.0.clone(),
            Self::Unobserved(nexus::Unobserved::Unsubmitted) => {
                "The translator could not be reached.".into()
            }
            Self::Unobserved(nexus::Unobserved::Unanswered) => {
                "The translator gave no outcome.".into()
            }
        }
    }
}

trait NoticeText {
    fn text(&self) -> (&'static str, String);
}
impl NoticeText for Notice {
    fn text(&self) -> (&'static str, String) {
        match self {
            Self::BackendFailed(attempt) => (
                "Transcription attempt failed",
                format!(
                    "{}: {}",
                    attempt.backend_name.0,
                    attempt.failing.description()
                ),
            ),
            Self::Exhausted(id) => (
                "No transcription was returned",
                format!(
                    "The audio for job {} remains available under its retention policy.",
                    id.0
                ),
            ),
            Self::Copied(id) => ("Text copied", format!("Job {} is on the clipboard.", id.0)),
            Self::DeliveryFailed(failure) => {
                let title = match failure.delivery {
                    Delivery::Pasted => "Paste outcome",
                    Delivery::Copied => "Copy outcome",
                    Delivery::Kept => "Delivery outcome",
                };
                (
                    title,
                    format!(
                        "Job {}: {}",
                        failure.job_id.0,
                        failure.failing.description()
                    ),
                )
            }
            Self::RecordingInterrupted(interruption) => {
                let cause = match interruption.interruption_cause {
                    InterruptionCause::MicrophoneLost => "The audio source disconnected.",
                    InterruptionCause::Unwritable => "The recording could not be written in full.",
                    InterruptionCause::Gap => "The audio source reported a gap.",
                };
                (
                    "Recording interrupted",
                    format!("Job {}: {cause}", interruption.job_id.0),
                )
            }
        }
    }
}

pub trait NotifyingDesktop {
    async fn notify(self) -> Result<(), Failure>;
}
impl NotifyingDesktop for Notice {
    async fn notify(self) -> Result<(), Failure> {
        let (summary, body) = self.text();
        // The notification standard treats body text as an optional markup fragment.
        let body = body
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");
        let connection = zbus::Connection::session().await.map_err(|error| {
            Failure(format!(
                "Could not connect to the notification bus: {error}"
            ))
        })?;
        let hints = HashMap::<&str, zbus::zvariant::Value<'_>>::new();
        let message = connection
            .call_method(
                Some("org.freedesktop.Notifications"),
                "/org/freedesktop/Notifications",
                Some("org.freedesktop.Notifications"),
                "Notify",
                &(
                    "Listener",
                    0u32,
                    "",
                    summary,
                    body,
                    Vec::<&str>::new(),
                    hints,
                    -1i32,
                ),
            )
            .await
            .map_err(|error| {
                Failure(format!("The desktop notification did not succeed: {error}"))
            })?;
        message.body().deserialize::<u32>().map_err(|_| {
            Failure("The notification service did not return a notification ID.".into())
        })?;
        Ok(())
    }
}
