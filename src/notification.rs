use std::collections::HashMap;

use signal_listener::TranscriptText;
use zbus::zvariant::OwnedValue;

const APPLICATION_NAME: &str = "Listener";
const TITLE: &str = "Listener Clipboard:";
const EXPIRE_TIMEOUT_MILLISECONDS: i32 = 2500;
const NOTIFICATION_DESTINATION: &str = "org.freedesktop.Notifications";
const NOTIFICATION_PATH: &str = "/org/freedesktop/Notifications";
const NOTIFICATION_INTERFACE: &str = "org.freedesktop.Notifications";

pub trait SuccessNotifier: Send + Sync {
    fn notify(&self, transcript_text: &TranscriptText);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClipboardSuccessNotification {
    body: String,
}

impl ClipboardSuccessNotification {
    pub fn from_transcript(transcript_text: &TranscriptText) -> Self {
        Self {
            body: Self::excerpt(transcript_text.as_str()),
        }
    }

    pub fn application_name(&self) -> &'static str {
        APPLICATION_NAME
    }

    pub fn title(&self) -> &'static str {
        TITLE
    }

    pub fn body(&self) -> &str {
        &self.body
    }

    fn excerpt(transcript_text: &str) -> String {
        let words = transcript_text.split_whitespace().collect::<Vec<_>>();
        if words.len() <= 12 {
            return transcript_text.to_owned();
        }

        let first = words[..6].join(" ");
        let last = words[words.len() - 6..].join(" ");
        format!("{first} … {last}")
    }
}

#[derive(Default)]
pub struct FreedesktopSuccessNotifier;

impl SuccessNotifier for FreedesktopSuccessNotifier {
    fn notify(&self, transcript_text: &TranscriptText) {
        let notification = ClipboardSuccessNotification::from_transcript(transcript_text);
        let Ok(connection) = zbus::blocking::Connection::session() else {
            return;
        };
        let hints = HashMap::from([("transient".to_owned(), OwnedValue::from(true))]);
        let _ = connection.call_method(
            Some(NOTIFICATION_DESTINATION),
            NOTIFICATION_PATH,
            Some(NOTIFICATION_INTERFACE),
            "Notify",
            &(
                notification.application_name(),
                0_u32,
                "",
                notification.title(),
                notification.body(),
                Vec::<String>::new(),
                hints,
                EXPIRE_TIMEOUT_MILLISECONDS,
            ),
        );
    }
}

#[derive(Default)]
pub struct SilentSuccessNotifier;

impl SuccessNotifier for SilentSuccessNotifier {
    fn notify(&self, _transcript_text: &TranscriptText) {}
}

#[cfg(test)]
mod tests {
    use signal_listener::TranscriptText;

    use super::ClipboardSuccessNotification;

    fn generated_transcript(word_count: usize) -> TranscriptText {
        TranscriptText::new(
            (0..word_count)
                .map(|index| format!("generated{index}"))
                .collect::<Vec<_>>()
                .join(" "),
        )
    }

    #[test]
    fn complete_short_transcript_is_the_notification_body() {
        let transcript = generated_transcript(12);
        let notification = ClipboardSuccessNotification::from_transcript(&transcript);

        assert!(notification.application_name() == "Listener");
        assert!(notification.title() == "Listener Clipboard:");
        assert!(notification.body() == transcript.as_str());
    }

    #[test]
    fn long_transcript_body_keeps_six_words_from_each_end() {
        let transcript = generated_transcript(13);
        let notification = ClipboardSuccessNotification::from_transcript(&transcript);
        let expected = format!(
            "{} … {}",
            (0..6)
                .map(|index| format!("generated{index}"))
                .collect::<Vec<_>>()
                .join(" "),
            (7..13)
                .map(|index| format!("generated{index}"))
                .collect::<Vec<_>>()
                .join(" "),
        );

        assert!(notification.body() == expected);
    }
}
