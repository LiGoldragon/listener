use std::{collections::HashMap, sync::Arc};

use signal_listener::TranscriptText;
use zbus::{Message, message::Flags, zvariant::OwnedValue};

const APPLICATION_NAME: &str = "Listener";
const TITLE: &str = "Listener Clipboard:";
const EXPIRE_TIMEOUT_MILLISECONDS: i32 = 2500;
const NOTIFICATION_DESTINATION: &str = "org.freedesktop.Notifications";
const NOTIFICATION_PATH: &str = "/org/freedesktop/Notifications";
const NOTIFICATION_INTERFACE: &str = "org.freedesktop.Notifications";
const NOTIFICATION_METHOD: &str = "Notify";

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

struct FreedesktopNotifyRequest {
    notification: ClipboardSuccessNotification,
}

impl FreedesktopNotifyRequest {
    fn message(&self) -> zbus::Result<Message> {
        let hints = HashMap::from([("transient".to_owned(), OwnedValue::from(true))]);
        Message::method(NOTIFICATION_PATH, NOTIFICATION_METHOD)?
            .destination(NOTIFICATION_DESTINATION)?
            .interface(NOTIFICATION_INTERFACE)?
            .with_flags(Flags::NoReplyExpected)?
            .build(&(
                self.notification.application_name(),
                0_u32,
                "",
                self.notification.title(),
                self.notification.body(),
                Vec::<String>::new(),
                hints,
                EXPIRE_TIMEOUT_MILLISECONDS,
            ))
    }
}

/// The only production boundary for desktop notification delivery.
pub trait FreedesktopNotificationTransport: Send + Sync {
    fn notify(&self, notification: ClipboardSuccessNotification);
}

#[derive(Default)]
pub struct FreedesktopDbusNotificationTransport;

impl FreedesktopNotificationTransport for FreedesktopDbusNotificationTransport {
    fn notify(&self, notification: ClipboardSuccessNotification) {
        let Ok(message) = (FreedesktopNotifyRequest { notification }).message() else {
            return;
        };
        let Ok(connection) = zbus::blocking::Connection::session() else {
            return;
        };
        let _ = connection.send(&message);
    }
}

pub struct FreedesktopSuccessNotifier {
    transport: Arc<dyn FreedesktopNotificationTransport>,
}

impl FreedesktopSuccessNotifier {
    pub fn new(transport: Arc<dyn FreedesktopNotificationTransport>) -> Self {
        Self { transport }
    }
}

impl Default for FreedesktopSuccessNotifier {
    fn default() -> Self {
        Self::new(Arc::new(FreedesktopDbusNotificationTransport))
    }
}

impl SuccessNotifier for FreedesktopSuccessNotifier {
    fn notify(&self, transcript_text: &TranscriptText) {
        self.transport
            .notify(ClipboardSuccessNotification::from_transcript(
                transcript_text,
            ));
    }
}

#[derive(Default)]
pub struct SilentSuccessNotifier;

impl SuccessNotifier for SilentSuccessNotifier {
    fn notify(&self, _transcript_text: &TranscriptText) {}
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use signal_listener::TranscriptText;
    use zbus::zvariant::OwnedValue;

    use super::{
        ClipboardSuccessNotification, FreedesktopNotificationTransport, FreedesktopNotifyRequest,
        FreedesktopSuccessNotifier, SuccessNotifier,
    };

    type NotifyArguments = (
        String,
        u32,
        String,
        String,
        String,
        Vec<String>,
        HashMap<String, OwnedValue>,
        i32,
    );

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

    #[test]
    fn production_notify_request_has_the_expected_dbus_boundary() {
        let transcript = generated_transcript(13);
        let notification = ClipboardSuccessNotification::from_transcript(&transcript);
        let message = (FreedesktopNotifyRequest {
            notification: notification.clone(),
        })
        .message()
        .expect("notification request serializes");
        let header = message.header();

        assert_eq!(message.message_type(), zbus::message::Type::MethodCall);
        assert_eq!(
            header.destination().expect("destination").to_string(),
            "org.freedesktop.Notifications"
        );
        assert_eq!(
            header.path().expect("object path").to_string(),
            "/org/freedesktop/Notifications"
        );
        assert_eq!(
            header.interface().expect("interface").to_string(),
            "org.freedesktop.Notifications"
        );
        assert_eq!(header.member().expect("method").to_string(), "Notify");
        assert!(
            message
                .primary_header()
                .flags()
                .contains(super::Flags::NoReplyExpected)
        );
        assert_eq!(
            message
                .body()
                .signature()
                .expect("body signature")
                .to_string(),
            "susssasa{sv}i"
        );

        let (application_name, replaces_id, app_icon, summary, body, actions, hints, expiry):
            NotifyArguments = message.body()
            .deserialize()
            .expect("notification body deserializes");

        assert_eq!(application_name, notification.application_name());
        assert_eq!(replaces_id, 0);
        assert_eq!(app_icon, "");
        assert_eq!(summary, notification.title());
        assert_eq!(body, notification.body());
        assert!(actions.is_empty());
        assert_eq!(hints.len(), 1);
        assert!(
            bool::try_from(hints.get("transient").expect("transient hint")).expect("boolean hint")
        );
        assert_eq!(expiry, 2500);
    }

    #[derive(Default)]
    struct RecordingTransport {
        notifications: Mutex<Vec<ClipboardSuccessNotification>>,
    }

    impl RecordingTransport {
        fn received_expected_direct_notification(&self, transcript: &TranscriptText) -> bool {
            self.notifications
                .lock()
                .expect("recording notification transport")
                .as_slice()
                == [ClipboardSuccessNotification::from_transcript(transcript)]
        }
    }

    impl FreedesktopNotificationTransport for RecordingTransport {
        fn notify(&self, notification: ClipboardSuccessNotification) {
            self.notifications
                .lock()
                .expect("recording notification transport")
                .push(notification);
        }
    }

    #[test]
    fn notifier_passes_generated_fixture_directly_to_the_dbus_transport() {
        let transcript = generated_transcript(13);
        let transport = std::sync::Arc::new(RecordingTransport::default());
        let notifier = FreedesktopSuccessNotifier::new(transport.clone());

        notifier.notify(&transcript);

        assert!(transport.received_expected_direct_notification(&transcript));
    }
}
