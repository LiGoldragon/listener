use std::{sync::{Arc, Mutex}};

use listener::{
    FreedesktopProviderHealthNotifier, ProviderAttemptState, ProviderHealthEvent,
    ProviderHealthNotification, ProviderHealthNotificationTransport, ProviderHealthSink,
    ProviderIdentifier,
};

#[test]
fn provider_degradation_notification_is_redacted_and_preserves_audio_assurance() {
    let notification = ProviderHealthNotification::from_event(ProviderHealthEvent::Degraded {
        provider: listener::ProviderIdentifier::WisprFlow,
        state: ProviderAttemptState::AuthenticationExpired,
    });
    assert_eq!(notification.title(), "Listener transcription provider");
    assert_eq!(notification.body(), "Wispr Flow is unavailable; audio is preserved and fallback will be used.");
    assert!(!notification.body().contains("AuthenticationExpired"));
}

#[derive(Default)]
struct RecordingTransport(Mutex<Vec<ProviderHealthNotification>>);

impl ProviderHealthNotificationTransport for RecordingTransport {
    fn notify(&self, notification: ProviderHealthNotification) {
        self.0.lock().expect("health notification recorder").push(notification);
    }
}

#[test]
fn typed_health_sink_projects_only_redacted_desktop_notification() {
    let transport = Arc::new(RecordingTransport::default());
    let notifier = FreedesktopProviderHealthNotifier::new(transport.clone());
    notifier.publish(ProviderHealthEvent::Degraded {
        provider: ProviderIdentifier::WisprFlow,
        state: ProviderAttemptState::AuthenticationExpired,
    });

    let received = transport.0.lock().expect("health notification recorder");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].title(), "Listener transcription provider");
    assert_eq!(received[0].body(), "Wispr Flow is unavailable; audio is preserved and fallback will be used.");
    assert!(!received[0].body().contains("AuthenticationExpired"));
}
