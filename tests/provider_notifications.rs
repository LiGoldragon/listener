use listener::{ProviderAttemptState, ProviderHealthEvent, ProviderHealthNotification};

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
