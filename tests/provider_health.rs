use std::{sync::{Arc, Mutex}, time::Duration};

use listener::{
    ProviderAttemptState, ProviderCircuitBreaker, ProviderHealthEvent, ProviderHealthSink,
    ProviderIdentifier,
};

#[derive(Default)]
struct RecordingHealth { events: Mutex<Vec<ProviderHealthEvent>> }
impl ProviderHealthSink for RecordingHealth {
    fn publish(&self, event: ProviderHealthEvent) { self.events.lock().expect("events").push(event); }
}

#[test]
fn breaker_allows_one_half_open_probe_and_emits_recovery() {
    let events = Arc::new(RecordingHealth::default());
    let breaker = ProviderCircuitBreaker::new(Duration::ZERO, events.clone());
    assert!(breaker.permit(ProviderIdentifier::WisprFlow));
    breaker.record_failure(ProviderIdentifier::WisprFlow, ProviderAttemptState::Unavailable);
    assert!(breaker.permit(ProviderIdentifier::WisprFlow));
    assert!(!breaker.permit(ProviderIdentifier::WisprFlow));
    breaker.record_success(ProviderIdentifier::WisprFlow);
    assert!(breaker.permit(ProviderIdentifier::WisprFlow));
    assert_eq!(
        events.events.lock().expect("events").as_slice(),
        [
            ProviderHealthEvent::Degraded {
                provider: ProviderIdentifier::WisprFlow,
                state: ProviderAttemptState::Unavailable,
            },
            ProviderHealthEvent::Recovered { provider: ProviderIdentifier::WisprFlow },
        ],
    );
}
