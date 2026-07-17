use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::ListenerStatusState;

/// Optional, transition-only latency trace for an explicitly requested timing
/// capture. The normal daemon path keeps this disabled and performs no trace
/// I/O.
#[derive(Clone)]
pub struct LatencyInstrumentation {
    trace_path: Option<PathBuf>,
    last_published_state: Arc<Mutex<Option<ListenerStatusState>>>,
}

impl LatencyInstrumentation {
    pub fn disabled() -> Self {
        Self {
            trace_path: None,
            last_published_state: Arc::new(Mutex::new(None)),
        }
    }

    pub fn from_environment() -> Self {
        std::env::var_os("LISTENER_LATENCY_TRACE")
            .map(PathBuf::from)
            .map(Self::for_path)
            .unwrap_or_else(Self::disabled)
    }

    pub fn for_path(path: impl Into<PathBuf>) -> Self {
        Self {
            trace_path: Some(path.into()),
            last_published_state: Arc::new(Mutex::new(None)),
        }
    }

    pub fn record_request_received(&self) {
        self.record("request-received");
    }

    pub fn record_capture_process_started(&self) {
        self.record("capture-process-started");
    }

    pub fn record_encoder_started(&self) {
        self.record("encoder-started");
    }

    pub fn record_state_publication(&self, state: ListenerStatusState) {
        let should_record = self
            .last_published_state
            .lock()
            .map(|mut last_state| {
                if last_state.as_ref() == Some(&state) {
                    false
                } else {
                    *last_state = Some(state);
                    true
                }
            })
            .unwrap_or(false);
        if should_record {
            self.record(&format!("state-published:{}", state.as_str()));
        }
    }

    fn record(&self, event: &str) {
        let Some(path) = &self.trace_path else {
            return;
        };
        let Ok(timestamp) = SystemTime::now().duration_since(UNIX_EPOCH) else {
            return;
        };
        let Ok(mut file) = OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(path)
        else {
            return;
        };
        let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
        let _ = writeln!(file, "{}\t{event}", timestamp.as_millis());
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn trace_records_requested_transition_boundaries_without_level_noise() {
        let directory = tempfile::TempDir::new().expect("trace directory");
        let path = directory.path().join("latency.trace");
        let trace = LatencyInstrumentation::for_path(&path);

        trace.record_request_received();
        trace.record_capture_process_started();
        trace.record_encoder_started();
        trace.record_state_publication(ListenerStatusState::Starting);
        trace.record_state_publication(ListenerStatusState::Recording);
        trace.record_state_publication(ListenerStatusState::Recording);

        let trace = fs::read_to_string(path).expect("read trace");
        assert!(trace.contains("request-received"));
        assert!(trace.contains("capture-process-started"));
        assert!(trace.contains("encoder-started"));
        assert!(trace.contains("state-published:starting"));
        assert_eq!(
            trace.matches("state-published:recording").count(),
            1,
            "repeated level publications must not create trace noise"
        );
    }
}
