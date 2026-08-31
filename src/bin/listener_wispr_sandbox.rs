use std::{
    env,
    io::Write,
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    process::ExitCode,
};

use listener::{
    sandbox_wispr_witness_checkpointed, RecordingAudioFormat, RecordingInputSource,
    RecordingLogHeader, RecordingLogWriter, RecordingStartTime, WisprWitnessCheckpoint,
    WisprWitnessDiagnostics, WisprWitnessRoute,
};
use signal_listener::CaptureSession;

struct SandboxWitness {
    session_descriptor: i32,
    bundle_descriptor: i32,
    artifact_path: PathBuf,
    route: WisprWitnessRoute,
}

impl SandboxWitness {
    fn from_environment(route: WisprWitnessRoute) -> Result<Self, &'static str> {
        Ok(Self {
            session_descriptor: required_descriptor("LISTENER_WISPR_SESSION_FD")
                .map_err(|_| "session-descriptor")?,
            bundle_descriptor: required_descriptor("LISTENER_WISPR_DESKTOP_BUNDLE_FD")
                .map_err(|_| "bundle-descriptor")?,
            artifact_path: required_path("LISTENER_WISPR_SANDBOX_ARTIFACT")
                .map_err(|_| "artifact-path")?,
            route,
        })
    }

    fn run(
        &self,
        checkpoint: &WisprWitnessCheckpoint,
    ) -> Result<WisprWitnessDiagnostics, &'static str> {
        create_synthetic_recording(&self.artifact_path).map_err(|_| "recording")?;
        checkpoint.advance("synthetic-audio");
        Ok(sandbox_wispr_witness_checkpointed(
            self.session_descriptor,
            self.bundle_descriptor,
            &self.artifact_path,
            self.route,
            checkpoint,
        ))
    }
}

fn route_from_environment() -> Result<WisprWitnessRoute, &'static str> {
    route_from_selector(env::var("LISTENER_WISPR_ROUTE").ok().as_deref())
}

fn route_from_selector(selector: Option<&str>) -> Result<WisprWitnessRoute, &'static str> {
    match selector {
        None | Some("default-direct") => Ok(WisprWitnessRoute::DefaultDirect),
        Some("edge-proxy") => Ok(WisprWitnessRoute::EdgeProxy),
        Some(_) => Err("route-selector"),
    }
}

fn create_synthetic_recording(path: &PathBuf) -> Result<(), &'static str> {
    let header = RecordingLogHeader::new(
        CaptureSession::new(1),
        RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
        RecordingInputSource::SystemDefault,
        RecordingStartTime::now().map_err(|_| "sandbox-clock-unavailable")?,
        8192,
    )
    .map_err(|_| "sandbox-recording-header-invalid")?;
    let mut writer =
        RecordingLogWriter::create(path, header).map_err(|_| "sandbox-recording-unavailable")?;
    // 10 ms of zero-valued PCM is synthetic, non-private audio.
    writer
        .append_record(&[0_u8; 320])
        .map_err(|_| "sandbox-recording-unavailable")?;
    writer.finish().map_err(|_| "sandbox-recording-unavailable")
}

fn required_descriptor(variable: &'static str) -> Result<i32, &'static str> {
    let descriptor = env::var(variable)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|descriptor| *descriptor >= 3)
        .ok_or("invalid-inherited-descriptor")?;
    Ok(descriptor)
}

fn required_path(variable: &'static str) -> Result<PathBuf, &'static str> {
    let path = env::var_os(variable)
        .map(PathBuf::from)
        .ok_or("missing-sandbox-path")?;
    if !path.is_absolute() || path.exists() {
        return Err("unsafe-sandbox-path");
    }
    path.parent()
        .filter(|parent| parent.is_dir())
        .ok_or("missing-sandbox-parent")?;
    Ok(path)
}

fn finish_with_diagnostics(
    diagnostics_path: &Path,
    checkpoint: &WisprWitnessCheckpoint,
    route: WisprWitnessRoute,
    operation: impl FnOnce(&WisprWitnessCheckpoint) -> Result<WisprWitnessDiagnostics, &'static str>,
) -> Result<(), &'static str> {
    let (diagnostics, outcome) = match catch_unwind(AssertUnwindSafe(|| operation(checkpoint))) {
        Ok(Ok(diagnostics)) => {
            let outcome = diagnostics
                .has_completed_response()
                .then_some(())
                .ok_or("witness-failed");
            (diagnostics, outcome)
        }
        Ok(Err(stage)) => (
            WisprWitnessDiagnostics::setup_failure(stage, route),
            Err("witness-failed"),
        ),
        Err(_) => (
            WisprWitnessDiagnostics::setup_failure(checkpoint.stage(), route),
            Err("witness-failed"),
        ),
    };
    let contents = serde_json::to_vec(&diagnostics).unwrap_or_else(|_| {
        br#"{"local_stage":"diagnostics","route":"default-direct","http_status":null,"grpc_status":null,"content_type":null,"bearer_state":"unknown","permission_category":"absent","response_frame_count":0,"response_frame_lengths":[],"protobuf_top_level_tag_histograms":[]}"#.to_vec()
    });
    write_diagnostics_atomically(diagnostics_path, &contents)?;
    outcome
}

fn write_diagnostics_atomically(path: &Path, contents: &[u8]) -> Result<(), &'static str> {
    let parent = path.parent().ok_or("diagnostics-parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|_| "diagnostics-open")?;
    temporary
        .write_all(contents)
        .map_err(|_| "diagnostics-write")?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|_| "diagnostics-sync")?;
    temporary
        .persist_noclobber(path)
        .map_err(|_| "diagnostics-persist")?;
    Ok(())
}

fn main() -> ExitCode {
    // A caught unwind is represented solely by its last completed static
    // checkpoint. Do not let a dependency's panic payload cross this consumer
    // boundary through stderr.
    std::panic::set_hook(Box::new(|_| {}));
    let diagnostics_path = match required_path("LISTENER_WISPR_SANDBOX_DIAGNOSTICS") {
        Ok(path) => path,
        Err(_) => {
            eprintln!("wispr-sandbox: diagnostics-path-invalid");
            return ExitCode::FAILURE;
        }
    };
    let checkpoint = WisprWitnessCheckpoint::new("validated-output");
    let route = match route_from_environment() {
        Ok(route) => route,
        Err(stage) => {
            let _ = finish_with_diagnostics(
                &diagnostics_path,
                &checkpoint,
                WisprWitnessRoute::DefaultDirect,
                |_| Err(stage),
            );
            eprintln!("wispr-sandbox: witness-failed");
            return ExitCode::FAILURE;
        }
    };
    let result = finish_with_diagnostics(&diagnostics_path, &checkpoint, route, |checkpoint| {
        let witness = SandboxWitness::from_environment(route)?;
        checkpoint.advance("preflight");
        witness.run(checkpoint)
    });
    match result {
        Ok(()) => {
            println!("wispr-sandbox: diagnostics-written");
            ExitCode::SUCCESS
        }
        Err(status) => {
            eprintln!("wispr-sandbox: {status}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_after_diagnostics_path_validation_writes_structural_diagnostics() {
        let directory = tempfile::tempdir().expect("temporary diagnostics directory");
        let path = directory.path().join("diagnostics.json");

        let checkpoint = WisprWitnessCheckpoint::new("synthetic-audio");
        let outcome = finish_with_diagnostics(
            &path,
            &checkpoint,
            WisprWitnessRoute::DefaultDirect,
            |_| -> Result<WisprWitnessDiagnostics, _> { panic!("synthetic setup panic") },
        );
        assert_eq!(outcome, Err("witness-failed"));

        let diagnostics: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&path).expect("diagnostics artifact exists after panic"),
        )
        .expect("diagnostics are valid JSON");
        assert_eq!(diagnostics["local_stage"], "synthetic-audio");
        assert_eq!(
            diagnostics.as_object().expect("diagnostics object").len(),
            10
        );
        assert_eq!(diagnostics["bearer_state"], "unknown");
        assert_eq!(diagnostics["permission_category"], "absent");
        assert_eq!(diagnostics["route"], "default-direct");
        assert!(diagnostics.get("error").is_none());
        assert!(diagnostics.get("message").is_none());
    }

    #[test]
    fn no_response_diagnostics_are_a_failed_witness_outcome() {
        let directory = tempfile::tempdir().expect("temporary diagnostics directory");
        let path = directory.path().join("diagnostics.json");
        let checkpoint = WisprWitnessCheckpoint::new("tcp-dial-attempted");

        let outcome = finish_with_diagnostics(
            &path,
            &checkpoint,
            WisprWitnessRoute::EdgeProxy,
            |_| {
                Ok(WisprWitnessDiagnostics::setup_failure(
                    "tcp-dial-attempted",
                    WisprWitnessRoute::EdgeProxy,
                ))
            },
        );

        assert_eq!(outcome, Err("witness-failed"));
        let diagnostics: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&path).expect("diagnostics artifact exists after transport failure"),
        )
        .expect("diagnostics are valid JSON");
        assert_eq!(diagnostics["local_stage"], "tcp-dial-attempted");
        assert_eq!(diagnostics["http_status"], serde_json::Value::Null);
        assert_eq!(diagnostics["bearer_state"], "unknown");
        assert_eq!(diagnostics["permission_category"], "absent");
        assert_eq!(diagnostics["route"], "edge-proxy");
    }

    #[test]
    fn route_selector_is_closed_and_defaults_to_direct() {
        assert_eq!(
            route_from_selector(None),
            Ok(WisprWitnessRoute::DefaultDirect)
        );
        assert_eq!(
            route_from_selector(Some("edge-proxy")),
            Ok(WisprWitnessRoute::EdgeProxy)
        );
        assert_eq!(route_from_selector(Some("unexpected")), Err("route-selector"));
    }
}
