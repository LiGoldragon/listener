use std::{env, fs::OpenOptions, io::Write, path::PathBuf, process::ExitCode};

use listener::{
    sandbox_wispr_witness, RecordingAudioFormat, RecordingInputSource, RecordingLogHeader,
    RecordingLogWriter, RecordingStartTime,
};
use signal_listener::CaptureSession;

struct SandboxWitness {
    session_descriptor: i32,
    bundle_descriptor: i32,
    artifact_path: PathBuf,
    diagnostics_path: PathBuf,
}

impl SandboxWitness {
    fn from_environment() -> Result<Self, &'static str> {
        Ok(Self {
            session_descriptor: required_descriptor("LISTENER_WISPR_SESSION_FD")?,
            bundle_descriptor: required_descriptor("LISTENER_WISPR_DESKTOP_BUNDLE_FD")?,
            artifact_path: required_path("LISTENER_WISPR_SANDBOX_ARTIFACT")?,
            diagnostics_path: required_path("LISTENER_WISPR_SANDBOX_DIAGNOSTICS")?,
        })
    }

    fn run(&self) -> Result<(), &'static str> {
        create_synthetic_recording(&self.artifact_path)?;
        let diagnostics = sandbox_wispr_witness(
            self.session_descriptor,
            self.bundle_descriptor,
            &self.artifact_path,
        );
        let diagnostics = serde_json::to_vec(&diagnostics)
            .map_err(|_| "sandbox-diagnostics-serialization-failed")?;
        write_new(&self.diagnostics_path, &diagnostics)
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

fn write_new(path: &PathBuf, contents: &[u8]) -> Result<(), &'static str> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| "sandbox-file-unavailable")?;
    file.write_all(contents).map_err(|_| "sandbox-write-failed")
}

fn main() -> ExitCode {
    let result = SandboxWitness::from_environment().and_then(|witness| witness.run());
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
