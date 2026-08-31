use std::{
    env,
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::net::UnixListener,
    path::PathBuf,
    process::ExitCode,
};

use listener::{
    RecordingAudioFormat, RecordingInputSource, RecordingLogHeader, RecordingLogWriter,
    RecordingStartTime, sandbox_wispr_transcribe,
};
use signal_listener::CaptureSession;

struct SandboxWitness {
    session_descriptor: i32,
    bundle_descriptor: i32,
    artifact_path: PathBuf,
    pcm_descriptor: i32,
    clipboard_sink: PathBuf,
    state_store: PathBuf,
    ordinary_socket: PathBuf,
    meta_socket: PathBuf,
}

impl SandboxWitness {
    fn from_environment() -> Result<Self, &'static str> {
        Ok(Self {
            session_descriptor: required_descriptor("LISTENER_WISPR_SESSION_FD")?,
            bundle_descriptor: required_descriptor("LISTENER_WISPR_DESKTOP_BUNDLE_FD")?,
            artifact_path: required_path("LISTENER_WISPR_SANDBOX_ARTIFACT")?,
            pcm_descriptor: required_descriptor("LISTENER_WISPR_SANDBOX_PCM_FD")?,
            clipboard_sink: required_path("LISTENER_WISPR_SANDBOX_CLIPBOARD_SINK")?,
            state_store: required_path("LISTENER_WISPR_SANDBOX_STATE_STORE")?,
            ordinary_socket: required_path("LISTENER_WISPR_SANDBOX_SOCKET")?,
            meta_socket: required_path("LISTENER_WISPR_SANDBOX_META_SOCKET")?,
        })
    }

    fn run(&self) -> Result<String, &'static str> {
        let _ordinary = reserve_socket(&self.ordinary_socket)?;
        let _meta = reserve_socket(&self.meta_socket)?;
        write_new(&self.state_store, b"listener-wispr-sandbox\n")?;
        create_sandbox_recording(&self.artifact_path, self.pcm_descriptor)?;
        let transcript = sandbox_wispr_transcribe(
            self.session_descriptor,
            self.bundle_descriptor,
            &self.artifact_path,
        )
        .map_err(redacted_provider_failure)?;
        write_new(&self.clipboard_sink, transcript.as_str().as_bytes())?;
        Ok(transcript.as_str().to_owned())
    }
}

fn create_sandbox_recording(path: &PathBuf, descriptor: i32) -> Result<(), &'static str> {
    let mut source = std::fs::File::open(format!("/proc/self/fd/{descriptor}"))
        .map_err(|_| "sandbox-pcm-unavailable")?;
    let mut pcm = Vec::new();
    Read::by_ref(&mut source)
        .take(12 * 1024 * 1024)
        .read_to_end(&mut pcm)
        .map_err(|_| "sandbox-pcm-unavailable")?;
    if pcm.is_empty() || pcm.len() % 2 != 0 {
        return Err("sandbox-pcm-invalid");
    }
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
    for record in pcm.chunks(8192) {
        writer
            .append_record(record)
            .map_err(|_| "sandbox-recording-unavailable")?;
    }
    writer.finish().map_err(|_| "sandbox-recording-unavailable")
}

fn redacted_provider_failure(state: listener::ProviderAttemptState) -> &'static str {
    match state {
        listener::ProviderAttemptState::Unavailable => "redacted-provider-unavailable",
        listener::ProviderAttemptState::Rejected => "redacted-provider-rejected",
        listener::ProviderAttemptState::TransientFailure => "redacted-provider-transient-failure",
        listener::ProviderAttemptState::ProtocolFailure => "redacted-provider-protocol-failure",
        listener::ProviderAttemptState::SizeLimit => "redacted-provider-size-limit",
        listener::ProviderAttemptState::AuthenticationExpired => {
            "redacted-provider-authentication-expired"
        }
        listener::ProviderAttemptState::Cancelled => "redacted-provider-cancelled",
        listener::ProviderAttemptState::LocalArtifactFailure => "redacted-local-artifact-failure",
        listener::ProviderAttemptState::AmbiguousAfterSubmit => {
            "redacted-provider-ambiguous-after-submit"
        }
        listener::ProviderAttemptState::Succeeded => "redacted-provider-invalid-success-state",
    }
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

fn reserve_socket(path: &PathBuf) -> Result<UnixListener, &'static str> {
    UnixListener::bind(path).map_err(|_| "sandbox-socket-unavailable")
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
        Ok(transcript) => {
            println!("wispr-sandbox-transcript: {transcript}");
            ExitCode::SUCCESS
        }
        Err(status) => {
            eprintln!("wispr-sandbox: {status}");
            ExitCode::FAILURE
        }
    }
}
