use std::{
    env,
    path::{Path, PathBuf},
};

use signal_listener::ListenerDaemonConfiguration;
use signal_listener::{
    CaptureStoreDirectory, InputSource, MetaSocketMode, MetaSocketPath, OutputTarget,
    OutputTargets, SocketMode, TranscriptionMode, WirePath, WorkingSocketMode, WorkingSocketPath,
};

use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    inner: ListenerDaemonConfiguration,
}

impl Configuration {
    pub fn from_environment() -> Self {
        Self::new(ConfigurationEnvironment::from_process().listener_configuration())
    }

    pub fn new(inner: ListenerDaemonConfiguration) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &ListenerDaemonConfiguration {
        &self.inner
    }

    pub fn into_inner(self) -> ListenerDaemonConfiguration {
        self.inner
    }

    pub fn working_socket_path(&self) -> PathBuf {
        self.inner.working_socket_path.payload().as_str().into()
    }

    pub fn working_socket_mode(&self) -> u32 {
        self.inner.working_socket_mode.payload().as_u32()
    }

    pub fn meta_socket_path(&self) -> PathBuf {
        self.inner.meta_socket_path.payload().as_str().into()
    }

    pub fn meta_socket_mode(&self) -> u32 {
        self.inner.meta_socket_mode.payload().as_u32()
    }

    pub fn capture_store_directory(&self) -> PathBuf {
        self.inner.capture_store_directory.payload().as_str().into()
    }

    pub fn input_source(&self) -> InputSource {
        self.inner.input_source
    }

    pub fn transcription_mode(&self) -> TranscriptionMode {
        self.inner.transcription_mode
    }

    pub fn output_targets(&self) -> &OutputTargets {
        &self.inner.output_targets
    }

    pub fn from_rkyv_bytes(bytes: &[u8]) -> Result<Self> {
        rkyv::from_bytes::<ListenerDaemonConfiguration, rkyv::rancor::Error>(bytes)
            .map(Self::new)
            .map_err(|_| Error::ConfigurationDecode)
    }

    pub fn to_rkyv_bytes(&self) -> Result<Vec<u8>> {
        rkyv::to_bytes::<rkyv::rancor::Error>(&self.inner)
            .map(|bytes| bytes.to_vec())
            .map_err(|_| Error::ConfigurationEncode)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigurationEnvironment {
    working_socket_path: PathBuf,
    meta_socket_path: PathBuf,
    capture_store_directory: PathBuf,
}

impl ConfigurationEnvironment {
    pub fn from_process() -> Self {
        let runtime_directory = env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from);
        let state_home = env::var_os("XDG_STATE_HOME").map(PathBuf::from);
        let home = env::var_os("HOME").map(PathBuf::from);

        Self {
            working_socket_path: env::var_os("LISTENER_SOCKET")
                .map(PathBuf::from)
                .unwrap_or_else(|| Self::socket_path(&runtime_directory, "listener.sock")),
            meta_socket_path: env::var_os("LISTENER_META_SOCKET")
                .map(PathBuf::from)
                .unwrap_or_else(|| Self::socket_path(&runtime_directory, "listener-meta.sock")),
            capture_store_directory: env::var_os("LISTENER_CAPTURE_STORE")
                .map(PathBuf::from)
                .unwrap_or_else(|| Self::capture_store_directory_path(&state_home, &home)),
        }
    }

    pub fn new(
        working_socket_path: impl Into<PathBuf>,
        meta_socket_path: impl Into<PathBuf>,
        capture_store_directory: impl Into<PathBuf>,
    ) -> Self {
        Self {
            working_socket_path: working_socket_path.into(),
            meta_socket_path: meta_socket_path.into(),
            capture_store_directory: capture_store_directory.into(),
        }
    }

    pub fn listener_configuration(&self) -> ListenerDaemonConfiguration {
        ListenerDaemonConfiguration {
            working_socket_path: WorkingSocketPath::new(Self::wire_path(&self.working_socket_path)),
            working_socket_mode: WorkingSocketMode::new(SocketMode::new(0o660)),
            meta_socket_path: MetaSocketPath::new(Self::wire_path(&self.meta_socket_path)),
            meta_socket_mode: MetaSocketMode::new(SocketMode::new(0o600)),
            capture_store_directory: CaptureStoreDirectory::new(Self::wire_path(
                &self.capture_store_directory,
            )),
            input_source: InputSource::SystemDefault,
            transcription_mode: TranscriptionMode::BatchOnStop,
            output_targets: OutputTargets::new(vec![OutputTarget::SystemClipboard]),
        }
    }

    fn socket_path(runtime_directory: &Option<PathBuf>, file_name: &str) -> PathBuf {
        runtime_directory
            .as_ref()
            .map(|directory| directory.join(file_name))
            .unwrap_or_else(|| env::temp_dir().join(file_name))
    }

    fn capture_store_directory_path(
        state_home: &Option<PathBuf>,
        home: &Option<PathBuf>,
    ) -> PathBuf {
        state_home
            .as_ref()
            .map(|directory| directory.join("listener/captures"))
            .or_else(|| {
                home.as_ref()
                    .map(|directory| directory.join(".local/state/listener/captures"))
            })
            .unwrap_or_else(|| env::temp_dir().join("listener/captures"))
    }

    fn wire_path(path: &Path) -> WirePath {
        WirePath::new(path.to_string_lossy().into_owned())
    }
}
