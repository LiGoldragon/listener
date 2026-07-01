use listener::Configuration;
use signal_listener::{
    CaptureStoreDirectory, InputSource, ListenerDaemonConfiguration, MetaSocketMode,
    MetaSocketPath, OutputTarget, SocketMode, TranscriptionMode, WirePath, WorkingSocketMode,
    WorkingSocketPath,
};

struct ConfigurationFixture;

impl ConfigurationFixture {
    fn path(value: &str) -> WirePath {
        WirePath::new(value.to_owned())
    }

    fn listener_configuration() -> ListenerDaemonConfiguration {
        ListenerDaemonConfiguration {
            working_socket_path: WorkingSocketPath::new(Self::path("/run/persona/X/listener.sock")),
            working_socket_mode: WorkingSocketMode::new(SocketMode::new(0o660)),
            meta_socket_path: MetaSocketPath::new(Self::path("/run/persona/X/listener-meta.sock")),
            meta_socket_mode: MetaSocketMode::new(SocketMode::new(0o600)),
            capture_store_directory: CaptureStoreDirectory::new(Self::path(
                "/var/lib/persona/listener/captures",
            )),
            input_source: InputSource::SystemDefault,
            transcription_mode: TranscriptionMode::BatchOnStop,
            output_target: OutputTarget::SystemClipboard,
        }
    }
}

#[test]
fn listener_configuration_round_trips_through_rkyv_archive() {
    let configuration = Configuration::new(ConfigurationFixture::listener_configuration());

    let bytes = configuration.to_rkyv_bytes().expect("encode configuration");
    let recovered = Configuration::from_rkyv_bytes(&bytes).expect("decode configuration");

    assert_eq!(recovered, configuration);
}
