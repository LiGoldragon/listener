use std::{os::unix::net::UnixListener, thread};

use listener::{CommandLine, ContractFrameCodec, ContractFrameStream, ListenerClient};
use signal_listener::{Input, NoActiveCapture, Output, ToggleCapture};

struct CommandLineIntegration;

impl CommandLineIntegration {
    fn listener_socket() -> (tempfile::TempDir, std::path::PathBuf, UnixListener) {
        let directory = tempfile::tempdir().expect("create temporary socket directory");
        let socket = directory.path().join("listener.sock");
        let listener = UnixListener::bind(&socket).expect("bind listener socket");
        (directory, socket, listener)
    }
}

#[test]
fn inline_toggle_nota_reaches_the_listener_contract_client() {
    let (_directory, socket, listener) = CommandLineIntegration::listener_socket();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept listener client");
        let mut stream = ContractFrameStream::new(stream, ContractFrameCodec::listener_default());
        let request = stream.receive_request().expect("receive Listener request");
        assert_eq!(request.input(), &Input::Toggle(ToggleCapture {}));
        stream
            .send_reply(request, Output::NoActive(NoActiveCapture {}))
            .expect("reply through Listener contract");
    });

    let command = CommandLine::from_arguments(vec!["listener".to_owned(), "Toggle.{}".to_owned()]);
    let client = ListenerClient::new(socket);
    let mut output = Vec::new();
    command
        .run_with_client(&client, &mut output)
        .expect("submit inline schema request");

    assert_eq!(
        String::from_utf8(output).expect("UTF-8 reply"),
        "NoActive.{}\n"
    );
    server.join().expect("Listener contract server joins");
}
