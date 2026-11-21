use listener_entry::Listener;
use nexus_client::{Calling, TextSocket};
use std::process::ExitCode;

fn main() -> ExitCode {
    Listener::call(TextSocket::Ordinary).into()
}
