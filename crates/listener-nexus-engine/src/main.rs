use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use listener_nexus_engine::StartingNexus;
use listener_place::Startup;
use signal::{Restorable, Signal};
use std::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let (Some(startup), None) = (arguments.next(), arguments.next()) else {
        return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
    };
    let payload = STANDARD_NO_PAD.decode(startup.as_encoded_bytes())?;
    Signal::<Startup>::from(payload).restore()?.start().await
}
