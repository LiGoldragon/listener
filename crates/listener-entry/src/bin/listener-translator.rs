use listener_entry::{Listener, ReadingLaunch};
use listener_place::{Preparing, Role};
use std::io;
use translator_runtime::{BindingTranslator, Place, Serving, Translator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let place = Listener::read_launch::<Place>()?;
    let _lease = place.prepare(Role::Translator)?;
    let translator = Translator { place };
    BindingTranslator::<Listener>::bind(translator)
        .map_err(|error| io::Error::other(format!("{error:?}")))?
        .serve()
        .await
        .map_err(|error| io::Error::other(format!("{error:?}")).into())
}
