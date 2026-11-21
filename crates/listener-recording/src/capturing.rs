use crate::{
    Capture,
    native::{NativeRecording, NativeRecovery},
};
use listener_library::AudioLocation;
use listener_place::AudioLocating;
use std::{
    fs::{DirBuilder, File, OpenOptions},
    os::unix::{
        fs::{DirBuilderExt, OpenOptionsExt},
        net::UnixStream as ControlStream,
    },
};
use tokio::{io::AsyncReadExt, net::UnixStream, sync::watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    Stopped,
    MicrophoneLost,
    Unwritable,
    Gap,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    NoMicrophone,
    Unwritable,
}

pub struct Recording {
    control: UnixStream,
    end: watch::Receiver<Option<End>>,
}

trait PreparingAudio {
    fn fresh_file(&self) -> std::io::Result<File>;
}
impl PreparingAudio for AudioLocation {
    fn fresh_file(&self) -> std::io::Result<File> {
        let path = self.audio_path();
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&self.directory.0)?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
    }
}

pub trait StartingRecording {
    fn begin(self) -> impl std::future::Future<Output = Result<Recording, Refusal>> + Send;
}
impl StartingRecording for Capture {
    async fn begin(self) -> Result<Recording, Refusal> {
        let (control, worker) = ControlStream::pair().map_err(|_| Refusal::Unwritable)?;
        control
            .set_nonblocking(true)
            .map_err(|_| Refusal::Unwritable)?;
        let mut control = UnixStream::from_std(control).map_err(|_| Refusal::Unwritable)?;
        let file = self
            .audio_location
            .fresh_file()
            .map_err(|_| Refusal::Unwritable)?;
        let path = self.audio_location.audio_path();
        let (ended, mut end) = watch::channel(None);
        let spawned = std::thread::Builder::new().spawn(move || {
            let result = self.record(&worker, &file);
            drop(file);
            ended.send_replace(Some(result));
        });
        let startup = match spawned {
            Ok(_) => match control.read_u8().await {
                Ok(0) => Ok(()),
                Ok(1) => Err(Refusal::NoMicrophone),
                Ok(_) => Err(Refusal::Unwritable),
                Err(_) => match *end.borrow_and_update() {
                    Some(End::MicrophoneLost) => Err(Refusal::NoMicrophone),
                    _ => Err(Refusal::Unwritable),
                },
            },
            Err(_) => Err(Refusal::Unwritable),
        };
        if let Err(refusal) = startup {
            let _ = std::fs::remove_file(path);
            return Err(refusal);
        }
        Ok(Recording { control, end })
    }
}
pub trait ControllingRecording {
    fn ending(&self) -> watch::Receiver<Option<End>>;
    fn stop(self) -> impl std::future::Future<Output = End> + Send;
}
impl ControllingRecording for Recording {
    fn ending(&self) -> watch::Receiver<Option<End>> {
        self.end.clone()
    }
    async fn stop(mut self) -> End {
        // A byte, or closing the control endpoint, is an event in the Pulse mainloop.
        drop(self.control);
        self.end
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|end| *end)
            .unwrap_or(End::Unwritable)
    }
}

pub trait RestoringAudio {
    fn recover_audio(&self) -> bool;
}
impl RestoringAudio for AudioLocation {
    fn recover_audio(&self) -> bool {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.audio_path())
            .is_ok_and(|file| file.finish_sound_pages())
    }
}
