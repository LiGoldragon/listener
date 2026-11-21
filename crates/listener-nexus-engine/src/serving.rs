//! Typed sockets. The reader can leave a quiet subscription without waiting
//! for another store change. All calls carry the Translator's caller evidence.
use crate::owner::{Deferred, Event, MetaReply, Reply};
use futures_util::StreamExt;
use meta_signal_listener as meta;
use nexus::{SignalReceiving, SignalSubmitting, TransmissionError, WireValue};
use signal::FrameCapacity;
use signal_listener as ordinary;
use std::io;
use tokio::{
    io::{AsyncReadExt, AsyncWrite},
    net::{UnixListener, UnixStream},
    sync::{mpsc, oneshot},
};
use translator::Call;

#[derive(Debug)]
enum ContactError {
    Io,
    Transmission,
    MissingOwner,
    Authority,
}
impl From<io::Error> for ContactError {
    fn from(_: io::Error) -> Self {
        Self::Io
    }
}
impl From<TransmissionError> for ContactError {
    fn from(_: TransmissionError) -> Self {
        Self::Transmission
    }
}

enum Answer<Output, State, Change> {
    Once(Output, Deferred),
    Subscribed(signal::Stream<State, Change>),
}
impl From<Reply>
    for Answer<ordinary::Output, Vec<listener_library::JobState>, ordinary::JobChange>
{
    fn from(reply: Reply) -> Self {
        match reply.answer {
            ordinary::Answer::Once(output) => Self::Once(output, reply.after),
            ordinary::Answer::Subscribed(subscription) => Self::Subscribed(subscription),
        }
    }
}
impl From<MetaReply> for Answer<meta::Output, listener_library::Configuration, meta::Set> {
    fn from(reply: MetaReply) -> Self {
        match reply.answer {
            meta::Answer::Once(output) => Self::Once(output, reply.after),
            meta::Answer::Subscribed(subscription) => Self::Subscribed(subscription),
        }
    }
}

pub struct Sockets {
    pub ordinary: UnixListener,
    pub meta: UnixListener,
}
pub trait Serving {
    async fn serve(self, events: mpsc::UnboundedSender<Event>) -> io::Result<()>;
}
impl Serving for Sockets {
    async fn serve(self, events: mpsc::UnboundedSender<Event>) -> io::Result<()> {
        loop {
            let events = events.clone();
            tokio::select! {
                accepted = self.ordinary.accept() => {
                    let (stream, _) = accepted?;
                    tokio::spawn(async move {
                        let _ = stream.exchange(events, Event::Ordinary, ordinary::Output::Subscribed).await;
                    });
                }
                accepted = self.meta.accept() => {
                    let (stream, _) = accepted?;
                    tokio::spawn(async move {
                        let _ = stream.exchange(events, Event::Meta, meta::Output::Subscribed).await;
                    });
                }
            }
        }
    }
}

trait Exchanging {
    async fn exchange<Input, Response, Output, State, Change>(
        self,
        events: mpsc::UnboundedSender<Event>,
        entered: fn(Input, oneshot::Sender<Response>) -> Event,
        subscribed: fn(signal::Frame<State, Change>) -> Output,
    ) -> Result<(), ContactError>
    where
        Call<Input>: WireValue,
        Response: Into<Answer<Output, State, Change>> + Send,
        Output: WireValue,
        State: Send + 'static,
        Change: Send + 'static;
}
impl Exchanging for UnixStream {
    async fn exchange<Input, Response, Output, State, Change>(
        mut self,
        events: mpsc::UnboundedSender<Event>,
        entered: fn(Input, oneshot::Sender<Response>) -> Event,
        subscribed: fn(signal::Frame<State, Change>) -> Output,
    ) -> Result<(), ContactError>
    where
        Call<Input>: WireValue,
        Response: Into<Answer<Output, State, Change>> + Send,
        Output: WireValue,
        State: Send + 'static,
        Change: Send + 'static,
    {
        let user_id = self.peer_cred()?.uid();
        if user_id != rustix::process::geteuid().as_raw() {
            return Err(ContactError::Authority);
        }
        let frame_capacity = FrameCapacity::default();
        let call: Call<Input> = self.receive(frame_capacity).await?;
        if call.caller.user_id.0 != i64::from(user_id) {
            return Err(ContactError::Authority);
        }
        let (response, wait) = oneshot::channel();
        events
            .send(entered(call.input, response))
            .map_err(|_| ContactError::MissingOwner)?;
        let (mut read, mut write) = self.into_split();
        let response = wait.await.map_err(|_| ContactError::MissingOwner)?.into();
        match response {
            Answer::Once(output, after) => {
                let result = write.submit(output, frame_capacity).await;
                let _ = events.send(Event::Replied(after));
                result?;
            }
            Answer::Subscribed(subscription) => {
                let mut extra = [0u8];
                tokio::select! {
                    result = write.publish(subscription, subscribed, frame_capacity) => result?,
                    _ = read.read(&mut extra) => {}
                }
            }
        }
        Ok(())
    }
}
trait Streaming {
    async fn publish<Output: WireValue, State, Change>(
        &mut self,
        subscription: signal::Stream<State, Change>,
        subscribed: fn(signal::Frame<State, Change>) -> Output,
        frame_capacity: FrameCapacity,
    ) -> Result<(), TransmissionError>;
}
impl<W: AsyncWrite + Unpin + Send> Streaming for W {
    async fn publish<Output: WireValue, State, Change>(
        &mut self,
        mut subscription: signal::Stream<State, Change>,
        subscribed: fn(signal::Frame<State, Change>) -> Output,
        frame_capacity: FrameCapacity,
    ) -> Result<(), TransmissionError> {
        while let Some(frame) = subscription.next().await {
            self.submit(subscribed(frame), frame_capacity).await?;
        }
        Ok(())
    }
}
