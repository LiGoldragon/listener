//! Listener's process adapters: text at ingress, service and desktop at egress.
#![forbid(unsafe_code)]
mod desktop;
mod transcription;

use listener_emission::{Copying_Data, Emission, Notifying_Data, Outcome, Pasting_Data};
use listener_place::COMPONENT;
use meta_signal_listener as meta;
use nexus_client::Named;
use signal::Frame;
use signal_listener as ordinary;
use translator::{DatomReadingBudget, OutputMode};
use translator_runtime::{ConfigurationFollowing, Emittable, Translating};

pub struct Listener;

impl Named for Listener {
    fn name() -> String {
        COMPONENT.to_owned()
    }
}

use desktop::{Delivering, NotifyingDesktop};
use nexus::{TransmissionError, WireValue};
use signal::{ByteViewable, FrameBody, FrameCapacity, Signal, Signalizable};
use transcription::Transcribing;

/// The accepted translator trait lives in a different crate from Emission.
/// This adapter delegates every wire operation to that one declared type.
pub struct ListenerEmission(Emission);
impl Signalizable for ListenerEmission {
    fn signalize(&self) -> Result<Signal<Self>, rkyv::rancor::Error> {
        self.0
            .signalize()
            .map(|signal| Signal::from(signal.bytes().to_vec()))
    }
}
impl WireValue for ListenerEmission {
    fn restored(body: FrameBody) -> Result<Self, TransmissionError> {
        Emission::restored(body).map(Self)
    }
    fn framed_value(&self, capacity: FrameCapacity) -> Result<Vec<u8>, TransmissionError> {
        self.0.framed_value(capacity)
    }
}
impl Emittable for ListenerEmission {
    type Outcome = Outcome;
    async fn emit(self) -> Outcome {
        match self.0 {
            Emission::Transcribe(request) => {
                Outcome::Transcription(request.transcribe().await.map_or_else(
                    listener_emission::Transcription_Data::Failure,
                    listener_emission::Transcription_Data::Heard,
                ))
            }
            Emission::Paste(request) => Outcome::Pasting(
                request
                    .program
                    .deliver(request.text)
                    .await
                    .map_or_else(Pasting_Data::Failure, |()| Pasting_Data::Pasted),
            ),
            Emission::Copy(request) => Outcome::Copying(
                request
                    .program
                    .deliver(request.text)
                    .await
                    .map_or_else(Copying_Data::Failure, |()| Copying_Data::Copied),
            ),
            Emission::Notify(notice) => Outcome::Notifying(
                notice
                    .notify()
                    .await
                    .map_or_else(Notifying_Data::Failure, |()| Notifying_Data::Notified),
            ),
        }
    }
}

impl Translating for Listener {
    type Input = ordinary::Input;
    type Output = ordinary::Output;
    type MetaInput = meta::Input;
    type MetaOutput = meta::Output;
    type Emission = ListenerEmission;
    type Outcome = Outcome;

    fn describe() -> String {
        include_str!("../ethos/ordinary.ethos").to_owned()
    }
    fn describe_meta() -> String {
        include_str!("../ethos/meta.ethos").to_owned()
    }
    fn output_mode(output: &Self::Output) -> OutputMode {
        match output {
            ordinary::Output::Subscribed(_) => OutputMode::Streaming,
            _ => OutputMode::Single,
        }
    }
    fn meta_output_mode(output: &Self::MetaOutput) -> OutputMode {
        match output {
            meta::Output::Subscribed(_) => OutputMode::Streaming,
            _ => OutputMode::Single,
        }
    }
}
impl ConfigurationFollowing for Listener {
    fn subscribe_configuration() -> Self::MetaInput {
        meta::Input::Subscribe
    }
    fn snapshot(output: &Self::MetaOutput) -> Option<DatomReadingBudget> {
        match output {
            meta::Output::Subscribed(
                Frame::Opened(configuration) | Frame::Lagged(configuration),
            ) => Some(configuration.budget.0.clone()),
            _ => None,
        }
    }
    fn after(
        current: &DatomReadingBudget,
        output: &Self::MetaOutput,
    ) -> Option<DatomReadingBudget> {
        match output {
            meta::Output::Subscribed(Frame::Changed(meta::Set::Budget(budget))) => {
                Some(budget.clone())
            }
            meta::Output::Subscribed(Frame::Changed(_)) => Some(current.clone()),
            other => Self::snapshot(other),
        }
    }
}

pub trait ReadingLaunch {
    fn read_launch<T: datom_codec::Composing>() -> Result<T, Box<dyn std::error::Error>>;
}
impl ReadingLaunch for Listener {
    fn read_launch<T: datom_codec::Composing>() -> Result<T, Box<dyn std::error::Error>> {
        use datom_codec::{Actualizing, Budget, Potential};
        use std::io::{Error, ErrorKind};
        let mut arguments = std::env::args().skip(1);
        let (Some(source), None) = (arguments.next(), arguments.next()) else {
            return Err(Error::from(ErrorKind::InvalidInput).into());
        };
        let steps = source.len().saturating_add(1);
        let mut budget = Budget {
            remaining: i64::try_from(steps)?,
            reader: protos::ReaderBudget::try_from(steps)
                .map_err(|_| Error::from(ErrorKind::InvalidInput))?,
            depth: 0,
            maximum_depth: signal::MAXIMUM_SIGNAL_DEPTH as i64,
        };
        Potential::<T>::from(source)
            .actualize(&mut budget)
            .map_err(|error| Error::new(ErrorKind::InvalidInput, format!("{error:?}")).into())
    }
}
