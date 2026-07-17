use std::{
    io::Write,
    process::{Command, Stdio},
    sync::Arc,
};

use signal_listener::{
    DeliveredTo, DeliveryFailure, DeliveryFailureReason, DeliveryOutcome, DeliveryOutcomes,
    OutputTarget, OutputTargets, TranscriptText,
};

pub trait TranscriptDelivery: Send + Sync {
    fn deliver(&self, request: TranscriptDeliveryRequest) -> DeliveryOutcome;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TranscriptDeliveryRequest {
    target: OutputTarget,
    transcript_text: TranscriptText,
}

impl TranscriptDeliveryRequest {
    pub fn new(target: OutputTarget, transcript_text: TranscriptText) -> Self {
        Self {
            target,
            transcript_text,
        }
    }

    pub fn target(&self) -> OutputTarget {
        self.target
    }

    pub fn transcript_text(&self) -> &TranscriptText {
        &self.transcript_text
    }
}

#[derive(Clone)]
pub struct OutputTargetDispatcher {
    system_clipboard: Arc<dyn TranscriptDelivery>,
}

impl OutputTargetDispatcher {
    pub fn from_environment() -> Self {
        Self::new(Box::new(ClipboardDelivery::from_environment()))
    }

    pub fn new(system_clipboard: Box<dyn TranscriptDelivery>) -> Self {
        Self {
            system_clipboard: Arc::from(system_clipboard),
        }
    }

    pub fn deliver(
        &self,
        output_targets: &OutputTargets,
        transcript_text: &TranscriptText,
    ) -> DeliveryOutcomes {
        let outcomes = output_targets
            .as_slice()
            .iter()
            .map(|target| self.deliver_to_target(*target, transcript_text.clone()))
            .collect();
        DeliveryOutcomes::new(outcomes)
    }

    fn deliver_to_target(
        &self,
        target: OutputTarget,
        transcript_text: TranscriptText,
    ) -> DeliveryOutcome {
        match target {
            OutputTarget::SystemClipboard => {
                self.system_clipboard
                    .deliver(TranscriptDeliveryRequest::new(
                        OutputTarget::SystemClipboard,
                        transcript_text,
                    ))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClipboardDelivery {
    command: ClipboardCommand,
}

impl ClipboardDelivery {
    pub fn from_environment() -> Self {
        Self::new(ClipboardCommand::from_environment())
    }

    pub fn new(command: ClipboardCommand) -> Self {
        Self { command }
    }
}

impl TranscriptDelivery for ClipboardDelivery {
    fn deliver(&self, request: TranscriptDeliveryRequest) -> DeliveryOutcome {
        self.command.deliver(request)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClipboardCommand {
    program: String,
}

impl ClipboardCommand {
    pub fn from_environment() -> Self {
        Self::new(
            std::env::var("LISTENER_CLIPBOARD_PROGRAM").unwrap_or_else(|_| "wl-copy".to_owned()),
        )
    }

    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
        }
    }

    pub fn deliver(&self, request: TranscriptDeliveryRequest) -> DeliveryOutcome {
        let mut child = match Command::new(&self.program)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(_) => {
                return Self::failure(request.target(), DeliveryFailureReason::TargetUnavailable);
            }
        };

        let Some(mut stdin) = child.stdin.take() else {
            return Self::failure(request.target(), DeliveryFailureReason::TargetRejected);
        };

        let write_result = stdin.write_all(request.transcript_text().as_str().as_bytes());
        drop(stdin);

        if write_result.is_err() {
            let _ = child.wait();
            return Self::failure(request.target(), DeliveryFailureReason::TargetRejected);
        }

        match child.wait() {
            Ok(status) if status.success() => {
                DeliveryOutcome::Delivered(DeliveredTo::new(request.target()))
            }
            Ok(_) | Err(_) => {
                Self::failure(request.target(), DeliveryFailureReason::TargetRejected)
            }
        }
    }

    fn failure(target: OutputTarget, reason: DeliveryFailureReason) -> DeliveryOutcome {
        DeliveryOutcome::Failed(DeliveryFailure {
            output_target: target,
            delivery_failure_reason: reason,
        })
    }
}
