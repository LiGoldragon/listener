//! Operations that await other programs. Workers cannot access Memory.
use listener_emission::*;
use listener_library::*;
use nexus::{EmissionSender, Sending, Unobserved};
use tokio::sync::Mutex;

pub struct Effects {
    pub emission: EmissionSender,
    pub desktop: Mutex<()>,
}
pub struct HearingWork {
    pub program: Program,
    pub location: AudioLocation,
    pub backends: Vec<Backend>,
    pub terms: Vec<Term>,
}
pub struct HandingWork {
    pub ready: Ready,
    pub paste_program: Program,
    pub copy_program: Program,
}

pub trait HearingJob {
    async fn hear(&self, job: HearingWork) -> Hearing;
}
pub trait HandingText {
    async fn hand(&self, job: HandingWork) -> Handing;
}
pub trait Notifying {
    async fn notify(&self, notice: Notice);
}

impl Notifying for Effects {
    async fn notify(&self, notice: Notice) {
        let _: Result<Outcome, Unobserved> = self.emission.send(Emission::Notify(notice)).await;
    }
}
impl HearingJob for Effects {
    async fn hear(&self, job: HearingWork) -> Hearing {
        let mut attempts = Vec::new();
        for backend in job.backends {
            let backend_name = backend.backend_name.clone();
            let sent = self
                .emission
                .send(Emission::Transcribe(Transcribe_Data {
                    program: job.program.clone(),
                    audio_location: job.location.clone(),
                    backend,
                    term_vector: job.terms.clone(),
                }))
                .await;
            let failing = match sent {
                Ok(Outcome::Transcription(Transcription_Data::Heard(heard))) => {
                    return Hearing::Heard(Heard_Data {
                        transcript: Transcript {
                            backend_name,
                            provider_id_option: heard.provider_id_option,
                            text: heard.text,
                        },
                        try_vector: attempts,
                    });
                }
                Ok(Outcome::Transcription(Transcription_Data::Failure(failure))) => {
                    Failing::Failure(failure)
                }
                Err(unobserved) => Failing::Unobserved(unobserved),
                Ok(_) => Failing::Unobserved(Unobserved::Unanswered),
            };
            let attempt = Try {
                backend_name,
                failing,
            };
            self.notify(Notice::BackendFailed(attempt.clone())).await;
            attempts.push(attempt);
        }
        self.notify(Notice::Exhausted(job.location.job_id)).await;
        Hearing::Exhausted(attempts)
    }
}

trait CopyingText {
    async fn copy(&self, program: Program, ready: Ready) -> Handing;
    async fn delivery_failed(&self, id: JobId, delivery: Delivery, failing: Failing);
}
impl CopyingText for Effects {
    async fn copy(&self, program: Program, ready: Ready) -> Handing {
        let sent = self
            .emission
            .send(Emission::Copy(Copy_Data {
                program,
                text: ready.text,
            }))
            .await;
        let failing = match sent {
            Ok(Outcome::Copying(Copying_Data::Copied)) => {
                self.notify(Notice::Copied(ready.job_id)).await;
                return Handing::Copied;
            }
            Ok(Outcome::Copying(Copying_Data::Failure(failure))) => Failing::Failure(failure),
            Err(unobserved) => Failing::Unobserved(unobserved),
            Ok(_) => Failing::Unobserved(Unobserved::Unanswered),
        };
        self.delivery_failed(ready.job_id, Delivery::Copied, failing)
            .await;
        Handing::Kept
    }
    async fn delivery_failed(&self, id: JobId, delivery: Delivery, failing: Failing) {
        self.notify(Notice::DeliveryFailed(DeliveryFailed_Data {
            job_id: id,
            delivery,
            failing,
        }))
        .await;
    }
}
impl HandingText for Effects {
    async fn hand(&self, work: HandingWork) -> Handing {
        let _exclusive = self.desktop.lock().await;
        let HandingWork {
            ready,
            paste_program,
            copy_program,
        } = work;
        match ready.delivery {
            Delivery::Kept => Handing::Kept,
            Delivery::Copied => self.copy(copy_program, ready).await,
            Delivery::Pasted => {
                let sent = self
                    .emission
                    .send(Emission::Paste(Paste_Data {
                        program: paste_program,
                        text: ready.text.clone(),
                    }))
                    .await;
                let failing = match sent {
                    Ok(Outcome::Pasting(Pasting_Data::Pasted)) => return Handing::Pasted,
                    Ok(Outcome::Pasting(Pasting_Data::Failure(failure))) => {
                        Failing::Failure(failure)
                    }
                    Err(unobserved) => Failing::Unobserved(unobserved),
                    Ok(_) => Failing::Unobserved(Unobserved::Unanswered),
                };
                let uncertain = matches!(failing, Failing::Unobserved(Unobserved::Unanswered));
                self.delivery_failed(ready.job_id.clone(), Delivery::Pasted, failing)
                    .await;
                if uncertain {
                    Handing::Kept
                } else {
                    self.copy(copy_program, ready).await
                }
            }
        }
    }
}
