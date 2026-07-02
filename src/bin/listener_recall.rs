use std::process::ExitCode;

use listener::TranscriptRecall;

fn main() -> ExitCode {
    match TranscriptRecall::from_environment().run() {
        Ok(outcome) => {
            outcome.report();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("listener-recall: {error}");
            ExitCode::FAILURE
        }
    }
}
