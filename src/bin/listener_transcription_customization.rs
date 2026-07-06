use std::{fs, path::PathBuf};

use listener::{Error, Result, TranscriptionCustomizationTextSource};

struct TranscriptionCustomizationCompiler {
    arguments: Vec<String>,
}

impl TranscriptionCustomizationCompiler {
    fn from_environment() -> Self {
        Self {
            arguments: std::env::args().collect(),
        }
    }

    fn run(&self) -> Result<()> {
        let source_path = self.source_path()?;
        let archive_path = self.archive_path()?;
        let source = fs::read_to_string(source_path)?;
        let archive = TranscriptionCustomizationTextSource::new(source)
            .into_customization()
            .to_rkyv_bytes()?;
        if let Some(parent) = archive_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(archive_path, archive)?;
        Ok(())
    }

    fn source_path(&self) -> Result<PathBuf> {
        self.required_path(1, "missing vocabulary terms source path")
    }

    fn archive_path(&self) -> Result<PathBuf> {
        self.required_path(2, "missing output archive path")
    }

    fn required_path(&self, index: usize, message: &'static str) -> Result<PathBuf> {
        self.arguments
            .get(index)
            .map(PathBuf::from)
            .ok_or_else(|| Error::InvalidCommand {
                message: format!(
                    "{message}; usage: listener-transcription-customization <terms.txt> <customization.rkyv>"
                ),
            })
    }
}

fn main() -> Result<()> {
    TranscriptionCustomizationCompiler::from_environment().run()
}
