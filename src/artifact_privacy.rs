use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub(crate) const OWNER_PRIVATE_DIRECTORY_MODE: u32 = 0o700;
pub(crate) const OWNER_PRIVATE_FILE_MODE: u32 = 0o600;

pub(crate) struct OwnerPrivateDirectory {
    path: PathBuf,
}

impl OwnerPrivateDirectory {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub(crate) fn ensure(&self) -> io::Result<()> {
        let missing_directories = self.missing_directories();
        fs::DirBuilder::new()
            .recursive(true)
            .mode(OWNER_PRIVATE_DIRECTORY_MODE)
            .create(&self.path)?;
        for directory in missing_directories.iter().rev() {
            fs::set_permissions(
                directory,
                fs::Permissions::from_mode(OWNER_PRIVATE_DIRECTORY_MODE),
            )?;
        }
        fs::set_permissions(
            &self.path,
            fs::Permissions::from_mode(OWNER_PRIVATE_DIRECTORY_MODE),
        )?;
        Ok(())
    }

    fn missing_directories(&self) -> Vec<PathBuf> {
        let mut directories = Vec::new();
        let mut candidate = self.path.as_path();
        loop {
            if candidate.exists() {
                break;
            }
            directories.push(candidate.to_path_buf());
            let Some(parent) = candidate.parent() else {
                break;
            };
            if parent == candidate {
                break;
            }
            candidate = parent;
        }
        directories
    }
}

pub(crate) struct OwnerPrivateFile {
    path: PathBuf,
}

impl OwnerPrivateFile {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub(crate) fn create_new_read_write(&self) -> io::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(OWNER_PRIVATE_FILE_MODE)
            .open(&self.path)?;
        file.set_permissions(fs::Permissions::from_mode(OWNER_PRIVATE_FILE_MODE))?;
        Ok(file)
    }

    pub(crate) fn create_truncated_write(&self) -> io::Result<File> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(OWNER_PRIVATE_FILE_MODE)
            .open(&self.path)?;
        file.set_permissions(fs::Permissions::from_mode(OWNER_PRIVATE_FILE_MODE))?;
        Ok(file)
    }
}
