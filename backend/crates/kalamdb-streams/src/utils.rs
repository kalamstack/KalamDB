use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::error::{Result, StreamLogError};

pub(crate) fn parse_log_window(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_string_lossy();
    let trimmed = file_name.strip_suffix(".log")?;
    trimmed.parse::<u64>().ok()
}

pub(crate) fn read_dirs(path: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    for entry in fs::read_dir(path).map_err(|e| StreamLogError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| StreamLogError::Io(e.to_string()))?;
        let path = entry.path();
        if path.is_dir() {
            dirs.push(path);
        }
    }
    Ok(dirs)
}

pub(crate) fn read_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path).map_err(|e| StreamLogError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| StreamLogError::Io(e.to_string()))?;
        let path = entry.path();
        if path.is_file() {
            files.push(path);
        }
    }
    Ok(files)
}

pub(crate) fn cleanup_empty_dir(path: &Path) {
    if let Ok(mut entries) = fs::read_dir(path) {
        if entries.next().is_none() {
            let _ = fs::remove_dir(path);
        }
    }
}
