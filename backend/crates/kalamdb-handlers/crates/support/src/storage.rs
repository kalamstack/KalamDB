use kalamdb_core::error::KalamDbError;

/// Ensure a filesystem directory exists for filesystem-backed storage definitions.
pub fn ensure_filesystem_directory(path: &str) -> Result<(), KalamDbError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(KalamDbError::InvalidOperation(
            "Filesystem storage requires a non-empty base directory".to_string(),
        ));
    }

    std::fs::create_dir_all(trimmed).map_err(|e| {
        KalamDbError::InvalidOperation(format!(
            "Failed to create filesystem storage directory '{}': {}",
            trimmed, e
        ))
    })?;

    Ok(())
}