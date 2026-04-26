//! File storage service for FILE datatype.
//!
//! Handles file finalization (staging → permanent) and cleanup operations.
//! Uses StorageRegistry to get the correct storage per table.

use std::{fs, sync::Arc};

use bytes::Bytes;
use kalamdb_commons::{
    ids::SnowflakeGenerator,
    models::{ids::StorageId, TableId, UserId},
    schemas::TableType,
};
use kalamdb_system::{FileRef, FileSubfolderState};

use crate::{
    error::{FilestoreError, Result},
    files::staging::{StagedFile, StagingManager},
    registry::{StorageCached, StorageRegistry},
};

/// File storage service for managing FILE column uploads.
///
/// Routes file operations to the correct storage backend based on
/// table configuration via StorageRegistry.
pub struct FileStorageService {
    /// Storage registry for looking up table storages
    storage_registry: Arc<StorageRegistry>,

    /// Staging manager for temp files
    staging: StagingManager,

    /// Snowflake ID generator for unique file IDs
    snowflake: SnowflakeGenerator,

    /// Maximum files per subfolder
    max_files_per_folder: u32,

    /// Maximum file size in bytes
    max_file_size: usize,

    /// Maximum files per request
    max_files_per_request: usize,

    /// Allowed MIME types (empty = allow all)
    allowed_mime_types: Vec<String>,
}

impl FileStorageService {
    /// Create a new file storage service.
    pub fn new(
        storage_registry: Arc<StorageRegistry>,
        staging_path: impl AsRef<std::path::Path>,
        max_files_per_folder: u32,
        max_file_size: usize,
        max_files_per_request: usize,
        allowed_mime_types: Vec<String>,
    ) -> Self {
        Self {
            storage_registry,
            staging: StagingManager::new(staging_path),
            snowflake: SnowflakeGenerator::new(0), // Worker ID 0, can be configured
            max_files_per_folder,
            max_file_size,
            max_files_per_request,
            allowed_mime_types,
        }
    }

    /// Get the storage for a table by its storage ID.
    fn get_storage(&self, storage_id: &StorageId) -> Result<Arc<StorageCached>> {
        self.storage_registry
            .get_cached(storage_id)?
            .ok_or_else(|| FilestoreError::Other(format!("Storage '{}' not found", storage_id)))
    }

    /// Get max files per request limit.
    pub fn max_files_per_request(&self) -> usize {
        self.max_files_per_request
    }

    /// Get max file size limit.
    pub fn max_file_size(&self) -> usize {
        self.max_file_size
    }

    /// Create a staging directory for a request.
    pub fn create_staging_dir(
        &self,
        request_id: &str,
        user_id: &kalamdb_commons::UserId,
    ) -> Result<std::path::PathBuf> {
        self.staging.create_request_dir(request_id, user_id)
    }

    /// Stage a file upload.
    pub fn stage_file(
        &self,
        request_dir: &std::path::Path,
        part_name: &str,
        original_name: &str,
        data: Bytes,
        provided_mime: Option<&str>,
    ) -> Result<StagedFile> {
        // Validate size
        if data.len() > self.max_file_size {
            return Err(FilestoreError::Other(format!(
                "File '{}' exceeds maximum size of {} bytes",
                original_name, self.max_file_size
            )));
        }

        self.staging
            .stage_file(request_dir, part_name, original_name, data, provided_mime)
    }

    /// Finalize a staged file to permanent storage.
    ///
    /// Returns a FileRef that can be stored in the database.
    pub async fn finalize_file(
        &self,
        staged: &StagedFile,
        storage_id: &StorageId,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        subfolder_state: &mut FileSubfolderState,
        shard_id: Option<u32>,
    ) -> Result<FileRef> {
        // Validate MIME type
        if !self.allowed_mime_types.is_empty() {
            let allowed = self.allowed_mime_types.iter().any(|allowed| {
                if allowed.ends_with("/*") {
                    let prefix = allowed.trim_end_matches("/*");
                    staged.mime_type.starts_with(prefix)
                } else {
                    &staged.mime_type == allowed
                }
            });
            if !allowed {
                return Err(FilestoreError::Other(format!(
                    "MIME type '{}' not allowed for file '{}'",
                    staged.mime_type, staged.original_name
                )));
            }
        }

        // Generate unique file ID
        let file_id = self
            .snowflake
            .next_id()
            .map_err(|e| FilestoreError::Other(format!("Failed to generate file ID: {}", e)))?
            .to_string();

        // Allocate subfolder
        let subfolder = subfolder_state.allocate_file(self.max_files_per_folder);

        // Create FileRef
        let file_ref = if let Some(shard) = shard_id {
            FileRef::with_shard(
                file_id.clone(),
                subfolder.clone(),
                staged.original_name.clone(),
                staged.size,
                staged.mime_type.clone(),
                staged.sha256.clone(),
                shard,
            )
        } else {
            FileRef::new(
                file_id.clone(),
                subfolder.clone(),
                staged.original_name.clone(),
                staged.size,
                staged.mime_type.clone(),
                staged.sha256.clone(),
            )
        };

        // Read staged file content
        let content = fs::read(&staged.path)
            .map_err(|e| FilestoreError::Other(format!("Failed to read staged file: {}", e)))?;

        // Build destination path
        let relative_path = file_ref.relative_path();

        // Get storage from registry and write to permanent storage
        let storage = self.get_storage(storage_id)?;
        storage
            .put(table_type, table_id, user_id, &relative_path, Bytes::from(content))
            .await?;

        log::info!(
            "Finalized file: table={}, storage={}, path={}, size={}, mime={}",
            table_id,
            storage_id,
            relative_path,
            staged.size,
            staged.mime_type
        );

        Ok(file_ref)
    }

    /// Delete a file from storage.
    pub async fn delete_file(
        &self,
        file_ref: &FileRef,
        storage_id: &StorageId,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<()> {
        let relative_path = file_ref.relative_path();

        let storage = self.get_storage(storage_id)?;
        storage.delete(table_type, table_id, user_id, &relative_path).await?;

        log::info!(
            "Deleted file: table={}, storage={}, path={}",
            table_id,
            storage_id,
            relative_path
        );

        Ok(())
    }

    /// Get file content for download.
    pub async fn get_file(
        &self,
        file_ref: &FileRef,
        storage_id: &StorageId,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Bytes> {
        let relative_path = file_ref.relative_path();
        let storage = self.get_storage(storage_id)?;
        let result = storage.get(table_type, table_id, user_id, &relative_path).await?;
        Ok(result.data)
    }

    /// Get file content for download by relative path.
    ///
    /// Used for download endpoint where we have subfolder and stored filename directly.
    pub async fn get_file_by_path(
        &self,
        storage_id: &StorageId,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
        relative_path: &str,
    ) -> Result<Bytes> {
        let storage = self.get_storage(storage_id)?;
        let result = storage.get(table_type, table_id, user_id, relative_path).await?;
        Ok(result.data)
    }

    /// Cleanup a staging directory.
    pub fn cleanup_staging(&self, request_dir: &std::path::Path) -> Result<()> {
        self.staging.cleanup_request_dir(request_dir)
    }

    /// Cleanup stale staging directories.
    pub fn cleanup_stale_staging(&self, max_age_secs: u64) -> Result<usize> {
        self.staging.cleanup_stale_directories(max_age_secs)
    }

    /// Delete multiple files (for row deletion).
    pub async fn delete_files(
        &self,
        file_refs: &[FileRef],
        storage_id: &StorageId,
        table_type: TableType,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Vec<Result<()>> {
        let mut results = Vec::with_capacity(file_refs.len());
        for file_ref in file_refs {
            results
                .push(self.delete_file(file_ref, storage_id, table_type, table_id, user_id).await);
        }
        results
    }
}

// Tests moved to integration tests since they require a full StorageRegistry
// with StoragesTableProvider backed by RocksDB.
