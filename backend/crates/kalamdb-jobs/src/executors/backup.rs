//! Backup Job Executor
//!
//! Backs up the entire KalamDB database using RocksDB's native `BackupEngine`
//! for the key-value store, plus a recursive directory copy for Parquet storage,
//! snapshots, streams, and the server.toml configuration file.
//!
//! ## Backup Directory Layout
//! ```text
//! <backup_path>/
//!   rocksdb/      ← RocksDB BackupEngine output (hot, consistent, deduplicated)
//!   storage/      ← Parquet segment files
//!   snapshots/    ← Parquet snapshot files
//!   streams/      ← Stream commit log files
//!   server.toml   ← Server configuration (if present next to the binary)
//! ```
//!
//! ## Parameters Format
//! ```json
//! {
//!   "backup_path": "/backups/kalamdb-20260224"
//! }
//! ```

use std::{fs, path::Path};

use async_trait::async_trait;
use kalamdb_core::error::KalamDbError;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};

use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};

/// Typed parameters for full database backup operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupParams {
    /// Destination directory for the backup (created if it does not exist).
    /// RocksDB native BackupEngine output goes into `<backup_path>/rocksdb/`.
    pub backup_path: String,
}

impl JobParams for BackupParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.backup_path.is_empty() {
            return Err(KalamDbError::InvalidOperation("backup_path cannot be empty".to_string()));
        }
        Ok(())
    }
}

/// Backup Job Executor
///
/// Uses RocksDB's native `BackupEngine` for the key-value store and performs a
/// recursive directory copy for Parquet and stream files.
pub struct BackupExecutor;

impl BackupExecutor {
    pub fn new() -> Self {
        Self
    }

    /// Recursively copy `src` into `dst`, creating `dst` if necessary.
    fn copy_dir(src: &Path, dst: &Path) -> Result<u64, KalamDbError> {
        if !src.exists() {
            return Ok(0);
        }
        fs::create_dir_all(dst).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to create directory '{}': {}",
                dst.display(),
                e
            ))
        })?;
        let mut bytes_copied: u64 = 0;
        for entry in fs::read_dir(src).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to read directory '{}': {}",
                src.display(),
                e
            ))
        })? {
            let entry = entry.map_err(|e| {
                KalamDbError::InvalidOperation(format!("Directory entry error: {}", e))
            })?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if src_path.is_dir() {
                bytes_copied += Self::copy_dir(&src_path, &dst_path)?;
            } else {
                fs::copy(&src_path, &dst_path).map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to copy '{}' to '{}': {}",
                        src_path.display(),
                        dst_path.display(),
                        e
                    ))
                })?;
                bytes_copied += src_path.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
        Ok(bytes_copied)
    }
}

#[async_trait]
impl JobExecutor for BackupExecutor {
    type Params = BackupParams;

    fn job_type(&self) -> JobType {
        JobType::Backup
    }

    fn name(&self) -> &'static str {
        "BackupExecutor"
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        let params = ctx.params();
        let backup_dir = std::path::PathBuf::from(&params.backup_path);

        let config = ctx.app_ctx.config();
        let storage = &config.storage;

        ctx.log_info(&format!("Starting full database backup to '{}'", backup_dir.display()));

        let storage_backend = ctx.app_ctx.storage_backend();
        let rocksdb_backup_dir = backup_dir.join("rocksdb");
        let src_storage = storage.storage_dir();
        let src_snapshots = storage.resolved_snapshots_dir();
        let src_streams = storage.streams_dir();

        let result = tokio::task::spawn_blocking(move || -> Result<u64, KalamDbError> {
            // 1. RocksDB native backup (hot, consistent, with flush)
            storage_backend.backup_to(&rocksdb_backup_dir).map_err(|e| {
                KalamDbError::InvalidOperation(format!("RocksDB backup failed: {}", e))
            })?;

            let mut total_bytes: u64 = 0;

            // 2. Parquet storage files
            total_bytes += Self::copy_dir(&src_storage, &backup_dir.join("storage"))?;

            // 3. Snapshot files
            total_bytes += Self::copy_dir(&src_snapshots, &backup_dir.join("snapshots"))?;

            // 4. Stream commit log files
            total_bytes += Self::copy_dir(&src_streams, &backup_dir.join("streams"))?;

            // 5. server.toml (look next to the binary / CWD)
            let server_toml = Path::new("server.toml");
            if server_toml.exists() {
                fs::copy(server_toml, backup_dir.join("server.toml")).map_err(|e| {
                    KalamDbError::InvalidOperation(format!("Failed to copy server.toml: {}", e))
                })?;
                total_bytes += server_toml.metadata().map(|m| m.len()).unwrap_or(0);
            }

            Ok(total_bytes)
        })
        .await
        .map_err(|e| KalamDbError::InvalidOperation(format!("Backup task panicked: {}", e)))?;

        match result {
            Ok(total_bytes) => {
                let size_mb = total_bytes as f64 / (1024.0 * 1024.0);
                ctx.log_info(&format!(
                    "Backup completed: '{}' ({:.2} MB written)",
                    params.backup_path, size_mb
                ));
                Ok(JobDecision::Completed {
                    message: Some(format!(
                        "Backup completed: '{}' ({:.2} MB)",
                        params.backup_path, size_mb
                    )),
                })
            },
            Err(e) => {
                ctx.log_error(&format!("Backup failed: {}", e));
                Ok(JobDecision::Failed {
                    message: format!("Backup failed: {}", e),
                    exception_trace: Some(format!("{:?}", e)),
                })
            },
        }
    }

    async fn execute_leader(
        &self,
        ctx: &JobContext<Self::Params>,
    ) -> Result<JobDecision, KalamDbError> {
        self.execute(ctx).await
    }
}

impl Default for BackupExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_properties() {
        let executor = BackupExecutor::new();
        assert_eq!(executor.job_type(), JobType::Backup);
        assert_eq!(executor.name(), "BackupExecutor");
    }
}
