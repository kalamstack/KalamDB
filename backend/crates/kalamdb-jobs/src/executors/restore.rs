//! Restore Job Executor
//!
//! Restores the entire KalamDB database from a backup directory created by the
//! backup job. Uses RocksDB's native `BackupEngine` for the key-value store and
//! a recursive directory copy for Parquet and stream files.
//!
//! ## Backup Directory Layout Expected
//! ```text
//! <backup_path>/
//!   rocksdb/      ← RocksDB BackupEngine output
//!   storage/      ← Parquet segment files
//!   snapshots/    ← Parquet snapshot files
//!   streams/      ← Stream commit log files
//!   server.toml   ← Server configuration (optional)
//! ```
//!
//! ## Parameters Format
//! ```json
//! {
//!   "backup_path": "/backups/kalamdb-20260224"
//! }
//! ```
//!
//! ## IMPORTANT
//! Restore requires a server restart after completion to reload the restored data.

use kalamdb_core::error::KalamDbError;
use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use async_trait::async_trait;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Typed parameters for full database restore operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreParams {
    /// Source backup directory (created by BACKUP DATABASE TO '<path>')
    pub backup_path: String,
}

impl JobParams for RestoreParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.backup_path.is_empty() {
            return Err(KalamDbError::InvalidOperation("backup_path cannot be empty".to_string()));
        }
        Ok(())
    }
}

/// Restore Job Executor
///
/// Uses RocksDB's native `BackupEngine` to restore the key-value store and
/// performs a recursive directory copy for Parquet and stream files.
pub struct RestoreExecutor;

impl RestoreExecutor {
    pub fn new() -> Self {
        Self
    }

    /// Recursively copy `src` into `dst`, creating `dst` if needed.
    /// Overwrites existing files. Skips if `src` doesn't exist.
    fn copy_dir(src: &Path, dst: &Path) -> Result<(), KalamDbError> {
        if !src.exists() {
            return Ok(());
        }
        fs::create_dir_all(dst).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to create directory '{}': {}",
                dst.display(),
                e
            ))
        })?;
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
                Self::copy_dir(&src_path, &dst_path)?;
            } else {
                fs::copy(&src_path, &dst_path).map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to copy '{}' to '{}': {}",
                        src_path.display(),
                        dst_path.display(),
                        e
                    ))
                })?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl JobExecutor for RestoreExecutor {
    type Params = RestoreParams;

    fn job_type(&self) -> JobType {
        JobType::Restore
    }

    fn name(&self) -> &'static str {
        "RestoreExecutor"
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        let params = ctx.params();
        let backup_dir = std::path::PathBuf::from(&params.backup_path);

        let config = ctx.app_ctx.config();
        let storage = &config.storage;

        ctx.log_info(&format!("Starting full database restore from '{}'", backup_dir.display()));

        let storage_backend = ctx.app_ctx.storage_backend();
        let rocksdb_backup_dir = backup_dir.join("rocksdb");

        let dst_storage = storage.storage_dir();
        let dst_snapshots = storage.resolved_snapshots_dir();
        let dst_streams = storage.streams_dir();

        let result = tokio::task::spawn_blocking(move || -> Result<(), KalamDbError> {
            // 1. RocksDB native restore (overwrites current data)
            if rocksdb_backup_dir.exists() {
                storage_backend.restore_from(&rocksdb_backup_dir).map_err(|e| {
                    KalamDbError::InvalidOperation(format!("RocksDB restore failed: {}", e))
                })?;
            }

            // 2. Parquet storage files
            Self::copy_dir(&backup_dir.join("storage"), &dst_storage)?;

            // 3. Snapshot files
            Self::copy_dir(&backup_dir.join("snapshots"), &dst_snapshots)?;

            // 4. Stream commit log files
            Self::copy_dir(&backup_dir.join("streams"), &dst_streams)?;

            // 5. server.toml (optional)
            let backup_toml = backup_dir.join("server.toml");
            if backup_toml.exists() {
                fs::copy(&backup_toml, Path::new("server.toml")).map_err(|e| {
                    KalamDbError::InvalidOperation(format!("Failed to restore server.toml: {}", e))
                })?;
            }

            Ok(())
        })
        .await
        .map_err(|e| KalamDbError::InvalidOperation(format!("Restore task panicked: {}", e)))?;

        match result {
            Ok(()) => {
                ctx.log_info(&format!(
                    "Restore staged from '{}'. RocksDB restore is pending server restart.",
                    params.backup_path
                ));
                Ok(JobDecision::Completed {
                    message: Some(format!(
                        "Restore staged from '{}'. Restart server to activate restored data.",
                        params.backup_path
                    )),
                })
            },
            Err(e) => {
                ctx.log_error(&format!("Restore failed: {}", e));
                Ok(JobDecision::Failed {
                    message: format!("Restore failed: {}", e),
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

impl Default for RestoreExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_properties() {
        let executor = RestoreExecutor::new();
        assert_eq!(executor.job_type(), JobType::Restore);
        assert_eq!(executor.name(), "RestoreExecutor");
    }
}
