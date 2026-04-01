//! User Export Job Executor
//!
//! Exports all user tables for a specific user across all namespaces by:
//! 1. Triggering a flush job for every user table (ensures all buffered RocksDB
//!    writes are persisted as Parquet files).
//! 2. Waiting for all flush jobs to reach a terminal state.
//! 3. Reading the raw Parquet files from `StorageCached` and packaging them
//!    into a ZIP archive at `{data_path}/exports/{user_id}/{export_id}.zip`.
//!
//! ## Parameters Format
//! ```json
//! {
//!   "user_id": "user-123",
//!   "export_id": "UE-20260101-abcdef"
//! }
//! ```

use crate::executors::flush::FlushParams;
use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use crate::AppContextJobsExt;
use async_trait::async_trait;
use kalamdb_commons::ids::UserTableRowId;
use kalamdb_commons::models::UserId;
use kalamdb_commons::schemas::{TableOptions, TableType};
use kalamdb_commons::{JobId, TableId};
use kalamdb_core::error::KalamDbError;
use kalamdb_core::providers::UserTableProvider;
use kalamdb_store::EntityStore;
use kalamdb_system::JobStatus;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Write};
use std::time::Duration;
use tokio::time::sleep;
use zip::write::SimpleFileOptions;
use zip::ZipWriter;

/// Maximum time to wait for all flush jobs to complete.
const FLUSH_WAIT_TIMEOUT: Duration = Duration::from_secs(5 * 60); // 5 min
/// Poll interval while waiting for flush jobs.
const FLUSH_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Typed parameters for user data export operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserExportParams {
    /// User ID whose data to export
    pub user_id: String,
    /// Unique export identifier (used for filename and lookup)
    pub export_id: String,
}

impl JobParams for UserExportParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.user_id.is_empty() {
            return Err(KalamDbError::InvalidOperation("user_id cannot be empty".to_string()));
        }
        if self.export_id.is_empty() {
            return Err(KalamDbError::InvalidOperation("export_id cannot be empty".to_string()));
        }
        Ok(())
    }
}

/// User Export Executor
///
/// Flush-first approach: triggers flush jobs for every user table, waits for
/// completion, then reads the raw Parquet files and bundles them into a ZIP.
pub struct UserExportExecutor;

impl UserExportExecutor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for UserExportExecutor {
    type Params = UserExportParams;

    fn job_type(&self) -> JobType {
        JobType::UserExport
    }

    fn name(&self) -> &'static str {
        "UserExportExecutor"
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        let params = ctx.params();
        let user_id = UserId::from(params.user_id.as_str());
        let export_id = &params.export_id;

        ctx.log_info(&format!(
            "Starting user data export for user '{}', export_id='{}'",
            user_id, export_id
        ));

        // ── Phase 1: Discover all user tables ───────────────────────────────
        let schema_registry = ctx.app_ctx.schema_registry();
        let all_tables = schema_registry.scan_all_table_definitions()?;
        let user_tables: Vec<_> =
            all_tables.into_iter().filter(|t| t.table_type == TableType::User).collect();

        ctx.log_info(&format!("Found {} user tables to flush and export", user_tables.len()));

        if user_tables.is_empty() {
            return Ok(JobDecision::Completed {
                message: Some("No user tables found to export".to_string()),
            });
        }

        // ── Phase 2: Submit flush jobs for every user table ─────────────────
        let job_manager = ctx.app_ctx.job_manager();
        // Maps JobId → table_id string (for logging) for flushed tables.
        let mut flush_jobs: HashMap<JobId, String> = HashMap::new();
        // Tables whose flush was skipped (nothing to flush) — still copy files.
        let mut tables_with_storage: Vec<_> = Vec::new();

        for table_def in &user_tables {
            let table_id =
                TableId::new(table_def.namespace_id.clone(), table_def.table_name.clone());
            let table_label =
                format!("{}.{}", table_def.namespace_id.as_str(), table_def.table_name.as_str());

            // Extract storage_id; skip non-user table_options (shouldn't happen).
            let storage_id = match &table_def.table_options {
                TableOptions::User(opts) => opts.storage_id.clone(),
                _ => {
                    ctx.log_warn(&format!(
                        "Table {} has unexpected table_options variant, skipping",
                        table_label
                    ));
                    continue;
                },
            };

            // ── Quick check: does this table have any RocksDB data for this user? ──
            // If not, skip the flush job entirely. The Parquet files (if any) will
            // still be copied in Phase 4.
            let has_rocksdb_data = {
                let provider_opt = schema_registry.get_provider(&table_id);
                if let Some(provider_arc) = provider_opt {
                    if let Some(provider) =
                        provider_arc.as_any().downcast_ref::<UserTableProvider>()
                    {
                        let store = provider.store();
                        // Scan with the user-specific prefix to check only this user's data
                        let user_prefix = UserTableRowId::user_prefix(&user_id);
                        let partition = store.partition();
                        store
                            .backend()
                            .scan(&partition, Some(&user_prefix), None, Some(1))
                            .map(|mut iter| iter.next().is_some())
                            .unwrap_or(true) // on error, assume data exists
                    } else {
                        true // can't determine, assume there's data
                    }
                } else {
                    false // no provider, skip flush
                }
            };

            tables_with_storage.push((table_def.clone(), storage_id));

            // Skip flush if no RocksDB data for this user
            if !has_rocksdb_data {
                ctx.log_debug(&format!(
                    "Table {} has no RocksDB data for user {}, skipping flush",
                    table_label, user_id
                ));
                continue;
            }

            let flush_params = FlushParams {
                table_id: table_id.clone(),
                table_type: TableType::User,
                flush_threshold: None,
            };
            let flush_key = format!("export-flush:{}:{}", params.user_id, table_id);

            match job_manager
                .create_job_typed(JobType::Flush, flush_params, Some(flush_key), None)
                .await
            {
                Ok(job_id) => {
                    ctx.log_debug(&format!(
                        "Submitted flush job {} for table {}",
                        job_id, table_label
                    ));
                    flush_jobs.insert(job_id, table_label);
                },
                Err(KalamDbError::IdempotentConflict(_)) => {
                    // A flush job with this key already exists — OK, we'll wait for it.
                    ctx.log_debug(&format!(
                        "Flush job already pending for table {}, will wait",
                        table_label
                    ));
                },
                Err(KalamDbError::Other(ref msg))
                    if msg.contains("pre-validation returned false") =>
                {
                    // Nothing to flush (table is already fully on disk).
                    ctx.log_debug(&format!(
                        "Table {} has nothing to flush, proceeding to copy",
                        table_label
                    ));
                },
                Err(e) => {
                    ctx.log_warn(&format!(
                        "Failed to create flush job for table {}: {}",
                        table_label, e
                    ));
                    // Continue — we will still try to copy whatever Parquet files exist.
                },
            }
        }

        // ── Phase 3: Wait for all flush jobs to reach terminal state ─────────
        if !flush_jobs.is_empty() {
            ctx.log_info(&format!("Waiting for {} flush jobs to complete…", flush_jobs.len()));

            let deadline = tokio::time::Instant::now() + FLUSH_WAIT_TIMEOUT;
            let mut pending: HashMap<JobId, String> = flush_jobs.clone();

            loop {
                if pending.is_empty() {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    ctx.log_warn(&format!(
                        "Timed out waiting for {} flush job(s); proceeding with available data",
                        pending.len()
                    ));
                    break;
                }

                let mut still_pending: HashMap<JobId, String> = HashMap::new();
                for (job_id, label) in &pending {
                    match job_manager.get_job(job_id).await {
                        Ok(Some(job)) => {
                            let terminal = matches!(
                                job.status,
                                JobStatus::Completed
                                    | JobStatus::Failed
                                    | JobStatus::Cancelled
                                    | JobStatus::Skipped
                            );
                            if terminal {
                                ctx.log_debug(&format!(
                                    "Flush job {} for {} finished with status {:?}",
                                    job_id, label, job.status
                                ));
                            } else {
                                still_pending.insert(job_id.clone(), label.clone());
                            }
                        },
                        Ok(None) => {
                            // Job disappeared — treat as done.
                            ctx.log_debug(&format!(
                                "Flush job {} not found (may have been cleaned up), continuing",
                                job_id
                            ));
                        },
                        Err(e) => {
                            ctx.log_warn(&format!("Error polling flush job {}: {}", job_id, e));
                            still_pending.insert(job_id.clone(), label.clone());
                        },
                    }
                }

                pending = still_pending;
                if !pending.is_empty() {
                    sleep(FLUSH_POLL_INTERVAL).await;
                }
            }
        }

        // ── Phase 4: Read raw Parquet files and build ZIP ────────────────────
        ctx.log_info("Reading Parquet files and building ZIP archive…");

        let zip_buffer = Cursor::new(Vec::new());
        let mut zip_writer = ZipWriter::new(zip_buffer);
        let zip_options = SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .compression_level(Some(6));

        let storage_registry = ctx.app_ctx.storage_registry();
        let mut total_files: usize = 0;
        let mut total_bytes: usize = 0;

        for (table_def, storage_id) in &tables_with_storage {
            let table_label =
                format!("{}.{}", table_def.namespace_id.as_str(), table_def.table_name.as_str());
            let table_id =
                TableId::new(table_def.namespace_id.clone(), table_def.table_name.clone());

            let storage_cached = match storage_registry.get_cached(storage_id) {
                Ok(Some(sc)) => sc,
                Ok(None) => {
                    ctx.log_warn(&format!(
                        "Storage backend '{}' not found for table {}, skipping",
                        storage_id, table_label
                    ));
                    continue;
                },
                Err(e) => {
                    ctx.log_warn(&format!(
                        "Failed to get storage for table {}: {}",
                        table_label, e
                    ));
                    continue;
                },
            };

            let files = match storage_cached
                .list_parquet_files(TableType::User, &table_id, Some(&user_id))
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    ctx.log_warn(&format!(
                        "Failed to list Parquet files for table {}: {}",
                        table_label, e
                    ));
                    continue;
                },
            };

            if files.is_empty() {
                ctx.log_debug(&format!("No Parquet files for table {}, skipping", table_label));
                continue;
            }

            for filename in &files {
                let get_result = match storage_cached
                    .get(TableType::User, &table_id, Some(&user_id), filename)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        ctx.log_warn(&format!(
                            "Failed to read '{}' from table {}: {}",
                            filename, table_label, e
                        ));
                        continue;
                    },
                };

                let zip_entry = format!(
                    "{}/{}/{}",
                    table_def.namespace_id.as_str(),
                    table_def.table_name.as_str(),
                    filename
                );

                zip_writer.start_file(&zip_entry, zip_options).map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to add '{}' to ZIP: {}",
                        zip_entry, e
                    ))
                })?;
                zip_writer.write_all(&get_result.data).map_err(|e| {
                    KalamDbError::InvalidOperation(format!(
                        "Failed to write '{}' to ZIP: {}",
                        zip_entry, e
                    ))
                })?;

                total_bytes += get_result.data.len();
                total_files += 1;
            }
        }

        if total_files == 0 {
            return Ok(JobDecision::Completed {
                message: Some("No data found for this user in any table".to_string()),
            });
        }

        // ── Phase 5: Flush ZIP to disk ───────────────────────────────────────
        let finished = zip_writer.finish().map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to finalize ZIP archive: {}", e))
        })?;
        let zip_bytes = finished.into_inner();
        let zip_size = zip_bytes.len();

        let exports_dir = ctx.app_ctx.config().storage.exports_dir();
        let user_exports_dir = exports_dir.join(params.user_id.as_str());
        fs::create_dir_all(&user_exports_dir).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to create exports directory '{}': {}",
                user_exports_dir.display(),
                e
            ))
        })?;

        let zip_filename = format!("{}.zip", export_id);
        let zip_path = user_exports_dir.join(&zip_filename);

        fs::write(&zip_path, &zip_bytes).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to write export ZIP '{}': {}",
                zip_path.display(),
                e
            ))
        })?;

        ctx.log_info(&format!(
            "Export complete: {} Parquet files, {} raw bytes, ZIP {} bytes written to {}",
            total_files,
            total_bytes,
            zip_size,
            zip_path.display()
        ));

        Ok(JobDecision::Completed {
            message: Some(format!(
                "Exported {} Parquet files ({} bytes) to {}",
                total_files, zip_size, zip_filename
            )),
        })
    }

    async fn execute_leader(
        &self,
        ctx: &JobContext<Self::Params>,
    ) -> Result<JobDecision, KalamDbError> {
        self.execute(ctx).await
    }
}
