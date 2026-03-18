use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::jobs::executors::flush::FlushParams;
use crate::jobs::JobsManager;
use kalamdb_commons::TableType;
use kalamdb_system::JobType;
use std::sync::Arc;

/// Periodic scheduler that checks for tables with pending (unflushed) writes
/// and creates flush jobs for them.
///
/// Modelled after [`StreamEvictionScheduler`] — called from the job loop on a
/// configurable interval (`flush.check_interval_seconds`, default 60 s).
pub struct FlushScheduler;

impl FlushScheduler {
    /// Scan the pending-write manifest index and create flush jobs for every
    /// eligible table that has buffered data in RocksDB.
    ///
    /// * System tables are skipped (cannot be flushed).
    /// * Stream tables are skipped (not yet implemented in FlushExecutor).
    /// * Idempotency keys prevent duplicate concurrent flush jobs.
    pub async fn check_and_schedule(
        app_context: &Arc<AppContext>,
        jobs_manager: &JobsManager,
    ) -> Result<(), KalamDbError> {
        let manifest_service = app_context.manifest_service();

        let pending_iter = manifest_service
            .pending_manifest_ids_iter()
            .map_err(|e| KalamDbError::Other(format!("Pending manifest scan failed: {}", e)))?;

        let schema_registry = app_context.schema_registry();

        let mut tables_checked: u32 = 0;
        let mut jobs_created: u32 = 0;

        for manifest_id_result in pending_iter {
            let manifest_id = match manifest_id_result {
                Ok(id) => id,
                Err(e) => {
                    log::warn!("FlushScheduler: failed to read pending manifest entry: {}", e);
                    continue;
                },
            };

            let table_id = manifest_id.table_id().clone();

            // Look up the table definition to determine its type
            let table_def = match schema_registry.get_table_if_exists(&table_id) {
                Ok(Some(def)) => def,
                Ok(None) => {
                    // Table dropped after pending write was recorded — skip
                    continue;
                },
                Err(e) => {
                    log::warn!(
                        "FlushScheduler: failed to look up table {}: {}",
                        table_id,
                        e
                    );
                    continue;
                },
            };

            // Only flush User and Shared tables
            match table_def.table_type {
                TableType::User | TableType::Shared => {},
                TableType::System | TableType::Stream => continue,
            }

            tables_checked += 1;

            let params = FlushParams {
                table_id: table_id.clone(),
                table_type: table_def.table_type,
                flush_threshold: None,
            };

            // Hourly idempotency key prevents duplicate flush jobs
            let now = chrono::Utc::now();
            let date_key = now.format("%Y-%m-%d-%H").to_string();
            let idempotency_key = format!("FL:{}:{}", table_id, date_key);

            match jobs_manager
                .create_job_typed(JobType::Flush, params, Some(idempotency_key), None)
                .await
            {
                Ok(job_id) => {
                    jobs_created += 1;
                    log::debug!(
                        "FlushScheduler: created flush job {} for {}",
                        job_id.as_str(),
                        table_id
                    );
                },
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.contains("already running")
                        || err_msg.contains("already exists")
                    {
                        log::trace!(
                            "FlushScheduler: flush job for {} already exists (idempotent)",
                            table_id
                        );
                    } else if err_msg.contains("pre-validation returned false") {
                        log::trace!(
                            "FlushScheduler: flush job for {} skipped (no data to flush)",
                            table_id
                        );
                    } else {
                        log::warn!(
                            "FlushScheduler: failed to create flush job for {}: {}",
                            table_id,
                            e
                        );
                    }
                },
            }
        }

        if tables_checked > 0 {
            log::trace!(
                "FlushScheduler: checked {} table(s), created {} flush job(s)",
                tables_checked,
                jobs_created
            );
        } else {
            log::trace!("FlushScheduler: no tables with pending writes found");
        }

        Ok(())
    }
}
