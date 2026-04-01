use crate::JobsManager;
use kalamdb_commons::models::{schemas::TableOptions, TableId};
use kalamdb_commons::TableType;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_system::JobType;
use std::sync::Arc;

/// Scheduler for stream table eviction jobs
pub struct StreamEvictionScheduler;

impl StreamEvictionScheduler {
    /// Check for stream tables requiring TTL eviction
    ///
    /// Scans system.tables for STREAM tables with ttl_seconds > 0 and creates
    /// StreamEviction jobs with idempotency keys to prevent duplicates.
    pub async fn check_and_schedule(
        app_context: &Arc<AppContext>,
        jobs_manager: &JobsManager,
    ) -> Result<(), KalamDbError> {
        let tables = app_context.schema_registry().scan_all_table_definitions().map_err(|e| {
            KalamDbError::io_message(format!("Failed to scan table definitions: {}", e))
        })?;

        let mut stream_tables_found = 0;
        let mut jobs_created = 0;

        //Loop over the tables
        for table in tables.iter() {
            // Only process STREAM tables
            if table.table_type != TableType::Stream {
                continue;
            }
            // Stream tables store TTL in the StreamTableOptions.ttl_seconds field, not cache TTL.
            // TableOptions::cache_ttl_seconds() returns None for Stream tables so reading that
            // value will incorrectly indicate there is no TTL configured.
            let ttl_seconds = match &table.table_options {
                TableOptions::Stream(opts) => opts.ttl_seconds,
                _ => 0,
            };
            if ttl_seconds == 0 {
                continue;
            }

            stream_tables_found += 1;

            let table_id = TableId::new(table.namespace_id.clone(), table.table_name.clone());

            // Create eviction job with typed parameters
            let params = crate::executors::stream_eviction::StreamEvictionParams {
                table_id: table_id.clone(),
                table_type: TableType::Stream,
                ttl_seconds,
                batch_size: 10000,
            };

            // Generate idempotency key (hourly granularity)
            let now = chrono::Utc::now();
            let date_key = now.format("%Y-%m-%d-%H").to_string();
            let idempotency_key = format!("SE:{}:{}", table_id, date_key);

            // Create eviction job
            match jobs_manager
                .create_job_typed(JobType::StreamEviction, params, Some(idempotency_key), None)
                .await
            {
                Ok(job_id) => {
                    jobs_created += 1;
                    log::debug!(
                        "Created stream eviction job {} for {} (ttl: {}s)",
                        job_id.as_str(),
                        table_id,
                        ttl_seconds
                    );
                },
                Err(e) => {
                    let err_msg = e.to_string();
                    // Idempotency errors are expected (job already exists for this hour)
                    if err_msg.contains("already running") || err_msg.contains("already exists") {
                        log::trace!(
                            "Stream eviction job for {} already exists (idempotent)",
                            table_id
                        );
                    } else if err_msg.contains("pre-validation returned false") {
                        // Pre-validation skipped job creation (nothing to evict)
                        log::trace!(
                            "Stream eviction job for {} skipped (no expired rows)",
                            table_id
                        );
                    } else {
                        log::warn!("Failed to create stream eviction job for {}: {}", table_id, e);
                    }
                },
            }
        }

        if stream_tables_found > 0 {
            log::trace!(
                "Stream eviction check: found {} stream tables, created {} new eviction jobs",
                stream_tables_found,
                jobs_created
            );
        } else {
            log::trace!("Stream eviction check: no stream tables with TTL found");
        }

        Ok(())
    }
}
