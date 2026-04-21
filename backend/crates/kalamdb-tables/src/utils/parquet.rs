use crate::error::KalamDbError;
use crate::manifest::ManifestAccessPlanner;
use crate::utils::core::TableProviderCore;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr;
use kalamdb_commons::models::schemas::TableType;
use kalamdb_commons::models::UserId;
use kalamdb_commons::TableId;

/// Async helper for loading Parquet batches via ManifestAccessPlanner.
///
/// Uses async file I/O to avoid blocking the tokio runtime.
pub(crate) async fn scan_parquet_files_as_batch_async(
    core: &TableProviderCore,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<&UserId>,
    schema: SchemaRef,
    filter: Option<&Expr>,
    columns: Option<&[String]>,
) -> Result<RecordBatch, KalamDbError> {
    let scope_label = user_id
        .map(|uid| format!("user={}", uid.as_str()))
        .unwrap_or_else(|| format!("scope={}", table_type.as_str()));

    let manifest_service = core.services.manifest_service.clone();
    log::trace!(
        "[PARQUET_SCAN_ASYNC] About to get_or_load manifest: table={} {}",
        table_id,
        scope_label
    );
    let cache_result = manifest_service.get_or_load_async(table_id, user_id).await;
    let mut manifest_entry = None;
    let mut use_degraded_mode = false;

    // Fast path: if manifest loaded successfully and has no segments,
    // skip the entire cold path (storage registry, planner, file I/O)
    if let Ok(Some(entry)) = &cache_result {
        if entry.manifest.segments.is_empty() {
            log::trace!(
                "[PARQUET_SCAN_ASYNC] Manifest empty, skipping cold path: table={} {}",
                table_id,
                scope_label
            );
            return Ok(RecordBatch::new_empty(schema));
        }
    }

    match &cache_result {
        Ok(Some(entry)) => {
            log::trace!(
                "[PARQUET_SCAN_ASYNC] Got manifest: table={} {} segments={} sync_state={:?}",
                table_id,
                scope_label,
                entry.manifest.segments.len(),
                entry.sync_state
            );
            // Validate manifest using service
            if let Err(e) = manifest_service.validate_manifest(&entry.manifest) {
                log::warn!(
                    "⚠️  [MANIFEST CORRUPTION] table={} {} error={} | Triggering rebuild",
                    table_id,
                    scope_label,
                    e
                );
                if let Err(mark_err) = manifest_service.mark_as_stale(table_id, user_id) {
                    log::warn!(
                        "⚠️  Failed to mark manifest as stale: table={} {} error={}",
                        table_id,
                        scope_label,
                        mark_err
                    );
                }
                use_degraded_mode = true;
                let uid = user_id.cloned();
                let scope_for_spawn = scope_label.clone();
                let table_id_for_spawn = table_id.clone();
                let manifest_service_clone = core.services.manifest_service.clone();
                tokio::task::spawn_blocking(move || {
                    log::info!(
                        "🔧 [MANIFEST REBUILD STARTED] table={} {}",
                        table_id_for_spawn,
                        scope_for_spawn
                    );
                    match manifest_service_clone.rebuild_manifest(&table_id_for_spawn, uid.as_ref())
                    {
                        Ok(_) => {
                            log::info!(
                                "✅ [MANIFEST REBUILD COMPLETED] table={} {}",
                                table_id_for_spawn,
                                scope_for_spawn
                            );
                        },
                        Err(e) => {
                            log::error!(
                                "❌ [MANIFEST REBUILD FAILED] table={} {} error={}",
                                table_id_for_spawn,
                                scope_for_spawn,
                                e
                            );
                        },
                    }
                });
            } else {
                manifest_entry = Some(entry.clone());
            }
        },
        Ok(None) => {
            log::trace!(
                "[PARQUET_SCAN_ASYNC] Manifest cache MISS | table={} | {} | fallback=directory_scan",
                table_id,
                scope_label
            );
            use_degraded_mode = true;
        },
        Err(e) => {
            log::warn!(
                "⚠️  Manifest cache ERROR | table={} | {} | error={} | fallback=directory_scan",
                table_id,
                scope_label,
                e
            );
            use_degraded_mode = true;
        },
    }

    // Resolve storage only when the manifest cannot already prove the cold path is empty.
    let storage_id = core
        .schema_registry()
        .get_storage_id(table_id)
        .map_err(|_| KalamDbError::TableNotFound(format!("Table not found: {}", table_id)))?;

    let storage_registry = core.services.storage_registry.as_ref().ok_or_else(|| {
        KalamDbError::InvalidOperation("Storage registry not configured".to_string())
    })?;
    let storage_cached = storage_registry.get_cached(&storage_id)?.ok_or_else(|| {
        KalamDbError::InvalidOperation(format!("Storage '{}' not found", storage_id.as_str()))
    })?;

    let planner = ManifestAccessPlanner::new();
    let (min_seq, max_seq) = filter
        .map(crate::utils::row_utils::extract_seq_bounds_from_filter)
        .unwrap_or((None, None));
    let seq_range = match (min_seq, max_seq) {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    };

    let (combined, (total_batches, skipped, scanned)) = planner
        .scan_parquet_files_async(
            manifest_entry.as_ref().map(|entry| &entry.manifest),
            storage_cached,
            table_type,
            table_id,
            user_id,
            seq_range,
            use_degraded_mode,
            schema.clone(),
            core.services.schema_registry.as_ref(),
            columns,
        )
        .await?;

    log::trace!(
        "[PARQUET_SCAN_ASYNC] Scan complete: table={} {} total_batches={} skipped={} scanned={} rows={} use_degraded_mode={}",
        table_id,
        scope_label,
        total_batches,
        skipped,
        scanned,
        combined.num_rows(),
        use_degraded_mode
    );

    if total_batches > 0 {
        log::trace!(
            "[Manifest Pruning] table={} {} batches_total={} skipped={} scanned={} rows={}",
            table_id,
            scope_label,
            total_batches,
            skipped,
            scanned,
            combined.num_rows()
        );
    }

    Ok(combined)
}
