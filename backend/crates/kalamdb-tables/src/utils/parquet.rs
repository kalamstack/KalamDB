use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    logical_expr::Expr,
};
use kalamdb_commons::{
    constants::SystemColumnNames,
    models::{
        schemas::{TableDefinition, TableType},
        UserId,
    },
    TableId,
};

use crate::{
    error::KalamDbError,
    manifest::{ColdScanPruning, ManifestAccessPlanner, ParquetScanStats},
    utils::{
        core::TableProviderCore,
        row_utils::{
            extract_equality_predicates, extract_seq_bounds_from_filter, scalar_to_prune_string,
        },
    },
};

#[derive(Debug)]
pub(crate) struct ParquetScanResult {
    pub batch: RecordBatch,
    pub stats: ParquetScanStats,
}

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
    Ok(scan_parquet_files_as_result_async(
        core, table_id, table_type, user_id, schema, filter, columns,
    )
    .await?
    .batch)
}

pub(crate) async fn scan_parquet_files_as_result_async(
    core: &TableProviderCore,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<&UserId>,
    schema: SchemaRef,
    filter: Option<&Expr>,
    columns: Option<&[String]>,
) -> Result<ParquetScanResult, KalamDbError> {
    scan_parquet_files_internal_async(
        core, table_id, table_type, user_id, schema, filter, columns, false,
    )
    .await
}

pub(crate) async fn scan_parquet_files_with_stats_async(
    core: &TableProviderCore,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<&UserId>,
    schema: SchemaRef,
    filter: Option<&Expr>,
    columns: Option<&[String]>,
) -> Result<ParquetScanResult, KalamDbError> {
    scan_parquet_files_internal_async(
        core, table_id, table_type, user_id, schema, filter, columns, true,
    )
    .await
}

async fn scan_parquet_files_internal_async(
    core: &TableProviderCore,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<&UserId>,
    schema: SchemaRef,
    filter: Option<&Expr>,
    columns: Option<&[String]>,
    record_visited_files: bool,
) -> Result<ParquetScanResult, KalamDbError> {
    let cache_result = core.services.manifest_service.get_or_load_async(table_id, user_id).await;

    // Fast path: if manifest loaded successfully and has no segments,
    // skip the entire cold path (storage registry, planner, file I/O)
    if let Ok(Some(entry)) = &cache_result {
        if entry.manifest.segments.is_empty() {
            return Ok(ParquetScanResult {
                batch: RecordBatch::new_empty(schema),
                stats: ParquetScanStats::default(),
            });
        }
    }

    let scope_label = user_id
        .map(|uid| format!("user={}", uid.as_str()))
        .unwrap_or_else(|| format!("scope={}", table_type.as_str()));
    let mut manifest_entry = None;
    let mut use_degraded_mode = false;

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
            if let Err(e) = core.services.manifest_service.validate_manifest(&entry.manifest) {
                log::warn!(
                    "⚠️  [MANIFEST CORRUPTION] table={} {} error={} | Triggering rebuild",
                    table_id,
                    scope_label,
                    e
                );
                if let Err(mark_err) =
                    core.services.manifest_service.mark_as_stale(table_id, user_id)
                {
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
                "[PARQUET_SCAN_ASYNC] Manifest cache MISS | table={} | {} | \
                 fallback=directory_scan",
                table_id,
                scope_label
            );
            use_degraded_mode = true;
        },
        Err(kalamdb_store::StorageError::SerializationError(e)) => {
            return Err(KalamDbError::InvalidOperation(format!(
                "Failed to load manifest for {} {}: {}",
                table_id, scope_label, e
            )));
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
    let table_def = core.schema_registry().get_table_if_exists(table_id).ok().flatten();
    let pruning = cold_scan_pruning(filter, table_def.as_deref());

    let (combined, stats) = planner
        .scan_parquet_files_async(
            manifest_entry.as_ref().map(|entry| &entry.manifest),
            storage_cached,
            table_type,
            table_id,
            user_id,
            pruning,
            use_degraded_mode,
            schema.clone(),
            core.services.schema_registry.as_ref(),
            columns,
            record_visited_files,
        )
        .await?;

    log::trace!(
        "[PARQUET_SCAN_ASYNC] Scan complete: table={} {} total_batches={} skipped={} scanned={} \
         rows={} use_degraded_mode={}",
        table_id,
        scope_label,
        stats.total_files,
        stats.skipped_files,
        stats.scanned_files,
        combined.num_rows(),
        use_degraded_mode
    );

    if stats.total_files > 0 {
        log::trace!(
            "[Manifest Pruning] table={} {} batches_total={} skipped={} scanned={} rows={}",
            table_id,
            scope_label,
            stats.total_files,
            stats.skipped_files,
            stats.scanned_files,
            combined.num_rows()
        );
    }

    Ok(ParquetScanResult {
        batch: combined,
        stats,
    })
}

fn cold_scan_pruning(
    filter: Option<&Expr>,
    table_def: Option<&TableDefinition>,
) -> ColdScanPruning {
    let (min_seq, max_seq) = filter.map(extract_seq_bounds_from_filter).unwrap_or((None, None));
    let seq_range = match (min_seq, max_seq) {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    };
    let mut pruning = ColdScanPruning {
        seq_range,
        indexed_equalities: Vec::new(),
        bloom: None,
    };
    let Some(filter) = filter else {
        return pruning;
    };
    let Some(table) = table_def else {
        return pruning;
    };

    let mut pk_bloom = None;
    let mut scalar_bloom = None;
    for (name, value) in extract_equality_predicates(filter) {
        if name == SystemColumnNames::SEQ {
            continue;
        }
        let Some(column) = table.columns.iter().find(|column| column.column_name == name) else {
            continue;
        };
        let indexed = column.is_primary_key
            || table.scalar_indexes.iter().any(|index| {
                index.columns.iter().any(|column_id| column_id.as_u64() == column.column_id)
            });
        if !indexed {
            continue;
        }
        let Some(value_str) = scalar_to_prune_string(&value) else {
            continue;
        };
        pruning.indexed_equalities.push((column.column_id, value_str.clone()));
        if column.data_type.supports_equality_bloom() {
            if column.is_primary_key {
                pk_bloom.get_or_insert((name.to_string(), value_str));
            } else {
                scalar_bloom.get_or_insert((name.to_string(), value_str));
            }
        }
    }
    pruning.bloom = scalar_bloom.or(pk_bloom);
    pruning
}
