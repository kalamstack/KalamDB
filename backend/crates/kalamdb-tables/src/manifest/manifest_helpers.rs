use datafusion::{
    arrow::datatypes::SchemaRef,
    logical_expr::Expr,
    prelude::{col, lit},
};
use kalamdb_commons::{
    constants::SystemColumnNames,
    ids::SeqId,
    models::{schemas::TableType, UserId},
};

use crate::{
    error::KalamDbError,
    utils::{
        core::TableProviderCore,
        parquet::scan_parquet_files_as_batch_async,
        version_resolution::{parquet_batch_to_rows, ParquetRowData},
    },
};

/// Ensure manifest.json exists (and is cached) for the current scope before hot writes.
pub fn ensure_manifest_ready(
    core: &TableProviderCore,
    _table_type: TableType,
    user_id: Option<&UserId>,
    log_label: &str,
) -> Result<(), KalamDbError> {
    let table_id = core.table_id();
    let namespace = table_id.namespace_id().clone();
    let table = table_id.table_name().clone();
    let manifest_service = core.services.manifest_service.clone();

    match manifest_service.get_or_load(table_id, user_id) {
        Ok(Some(_)) => return Ok(()),
        Ok(None) => {},
        Err(e) => {
            log::warn!(
                "[{}] Manifest cache lookup failed for {}.{} scope={} err={}",
                log_label,
                namespace.as_str(),
                table.as_str(),
                user_id
                    .map(|u| format!("user={}", u.as_str()))
                    .unwrap_or_else(|| "shared".to_string()),
                e
            );
        },
    }

    let manifest = manifest_service.ensure_manifest_initialized(table_id, user_id)?;

    // Get cached table data for path resolution using storage templates
    let _cached = core
        .services
        .schema_registry
        .get_table_if_exists(table_id)
        .ok()
        .flatten()
        .ok_or_else(|| {
            KalamDbError::TableNotFound(format!(
                "Table {}.{} not found in schema registry",
                namespace.as_str(),
                table.as_str()
            ))
        })?;

    manifest_service.stage_before_flush(table_id, user_id, &manifest)?;

    Ok(())
}

/// Load a row from Parquet cold storage by SeqId with a scoped filter.
///
/// `build_row` maps parsed Parquet data into the provider's row type.
pub async fn load_row_from_parquet_by_seq<T, F>(
    core: &TableProviderCore,
    table_type: TableType,
    schema: &SchemaRef,
    user_id: Option<&UserId>,
    seq_id: SeqId,
    build_row: F,
) -> Result<Option<T>, KalamDbError>
where
    F: FnOnce(ParquetRowData) -> T,
{
    let filter: Expr = col(SystemColumnNames::SEQ).eq(lit(seq_id.as_i64()));
    let batch = scan_parquet_files_as_batch_async(
        core,
        core.table_id(),
        table_type,
        user_id,
        schema.clone(),
        Some(&filter),
        None,
    )
    .await?;
    let rows = parquet_batch_to_rows(&batch)?;

    for row_data in rows.into_iter() {
        if row_data.seq_id == seq_id {
            return Ok(Some(build_row(row_data)));
        }
    }

    Ok(None)
}
