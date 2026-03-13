use crate::error::KalamDbError;
use crate::jobs::executors::{JobContext, JobParams};
use crate::providers::{row_utils::system_user_id, BaseTableProvider, SharedTableProvider};
use kalamdb_commons::TableId;

pub(crate) async fn cleanup_empty_shared_scope_if_needed<T: JobParams>(
    ctx: &JobContext<T>,
    table_id: &TableId,
) -> Result<(), KalamDbError> {
    let provider = ctx.app_ctx.schema_registry().get_provider(table_id).ok_or_else(|| {
        KalamDbError::TableNotFound(format!("Provider not found for {}", table_id))
    })?;

    let shared_provider =
        provider.as_any().downcast_ref::<SharedTableProvider>().ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "Provider for {} is not a SharedTableProvider",
                table_id
            ))
        })?;

    let live_rows = shared_provider
        .scan_with_version_resolution_to_kvs_async(system_user_id(), None, None, Some(1), false)
        .await?;

    if !live_rows.is_empty() {
        return Ok(());
    }

    let manifest_service = ctx.app_ctx.manifest_service();
    let cleanup_table_id = table_id.clone();
    let deleted_files = tokio::task::spawn_blocking(move || {
        manifest_service.clear_segments_and_delete_files(&cleanup_table_id, None)
    })
    .await
    .map_err(|e| {
        KalamDbError::InvalidOperation(format!("Empty-scope cleanup task panicked: {}", e))
    })??;

    ctx.log_info(&format!(
        "Removed {} cold segment file(s) for empty shared table {}",
        deleted_files, table_id
    ));

    Ok(())
}
