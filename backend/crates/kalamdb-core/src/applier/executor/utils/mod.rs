pub mod fileref_util;

use std::future::Future;
use std::sync::Arc;

use crate::app_context::AppContext;
use crate::applier::ApplierError;

pub(super) async fn run_blocking_applier<T, F>(operation: F) -> Result<T, ApplierError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ApplierError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|e| ApplierError::Execution(format!("Task join error: {}", e)))?
}

pub(super) async fn with_plan_cache_invalidation<T, F, Fut>(
    app_context: Arc<AppContext>,
    operation: F,
) -> Result<T, ApplierError>
where
    F: FnOnce(Arc<AppContext>) -> Fut,
    Fut: Future<Output = Result<T, ApplierError>>,
{
    let result = operation(app_context.clone()).await?;
    if let Some(sql_executor) = app_context.try_sql_executor() {
        sql_executor.clear_plan_cache();
        log::debug!("DdlExecutor: Cleared SQL plan cache after DDL");
    }
    Ok(result)
}
