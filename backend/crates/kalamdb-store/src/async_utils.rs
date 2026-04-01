use crate::storage_trait::{Result, StorageError};

pub(crate) async fn run_blocking_result<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|e| StorageError::Other(format!("spawn_blocking join error: {}", e)))?
}
