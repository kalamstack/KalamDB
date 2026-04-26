//! Storage Executor - CREATE/DROP STORAGE operations
//!
//! This is the SINGLE place where storage mutations happen.
//! All methods use spawn_blocking to avoid blocking the tokio runtime
//! with synchronous RocksDB calls.

use std::sync::Arc;

use kalamdb_commons::models::StorageId;
use kalamdb_system::Storage;

use crate::{
    app_context::AppContext,
    applier::{executor::utils::run_blocking_applier, ApplierError},
};

/// Executor for storage operations
pub struct StorageExecutor {
    app_context: Arc<AppContext>,
}

impl StorageExecutor {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    /// Execute CREATE STORAGE
    pub async fn create_storage(&self, storage: &Storage) -> Result<String, ApplierError> {
        log::info!("CommandExecutorImpl: Creating storage {}", storage.storage_id);
        let app_context = self.app_context.clone();
        let storage = storage.clone();
        run_blocking_applier(move || {
            let storage_id = storage.storage_id.clone();
            app_context
                .system_tables()
                .storages()
                .create_storage(storage)
                .map_err(|e| ApplierError::Execution(format!("Failed to create storage: {}", e)))?;
            Ok(format!("Storage {} created successfully", storage_id))
        })
        .await
    }

    /// Execute DROP STORAGE
    pub async fn drop_storage(&self, storage_id: &StorageId) -> Result<String, ApplierError> {
        log::info!("CommandExecutorImpl: Dropping storage {}", storage_id);
        let app_context = self.app_context.clone();
        let storage_id = storage_id.clone();
        run_blocking_applier(move || {
            app_context
                .system_tables()
                .storages()
                .delete_storage(&storage_id)
                .map_err(|e| ApplierError::Execution(format!("Failed to drop storage: {}", e)))?;
            Ok(format!("Storage {} dropped successfully", storage_id))
        })
        .await
    }
}
