//! Namespace Executor - CREATE/DROP NAMESPACE operations
//!
//! This is the SINGLE place where namespace mutations happen.
//! All methods use spawn_blocking to avoid blocking the tokio runtime
//! with synchronous RocksDB calls.

use std::sync::Arc;

use kalamdb_commons::models::NamespaceId;
use kalamdb_system::Namespace;

use crate::{
    app_context::AppContext,
    applier::{executor::utils::run_blocking_applier, ApplierError},
};

/// Executor for namespace operations
pub struct NamespaceExecutor {
    app_context: Arc<AppContext>,
}

impl NamespaceExecutor {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    /// Execute CREATE NAMESPACE
    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<String, ApplierError> {
        log::debug!("CommandExecutorImpl: Creating namespace {}", namespace_id);
        let app_context = self.app_context.clone();
        let namespace_id = namespace_id.clone();
        run_blocking_applier(move || {
            let namespace = Namespace::new(namespace_id.as_str());
            app_context
                .system_tables()
                .namespaces()
                .create_namespace(namespace)
                .map_err(|e| {
                    ApplierError::Execution(format!("Failed to create namespace: {}", e))
                })?;
            Ok(format!("Namespace {} created successfully", namespace_id))
        })
        .await
    }

    /// Execute DROP NAMESPACE
    pub async fn drop_namespace(&self, namespace_id: &NamespaceId) -> Result<String, ApplierError> {
        log::debug!("CommandExecutorImpl: Dropping namespace {}", namespace_id);
        let app_context = self.app_context.clone();
        let namespace_id = namespace_id.clone();
        run_blocking_applier(move || {
            app_context
                .system_tables()
                .namespaces()
                .delete_namespace(&namespace_id)
                .map_err(|e| ApplierError::Execution(format!("Failed to drop namespace: {}", e)))?;
            Ok(format!("Namespace {} dropped successfully", namespace_id))
        })
        .await
    }
}
