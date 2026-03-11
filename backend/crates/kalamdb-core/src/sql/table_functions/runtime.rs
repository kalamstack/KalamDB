use crate::app_context::AppContext;
use datafusion::common::{DataFusionError, Result};
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_vector::{VectorSearchRuntime, VectorSearchScope};
use std::sync::Weak;

#[derive(Debug, Clone)]
pub struct CoreVectorSearchRuntime {
    app_context: Weak<AppContext>,
}

impl CoreVectorSearchRuntime {
    pub fn new(app_context: Weak<AppContext>) -> Self {
        Self { app_context }
    }
}

impl VectorSearchRuntime for CoreVectorSearchRuntime {
    fn resolve_scope(
        &self,
        table_id: &TableId,
        column_name: &str,
        session_user: &UserId,
    ) -> Result<Option<VectorSearchScope>> {
        let app_context = self.app_context.upgrade().ok_or_else(|| {
            DataFusionError::Execution(
                "vector_search runtime is not attached to AppContext".to_string(),
            )
        })?;

        let schema_registry = app_context.schema_registry();
        let cached_table = schema_registry
            .get(table_id)
            .ok_or_else(|| DataFusionError::Execution(format!("Table not found: {}", table_id)))?;

        let table_type = cached_table.table.table_type;
        if !matches!(table_type, TableType::User | TableType::Shared) {
            return Err(DataFusionError::Execution(format!(
                "vector_search supports only user/shared tables, got {:?}",
                table_type
            )));
        }

        let manifest_user = if matches!(table_type, TableType::User) {
            Some(session_user.clone())
        } else {
            None
        };

        let manifest = app_context
            .manifest_service()
            .ensure_manifest_initialized(table_id, manifest_user.as_ref())
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to initialize manifest for {}: {}",
                    table_id, e
                ))
            })?;

        let vector_meta = match manifest.vector_indexes.get(column_name) {
            Some(meta) if meta.enabled => meta.clone(),
            None => return Ok(None),
            Some(_) => return Ok(None),
        };

        let storage_cached =
            cached_table.storage_cached(&app_context.storage_registry()).map_err(|e| {
                DataFusionError::Execution(format!("Failed to resolve storage cache: {}", e))
            })?;

        Ok(Some(VectorSearchScope {
            table_type,
            manifest_user,
            metric: vector_meta.metric,
            last_applied_seq: vector_meta.last_applied_seq,
            snapshot_path: vector_meta.snapshot_path,
            storage_cached,
            backend: app_context.storage_backend(),
        }))
    }
}
