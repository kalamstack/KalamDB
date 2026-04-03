//! SHOW MANIFEST CACHE handler (Phase 4, US6, T089-T090)
//!
//! Returns all manifest cache entries with their metadata.
//! Uses ManifestTableProvider from kalamdb-system for consistent schema.

use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_sql::ShowManifestStatement;
use kalamdb_system::providers::ManifestTableProvider;
use std::sync::Arc;

/// Handler for SHOW MANIFEST CACHE command
///
/// Delegates to ManifestTableProvider for consistent schema and implementation.
pub struct ShowManifestCacheHandler {
    app_context: Arc<kalamdb_core::app_context::AppContext>,
}

impl ShowManifestCacheHandler {
    /// Create a new ShowManifestCacheHandler
    pub fn new(app_context: Arc<kalamdb_core::app_context::AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<ShowManifestStatement> for ShowManifestCacheHandler {
    async fn execute(
        &self,
        _stmt: ShowManifestStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let start_time = std::time::Instant::now();
        // Use ManifestTableProvider to scan the manifest cache
        let provider = ManifestTableProvider::new(self.app_context.storage_backend());

        let batch = provider
            .scan_to_record_batch()
            .into_kalamdb_error("Failed to read manifest cache")?;

        let row_count = batch.num_rows();

        log::info!("SHOW MANIFEST CACHE returned {} entries", row_count);

        // Log query operation
        let duration = start_time.elapsed().as_secs_f64() * 1000.0;
        use crate::helpers::audit;
        let audit_entry =
            audit::log_query_operation(context, "SHOW", "MANIFEST CACHE", duration, None);
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        Ok(ExecutionResult::Rows {
            batches: vec![batch],
            row_count,
            schema: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_core::app_context::AppContext;
    use kalamdb_core::test_helpers::create_test_session_simple;

    #[tokio::test]
    async fn test_show_manifest_cache_empty() {
        use kalamdb_commons::models::{Role, UserId};
        use kalamdb_core::sql::context::ExecutionContext;

        let app_context = AppContext::new_test();
        let handler = ShowManifestCacheHandler::new(app_context.clone());
        let stmt = ShowManifestStatement;
        let exec_ctx =
            ExecutionContext::new(UserId::from("1"), Role::System, create_test_session_simple());

        let result = handler.execute(stmt, vec![], &exec_ctx).await;
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Rows {
            batches, row_count, ..
        }) = result
        {
            assert_eq!(row_count, 0);
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 0);
        } else {
            panic!("Expected Rows result");
        }
    }

    #[test]
    fn test_schema_structure() {
        use kalamdb_system::providers::manifest::manifest_arrow_schema;

        let schema = manifest_arrow_schema();
        assert_eq!(schema.fields().len(), 10);
        assert_eq!(schema.field(0).name(), "cache_key");
        assert_eq!(schema.field(1).name(), "namespace_id");
        assert_eq!(schema.field(2).name(), "table_name");
        assert_eq!(schema.field(3).name(), "scope");
        assert_eq!(schema.field(4).name(), "etag");
        assert_eq!(schema.field(5).name(), "last_refreshed");
        assert_eq!(schema.field(6).name(), "last_accessed");
        assert_eq!(schema.field(7).name(), "in_memory");
        assert_eq!(schema.field(8).name(), "sync_state");
        assert_eq!(schema.field(9).name(), "manifest_json");
    }
}
