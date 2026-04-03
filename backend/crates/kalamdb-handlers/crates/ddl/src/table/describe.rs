//! Typed DDL handler for DESCRIBE TABLE statements

use kalamdb_commons::models::{NamespaceId, TableId};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_core::views::DescribeView;
use kalamdb_sql::ddl::DescribeTableStatement;
use std::sync::Arc;

/// Typed handler for DESCRIBE TABLE statements
pub struct DescribeTableHandler {
    app_context: Arc<AppContext>,
}

impl DescribeTableHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl TypedStatementHandler<DescribeTableStatement> for DescribeTableHandler {
    async fn execute(
        &self,
        statement: DescribeTableStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let start_time = std::time::Instant::now();
        let ns = statement.namespace_id.clone().unwrap_or_else(|| NamespaceId::default());
        let table_id = TableId::from_strings(ns.as_str(), statement.table_name.as_str());
        let def = self.app_context.schema_registry().get_table_if_exists(&table_id)?.ok_or_else(
            || {
                KalamDbError::NotFound(format!(
                    "Table '{}' not found in namespace '{}'",
                    statement.table_name.as_str(),
                    ns.as_str()
                ))
            },
        )?;

        // Use the DescribeView to build the batch with extended columns
        let batch = DescribeView::build_batch(&def)?;
        let row_count = batch.num_rows();

        // Log query operation
        let duration = start_time.elapsed().as_secs_f64() * 1000.0;
        use crate::helpers::audit;
        let audit_entry =
            audit::log_query_operation(context, "DESCRIBE", &table_id.full_name(), duration, None);
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        Ok(ExecutionResult::Rows {
            batches: vec![batch],
            row_count,
            schema: None,
        })
    }

    async fn check_authorization(
        &self,
        _statement: &DescribeTableStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // DESCRIBE TABLE allowed for all authenticated users who can access the table
        Ok(())
    }
}

// Note: build_describe_batch() has been moved to DescribeView::build_batch()
// in backend/crates/kalamdb-core/src/views/describe.rs
// This provides a centralized implementation with extended columns (column_id, schema_version)
