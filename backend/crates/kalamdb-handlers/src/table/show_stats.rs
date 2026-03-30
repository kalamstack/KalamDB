//! Typed DDL handler for SHOW STATS statements

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use kalamdb_commons::arrow_utils::{field_uint64, field_utf8, schema};
use kalamdb_commons::models::{NamespaceId, TableId};
use kalamdb_sql::ddl::ShowTableStatsStatement;
use std::sync::Arc;

/// Typed handler for SHOW STATS statements
pub struct ShowStatsHandler {
    app_context: Arc<AppContext>,
}

impl ShowStatsHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<ShowTableStatsStatement> for ShowStatsHandler {
    async fn execute(
        &self,
        statement: ShowTableStatsStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let start_time = std::time::Instant::now();
        let ns = statement.namespace_id.clone().unwrap_or_else(|| NamespaceId::default());
        let table_id = TableId::from_strings(ns.as_str(), statement.table_name.as_str());

        // TableDefinition gives us metadata only; stats system not yet implemented.
        // Provide placeholder zero metrics plus schema version.
        let registry = self.app_context.schema_registry();
        let def = registry.get_table_if_exists(&table_id)?.ok_or_else(|| {
            KalamDbError::NotFound(format!(
                "Table '{}' not found in namespace '{}'",
                statement.table_name.as_str(),
                ns.as_str()
            ))
        })?;

        let logical_rows = 0u64; // TODO: integrate row count tracking
        let flushed_segments = 0u64; // TODO: integrate flush segment counters
        let active_streams = 0u64; // TODO: integrate stream activity metrics
        let memory_bytes = 0u64; // TODO: integrate in-memory size tracking
        let schema_version = def.schema_version as u64;

        let schema = schema(vec![
            field_utf8("table_name", false),
            field_utf8("namespace", false),
            field_uint64("schema_version", false),
            field_uint64("logical_rows", false),
            field_uint64("flushed_segments", false),
            field_uint64("active_streams", false),
            field_uint64("memory_bytes", false),
        ]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![def.table_name.as_str().to_string()])) as ArrayRef,
                Arc::new(StringArray::from(vec![def.namespace_id.as_str().to_string()]))
                    as ArrayRef,
                Arc::new(UInt64Array::from(vec![schema_version])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![logical_rows])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![flushed_segments])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![active_streams])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![memory_bytes])) as ArrayRef,
            ],
        )
        .into_arrow_error()?;

        // Log query operation
        let duration = start_time.elapsed().as_secs_f64() * 1000.0;
        use crate::helpers::audit;
        let audit_entry = audit::log_query_operation(
            context,
            "SHOW",
            &format!("STATS {}", table_id.full_name()),
            duration,
            None,
        );
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        Ok(ExecutionResult::Rows {
            batches: vec![batch],
            row_count: 1,
            schema: None,
        })
    }

    async fn check_authorization(
        &self,
        _statement: &ShowTableStatsStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // SHOW STATS allowed for all authenticated users
        Ok(())
    }
}
