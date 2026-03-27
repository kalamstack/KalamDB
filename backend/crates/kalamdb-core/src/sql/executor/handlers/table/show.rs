//! Typed DDL handler for SHOW TABLES statements

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use crate::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use crate::sql::executor::handlers::typed::TypedStatementHandler;
use datafusion::arrow::array::{
    ArrayRef, Int32Array, RecordBatch, StringBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use kalamdb_commons::schemas::TableDefinition;
use kalamdb_sql::ddl::ShowTablesStatement;
use std::sync::Arc;

/// Typed handler for SHOW TABLES statements
pub struct ShowTablesHandler {
    app_context: Arc<AppContext>,
}

impl ShowTablesHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<ShowTablesStatement> for ShowTablesHandler {
    async fn execute(
        &self,
        statement: ShowTablesStatement,
        _params: Vec<ScalarValue>,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let start_time = std::time::Instant::now();

        // Offload sync RocksDB reads to blocking pool
        let app_ctx = self.app_context.clone();
        let ns_filter = statement.namespace_id;
        let result = tokio::task::spawn_blocking(move || {
            let tables_provider = app_ctx.system_tables().tables();
            if let Some(ns) = ns_filter {
                let defs = tables_provider.list_tables()?;
                let filtered: Vec<TableDefinition> =
                    defs.into_iter().filter(|t| t.namespace_id == ns).collect();
                let batch = build_tables_batch(tables_provider.schema(), filtered)?;
                let row_count = batch.num_rows();
                Ok::<_, KalamDbError>(ExecutionResult::Rows {
                    batches: vec![batch],
                    row_count,
                    schema: None,
                })
            } else {
                let batch = tables_provider.scan_all_tables()?;
                let row_count = batch.num_rows();
                Ok(ExecutionResult::Rows {
                    batches: vec![batch],
                    row_count,
                    schema: None,
                })
            }
        })
        .await
        .map_err(|e| KalamDbError::ExecutionError(format!("Task join error: {}", e)))?;

        // Log query operation
        let duration = start_time.elapsed().as_secs_f64() * 1000.0;
        use crate::sql::executor::helpers::audit;
        let audit_entry = audit::log_query_operation(context, "SHOW", "TABLES", duration, None);
        audit::persist_audit_entry(&self.app_context, &audit_entry).await?;

        result
    }

    async fn check_authorization(
        &self,
        _statement: &ShowTablesStatement,
        _context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        // SHOW TABLES allowed for all authenticated users
        Ok(())
    }
}

/// Build a RecordBatch for system.tables-like view from definitions
fn build_tables_batch(
    provider_schema: SchemaRef,
    tables: Vec<TableDefinition>,
) -> Result<RecordBatch, KalamDbError> {
    let mut table_ids = StringBuilder::new();
    let mut table_names = StringBuilder::new();
    let mut namespaces = StringBuilder::new();
    let mut table_types = StringBuilder::new();
    let mut created_ats = Vec::new();
    let mut schema_versions = Vec::new();
    let mut table_comments = StringBuilder::new();
    let mut updated_ats = Vec::new();

    for t in tables.iter() {
        let table_id = kalamdb_commons::TableId::new(t.namespace_id.clone(), t.table_name.clone());
        table_ids.append_value(&table_id);
        table_names.append_value(t.table_name.as_str());
        namespaces.append_value(t.namespace_id.as_str());
        table_types.append_value(t.table_type.as_str());
        created_ats.push(Some(t.created_at.timestamp_millis()));
        schema_versions.push(Some(t.schema_version as i32));
        table_comments.append_option(t.table_comment.as_deref());
        updated_ats.push(Some(t.updated_at.timestamp_millis()));
    }

    let batch = RecordBatch::try_new(
        provider_schema,
        vec![
            Arc::new(table_ids.finish()) as ArrayRef,
            Arc::new(table_names.finish()) as ArrayRef,
            Arc::new(namespaces.finish()) as ArrayRef,
            Arc::new(table_types.finish()) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                created_ats.into_iter().map(|ts| ts.map(|ms| ms * 1000)).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int32Array::from(schema_versions)) as ArrayRef,
            Arc::new(table_comments.finish()) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                updated_ats.into_iter().map(|ts| ts.map(|ms| ms * 1000)).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .into_arrow_error()?;

    Ok(batch)
}
