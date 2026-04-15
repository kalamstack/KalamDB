//! Adapters that implement kalamdb-live traits using kalamdb-core types.
//!
//! These bridge the boundary between the live-query crate and the core server,
//! so kalamdb-live never depends on kalamdb-core directly.

use crate::schema_registry::SchemaRegistry;
use crate::sql::context::{ExecutionContext, ExecutionResult};
use crate::sql::executor::SqlExecutor;
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use kalamdb_commons::models::{ReadContext, TableId, UserId};
use kalamdb_commons::schemas::TableDefinition;
use kalamdb_commons::Role;
use kalamdb_live::error::LiveError;
use kalamdb_live::traits::{LiveSchemaLookup, LiveSqlExecutor};
use std::sync::Arc;

/// Adapts [`SchemaRegistry`] to the [`LiveSchemaLookup`] trait.
pub struct SchemaRegistryLookup {
    registry: Arc<SchemaRegistry>,
}

impl SchemaRegistryLookup {
    pub fn new(registry: Arc<SchemaRegistry>) -> Self {
        Self { registry }
    }
}

impl LiveSchemaLookup for SchemaRegistryLookup {
    fn get_table_definition(&self, table_id: &TableId) -> Option<Arc<TableDefinition>> {
        self.registry.get(table_id).map(|cached| Arc::clone(&cached.table))
    }

    fn get_arrow_schema(&self, table_id: &TableId) -> Result<Arc<ArrowSchema>, LiveError> {
        self.registry
            .get_arrow_schema(table_id)
            .map_err(|e| LiveError::TableNotFound(e.to_string()))
    }
}

/// Adapts [`SqlExecutor`] to the [`LiveSqlExecutor`] trait.
pub struct SqlExecutorAdapter {
    executor: Arc<SqlExecutor>,
    base_session_context: Arc<SessionContext>,
}

impl SqlExecutorAdapter {
    pub fn new(executor: Arc<SqlExecutor>, base_session_context: Arc<SessionContext>) -> Self {
        Self {
            executor,
            base_session_context,
        }
    }
}

#[async_trait]
impl LiveSqlExecutor for SqlExecutorAdapter {
    async fn execute_for_batches(
        &self,
        sql: &str,
        user_id: UserId,
        role: Role,
        read_context: ReadContext,
    ) -> Result<Vec<RecordBatch>, LiveError> {
        let exec_ctx = ExecutionContext::new(user_id, role, Arc::clone(&self.base_session_context))
            .with_read_context(read_context);

        let result = self
            .executor
            .execute(sql, &exec_ctx, vec![])
            .await
            .map_err(|e| LiveError::ExecutionError(e.to_string()))?;

        match result {
            ExecutionResult::Rows { batches, .. } => Ok(batches),
            other => Err(LiveError::InvalidOperation(format!(
                "Expected row result from initial data query, got {:?}",
                std::mem::discriminant(&other)
            ))),
        }
    }
}
