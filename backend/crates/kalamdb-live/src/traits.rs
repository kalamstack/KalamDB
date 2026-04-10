//! Integration traits for the live query subsystem.
//!
//! These traits define the boundaries between kalamdb-live and the rest of the
//! system (schema registry, SQL executor). Implementations are provided by
//! kalamdb-core during wiring.

use crate::error::LiveError;
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use kalamdb_commons::models::{ReadContext, TableId, UserId};
use kalamdb_commons::schemas::TableDefinition;
use kalamdb_commons::Role;
use std::sync::Arc;

/// Schema operations needed by the live subsystem.
///
/// Provides access to table definitions and Arrow schemas without
/// depending on kalamdb-core's SchemaRegistry struct directly.
pub trait LiveSchemaLookup: Send + Sync {
    /// Get table definition if it exists.
    fn get_table_definition(&self, table_id: &TableId) -> Option<Arc<TableDefinition>>;

    /// Get memoized Arrow schema for a table.
    fn get_arrow_schema(&self, table_id: &TableId) -> Result<Arc<ArrowSchema>, LiveError>;
}

/// SQL execution needed by the live subsystem (initial data, snapshot boundary).
///
/// Abstracts over kalamdb-core's SqlExecutor so kalamdb-live does not depend
/// on kalamdb-core directly.
#[async_trait]
pub trait LiveSqlExecutor: Send + Sync {
    /// Execute a SQL query and return record batches.
    ///
    /// The implementation should apply row-level security for the given user/role
    /// and use the specified read context.
    async fn execute_for_batches(
        &self,
        sql: &str,
        user_id: UserId,
        role: Role,
        read_context: ReadContext,
    ) -> Result<Vec<RecordBatch>, LiveError>;
}
