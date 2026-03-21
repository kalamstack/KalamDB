use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;

use super::error::OperationError;
use crate::schema_registry::SchemaRegistry;
use kalamdb_commons::TableId;

/// Execute a direct provider scan without SQL reconstruction or a per-request SessionContext.
///
/// 1. Resolve the table provider from the schema registry.
/// 2. Convert the requested column names to projection indices.
/// 3. Call `TableProvider::scan(session_state, projection, filters=[], limit)` → `ExecutionPlan`.
/// 4. Call `collect(plan, task_ctx)` → `Vec<RecordBatch>`.
///
/// The `base_session` is used to extract a `SessionState` (implements `Session` trait)
/// and a `TaskContext` needed by the DataFusion physical execution layer.
/// No logical plan, no SQL parsing.
pub async fn execute_scan(
    schema_registry: &SchemaRegistry,
    base_session: &SessionContext,
    table_id: &TableId,
    columns: &[String],
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>, OperationError> {
    // 1. Resolve table provider
    let cached = schema_registry.get(table_id).ok_or_else(|| {
        OperationError::TableNotFound(table_id.full_name())
    })?;

    let provider = cached.get_provider().ok_or_else(|| {
        OperationError::ProviderNotAvailable(table_id.full_name())
    })?;

    // 2. Build projection from column names → indices
    let projection = if columns.is_empty() {
        None
    } else {
        let schema = provider.schema();
        let mut indices = Vec::with_capacity(columns.len());
        for col in columns {
            let idx = schema.index_of(col).map_err(|_| {
                OperationError::InvalidArgument(format!(
                    "column '{}' not found in table '{}'",
                    col,
                    table_id.full_name()
                ))
            })?;
            indices.push(idx);
        }
        Some(indices)
    };

    // 3. Execute physical scan — no LogicalPlan, no SQL
    //    SessionState implements the Session trait; SessionContext does not.
    let state = base_session.state();
    let plan: Arc<dyn ExecutionPlan> = provider
        .scan(&state, projection.as_ref(), &[], limit)
        .await
        .map_err(OperationError::DataFusion)?;

    // 4. Collect results using TaskContext only
    let task_ctx = state.task_ctx();
    let batches = collect(plan, task_ctx)
        .await
        .map_err(OperationError::DataFusion)?;

    Ok(batches)
}
