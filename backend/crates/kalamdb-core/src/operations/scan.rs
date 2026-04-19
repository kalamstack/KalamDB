use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{col, lit, SessionContext};

use super::error::OperationError;
use crate::schema_registry::SchemaRegistry;
use kalamdb_commons::TableId;

/// Convert a string filter value to a typed DataFusion `Expr` literal
/// based on the Arrow column type. Falls back to string literal for unknown types.
#[inline]
fn typed_lit(value: &str, data_type: &DataType) -> datafusion::prelude::Expr {
    match data_type {
        DataType::Int8 => value.parse::<i8>().map_or_else(|_| lit(value), lit),
        DataType::Int16 => value.parse::<i16>().map_or_else(|_| lit(value), lit),
        DataType::Int32 => value.parse::<i32>().map_or_else(|_| lit(value), lit),
        DataType::Int64 => value.parse::<i64>().map_or_else(|_| lit(value), lit),
        DataType::UInt8 => value.parse::<u8>().map_or_else(|_| lit(value), lit),
        DataType::UInt16 => value.parse::<u16>().map_or_else(|_| lit(value), lit),
        DataType::UInt32 => value.parse::<u32>().map_or_else(|_| lit(value), lit),
        DataType::UInt64 => value.parse::<u64>().map_or_else(|_| lit(value), lit),
        DataType::Float32 => value.parse::<f32>().map_or_else(|_| lit(value), lit),
        DataType::Float64 => value.parse::<f64>().map_or_else(|_| lit(value), lit),
        DataType::Boolean => value.parse::<bool>().map_or_else(|_| lit(value), lit),
        _ => lit(value),
    }
}

/// Execute a direct provider scan without SQL reconstruction or a per-request SessionContext.
///
/// 1. Resolve the table provider from the schema registry.
/// 2. Convert the requested column names to projection indices.
/// 3. Call `TableProvider::scan(session_state, projection, filters=[], limit)` → `ExecutionPlan`.
/// 4. Call `collect(plan, task_ctx)` → `Vec<RecordBatch>`.
///
/// When `filters` are non-empty, uses DataFusion's DataFrame API to apply predicates
/// so that the optimizer can push them into the provider scan. Filter values are
/// cast to the column's Arrow type using the table schema for type-correct comparisons.
///
/// The `base_session` is used to extract a `SessionState` (implements `Session` trait)
/// and a `TaskContext` needed by the DataFusion physical execution layer.
pub async fn execute_scan(
    schema_registry: &SchemaRegistry,
    base_session: &SessionContext,
    table_id: &TableId,
    columns: &[String],
    limit: Option<usize>,
    filters: &[(String, String)],
) -> Result<Vec<RecordBatch>, OperationError> {
    // 1. Resolve table provider
    let cached = schema_registry
        .get(table_id)
        .ok_or_else(|| OperationError::TableNotFound(table_id.full_name()))?;

    let provider = cached
        .get_provider()
        .ok_or_else(|| OperationError::ProviderNotAvailable(table_id.full_name()))?;

    // 2. Filtered path: use DataFrame API for predicate pushdown
    if !filters.is_empty() {
        let schema = provider.schema();
        let mut df = base_session.read_table(provider).map_err(OperationError::DataFusion)?;

        for (column, value) in filters {
            // Look up the column type for type-correct literal casting
            let expr = match schema.field_with_name(column) {
                Ok(field) => col(column).eq(typed_lit(value, field.data_type())),
                Err(_) => col(column).eq(lit(value.as_str())),
            };
            df = df.filter(expr).map_err(OperationError::DataFusion)?;
        }

        if !columns.is_empty() {
            let col_exprs: Vec<datafusion::prelude::Expr> =
                columns.iter().map(|c| col(c)).collect();
            df = df.select(col_exprs).map_err(OperationError::DataFusion)?;
        }

        if let Some(lim) = limit {
            df = df.limit(0, Some(lim)).map_err(OperationError::DataFusion)?;
        }

        return df.collect().await.map_err(OperationError::DataFusion);
    }

    // 3. Unfiltered path: direct provider scan (no LogicalPlan, no SQL)
    let projection = if columns.is_empty() {
        None
    } else {
        let schema = provider.schema();
        let mut indices = Vec::with_capacity(columns.len());
        for col_name in columns {
            let idx = schema.index_of(col_name).map_err(|_| {
                OperationError::InvalidArgument(format!(
                    "column '{}' not found in table '{}'",
                    col_name,
                    table_id.full_name()
                ))
            })?;
            indices.push(idx);
        }
        Some(indices)
    };

    let state = base_session.state();
    let plan: Arc<dyn ExecutionPlan> = provider
        .scan(&state, projection.as_ref(), &[], limit)
        .await
        .map_err(OperationError::DataFusion)?;

    let task_ctx = state.task_ctx();
    let batches = collect(plan, task_ctx).await.map_err(OperationError::DataFusion)?;

    Ok(batches)
}
