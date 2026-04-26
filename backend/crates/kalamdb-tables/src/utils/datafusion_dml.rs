use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::Session,
    common::DFSchema,
    datasource::{source::DataSourceExec, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{utils::expr_to_columns, Expr},
    physical_plan::{collect, filter::FilterExec, projection::ProjectionExec, ExecutionPlan},
    scalar::ScalarValue,
};
use datafusion_datasource::memory::MemorySourceConfig;
use kalamdb_commons::{
    conversions::{
        arrow_json_conversion::{arrow_value_to_scalar, coerce_updates, json_rows_to_arrow_batch},
        scalar_to_pk_string,
    },
    models::{rows::Row, UserId},
    NotLeaderError, TableId, TableType,
};
use kalamdb_datafusion_sources::exec::{DeferredBatchExec, DeferredBatchSource};
use kalamdb_transactions::{
    build_insert_staged_mutations, StagedMutation, TransactionAccessError, TransactionQueryContext,
};

pub struct OverlayScanProjection {
    pub effective_projection: Option<Vec<usize>>,
    pub final_projection: Option<Vec<usize>>,
}

#[derive(Debug)]
struct RowsAffectedSource {
    batch: RecordBatch,
}

#[async_trait]
impl DeferredBatchSource for RowsAffectedSource {
    fn source_name(&self) -> &'static str {
        "dml_rows_affected"
    }

    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    async fn produce_batch(&self) -> DataFusionResult<RecordBatch> {
        Ok(self.batch.clone())
    }
}

/// DataFusion requires DML providers to return an execution plan that yields one row
/// with a single `count` column (`UInt64`) containing affected rows.
///
/// Use the shared deferred execution wrapper instead of building a temporary
/// `MemTable` just to hand DataFusion one batch.
pub async fn rows_affected_plan(
    _state: &dyn Session,
    rows_affected: u64,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(vec![rows_affected])) as ArrayRef],
    )?;

    Ok(Arc::new(DeferredBatchExec::new(Arc::new(RowsAffectedSource { batch }))))
}

pub fn prepare_overlay_scan_projection(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    pk_column: &str,
) -> DataFusionResult<OverlayScanProjection> {
    let pk_index = schema
        .index_of(pk_column)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    let needs_pk_projection = projection.is_some_and(|columns| !columns.contains(&pk_index));
    let effective_projection = projection.map(|columns| {
        if needs_pk_projection {
            let mut augmented = columns.clone();
            augmented.push(pk_index);
            augmented
        } else {
            columns.clone()
        }
    });
    let final_projection = if needs_pk_projection {
        projection.map(|columns| (0..columns.len()).collect::<Vec<_>>())
    } else {
        None
    };

    Ok(OverlayScanProjection {
        effective_projection,
        final_projection,
    })
}

pub fn stage_transaction_mutations(
    transaction_query_context: &TransactionQueryContext,
    mutations: Vec<StagedMutation>,
) -> DataFusionResult<()> {
    transaction_query_context
        .mutation_sink
        .stage_batch(&transaction_query_context.transaction_id, mutations)
        .map_err(|error| match error {
            TransactionAccessError::NotLeader { leader_addr } => {
                DataFusionError::External(Box::new(NotLeaderError::new(leader_addr)))
            },
            TransactionAccessError::InvalidOperation(message) => {
                DataFusionError::Execution(message)
            },
        })
}

pub fn stage_insert_rows(
    transaction_query_context: &TransactionQueryContext,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    pk_column: &str,
    rows: Vec<Row>,
) -> DataFusionResult<u64> {
    let inserted = rows.len() as u64;
    let mutations = build_insert_staged_mutations(
        &transaction_query_context.transaction_id,
        table_id,
        table_type,
        user_id,
        pk_column,
        rows,
    )
    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    stage_transaction_mutations(transaction_query_context, mutations)?;
    Ok(inserted)
}

pub async fn collect_input_rows(
    state: &dyn Session,
    input: Arc<dyn ExecutionPlan>,
) -> DataFusionResult<Vec<Row>> {
    tracing::debug!("dml.collect_input_rows");
    if let Some(rows) = try_collect_memory_input_rows(input.as_ref())? {
        return Ok(rows);
    }

    let task_ctx = state.task_ctx();

    // Try executing the input plan directly.
    // If it fails due to a type cast (e.g., Utf8 → FixedSizeBinary(16) for UUID columns),
    // fall back to collecting from the child plan (before the CastExec) and let coerce_rows
    // handle the conversion later.
    match collect(input.clone(), task_ctx.clone()).await {
        Ok(batches) => record_batches_to_rows(&batches),
        Err(e) => {
            let err_msg = e.to_string();
            // Check if this is a type mismatch cast failure (e.g., UUID from string literal)
            if err_msg.contains("type mismatch") || err_msg.contains("can't cast") {
                // Try to collect from the child plan (e.g., values before CastExec)
                if input.children().len() == 1 {
                    let child = input.children()[0].clone();
                    let child_batches = collect(child, task_ctx).await?;
                    return record_batches_to_rows(&child_batches);
                }
            }
            Err(e)
        },
    }
}

fn try_collect_memory_input_rows(input: &dyn ExecutionPlan) -> DataFusionResult<Option<Vec<Row>>> {
    if let Some(data_source_exec) = input.as_any().downcast_ref::<DataSourceExec>() {
        return try_collect_rows_from_data_source_exec(data_source_exec);
    }

    if let Some(projection_exec) = input.as_any().downcast_ref::<ProjectionExec>() {
        let Some(source_batches) =
            try_read_memory_source_batches(projection_exec.input().as_ref())?
        else {
            return Ok(None);
        };

        let projection_schema = projection_exec.schema();
        let projected_batches = source_batches
            .into_iter()
            .map(|batch| {
                let arrays = projection_exec
                    .expr()
                    .iter()
                    .map(|projection| {
                        projection.expr.evaluate(&batch)?.into_array(batch.num_rows())
                    })
                    .collect::<DataFusionResult<Vec<_>>>()?;
                RecordBatch::try_new(Arc::clone(&projection_schema), arrays)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        return record_batches_to_rows(&projected_batches).map(Some);
    }

    Ok(None)
}

fn try_collect_rows_from_data_source_exec(
    data_source_exec: &DataSourceExec,
) -> DataFusionResult<Option<Vec<Row>>> {
    let Some(batches) = try_read_memory_source_batches(data_source_exec)? else {
        return Ok(None);
    };

    record_batches_to_rows(&batches).map(Some)
}

fn try_read_memory_source_batches(
    input: &dyn ExecutionPlan,
) -> DataFusionResult<Option<Vec<RecordBatch>>> {
    let Some(data_source_exec) = input.as_any().downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };

    let Some(memory_source) =
        data_source_exec.data_source().as_any().downcast_ref::<MemorySourceConfig>()
    else {
        return Ok(None);
    };

    let mut batches = Vec::new();
    for partition in memory_source.partitions() {
        for batch in partition {
            let projected = if let Some(projection) = memory_source.projection() {
                batch
                    .project(projection)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?
            } else {
                batch.clone()
            };
            batches.push(projected);
        }
    }

    Ok(Some(batches))
}

pub async fn collect_matching_rows(
    provider: &dyn TableProvider,
    state: &dyn Session,
    filters: &[Expr],
) -> DataFusionResult<Vec<Row>> {
    collect_matching_rows_with_projection(provider, state, filters, None).await
}

pub async fn collect_matching_rows_with_projection(
    provider: &dyn TableProvider,
    state: &dyn Session,
    filters: &[Expr],
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<Vec<Row>> {
    // Pass filters to scan() which may apply partial or full filtering.
    // However, when called directly (not through the SQL planner), DataFusion does NOT
    // automatically add a FilterExec for Inexact pushdown — the planner does that.
    // We explicitly wrap with FilterExec to guarantee correct row-level filtering
    // regardless of whether the underlying scan implementation applied the filter.
    let scan_plan = provider.scan(state, projection, filters, None).await?;

    let plan: Arc<dyn ExecutionPlan> = if !filters.is_empty() {
        let schema = scan_plan.schema();
        let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
        let predicate = filters[1..].iter().cloned().fold(filters[0].clone(), |acc, f| acc.and(f));
        let physical_expr = state.create_physical_expr(predicate, &df_schema)?;
        Arc::new(FilterExec::try_new(physical_expr, scan_plan)?)
    } else {
        scan_plan
    };

    let task_ctx = state.task_ctx();
    let batches = collect(plan, task_ctx).await?;
    record_batches_to_rows(&batches)
}

pub fn dml_scan_projection(
    schema: &SchemaRef,
    filters: &[Expr],
    assignments: &[(String, Expr)],
    required_columns: &[&str],
) -> DataFusionResult<Option<Vec<usize>>> {
    let mut referenced_columns = HashSet::new();

    for filter in filters {
        collect_expr_columns(filter, &mut referenced_columns)?;
    }
    for (_, expr) in assignments {
        collect_expr_columns(expr, &mut referenced_columns)?;
    }
    referenced_columns.extend(assignments.iter().map(|(column, _)| column.clone()));
    referenced_columns.extend(required_columns.iter().copied().map(str::to_owned));

    let projection: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| referenced_columns.contains(field.name()).then_some(idx))
        .collect();

    if projection.is_empty() || projection.len() == schema.fields().len() {
        Ok(None)
    } else {
        Ok(Some(projection))
    }
}

pub fn row_matches_filters(
    state: &dyn Session,
    schema: &SchemaRef,
    row: &Row,
    filters: &[Expr],
) -> DataFusionResult<bool> {
    let batch = build_row_batch(schema, row)?;
    let df_schema = DFSchema::try_from(Arc::clone(schema))?;

    for filter in filters {
        let predicate = evaluate_expr_against_batch(state, &df_schema, &batch, filter)?;
        if !predicate_to_bool(predicate)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn validate_where_clause(filters: &[Expr], op_name: &str) -> DataFusionResult<()> {
    if filters.is_empty() {
        return Err(DataFusionError::Plan(format!("{} requires a WHERE clause", op_name)));
    }
    Ok(())
}

pub fn validate_update_assignments(
    assignments: &[(String, Expr)],
    pk_column: &str,
) -> DataFusionResult<()> {
    if assignments.is_empty() {
        return Err(DataFusionError::Plan("UPDATE requires at least one assignment".to_string()));
    }

    for (column, _) in assignments {
        if column.eq_ignore_ascii_case(pk_column) {
            return Err(DataFusionError::Plan(format!(
                "UPDATE cannot modify primary key column '{}'",
                pk_column
            )));
        }
    }

    Ok(())
}

pub fn extract_pk_value(row: &Row, pk_column: &str) -> DataFusionResult<String> {
    let scalar = row.values.get(pk_column).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Primary key column '{}' not found in matching row",
            pk_column
        ))
    })?;

    scalar_to_pk_string(scalar)
        .map_err(|e| DataFusionError::Execution(format!("Invalid primary key value: {}", e)))
}

pub fn evaluate_assignment_expr(
    state: &dyn Session,
    schema: &SchemaRef,
    row: &Row,
    expr: &Expr,
) -> DataFusionResult<ScalarValue> {
    let df_schema = DFSchema::try_from(Arc::clone(schema))?;
    let batch = build_row_batch(schema, row)?;

    evaluate_expr_against_batch(state, &df_schema, &batch, expr)
}

pub fn evaluate_assignment_values(
    state: &dyn Session,
    schema: &SchemaRef,
    row: &Row,
    assignments: &[(String, Expr)],
) -> DataFusionResult<Row> {
    let df_schema = DFSchema::try_from(Arc::clone(schema))?;
    let batch = build_row_batch(schema, row)?;
    let mut values = BTreeMap::new();

    for (column, expr) in assignments {
        let value = evaluate_expr_against_batch(state, &df_schema, &batch, expr)?;
        values.insert(column.clone(), value);
    }

    Ok(Row::new(values))
}

pub fn update_assignments_noop(
    schema: &SchemaRef,
    row: &Row,
    updates: &Row,
) -> DataFusionResult<bool> {
    let updates = coerce_updates(updates.clone(), schema).map_err(DataFusionError::Execution)?;

    for (column, value) in updates.values {
        match row.values.get(&column) {
            Some(existing) if existing == &value => {},
            _ => return Ok(false),
        }
    }

    Ok(true)
}

fn build_row_batch(schema: &SchemaRef, row: &Row) -> DataFusionResult<RecordBatch> {
    json_rows_to_arrow_batch(schema, vec![row.clone()]).map_err(DataFusionError::Execution)
}

fn collect_expr_columns(expr: &Expr, columns: &mut HashSet<String>) -> DataFusionResult<()> {
    let mut referenced = HashSet::new();
    expr_to_columns(expr, &mut referenced)
        .map_err(|e| DataFusionError::Execution(format!("Failed to inspect expression: {}", e)))?;
    columns.extend(referenced.into_iter().map(|column| column.name));
    Ok(())
}

fn evaluate_expr_against_batch(
    state: &dyn Session,
    df_schema: &DFSchema,
    batch: &RecordBatch,
    expr: &Expr,
) -> DataFusionResult<ScalarValue> {
    let physical_expr = state.create_physical_expr(expr.clone(), df_schema)?;
    let value = physical_expr.evaluate(batch)?;

    match value {
        datafusion::logical_expr::ColumnarValue::Scalar(scalar) => Ok(scalar),
        datafusion::logical_expr::ColumnarValue::Array(array) => {
            arrow_value_to_scalar(array.as_ref(), 0)
        },
    }
}

fn record_batches_to_rows(batches: &[RecordBatch]) -> DataFusionResult<Vec<Row>> {
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let _span =
        tracing::debug_span!("record_batches_to_rows", total_rows, batch_count = batches.len())
            .entered();
    let mut rows = Vec::with_capacity(total_rows);

    for batch in batches {
        let schema = batch.schema();
        let field_names: Vec<String> =
            schema.fields().iter().map(|field| field.name().to_string()).collect();
        let columns: Vec<&dyn arrow::array::Array> =
            batch.columns().iter().map(|column| column.as_ref()).collect();
        for row_idx in 0..batch.num_rows() {
            let mut values = BTreeMap::new();
            for (col_idx, field_name) in field_names.iter().enumerate() {
                let scalar = arrow_value_to_scalar(columns[col_idx], row_idx)?;
                values.insert(field_name.clone(), scalar);
            }
            rows.push(Row::new(values));
        }
    }

    Ok(rows)
}

fn predicate_to_bool(value: ScalarValue) -> DataFusionResult<bool> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(v),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(false),
        other => Err(DataFusionError::Execution(format!(
            "WHERE predicate did not evaluate to boolean: {:?}",
            other
        ))),
    }
}

/// Validate NOT NULL constraints on rows before INSERT/UPDATE
///
/// According to ADR-016, validation must occur before any RocksDB write.
/// This ensures atomicity - if validation fails, no data is written.
///
/// # Arguments
/// * `schema` - Arrow schema with field nullability information
/// * `rows` - Rows to validate (each row is a map of column name -> ScalarValue)
///
/// # Returns
/// * `Ok(())` if all non-nullable columns have non-NULL values
/// * `Err(DataFusionError)` if any NOT NULL constraint is violated
///
/// # Example
/// ```ignore
/// let schema = table_provider.schema();
/// validate_not_null_constraints(&schema, &rows)?;
/// // Safe to write to storage now - validation passed
/// ```
pub fn validate_not_null_constraints(schema: &SchemaRef, rows: &[Row]) -> DataFusionResult<()> {
    // Precompute non-nullable columns to avoid repeated checks
    let non_nullable_columns: Vec<Arc<Field>> =
        schema.fields().iter().filter(|f| !f.is_nullable()).cloned().collect();

    if non_nullable_columns.is_empty() {
        return Ok(()); // No constraints to validate
    }

    // Validate each row
    for (row_idx, row) in rows.iter().enumerate() {
        for field in &non_nullable_columns {
            let column_name = field.name();

            // Check if column value exists and is non-NULL
            match row.values.get(column_name) {
                None => {
                    return Err(DataFusionError::Execution(format!(
                        "NOT NULL constraint violation: column '{}' is missing in row {} (row \
                         index {})",
                        column_name,
                        row_idx + 1,
                        row_idx
                    )));
                },
                Some(value) if value.is_null() => {
                    // Use is_null() to catch both ScalarValue::Null and typed NULLs like
                    // Utf8(None), Int32(None), etc.
                    return Err(DataFusionError::Execution(format!(
                        "NOT NULL constraint violation: column '{}' cannot be NULL (row {})",
                        column_name,
                        row_idx + 1
                    )));
                },
                Some(_) => continue,
            }
        }
    }

    Ok(())
}

/// Validate NOT NULL constraints using a precomputed set of non-nullable column names.
///
/// This is faster than `validate_not_null_constraints` because it avoids recomputing
/// the non-nullable column set from the schema on every call.
///
/// # Arguments
/// * `non_null_columns` - Precomputed set of column names with NOT NULL constraints
/// * `rows` - Rows to validate
pub fn validate_not_null_with_set(
    non_null_columns: &std::collections::HashSet<String>,
    rows: &[Row],
) -> DataFusionResult<()> {
    if non_null_columns.is_empty() {
        return Ok(());
    }

    for (row_idx, row) in rows.iter().enumerate() {
        for column_name in non_null_columns {
            match row.values.get(column_name) {
                None => {
                    return Err(DataFusionError::Execution(format!(
                        "NOT NULL constraint violation: column '{}' is missing in row {} (row \
                         index {})",
                        column_name,
                        row_idx + 1,
                        row_idx
                    )));
                },
                Some(value) if value.is_null() => {
                    return Err(DataFusionError::Execution(format!(
                        "NOT NULL constraint violation: column '{}' cannot be NULL (row {})",
                        column_name,
                        row_idx + 1
                    )));
                },
                Some(_) => continue,
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, SchemaRef},
        logical_expr::{col, lit},
        prelude::SessionContext,
        scalar::ScalarValue,
    };
    use kalamdb_commons::models::rows::Row;

    use super::{dml_scan_projection, evaluate_assignment_values, row_matches_filters};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn test_row() -> Row {
        Row::from_vec(vec![
            ("id".to_string(), ScalarValue::Utf8(Some("row-1".to_string()))),
            ("value".to_string(), ScalarValue::Int64(Some(10))),
            ("name".to_string(), ScalarValue::Utf8(Some("alice".to_string()))),
        ])
    }

    #[test]
    fn evaluate_assignment_values_returns_all_assignment_results() {
        let ctx = SessionContext::new();
        let schema = test_schema();
        let row = test_row();
        let assignments = vec![
            ("value".to_string(), col("value") + lit(5_i64)),
            ("name".to_string(), lit("ALICE")),
        ];

        let values = evaluate_assignment_values(&ctx.state(), &schema, &row, &assignments).unwrap();

        assert_eq!(values.get("value"), Some(&ScalarValue::Int64(Some(15))));
        assert_eq!(values.get("name"), Some(&ScalarValue::Utf8(Some("ALICE".to_string()))));
    }

    #[test]
    fn row_matches_filters_evaluates_multiple_predicates_for_one_row() {
        let ctx = SessionContext::new();
        let schema = test_schema();
        let row = test_row();
        let filters = vec![col("value").gt(lit(5_i64)), col("name").eq(lit("alice"))];

        let matches = row_matches_filters(&ctx.state(), &schema, &row, &filters).unwrap();

        assert!(matches);
    }

    #[test]
    fn dml_scan_projection_only_keeps_columns_needed_for_delete() {
        let schema = test_schema();
        let filters = vec![col("name").eq(lit("alice"))];

        let projection = dml_scan_projection(&schema, &filters, &[], &["id"]).unwrap();

        assert_eq!(projection, Some(vec![0, 2]));
    }

    #[test]
    fn dml_scan_projection_includes_assignment_source_and_target_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ]));
        let filters = vec![col("name").eq(lit("alice"))];
        let assignments = vec![("score".to_string(), col("value") + lit(5_i64))];

        let projection = dml_scan_projection(&schema, &filters, &assignments, &["id"]).unwrap();

        assert_eq!(projection, None);
    }
}
