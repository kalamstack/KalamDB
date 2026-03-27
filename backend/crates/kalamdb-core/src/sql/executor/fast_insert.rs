//! Fast-path INSERT bypass for simple `INSERT INTO table (cols) VALUES (...)` statements.
//!
//! Bypasses DataFusion's optimizer and physical planner (~2.6ms overhead) by parsing
//! the INSERT SQL directly with sqlparser, resolving the KalamTableProvider from the
//! schema registry, converting values to Row objects, and calling `insert_rows()` directly.
//!
//! Falls back to DataFusion for:
//! - INSERT ... SELECT (subquery source)
//! - ON CONFLICT / ON DUPLICATE KEY
//! - Complex expressions in VALUES (functions, casts, subqueries)
//! - System namespace tables
//! - Columns with DEFAULT expressions omitted from INSERT column list
//! - Any parse failure

use crate::error::KalamDbError;
use crate::schema_registry::SchemaRegistry;
use crate::sql::{ExecutionContext, ExecutionResult};
use datafusion::scalar::ScalarValue;
use kalamdb_commons::models::rows::row::Row;
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::TableId;
use kalamdb_tables::KalamTableProvider;
use sqlparser::ast::{Expr, SetExpr, Statement};
use std::collections::BTreeMap;
use std::sync::Arc;

use super::helpers::ast_parsing;

/// Attempt a fast-path INSERT that bypasses DataFusion's optimizer/physical planner.
///
/// Returns `Ok(Some(result))` if the fast path succeeded,
/// `Ok(None)` if the SQL is too complex for the fast path (fall back to DataFusion),
/// or `Err(e)` if the INSERT was valid for fast-path but failed during execution.
pub async fn try_fast_insert(
    statement: &Statement,
    exec_ctx: &ExecutionContext,
    schema_registry: &Arc<SchemaRegistry>,
    prepared_table_id: Option<&TableId>,
    prepared_table_type: Option<TableType>,
) -> Result<Option<ExecutionResult>, KalamDbError> {
    let insert = match statement {
        Statement::Insert(insert) => insert,
        _ => return Ok(None),
    };
    let default_namespace = exec_ctx.default_namespace();

    // 2. Quick bail for features we don't optimize
    if insert.on.is_some() || insert.overwrite {
        return Ok(None);
    }

    // RETURNING clause support: only `RETURNING _seq` or `RETURNING *` is handled
    let returning_seq = if let Some(ref returning) = insert.returning {
        if returning.len() == 1 {
            match &returning[0] {
                sqlparser::ast::SelectItem::Wildcard(_) => true,
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                    Expr::Identifier(ident) => ident.value == "_seq",
                    _ => return Ok(None), // Complex expression — fall back
                },
                _ => return Ok(None),
            }
        } else {
            return Ok(None); // Multiple RETURNING columns — fall back
        }
    } else {
        false
    };

    // 3. Extract table name
    let table_id = match prepared_table_id {
        Some(id) => id.clone(),
        None => match ast_parsing::extract_table_id_from_insert(insert, default_namespace.as_str())
        {
            Some(id) => id,
            None => return Ok(None),
        },
    };

    // 4. Block system namespace DML
    if table_id.namespace_id().is_system_namespace() {
        return Ok(None);
    }

    // 5. Extract VALUES body (reject INSERT ... SELECT, etc.)
    let source = match insert.source.as_ref() {
        Some(source) => source,
        None => return Ok(None),
    };

    if source.with.is_some() || source.order_by.is_some() || source.limit_clause.is_some() {
        return Ok(None);
    }

    let value_rows = match &*source.body {
        SetExpr::Values(values) => &values.rows,
        _ => return Ok(None),
    };

    // 6. Get the KalamTableProvider from the schema registry (already cached there)
    let kalam_provider: Arc<dyn KalamTableProvider> =
        match schema_registry.get_kalam_provider(&table_id) {
            Some(p) => p,
            None => return Ok(None),
        };

    // 6b. Enforce SHARED table access control.
    //     The DataFusion path checks this in SharedTableProvider::insert_into(),
    //     but the fast path bypasses that — so we must check here.
    let is_shared = prepared_table_type == Some(TableType::Shared);
    if is_shared || prepared_table_type.is_none() {
        if let Some(cached) = schema_registry.get(&table_id) {
            let entry = cached.table_entry();
            if entry.table_type == kalamdb_commons::schemas::TableType::Shared {
                let access_level =
                    entry.access_level.unwrap_or(kalamdb_commons::TableAccess::Private);
                let role = exec_ctx.user_role();
                kalamdb_session::permissions::check_shared_table_write_access_level(
                    role,
                    access_level,
                    table_id.namespace_id(),
                    table_id.table_name(),
                )
                .map_err(|e| KalamDbError::PermissionDenied(e.to_string()))?;
            }
        }
    }

    // 7. Determine column names from the provider's schema
    //    KalamTableProvider extends TableProvider, so we can call schema() directly
    let schema = kalam_provider.schema();

    let column_names: Vec<String> = if insert.columns.is_empty() {
        // No columns specified — use all non-system columns from schema
        schema
            .fields()
            .iter()
            .filter(|f| !f.name().starts_with('_'))
            .map(|f| f.name().clone())
            .collect()
    } else {
        insert.columns.iter().map(|ident| ident.value.clone()).collect()
    };

    // 7b. If any schema column with a DEFAULT expression is missing from the
    //     INSERT column list, fall back to DataFusion which evaluates defaults
    //     correctly (SNOWFLAKE_ID(), UUID_V7(), ULID(), CURRENT_USER(), etc.).
    //     Without this check, coerce_rows fills type-zero values (0, "") instead.
    for field in schema.fields() {
        let col_name = field.name();
        if col_name.starts_with('_') {
            continue; // skip system columns
        }
        if !column_names.iter().any(|c| c == col_name) {
            // Column is missing from the INSERT — check if it has a default
            if kalam_provider.get_column_default(col_name).is_some() {
                return Ok(None); // fall back to DataFusion for default evaluation
            }
        }
    }

    // 8. Convert VALUES to Row objects
    let rows = match values_to_rows(value_rows, &column_names) {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };

    // 9. Call insert directly via KalamTableProvider, bypassing DataFusion
    let user_id = exec_ctx.user_id();
    let row_count = rows.len();
    tracing::debug!(table_id = %table_id, row_count = row_count, "sql.fast_insert");

    if returning_seq {
        // INSERT ... RETURNING _seq: return the generated sequence IDs as rows
        let seq_values = kalam_provider.insert_rows_returning(user_id, rows).await?;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "_seq",
            arrow::datatypes::DataType::Int64,
            false,
        )]));
        let seq_array: arrow::array::Int64Array = seq_values
            .iter()
            .map(|v| match v {
                ScalarValue::Int64(Some(i)) => Some(*i),
                _ => None,
            })
            .collect();
        let batch = arrow::array::RecordBatch::try_new(schema.clone(), vec![Arc::new(seq_array)])
            .map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to create RETURNING batch: {}", e))
        })?;
        let row_count = batch.num_rows();
        Ok(Some(ExecutionResult::Rows {
            batches: vec![batch],
            row_count,
            schema: Some(schema),
        }))
    } else {
        let rows_affected = kalam_provider.insert_rows(user_id, rows).await?;
        Ok(Some(ExecutionResult::Inserted { rows_affected }))
    }
}

/// Convert parsed VALUES rows into Row objects.
fn values_to_rows(
    value_rows: &[Vec<Expr>],
    column_names: &[String],
) -> Result<Vec<Row>, &'static str> {
    let mut rows = Vec::with_capacity(value_rows.len());

    for value_row in value_rows {
        if value_row.len() != column_names.len() {
            return Err("column count mismatch");
        }

        let mut values = BTreeMap::new();
        for (expr, col_name) in value_row.iter().zip(column_names.iter()) {
            let scalar = ast_parsing::expr_to_scalar(expr)?;
            values.insert(col_name.clone(), scalar);
        }
        rows.push(Row::new(values));
    }

    Ok(rows)
}


