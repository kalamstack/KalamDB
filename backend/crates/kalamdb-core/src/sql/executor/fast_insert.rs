//! Fast-path INSERT bypass for simple `INSERT INTO table (cols) VALUES (...)` statements.
//!
//! Bypasses DataFusion's optimizer and physical planner (~2.6ms overhead) by parsing
//! the INSERT SQL directly with sqlparser, resolving table metadata from the schema
//! registry, converting values to Row objects, and routing the mutation through the
//! unified applier when possible.
//!
//! Falls back to DataFusion for:
//! - INSERT ... SELECT (subquery source)
//! - ON CONFLICT / ON DUPLICATE KEY
//! - Complex expressions in VALUES (functions, casts, subqueries)
//! - System namespace tables
//! - Any parse failure

use crate::error::KalamDbError;
use crate::app_context::AppContext;
use crate::schema_registry::SchemaRegistry;
use crate::sql::{ExecutionContext, ExecutionResult};
use chrono::Utc;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::conversions::json_value_to_scalar;
use kalamdb_commons::ids::SnowflakeGenerator;
use kalamdb_commons::models::{OperationKind, TransactionId};
use kalamdb_commons::models::rows::row::Row;
use kalamdb_commons::schemas::{ColumnDefault, TableType};
use kalamdb_commons::TableId;
use kalamdb_tables::KalamTableProvider;
use sqlparser::ast::{Expr, SetExpr, Statement};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use ulid::Ulid;
use uuid::Uuid;

use super::helpers::ast_parsing;
use crate::transactions::StagedMutation;

static FAST_INSERT_SNOWFLAKE_GENERATOR: OnceLock<SnowflakeGenerator> = OnceLock::new();

fn fast_insert_snowflake_generator() -> &'static SnowflakeGenerator {
    FAST_INSERT_SNOWFLAKE_GENERATOR.get_or_init(|| SnowflakeGenerator::new(0))
}

enum PreparedDefaultValue {
    Constant(ScalarValue),
    Volatile(VolatileDefaultFunction),
}

enum VolatileDefaultFunction {
    SnowflakeId,
    UuidV7,
    Ulid,
}

/// Attempt a fast-path INSERT that bypasses DataFusion's optimizer/physical planner.
///
/// Returns `Ok(Some(result))` if the fast path succeeded,
/// `Ok(None)` if the SQL is too complex for the fast path (fall back to DataFusion),
/// or `Err(e)` if the INSERT was valid for fast-path but failed during execution.
pub async fn try_fast_insert(
    statement: &Statement,
    app_context: &AppContext,
    exec_ctx: &ExecutionContext,
    schema_registry: &Arc<SchemaRegistry>,
    prepared_table_id: Option<&TableId>,
    prepared_table_type: Option<TableType>,
    transaction_id: Option<&TransactionId>,
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

    let cached_table = match schema_registry.get(&table_id) {
        Some(cached) => cached,
        None => return Ok(None),
    };

    // 6b. Enforce SHARED table access control.
    //     The DataFusion path checks this in SharedTableProvider::insert_into(),
    //     but the fast path bypasses that — so we must check here.
    let cached_table_entry = cached_table.table_entry();
    let resolved_table_type = prepared_table_type.or(Some(cached_table_entry.table_type));

    if matches!(resolved_table_type, Some(TableType::Shared)) || prepared_table_type.is_none() {
        if cached_table_entry.table_type == kalamdb_commons::schemas::TableType::Shared {
            let access_level =
                cached_table_entry.access_level.unwrap_or(kalamdb_commons::TableAccess::Private);
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

    if !insert.columns.is_empty() {
        for column_name in &column_names {
            let exists = schema
                .fields()
                .iter()
                .any(|field| !field.name().starts_with('_') && field.name() == column_name.as_str());
            if !exists {
                return Err(KalamDbError::InvalidOperation(format!(
                    "Column '{}' does not exist",
                    column_name
                )));
            }
        }
    }

    // 7b. Materialize missing DEFAULT columns here so cluster writes still go
    //     through the applier/Raft path instead of falling back to local
    //     DataFusion provider.insert_into() execution.
    let mut missing_defaults = Vec::new();
    for column in &cached_table.table.columns {
        let col_name = &column.column_name;
        if col_name.starts_with('_') {
            continue; // skip system columns
        }
        if !column_names.iter().any(|c| c == col_name) && !column.default_value.is_none() {
            missing_defaults.push((col_name.clone(), column.default_value.clone()));
        }
    }

    // 8. Convert VALUES to Row objects
    let mut rows = match values_to_rows(value_rows, &column_names) {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };

    if !missing_defaults.is_empty() {
        apply_missing_defaults(&mut rows, &missing_defaults, exec_ctx)?;
    }

    // 9. Route the mutation through the unified applier when the table type is known.
    let user_id = exec_ctx.user_id();
    let row_count = rows.len();
    tracing::debug!(table_id = %table_id, row_count = row_count, "sql.fast_insert");

    if transaction_id.is_none() && !returning_seq {
        if let Some(table_type) = resolved_table_type {
            tracing::debug!(table_id = %table_id, row_count = row_count, table_type = %table_type, "sql.fast_insert_applier");
            let rows_affected = match table_type {
                TableType::User | TableType::Stream => app_context
                    .applier()
                    .insert_user_data(table_id.clone(), exec_ctx.user_id().clone(), rows)
                    .await?
                    .rows_affected(),
                TableType::Shared => app_context
                    .applier()
                    .insert_shared_data(table_id.clone(), rows)
                    .await?
                    .rows_affected(),
                TableType::System => return Ok(None),
            };

            return Ok(Some(ExecutionResult::Inserted { rows_affected }));
        }
    }

    if returning_seq {
        if transaction_id.is_some() {
            return Err(KalamDbError::InvalidOperation(
                "INSERT ... RETURNING is not supported inside explicit SQL transactions"
                    .to_string(),
            ));
        }
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
    } else if let Some(transaction_id) = transaction_id {
        let table_type = resolved_table_type.ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "could not resolve table type for '{}' while staging INSERT",
                table_id
            ))
        })?;
        let pk_columns = cached_table.table.get_primary_key_columns();
        if pk_columns.len() != 1 {
            return Ok(None);
        }
        let pk_column = pk_columns[0];

        for row in rows {
            let primary_key = row.values.get(pk_column).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "transactional INSERT requires primary key column '{}'",
                    pk_column
                ))
            })?;
            let mutation = StagedMutation::new(
                transaction_id.clone(),
                table_id.clone(),
                table_type,
                match table_type {
                    TableType::User | TableType::Stream => Some(exec_ctx.user_id().clone()),
                    TableType::Shared | TableType::System => None,
                },
                OperationKind::Insert,
                primary_key.to_string(),
                row,
                false,
            );
            app_context
                .transaction_coordinator()
                .stage(transaction_id, mutation)?;
        }

        Ok(Some(ExecutionResult::Inserted {
            rows_affected: row_count,
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

fn apply_missing_defaults(
    rows: &mut [Row],
    missing_defaults: &[(String, ColumnDefault)],
    exec_ctx: &ExecutionContext,
) -> Result<(), KalamDbError> {
    if rows.is_empty() || missing_defaults.is_empty() {
        return Ok(());
    }

    let mut prepared_defaults = Vec::with_capacity(missing_defaults.len());
    for (col_name, default_value) in missing_defaults {
        prepared_defaults.push((
            col_name.clone(),
            prepare_default_value(default_value, exec_ctx)?,
        ));
    }

    for row in rows.iter_mut() {
        for (col_name, prepared_default) in &prepared_defaults {
            let scalar = materialize_prepared_default(prepared_default, exec_ctx)?;
            row.values.insert(col_name.clone(), scalar);
        }
    }

    Ok(())
}

fn prepare_default_value(
    default_value: &ColumnDefault,
    exec_ctx: &ExecutionContext,
) -> Result<PreparedDefaultValue, KalamDbError> {
    match default_value {
        ColumnDefault::None => Err(KalamDbError::InvalidOperation(
            "Missing default value metadata for omitted insert column".to_string(),
        )),
        ColumnDefault::Literal(json) => Ok(PreparedDefaultValue::Constant(json_value_to_scalar(json))),
        ColumnDefault::FunctionCall { name, args } => {
            if !args.is_empty() {
                return Err(KalamDbError::InvalidOperation(format!(
                    "Default function '{}' with arguments is not supported in fast INSERT",
                    name
                )));
            }

            match name.to_uppercase().as_str() {
                "NOW" | "CURRENT_TIMESTAMP" => Ok(PreparedDefaultValue::Constant(
                    ScalarValue::TimestampMillisecond(Some(Utc::now().timestamp_millis()), None),
                )),
                "CURRENT_USER" => {
                    let username = exec_ctx.username().ok_or_else(|| {
                        KalamDbError::InvalidOperation(
                            "CURRENT_USER() default requires an authenticated username"
                                .to_string(),
                        )
                    })?;
                    Ok(PreparedDefaultValue::Constant(ScalarValue::Utf8(Some(
                        username.to_string(),
                    ))))
                },
                "SNOWFLAKE_ID" => Ok(PreparedDefaultValue::Volatile(
                    VolatileDefaultFunction::SnowflakeId,
                )),
                "UUID_V7" => Ok(PreparedDefaultValue::Volatile(
                    VolatileDefaultFunction::UuidV7,
                )),
                "ULID" => Ok(PreparedDefaultValue::Volatile(VolatileDefaultFunction::Ulid)),
                other => Err(KalamDbError::InvalidOperation(format!(
                    "Unsupported default function '{}' in fast INSERT",
                    other
                ))),
            }
        },
    }
}

fn materialize_prepared_default(
    prepared_default: &PreparedDefaultValue,
    _exec_ctx: &ExecutionContext,
) -> Result<ScalarValue, KalamDbError> {
    match prepared_default {
        PreparedDefaultValue::Constant(value) => Ok(value.clone()),
        PreparedDefaultValue::Volatile(VolatileDefaultFunction::SnowflakeId) => {
            let id = fast_insert_snowflake_generator().next_id().map_err(|e| {
                KalamDbError::InvalidOperation(format!(
                    "Failed to generate SNOWFLAKE_ID() default value: {}",
                    e
                ))
            })?;
            Ok(ScalarValue::Int64(Some(id)))
        },
        PreparedDefaultValue::Volatile(VolatileDefaultFunction::UuidV7) => {
            Ok(ScalarValue::Utf8(Some(Uuid::now_v7().to_string())))
        },
        PreparedDefaultValue::Volatile(VolatileDefaultFunction::Ulid) => {
            Ok(ScalarValue::Utf8(Some(Ulid::new().to_string())))
        },
    }
}
