use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::schema_registry::CachedTableData;
use crate::sql::plan_cache::{
    FastInsertDefaultEntry, FastInsertDefaultTemplate, FastInsertMetadata,
    InsertMetadataCacheKey, SqlCacheRegistry,
};
use crate::sql::ExecutionContext;
use chrono::Utc;
use kalamdb_commons::conversions::arrow_json_conversion::coerce_rows;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::conversions::json_value_to_scalar;
use kalamdb_commons::ids::SnowflakeGenerator;
use kalamdb_commons::models::rows::row::Row;
use kalamdb_commons::models::{TransactionId, UserId};
use kalamdb_commons::schemas::{ColumnDefault, TableType};
use kalamdb_commons::TableId;
use kalamdb_transactions::build_insert_staged_mutations;
use sqlparser::ast::{Expr, SetExpr, Statement};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use ulid::Ulid;
use uuid::Uuid;

use super::helpers::ast_parsing;

static INSERT_DEFAULT_SNOWFLAKE_GENERATOR: OnceLock<SnowflakeGenerator> = OnceLock::new();

fn insert_default_snowflake_generator() -> &'static SnowflakeGenerator {
    INSERT_DEFAULT_SNOWFLAKE_GENERATOR.get_or_init(|| SnowflakeGenerator::new(0))
}

fn build_insert_metadata(
    requested_columns: &[String],
    cached_table: &CachedTableData,
) -> Result<FastInsertMetadata, KalamDbError> {
    let available_columns: Vec<&str> = cached_table
        .table
        .columns
        .iter()
        .filter(|column| !column.column_name.starts_with('_'))
        .map(|column| column.column_name.as_str())
        .collect();

    let column_names = if requested_columns.is_empty() {
        available_columns
            .iter()
            .map(|column| (*column).to_string())
            .collect()
    } else {
        for column_name in requested_columns {
            if !available_columns
                .iter()
                .any(|candidate| *candidate == column_name.as_str())
            {
                return Err(KalamDbError::InvalidOperation(format!(
                    "Column '{}' does not exist",
                    column_name
                )));
            }
        }
        requested_columns.to_vec()
    };

    let missing_defaults = cached_table
        .table
        .columns
        .iter()
        .filter(|column| !column.column_name.starts_with('_'))
        .filter(|column| !column_names.iter().any(|name| name == &column.column_name))
        .filter(|column| !column.default_value.is_none())
        .map(|column| {
            Ok(FastInsertDefaultEntry::new(
                column.column_name.clone(),
                prepare_default_template(&column.default_value)?,
            ))
        })
        .collect::<Result<Vec<_>, KalamDbError>>()?;

    let pk_columns = cached_table.table.get_primary_key_columns();

    Ok(FastInsertMetadata {
        table_type: cached_table.table.table_type.into(),
        column_names,
        missing_defaults,
        primary_key_column: if pk_columns.len() == 1 {
            Some(pk_columns[0].to_string())
        } else {
            None
        },
    })
}

fn staged_mutation_user_id(table_type: TableType, exec_ctx: &ExecutionContext) -> Option<UserId> {
    match table_type {
        TableType::User | TableType::Stream => Some(exec_ctx.user_id().clone()),
        TableType::Shared | TableType::System => None,
    }
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
    missing_defaults: &[FastInsertDefaultEntry],
    exec_ctx: &ExecutionContext,
) -> Result<(), KalamDbError> {
    if rows.is_empty() || missing_defaults.is_empty() {
        return Ok(());
    }

    let mut prepared_defaults = Vec::with_capacity(missing_defaults.len());
    for default_entry in missing_defaults {
        prepared_defaults.push((
            default_entry.column_name.clone(),
            prepare_statement_default(&default_entry.template, exec_ctx)?,
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

pub(crate) struct LiteralInsertRows {
    pub table_type: TableType,
    pub rows: Vec<Row>,
}

pub(crate) fn try_build_literal_insert_rows(
    statement: &Statement,
    app_context: &AppContext,
    sql_cache_registry: &SqlCacheRegistry,
    exec_ctx: &ExecutionContext,
    table_id: &TableId,
) -> Result<Option<LiteralInsertRows>, KalamDbError> {
    if table_id.namespace_id().is_system_namespace() {
        return Ok(None);
    }

    let insert = match statement {
        Statement::Insert(insert) => insert,
        _ => return Ok(None),
    };

    if insert.on.is_some() || insert.overwrite || insert.returning.is_some() {
        return Ok(None);
    }

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

    let cached_table = match app_context.schema_registry().get(table_id) {
        Some(cached) => cached,
        None => return Ok(None),
    };

    let cached_table_entry = cached_table.table_entry();
    if cached_table_entry.table_type == kalamdb_commons::schemas::TableType::Shared {
        let access_level =
            cached_table_entry.access_level.unwrap_or(kalamdb_commons::TableAccess::Private);
        kalamdb_session::permissions::check_shared_table_write_access_level(
            exec_ctx.user_role(),
            access_level,
            table_id.namespace_id(),
            table_id.table_name(),
        )
        .map_err(|e| KalamDbError::PermissionDenied(e.to_string()))?;
    }

    let requested_columns: Vec<String> =
        insert.columns.iter().map(|ident| ident.value.clone()).collect();
    let metadata_cache_key = InsertMetadataCacheKey::new(table_id.clone(), requested_columns.clone());
    let insert_metadata = match sql_cache_registry.insert_metadata_cache().get(&metadata_cache_key)
    {
        Some(metadata) => metadata,
        None => {
            let metadata = Arc::new(build_insert_metadata(&requested_columns, cached_table.as_ref())?);
            sql_cache_registry
                .insert_metadata_cache()
                .insert_arc(metadata_cache_key, Arc::clone(&metadata));
            metadata
        },
    };

    let mut rows = match values_to_rows(value_rows, &insert_metadata.column_names) {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };

    if !insert_metadata.missing_defaults.is_empty() {
        apply_missing_defaults(&mut rows, &insert_metadata.missing_defaults, exec_ctx)?;
    }

    let schema = cached_table.arrow_schema()?;
    let rows = coerce_rows(rows, &schema)
        .map_err(|e| KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e)))?;

    Ok(Some(LiteralInsertRows {
        table_type: insert_metadata.table_type,
        rows,
    }))
}

fn prepare_default_template(
    default_value: &ColumnDefault,
) -> Result<FastInsertDefaultTemplate, KalamDbError> {
    match default_value {
        ColumnDefault::None => Err(KalamDbError::InvalidOperation(
            "Missing default value metadata for omitted insert column".to_string(),
        )),
        ColumnDefault::Literal(json) => {
            Ok(FastInsertDefaultTemplate::Literal(json_value_to_scalar(json)))
        },
        ColumnDefault::FunctionCall { name, args } => {
            if !args.is_empty() {
                return Err(KalamDbError::InvalidOperation(format!(
                    "Default function '{}' with arguments is not supported in transaction batch INSERT",
                    name
                )));
            }

            match name.to_uppercase().as_str() {
                "NOW" | "CURRENT_TIMESTAMP" => Ok(FastInsertDefaultTemplate::CurrentTimestamp),
                "CURRENT_USER" => Ok(FastInsertDefaultTemplate::CurrentUser),
                "SNOWFLAKE_ID" => Ok(FastInsertDefaultTemplate::SnowflakeId),
                "UUID_V7" => Ok(FastInsertDefaultTemplate::UuidV7),
                "ULID" => Ok(FastInsertDefaultTemplate::Ulid),
                other => Err(KalamDbError::InvalidOperation(format!(
                    "Unsupported default function '{}' in transaction batch INSERT",
                    other
                ))),
            }
        },
    }
}

fn prepare_statement_default(
    default_template: &FastInsertDefaultTemplate,
    exec_ctx: &ExecutionContext,
) -> Result<PreparedDefaultValue, KalamDbError> {
    match default_template {
        FastInsertDefaultTemplate::Literal(value) => {
            Ok(PreparedDefaultValue::Constant(value.clone()))
        },
        FastInsertDefaultTemplate::CurrentTimestamp => Ok(PreparedDefaultValue::Constant(
            ScalarValue::TimestampMillisecond(Some(Utc::now().timestamp_millis()), None),
        )),
        FastInsertDefaultTemplate::CurrentUser => {
            let username = exec_ctx.username().ok_or_else(|| {
                KalamDbError::InvalidOperation(
                    "CURRENT_USER() default requires an authenticated username".to_string(),
                )
            })?;
            Ok(PreparedDefaultValue::Constant(ScalarValue::Utf8(Some(
                username.to_string(),
            ))))
        },
        FastInsertDefaultTemplate::SnowflakeId => {
            Ok(PreparedDefaultValue::Volatile(VolatileDefaultFunction::SnowflakeId))
        },
        FastInsertDefaultTemplate::UuidV7 => {
            Ok(PreparedDefaultValue::Volatile(VolatileDefaultFunction::UuidV7))
        },
        FastInsertDefaultTemplate::Ulid => {
            Ok(PreparedDefaultValue::Volatile(VolatileDefaultFunction::Ulid))
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
            let id = insert_default_snowflake_generator().next_id().map_err(|e| {
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

/// Batch-process multiple INSERT statements for the same table in an active transaction.
///
/// Resolves table metadata once for the batch, converts all VALUES rows to staged
/// mutations, and submits them with a single `stage_batch()` call.
pub(crate) fn try_batch_inserts_in_transaction(
    statements: &[&Statement],
    app_context: &AppContext,
    sql_cache_registry: &SqlCacheRegistry,
    exec_ctx: &ExecutionContext,
    table_id: &TableId,
    transaction_id: &TransactionId,
) -> Result<Option<Vec<usize>>, KalamDbError> {
    if statements.is_empty() {
        return Ok(Some(vec![]));
    }

    if table_id.namespace_id().is_system_namespace() {
        return Ok(None);
    }

    let first_insert = match statements[0] {
        Statement::Insert(insert) => insert,
        _ => return Ok(None),
    };

    let cached_table = match app_context.schema_registry().get(table_id) {
        Some(cached) => cached,
        None => return Ok(None),
    };

    let cached_table_entry = cached_table.table_entry();
    let table_type = cached_table_entry.table_type;
    if cached_table_entry.table_type == kalamdb_commons::schemas::TableType::Shared {
        let access_level =
            cached_table_entry.access_level.unwrap_or(kalamdb_commons::TableAccess::Private);
        kalamdb_session::permissions::check_shared_table_write_access_level(
            exec_ctx.user_role(),
            access_level,
            table_id.namespace_id(),
            table_id.table_name(),
        )
        .map_err(|e| KalamDbError::PermissionDenied(e.to_string()))?;
    }

    let requested_columns: Vec<String> =
        first_insert.columns.iter().map(|ident| ident.value.clone()).collect();
    let metadata_cache_key = InsertMetadataCacheKey::new(table_id.clone(), requested_columns.clone());
    let insert_metadata = match sql_cache_registry.insert_metadata_cache().get(&metadata_cache_key)
    {
        Some(metadata) => metadata,
        None => {
            let metadata = Arc::new(build_insert_metadata(&requested_columns, cached_table.as_ref())?);
            sql_cache_registry
                .insert_metadata_cache()
                .insert_arc(metadata_cache_key, Arc::clone(&metadata));
            metadata
        },
    };

    let pk_column = match insert_metadata.primary_key_column.as_deref() {
        Some(column) => column,
        None => return Ok(None),
    };

    let user_id = staged_mutation_user_id(table_type, exec_ctx);
    let mut all_mutations = Vec::new();
    let mut per_statement_counts = Vec::with_capacity(statements.len());

    for statement in statements {
        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => return Ok(None),
        };

        if insert.on.is_some() || insert.overwrite || insert.returning.is_some() {
            return Ok(None);
        }

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

        let mut rows = match values_to_rows(value_rows, &insert_metadata.column_names) {
            Ok(rows) => rows,
            Err(_) => return Ok(None),
        };

        if !insert_metadata.missing_defaults.is_empty() {
            apply_missing_defaults(&mut rows, &insert_metadata.missing_defaults, exec_ctx)?;
        }

        rows = coerce_rows(rows, &cached_table.arrow_schema()?).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e))
        })?;

        per_statement_counts.push(rows.len());
        let mutations = build_insert_staged_mutations(
            transaction_id,
            table_id,
            table_type,
            user_id.clone(),
            pk_column,
            rows,
        )
        .map_err(|error| KalamDbError::InvalidOperation(error.to_string()))?;
        all_mutations.extend(mutations);
    }

    app_context
        .transaction_coordinator()
        .stage_batch(transaction_id, all_mutations)?;

    tracing::debug!(
        table_id = %table_id,
        statements = statements.len(),
        total_rows = per_statement_counts.iter().sum::<usize>(),
        "sql.transaction_batch_insert"
    );

    Ok(Some(per_statement_counts))
}