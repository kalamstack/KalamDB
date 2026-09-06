use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, OnceLock},
};

use chrono::Utc;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::{
    conversions::{arrow_json_conversion::coerce_rows, json_value_to_scalar},
    ids::SnowflakeGenerator,
    models::{rows::row::Row, OperationKind, TransactionId, UserId},
    schemas::{ColumnDefault, TableType},
    PolicyCommand, Role, TableId,
};
use kalamdb_sql::{
    expr_to_scalar_with_params, insert_columns_match, is_default_expr,
    on_conflict_update_should_apply, on_conflict_values_insert,
    parse_on_conflict_action_with_params, validate_primary_key_conflict_target,
    OnConflictUpdateAssignment, OnConflictUpdateValue, ParsedOnConflictAction,
    ValuesInsertShapeOptions, ValuesInsertView,
};
use kalamdb_tables::SharedTableProvider;
use kalamdb_transactions::{build_insert_staged_mutations, StagedMutation};
use sqlparser::ast::{Expr, Parens, SelectItem, Statement};
use ulid::Ulid;
use uuid::Uuid;

use crate::{
    app_context::AppContext,
    error::KalamDbError,
    functions::FunctionService,
    schema_registry::CachedTableData,
    sql::{
        plan_cache::{
            FastInsertDefaultEntry, FastInsertDefaultTemplate, FastInsertMetadata,
            InsertMetadataCacheKey, SqlCacheRegistry,
        },
        ExecutionContext,
    },
};

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
        available_columns.iter().map(|column| (*column).to_string()).collect()
    } else {
        for column_name in requested_columns {
            if !available_columns.iter().any(|candidate| *candidate == column_name.as_str()) {
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
    Procedure(kalamdb_commons::RoutineCall),
}

enum VolatileDefaultFunction {
    SnowflakeId,
    UuidV7,
    Ulid,
}

enum InsertValuesRowsError {
    Unsupported,
    Execution(KalamDbError),
}

impl From<KalamDbError> for InsertValuesRowsError {
    fn from(error: KalamDbError) -> Self {
        Self::Execution(error)
    }
}

async fn apply_missing_defaults(
    rows: &mut [Row],
    missing_defaults: &[FastInsertDefaultEntry],
    exec_ctx: &ExecutionContext,
    app_context: Arc<AppContext>,
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
            let scalar =
                materialize_prepared_default(prepared_default, exec_ctx, &app_context).await?;
            row.values.insert(col_name.clone(), scalar);
        }
    }

    Ok(())
}

async fn try_build_insert_rows_from_values_rows(
    value_rows: &[Parens<Vec<Expr>>],
    insert_metadata: &FastInsertMetadata,
    cached_table: &CachedTableData,
    exec_ctx: &ExecutionContext,
    params: &[ScalarValue],
    app_context: Arc<AppContext>,
) -> Result<Option<Vec<Row>>, KalamDbError> {
    let mut rows = match build_insert_rows_from_values_rows(
        value_rows,
        &insert_metadata.column_names,
        cached_table,
        exec_ctx,
        params,
        Arc::clone(&app_context),
    )
    .await
    {
        Ok(rows) => rows,
        Err(InsertValuesRowsError::Unsupported) => return Ok(None),
        Err(InsertValuesRowsError::Execution(error)) => return Err(error),
    };

    if !insert_metadata.missing_defaults.is_empty() {
        apply_missing_defaults(&mut rows, &insert_metadata.missing_defaults, exec_ctx, app_context)
            .await?;
    }

    Ok(Some(rows))
}

async fn build_insert_rows_from_values_rows(
    value_rows: &[Parens<Vec<Expr>>],
    column_names: &[String],
    cached_table: &CachedTableData,
    exec_ctx: &ExecutionContext,
    params: &[ScalarValue],
    app_context: Arc<AppContext>,
) -> Result<Vec<Row>, InsertValuesRowsError> {
    let mut rows = Vec::with_capacity(value_rows.len());
    let mut explicit_default_values = BTreeMap::new();

    for value_row in value_rows {
        if value_row.content.len() != column_names.len() {
            return Err(InsertValuesRowsError::Unsupported);
        }

        let mut values = BTreeMap::new();
        for (expr, column_name) in value_row.content.iter().zip(column_names.iter()) {
            values.insert(
                column_name.clone(),
                insert_value_expr_to_scalar(
                    expr,
                    column_name,
                    cached_table,
                    exec_ctx,
                    params,
                    &mut explicit_default_values,
                    &app_context,
                )
                .await?,
            );
        }
        rows.push(Row::new(values));
    }

    Ok(rows)
}

async fn insert_value_expr_to_scalar(
    expr: &Expr,
    column_name: &str,
    cached_table: &CachedTableData,
    exec_ctx: &ExecutionContext,
    params: &[ScalarValue],
    explicit_default_values: &mut BTreeMap<String, PreparedDefaultValue>,
    app_context: &Arc<AppContext>,
) -> Result<ScalarValue, InsertValuesRowsError> {
    if is_default_expr(expr) {
        return materialize_explicit_default(
            column_name,
            cached_table,
            exec_ctx,
            explicit_default_values,
            app_context,
        )
        .await
        .map_err(InsertValuesRowsError::Execution);
    }

    expr_to_scalar_with_params(expr, params).map_err(|_| InsertValuesRowsError::Unsupported)
}

async fn materialize_explicit_default(
    column_name: &str,
    cached_table: &CachedTableData,
    exec_ctx: &ExecutionContext,
    explicit_default_values: &mut BTreeMap<String, PreparedDefaultValue>,
    app_context: &Arc<AppContext>,
) -> Result<ScalarValue, KalamDbError> {
    let prepared_default = match explicit_default_values.entry(column_name.to_string()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            entry.insert(prepare_explicit_default(column_name, cached_table, exec_ctx)?)
        },
    };
    materialize_prepared_default(prepared_default, exec_ctx, app_context).await
}

fn prepare_explicit_default(
    column_name: &str,
    cached_table: &CachedTableData,
    exec_ctx: &ExecutionContext,
) -> Result<PreparedDefaultValue, KalamDbError> {
    let column = cached_table
        .table
        .columns
        .iter()
        .find(|column| column.column_name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| {
            KalamDbError::InvalidOperation(format!("Column '{}' does not exist", column_name))
        })?;

    if column.default_value.is_none() {
        return Ok(PreparedDefaultValue::Constant(ScalarValue::Null));
    }

    let template = prepare_default_template(&column.default_value)?;
    prepare_statement_default(&template, exec_ctx)
}

pub(crate) struct LiteralInsertRows {
    pub table_type:         TableType,
    pub primary_key_column: Option<String>,
    pub rows:               Vec<Row>,
    pub returning:          Option<Vec<SelectItem>>,
}

pub(crate) struct LiteralOnConflictUpdateRows {
    pub table_type:         TableType,
    pub primary_key_column: String,
    pub rows:               Vec<Row>,
    pub action:             ParsedOnConflictAction,
    pub returning:          Option<Vec<SelectItem>>,
}

pub(crate) struct OnConflictStagedMutation {
    pub mutation:     StagedMutation,
    pub returned_row: Row,
}

pub(crate) async fn try_build_literal_insert_rows(
    statement: &Statement,
    app_context: Arc<AppContext>,
    sql_cache_registry: &SqlCacheRegistry,
    exec_ctx: &ExecutionContext,
    table_id: &TableId,
    params: &[ScalarValue],
) -> Result<Option<LiteralInsertRows>, KalamDbError> {
    try_build_literal_insert_rows_inner(
        statement,
        app_context,
        sql_cache_registry,
        exec_ctx,
        table_id,
        false,
        params,
    )
    .await
}

async fn try_build_literal_insert_rows_inner(
    statement: &Statement,
    app_context: Arc<AppContext>,
    sql_cache_registry: &SqlCacheRegistry,
    exec_ctx: &ExecutionContext,
    table_id: &TableId,
    allow_on_conflict: bool,
    params: &[ScalarValue],
) -> Result<Option<LiteralInsertRows>, KalamDbError> {
    if table_id.namespace_id().is_system_namespace() {
        return Ok(None);
    }

    let shape_options = if allow_on_conflict {
        ValuesInsertShapeOptions::ON_CONFLICT_ROWS
    } else {
        ValuesInsertShapeOptions::PLAIN
    };
    let Some(view) = kalamdb_sql::values_insert_view_from_statement(statement, shape_options)
    else {
        return Ok(None);
    };

    build_literal_insert_rows_from_values(
        view,
        app_context,
        sql_cache_registry,
        exec_ctx,
        table_id,
        !allow_on_conflict,
        params,
    )
    .await
}

async fn build_literal_insert_rows_from_values(
    view: ValuesInsertView<'_>,
    app_context: Arc<AppContext>,
    sql_cache_registry: &SqlCacheRegistry,
    exec_ctx: &ExecutionContext,
    table_id: &TableId,
    include_returning: bool,
    params: &[ScalarValue],
) -> Result<Option<LiteralInsertRows>, KalamDbError> {
    let insert = view.insert;
    let value_rows = view.value_rows;

    let cached_table = match app_context.schema_registry().get(table_id) {
        Some(cached) => cached,
        None => return Ok(None),
    };

    let cached_table_entry = cached_table.table_entry();
    if cached_table_entry.table_type == TableType::Shared
        && matches!(exec_ctx.user_role(), Role::Anonymous)
    {
        return Err(KalamDbError::PermissionDenied(
            "Anonymous shared-table writes are denied".to_string(),
        ));
    }

    let requested_columns: Vec<String> =
        insert.columns.iter().filter_map(kalamdb_sql::object_name_to_string).collect();
    let metadata_cache_key =
        InsertMetadataCacheKey::new(table_id.clone(), requested_columns.clone());
    let insert_metadata = match sql_cache_registry.insert_metadata_cache().get(&metadata_cache_key)
    {
        Some(metadata) => metadata,
        None => {
            let metadata =
                Arc::new(build_insert_metadata(&requested_columns, cached_table.as_ref())?);
            sql_cache_registry
                .insert_metadata_cache()
                .insert_arc(metadata_cache_key, Arc::clone(&metadata));
            metadata
        },
    };

    let rows = match try_build_insert_rows_from_values_rows(
        value_rows,
        &insert_metadata,
        cached_table.as_ref(),
        exec_ctx,
        params,
        Arc::clone(&app_context),
    )
    .await?
    {
        Some(rows) => rows,
        None => return Ok(None),
    };

    let schema = cached_table.arrow_schema()?;
    let rows = coerce_rows(rows, &schema)
        .map_err(|e| KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e)))?;

    Ok(Some(LiteralInsertRows {
        table_type: insert_metadata.table_type,
        primary_key_column: insert_metadata.primary_key_column.clone(),
        rows,
        returning: if include_returning {
            insert.returning.clone()
        } else {
            None
        },
    }))
}

pub(crate) async fn try_build_literal_on_conflict_update_rows(
    statement: &Statement,
    app_context: Arc<AppContext>,
    sql_cache_registry: &SqlCacheRegistry,
    exec_ctx: &ExecutionContext,
    table_id: &TableId,
    params: &[ScalarValue],
) -> Result<Option<LiteralOnConflictUpdateRows>, KalamDbError> {
    let Some((view, on_conflict)) = on_conflict_values_insert(statement) else {
        return Ok(None);
    };

    let insert_rows = match build_literal_insert_rows_from_values(
        view,
        app_context,
        sql_cache_registry,
        exec_ctx,
        table_id,
        false,
        params,
    )
    .await?
    {
        Some(rows) => rows,
        None => return Ok(None),
    };

    let primary_key_column = insert_rows.primary_key_column.ok_or_else(|| {
        KalamDbError::InvalidOperation(
            "ON CONFLICT requires a single primary key column".to_string(),
        )
    })?;

    validate_primary_key_conflict_target(on_conflict, &primary_key_column)
        .map_err(KalamDbError::InvalidOperation)?;

    let action = parse_on_conflict_action_with_params(on_conflict, params)
        .map_err(KalamDbError::InvalidOperation)?;

    Ok(Some(LiteralOnConflictUpdateRows {
        table_type: insert_rows.table_type,
        primary_key_column,
        rows: insert_rows.rows,
        action,
        returning: view.insert.returning.clone(),
    }))
}

const SYSTEM_TABLE_DML_DENIED: &str = "cannot insert into system tables";

pub(crate) fn reject_system_table_dml(table_type: TableType) -> Result<(), KalamDbError> {
    if table_type == TableType::System {
        return Err(KalamDbError::PermissionDenied(SYSTEM_TABLE_DML_DENIED.to_string()));
    }
    Ok(())
}

pub(crate) struct OnConflictUserIds {
    pub lookup_user_id:           Option<UserId>,
    pub default_mutation_user_id: Option<UserId>,
}

pub(crate) fn on_conflict_user_ids(
    table_type: TableType,
    exec_ctx: &ExecutionContext,
) -> Result<OnConflictUserIds, KalamDbError> {
    reject_system_table_dml(table_type)?;
    let user_id = exec_ctx.user_id().clone();
    Ok(match table_type {
        TableType::User | TableType::Stream => OnConflictUserIds {
            lookup_user_id:           Some(user_id.clone()),
            default_mutation_user_id: Some(user_id),
        },
        TableType::Shared => OnConflictUserIds {
            lookup_user_id:           Some(user_id),
            default_mutation_user_id: None,
        },
        TableType::System => unreachable!("system tables rejected above"),
    })
}

pub(crate) fn build_on_conflict_staged_mutation_for_action(
    action: &ParsedOnConflictAction,
    transaction_id: &TransactionId,
    table_id: &TableId,
    table_type: TableType,
    mutation_user_id: Option<UserId>,
    primary_key: String,
    inserted_row: &Row,
    existing_row: Option<&Row>,
) -> Result<Option<OnConflictStagedMutation>, KalamDbError> {
    match action {
        ParsedOnConflictAction::DoNothing => {
            if existing_row.is_some() {
                return Ok(None);
            }
            Ok(Some(build_on_conflict_staged_mutation(
                transaction_id,
                table_id,
                table_type,
                mutation_user_id,
                primary_key,
                inserted_row,
                &[],
                None,
            )?))
        },
        ParsedOnConflictAction::DoUpdate {
            assignments,
            where_clause,
        } => {
            if existing_row.is_some()
                && !on_conflict_update_should_apply(where_clause)
                    .map_err(KalamDbError::InvalidOperation)?
            {
                return Ok(None);
            }
            Ok(Some(build_on_conflict_staged_mutation(
                transaction_id,
                table_id,
                table_type,
                mutation_user_id,
                primary_key,
                inserted_row,
                assignments,
                existing_row,
            )?))
        },
    }
}

pub(crate) fn build_on_conflict_staged_mutation(
    transaction_id: &TransactionId,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<UserId>,
    primary_key: String,
    inserted_row: &Row,
    assignments: &[OnConflictUpdateAssignment],
    existing_row: Option<&Row>,
) -> Result<OnConflictStagedMutation, KalamDbError> {
    if let Some(existing_row) = existing_row {
        let mut updates = BTreeMap::new();
        for assignment in assignments {
            let value = match &assignment.value {
                OnConflictUpdateValue::InsertedColumn(column_name) => {
                    inserted_row.values.get(column_name).cloned().ok_or_else(|| {
                        KalamDbError::InvalidOperation(format!(
                            "EXCLUDED.{} does not exist in the inserted row",
                            column_name
                        ))
                    })?
                },
                OnConflictUpdateValue::Literal(value) => value.clone(),
            };
            updates.insert(assignment.column_name.clone(), value);
        }

        let mut returned_values = existing_row.values.clone();
        for (column_name, value) in &updates {
            returned_values.insert(column_name.clone(), value.clone());
        }

        Ok(OnConflictStagedMutation {
            mutation:     StagedMutation::new(
                transaction_id.clone(),
                table_id.clone(),
                table_type,
                user_id,
                OperationKind::Update,
                primary_key,
                Row::new(updates),
                false,
            ),
            returned_row: Row::new(returned_values),
        })
    } else {
        Ok(OnConflictStagedMutation {
            mutation:     StagedMutation::new(
                transaction_id.clone(),
                table_id.clone(),
                table_type,
                user_id,
                OperationKind::Insert,
                primary_key,
                inserted_row.clone(),
                false,
            ),
            returned_row: inserted_row.clone(),
        })
    }
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
        ColumnDefault::FunctionCall(call) => {
            if call.has_placeholder() {
                return Err(KalamDbError::InvalidOperation(
                    "DEFAULT procedure arguments cannot use placeholders".to_string(),
                ));
            }
            if let Some(builtin) = call.scalar_udf_name() {
                match call.unqualified_name() {
                    "now" | "current_timestamp" => Ok(FastInsertDefaultTemplate::CurrentTimestamp),
                    "current_user" => Ok(FastInsertDefaultTemplate::CurrentUser),
                    "snowflake_id" | "auto_increment" => Ok(FastInsertDefaultTemplate::SnowflakeId),
                    "uuid_v7" => Ok(FastInsertDefaultTemplate::UuidV7),
                    "ulid" => Ok(FastInsertDefaultTemplate::Ulid),
                    _ => Err(KalamDbError::InvalidOperation(format!(
                        "Unsupported default function '{builtin}' in transaction batch INSERT"
                    ))),
                }
            } else {
                Ok(FastInsertDefaultTemplate::Procedure(call.clone()))
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
            ScalarValue::TimestampMicrosecond(Some(Utc::now().timestamp_micros()), None),
        )),
        FastInsertDefaultTemplate::CurrentUser => {
            let user_id = exec_ctx.user_id();
            Ok(PreparedDefaultValue::Constant(ScalarValue::Utf8(Some(
                user_id.as_str().to_string(),
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
        FastInsertDefaultTemplate::Procedure(call) => {
            Ok(PreparedDefaultValue::Procedure(call.clone()))
        },
    }
}

async fn materialize_prepared_default(
    prepared_default: &PreparedDefaultValue,
    exec_ctx: &ExecutionContext,
    app_context: &Arc<AppContext>,
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
            Ok(ScalarValue::Utf8(Some(Ulid::generate().to_string())))
        },
        PreparedDefaultValue::Procedure(call) => {
            FunctionService::execute_routine_call(Arc::clone(app_context), exec_ctx, call).await
        },
    }
}

/// Batch-process multiple INSERT statements for the same table in an active transaction.
///
/// Resolves table metadata once for the batch, converts all VALUES rows to staged
/// mutations, and submits them with a single `stage_batch()` call.
pub(crate) async fn try_batch_inserts_in_transaction(
    statements: &[&Statement],
    app_context: Arc<AppContext>,
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

    let first_insert = match kalamdb_sql::insert_from_statement(statements[0]) {
        Some(insert) => insert,
        None => return Ok(None),
    };

    let cached_table = match app_context.schema_registry().get(table_id) {
        Some(cached) => cached,
        None => return Ok(None),
    };

    let cached_table_entry = cached_table.table_entry();
    let table_type = cached_table_entry.table_type;
    if table_type == TableType::Shared && matches!(exec_ctx.user_role(), Role::Anonymous) {
        return Err(KalamDbError::PermissionDenied(
            "Anonymous shared-table writes are denied".to_string(),
        ));
    }

    let requested_columns: Vec<String> = first_insert
        .columns
        .iter()
        .filter_map(kalamdb_sql::object_name_to_string)
        .collect();
    let metadata_cache_key =
        InsertMetadataCacheKey::new(table_id.clone(), requested_columns.clone());
    let insert_metadata = match sql_cache_registry.insert_metadata_cache().get(&metadata_cache_key)
    {
        Some(metadata) => metadata,
        None => {
            let metadata =
                Arc::new(build_insert_metadata(&requested_columns, cached_table.as_ref())?);
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

    let schema = cached_table.arrow_schema()?;
    let user_id = staged_mutation_user_id(table_type, exec_ctx);
    let mut all_rows = Vec::with_capacity(statements.len());
    let mut per_statement_counts = Vec::with_capacity(statements.len());
    let mut total_rows = 0usize;

    for statement in statements {
        if !insert_columns_match(statement, &requested_columns) {
            return Ok(None);
        }

        let Some(view) = kalamdb_sql::values_insert_view_from_statement(
            statement,
            ValuesInsertShapeOptions::BATCH,
        ) else {
            return Ok(None);
        };

        let rows = match try_build_insert_rows_from_values_rows(
            view.value_rows,
            &insert_metadata,
            cached_table.as_ref(),
            exec_ctx,
            &[],
            Arc::clone(&app_context),
        )
        .await?
        {
            Some(rows) => rows,
            None => return Ok(None),
        };

        per_statement_counts.push(rows.len());
        total_rows += rows.len();
        all_rows.extend(rows);
    }

    let all_rows = coerce_rows(all_rows, &schema)
        .map_err(|e| KalamDbError::InvalidOperation(format!("Schema coercion failed: {}", e)))?;
    if table_type == TableType::Shared {
        let provider = cached_table.get_provider().ok_or_else(|| {
            KalamDbError::InvalidOperation(format!("Shared table provider not found: {table_id}"))
        })?;
        let provider = (provider.as_ref() as &dyn std::any::Any)
            .downcast_ref::<SharedTableProvider>()
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Shared table provider type mismatch: {table_id}"
                ))
            })?;
        let snapshot_commit_seq = app_context
            .transaction_coordinator()
            .get_handle(transaction_id)
            .map(|handle| handle.snapshot_commit_seq);
        provider
            .check_rows_authorized(
                exec_ctx.user_id(),
                exec_ctx.user_role(),
                PolicyCommand::Insert,
                true,
                &all_rows,
                snapshot_commit_seq,
            )
            .await?;
    }
    let all_mutations = build_insert_staged_mutations(
        transaction_id,
        table_id,
        table_type,
        user_id,
        pk_column,
        all_rows,
    )
    .map_err(|error| KalamDbError::InvalidOperation(error.to_string()))?;

    app_context
        .transaction_coordinator()
        .stage_batch(transaction_id, all_mutations)?;

    tracing::debug!(
        table_id = %table_id,
        statements = statements.len(),
        total_rows,
        "sql.transaction_batch_insert"
    );

    Ok(Some(per_statement_counts))
}
