use crate::EmbeddedExtensionState;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::scalar::ScalarValue;
use kalam_pg_common::KalamPgError;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{NamespaceId, UserId};
use kalamdb_commons::Role;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::context::ExecutionResult;
use kalamdb_core::sql::executor::helpers::table_creation::build_table_definition;
use kalamdb_core::sql::ExecutionContext;
use kalamdb_sql::ddl::{CreateNamespaceStatement, CreateTableStatement};
use kalamdb_system::Namespace;
use serde_json::{json, Map, Number, Value};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// SQL-facing embedded service used by PostgreSQL helper functions.
pub struct EmbeddedSqlService {
    extension_state: Arc<EmbeddedExtensionState>,
}

impl EmbeddedSqlService {
    /// Create the service from shared embedded extension state.
    pub fn new(extension_state: Arc<EmbeddedExtensionState>) -> Self {
        Self { extension_state }
    }

    /// Execute SQL against the embedded KalamDB runtime and serialize the result as JSON.
    pub fn execute_json(
        &self,
        sql: &str,
        user_id: Option<UserId>,
        role: Role,
    ) -> Result<Value, KalamPgError> {
        let sql = sql.trim();
        if sql.is_empty() {
            return Err(KalamPgError::Validation("embedded SQL must not be empty".to_string()));
        }

        let runtime = Arc::clone(self.extension_state.runtime());
        let app_context = Arc::clone(runtime.app_context());
        let sql_executor = app_context.sql_executor();
        if let Some(result) = try_execute_direct_ddl(&app_context, sql, user_id.as_ref(), role)? {
            return Ok(result);
        }
        let exec_ctx = execution_context(&app_context, user_id, role);
        let sql_text = sql.to_string();

        runtime.block_on(async move {
            let result = sql_executor
                .execute(&sql_text, &exec_ctx, Vec::new())
                .await
                .map_err(|err| KalamPgError::Execution(err.to_string()))?;
            execution_result_to_json(result)
        })
    }
}

fn try_execute_direct_ddl(
    app_context: &Arc<AppContext>,
    sql: &str,
    user_id: Option<&UserId>,
    role: Role,
) -> Result<Option<Value>, KalamPgError> {
    if !matches!(role, Role::System | Role::Dba) {
        return Ok(None);
    }

    if let Ok(statement) = CreateNamespaceStatement::parse(sql) {
        return create_namespace_direct(app_context, statement).map(Some);
    }

    if let Ok(statement) = CreateTableStatement::parse(sql, "default") {
        let user_id = user_id
            .ok_or_else(|| KalamPgError::Validation("admin DDL requires a user_id".to_string()))?;
        return create_table_direct(app_context, statement, user_id, role).map(Some);
    }

    Ok(None)
}

fn execution_context(
    app_context: &Arc<AppContext>,
    user_id: Option<UserId>,
    role: Role,
) -> ExecutionContext {
    match user_id {
        Some(user_id) => ExecutionContext::new(user_id, role, app_context.base_session_context()),
        None => ExecutionContext::anonymous(app_context.base_session_context()),
    }
}

fn create_namespace_direct(
    app_context: &Arc<AppContext>,
    statement: CreateNamespaceStatement,
) -> Result<Value, KalamPgError> {
    let namespaces_provider = app_context.system_tables().namespaces();
    let namespace_id = NamespaceId::new(statement.name.as_str());

    if namespaces_provider
        .get_namespace(&namespace_id)
        .map_err(|err| KalamPgError::Execution(err.to_string()))?
        .is_some()
    {
        if statement.if_not_exists {
            return Ok(json!({
                "kind": "success",
                "message": format!("Namespace '{}' already exists", namespace_id),
            }));
        }

        return Err(KalamPgError::Validation(format!(
            "Namespace '{}' already exists",
            namespace_id
        )));
    }

    namespaces_provider
        .create_namespace(Namespace {
            namespace_id: namespace_id.clone(),
            name: namespace_id.as_str().to_string(),
            created_at: current_timestamp_millis()?,
            options: Some("{}".to_string()),
            table_count: 0,
        })
        .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    register_namespace_schema(app_context, &namespace_id)?;

    Ok(json!({
        "kind": "success",
        "message": format!("Namespace '{}' created successfully", namespace_id),
    }))
}

fn create_table_direct(
    app_context: &Arc<AppContext>,
    statement: CreateTableStatement,
    user_id: &UserId,
    role: Role,
) -> Result<Value, KalamPgError> {
    register_namespace_schema(app_context, &statement.namespace_id)?;

    let table_definition =
        build_table_definition(Arc::clone(app_context), &statement, user_id, role)
            .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    app_context
        .schema_registry()
        .put(table_definition)
        .map_err(|err| KalamPgError::Execution(err.to_string()))?;

    Ok(json!({
        "kind": "success",
        "message": format!("Table '{}.{}' created successfully", statement.namespace_id, statement.table_name),
    }))
}

fn register_namespace_schema(
    app_context: &Arc<AppContext>,
    namespace_id: &NamespaceId,
) -> Result<(), KalamPgError> {
    let base_session = app_context.base_session_context();
    let catalog = base_session
        .catalog("kalam")
        .ok_or_else(|| KalamPgError::Execution("kalam catalog not found in session".to_string()))?;

    if catalog.schema(namespace_id.as_str()).is_some() {
        return Ok(());
    }

    catalog
        .register_schema(namespace_id.as_str(), Arc::new(MemorySchemaProvider::new()))
        .map_err(|err| KalamPgError::Execution(err.to_string()))?;

    Ok(())
}

fn execution_result_to_json(result: ExecutionResult) -> Result<Value, KalamPgError> {
    match result {
        ExecutionResult::Success { message } => Ok(json!({
            "kind": "success",
            "message": message,
        })),
        ExecutionResult::Rows {
            batches,
            row_count,
            schema,
        } => {
            let columns = schema
                .as_ref()
                .map(|schema| {
                    schema
                        .fields()
                        .iter()
                        .map(|field| Value::String(field.name().clone()))
                        .collect::<Vec<Value>>()
                })
                .unwrap_or_default();
            let rows = materialize_rows(batches)?
                .into_iter()
                .map(|row| row_to_json(&row))
                .collect::<Vec<Value>>();

            Ok(json!({
                "kind": "rows",
                "row_count": row_count,
                "columns": columns,
                "rows": rows,
            }))
        },
        ExecutionResult::Inserted { rows_affected } => Ok(json!({
            "kind": "inserted",
            "rows_affected": rows_affected,
        })),
        ExecutionResult::Updated { rows_affected } => Ok(json!({
            "kind": "updated",
            "rows_affected": rows_affected,
        })),
        ExecutionResult::Deleted { rows_affected } => Ok(json!({
            "kind": "deleted",
            "rows_affected": rows_affected,
        })),
        ExecutionResult::Flushed {
            tables,
            bytes_written,
        } => Ok(json!({
            "kind": "flushed",
            "tables": tables,
            "bytes_written": bytes_written,
        })),
        ExecutionResult::Subscription {
            subscription_id,
            channel,
            select_query,
        } => Ok(json!({
            "kind": "subscription",
            "subscription_id": subscription_id,
            "channel": channel,
            "select_query": select_query,
        })),
        ExecutionResult::JobKilled { job_id, status } => Ok(json!({
            "kind": "job_killed",
            "job_id": job_id,
            "status": status,
        })),
    }
}

fn materialize_rows(
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
) -> Result<Vec<Row>, KalamPgError> {
    let total_rows = batches.iter().map(|batch| batch.num_rows()).sum();
    let mut rows = Vec::with_capacity(total_rows);

    for batch in batches {
        let schema = batch.schema();

        for row_index in 0..batch.num_rows() {
            let mut values = std::collections::BTreeMap::new();

            for (column_index, field) in schema.fields().iter().enumerate() {
                let value =
                    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
                        .map_err(|err| KalamPgError::Execution(err.to_string()))?;
                values.insert(field.name().clone(), value);
            }

            rows.push(Row::new(values));
        }
    }

    Ok(rows)
}

fn row_to_json(row: &Row) -> Value {
    let mut object = Map::new();

    for (column_name, value) in row.iter() {
        object.insert(column_name.clone(), scalar_to_json(value));
    }

    Value::Object(object)
}

fn scalar_to_json(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(value) => value.map(Value::Bool).unwrap_or(Value::Null),
        ScalarValue::Int8(value) => number_from_i64(value.map(i64::from)),
        ScalarValue::Int16(value) => number_from_i64(value.map(i64::from)),
        ScalarValue::Int32(value) => number_from_i64(value.map(i64::from)),
        ScalarValue::Int64(value) => number_from_i64(*value),
        ScalarValue::UInt8(value) => number_from_u64(value.map(u64::from)),
        ScalarValue::UInt16(value) => number_from_u64(value.map(u64::from)),
        ScalarValue::UInt32(value) => number_from_u64(value.map(u64::from)),
        ScalarValue::UInt64(value) => number_from_u64(*value),
        ScalarValue::Float32(value) => number_from_f64(value.map(f64::from)),
        ScalarValue::Float64(value) => number_from_f64(*value),
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => {
            value.clone().map(Value::String).unwrap_or(Value::Null)
        },
        ScalarValue::Binary(value) | ScalarValue::LargeBinary(value) => value
            .as_ref()
            .map(|bytes| {
                Value::Array(bytes.iter().map(|byte| Value::Number(Number::from(*byte))).collect())
            })
            .unwrap_or(Value::Null),
        ScalarValue::Date32(value) => number_from_i64(value.map(i64::from)),
        ScalarValue::Time64Microsecond(value) => number_from_i64(*value),
        ScalarValue::TimestampMillisecond(value, _)
        | ScalarValue::TimestampMicrosecond(value, _)
        | ScalarValue::TimestampNanosecond(value, _) => number_from_i64(*value),
        ScalarValue::Decimal128(value, _, scale) => value
            .map(|value| Value::String(decimal_to_string(value, *scale)))
            .unwrap_or(Value::Null),
        other => Value::String(other.to_string()),
    }
}

fn number_from_i64(value: Option<i64>) -> Value {
    value.map(Number::from).map(Value::Number).unwrap_or(Value::Null)
}

fn number_from_u64(value: Option<u64>) -> Value {
    value.map(Number::from).map(Value::Number).unwrap_or(Value::Null)
}

fn number_from_f64(value: Option<f64>) -> Value {
    value.and_then(Number::from_f64).map(Value::Number).unwrap_or(Value::Null)
}

fn decimal_to_string(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }

    let scale = scale as u32;
    let divisor = 10_i128.pow(scale);
    let integer = value / divisor;
    let fraction = (value % divisor).abs();
    format!("{integer}.{fraction:0width$}", width = scale as usize)
}

fn current_timestamp_millis() -> Result<i64, KalamPgError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    Ok(duration.as_millis() as i64)
}
