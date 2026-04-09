//! Core cluster message handler
//!
//! Handles incoming inter-node gRPC calls and dispatches them into core services.

use std::sync::Arc;
use std::time::Instant;

use kalamdb_auth::{authenticate, AuthRequest, CoreUsersRepo, UserRepository};
use kalamdb_commons::conversions::{
    mask_sensitive_rows_for_role, record_batch_to_json_arrays, schema_fields_from_arrow_schema,
};
use kalamdb_commons::models::{ConnectionInfo, KalamCellValue, NamespaceId, Username};
use kalamdb_commons::schemas::SchemaField;
use kalamdb_commons::Role;
use kalamdb_raft::{
    forward_sql_param, ClusterMessageHandler, ForwardSqlParam, ForwardSqlRequest,
    ForwardSqlResponsePayload, GetNodeInfoRequest, GetNodeInfoResponse, PingRequest,
    RaftExecutor,
};
use kalamdb_session::{AuthMethod, AuthSession};
use serde::Serialize;

use crate::app_context::AppContext;
use crate::sql::context::ExecutionContext;
use crate::sql::executor::PreparedExecutionStatement;
use crate::sql::SqlImpersonationService;
use crate::sql::ExecutionResult;

// ── Response types (match SqlResponse JSON shape, no intermediate Value tree) ──

#[derive(Serialize)]
struct ForwardedResponse<'a> {
    status: &'static str,
    results: &'a [ForwardedResult],
    took: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ForwardedError<'a>>,
}

#[derive(Serialize)]
struct ForwardedResult {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    schema: Vec<SchemaField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<Vec<Vec<KalamCellValue>>>,
    row_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    as_user: Username,
}

#[derive(Serialize)]
struct ForwardedError<'a> {
    code: &'a str,
    message: &'a str,
}

/// Core implementation of cluster message handling.
pub struct CoreClusterHandler {
    app_context: Arc<AppContext>,
}

impl CoreClusterHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn error_payload(
        status_code: u16,
        error_code: &str,
        message: &str,
        started_at: Instant,
    ) -> ForwardSqlResponsePayload {
        let resp = ForwardedResponse {
            status: "error",
            results: &[],
            took: started_at.elapsed().as_secs_f64() * 1000.0,
            error: Some(ForwardedError { code: error_code, message }),
        };
        let body = serde_json::to_vec(&resp).unwrap_or_else(|_| {
            b"{\"status\":\"error\",\"results\":[],\"took\":0,\"error\":{\"code\":\"INTERNAL_ERROR\",\"message\":\"Failed to serialize error payload\"}}".to_vec()
        });
        ForwardSqlResponsePayload { status_code, body }
    }

    fn forwarded_param_to_scalar(
        param: &ForwardSqlParam,
    ) -> Result<datafusion::scalar::ScalarValue, String> {
        match param.value.as_ref() {
            Some(forward_sql_param::Value::NullValue(_)) => Ok(datafusion::scalar::ScalarValue::Utf8(None)),
            Some(forward_sql_param::Value::BoolValue(v)) => Ok(datafusion::scalar::ScalarValue::Boolean(Some(*v))),
            Some(forward_sql_param::Value::Int64Value(v)) => Ok(datafusion::scalar::ScalarValue::Int64(Some(*v))),
            Some(forward_sql_param::Value::Float64Value(v)) => Ok(datafusion::scalar::ScalarValue::Float64(Some(*v))),
            Some(forward_sql_param::Value::StringValue(v)) => Ok(datafusion::scalar::ScalarValue::Utf8(Some(v.clone()))),
            None => Err("Missing parameter value".to_string()),
        }
    }

    fn execution_result_to_forwarded(
        result: ExecutionResult,
        as_user: &Username,
        user_role: Role,
    ) -> Result<ForwardedResult, String> {
        match result {
            ExecutionResult::Success { message } => Ok(ForwardedResult {
                schema: Vec::new(),
                rows: None,
                row_count: 0,
                message: Some(message),
                as_user: as_user.clone(),
            }),
            ExecutionResult::Rows { batches, row_count, schema } => {
                let arrow_schema = batches.first().map(|b| b.schema()).or(schema);
                let schema_fields = arrow_schema
                    .as_ref()
                    .map(schema_fields_from_arrow_schema)
                    .unwrap_or_default();

                let mut rows = Vec::new();
                for batch in &batches {
                    let mut batch_rows = record_batch_to_json_arrays(batch)
                        .map_err(|e| format!("Failed to convert batch: {}", e))?;
                    rows.append(&mut batch_rows);
                }

                mask_sensitive_rows_for_role(&mut rows, &schema_fields, user_role);

                Ok(ForwardedResult {
                    schema: schema_fields,
                    rows: Some(rows),
                    row_count,
                    message: None,
                    as_user: as_user.clone(),
                })
            },
            ExecutionResult::Inserted { rows_affected } => Ok(ForwardedResult {
                schema: Vec::new(),
                rows: None,
                row_count: rows_affected,
                message: Some(format!("Inserted {} row(s)", rows_affected)),
                as_user: as_user.clone(),
            }),
            ExecutionResult::Updated { rows_affected } => Ok(ForwardedResult {
                schema: Vec::new(),
                rows: None,
                row_count: rows_affected,
                message: Some(format!("Updated {} row(s)", rows_affected)),
                as_user: as_user.clone(),
            }),
            ExecutionResult::Deleted { rows_affected } => Ok(ForwardedResult {
                schema: Vec::new(),
                rows: None,
                row_count: rows_affected,
                message: Some(format!("Deleted {} row(s)", rows_affected)),
                as_user: as_user.clone(),
            }),
            ExecutionResult::Flushed { tables, bytes_written } => Ok(ForwardedResult {
                schema: Vec::new(),
                rows: None,
                row_count: tables.len(),
                message: Some(format!("Flushed {} table(s), {} bytes written", tables.len(), bytes_written)),
                as_user: as_user.clone(),
            }),
            ExecutionResult::Subscription { subscription_id, channel, select_query } => Ok(ForwardedResult {
                schema: Vec::new(),
                rows: None,
                row_count: 1,
                message: Some(format!("Subscription {} on channel {} for query: {}", subscription_id, channel, select_query)),
                as_user: as_user.clone(),
            }),
            ExecutionResult::JobKilled { job_id, status } => Ok(ForwardedResult {
                schema: Vec::new(),
                rows: None,
                row_count: 1,
                message: Some(format!("Job {} killed: {}", job_id, status)),
                as_user: as_user.clone(),
            }),
        }
    }

    fn resolve_result_username(
        authenticated_username: &Username,
        execute_as_username: Option<&Username>,
    ) -> Username {
        execute_as_username
            .cloned()
            .unwrap_or_else(|| authenticated_username.clone())
    }

    fn prepare_forwarded_statement(
        &self,
        statement: &str,
        default_namespace: &NamespaceId,
        actor_role: Role,
    ) -> Result<(PreparedExecutionStatement, Option<Username>), String> {
        let trimmed = statement.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            return Err("Empty SQL statement".to_string());
        }

        let (sql, execute_as_username) = match kalamdb_sql::execute_as::parse_execute_as(statement)? {
            Some(envelope) => {
                let execute_as_username = Username::try_new(&envelope.username)
                    .map_err(|e| format!("Invalid execute-as username: {}", e))?;
                (envelope.inner_sql, Some(execute_as_username))
            },
            None => (trimmed.to_string(), None),
        };

        let prepared_statement = self
            .app_context
            .sql_executor()
            .prepare_statement_metadata_for_role(&sql, default_namespace, actor_role)
            .map_err(|err| err.to_string())?;

        Ok((
            prepared_statement,
            execute_as_username,
        ))
    }
}

#[async_trait::async_trait]
impl ClusterMessageHandler for CoreClusterHandler {
    async fn handle_forward_sql(
        &self,
        req: ForwardSqlRequest,
    ) -> Result<ForwardSqlResponsePayload, String> {
        let started_at = Instant::now();

        let auth_header = req.authorization_header.as_deref().unwrap_or("").trim();
        if auth_header.is_empty() {
            return Ok(Self::error_payload(
                401,
                "PERMISSION_DENIED",
                "Missing Authorization header",
                started_at,
            ));
        }

        let user_repo: Arc<dyn UserRepository> =
            Arc::new(CoreUsersRepo::new(self.app_context.system_tables().users()));
        let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));
        let auth_result = match authenticate(
            AuthRequest::Header(auth_header.to_string()),
            &connection_info,
            &user_repo,
        )
        .await
        {
            Ok(result) => result,
            Err(e) => {
                return Ok(Self::error_payload(
                    401,
                    "PERMISSION_DENIED",
                    &format!("Authentication failed: {}", e),
                    started_at,
                ));
            },
        };

        if !matches!(auth_result.method, AuthMethod::Bearer) {
            return Ok(Self::error_payload(
                401,
                "PERMISSION_DENIED",
                "Forwarded SQL requires Bearer authentication",
                started_at,
            ));
        }

        let authenticated_username = auth_result.user.username.clone();
        let authenticated_role = auth_result.user.role;

        let mut session = AuthSession::with_username_and_auth_details(
            auth_result.user.user_id,
            authenticated_username.clone(),
            authenticated_role,
            connection_info,
            auth_result.method,
        );
        if let Some(request_id) = req.request_id {
            session = session.with_request_id(request_id);
        }

        let base_session = self.app_context.base_session_context();
        let mut exec_ctx = ExecutionContext::from_session(session, base_session);
        if let Some(namespace_id) = req.namespace_id.as_deref() {
            let namespace_id = NamespaceId::try_new(namespace_id).map_err(|e| e.to_string());
            let namespace_id = match namespace_id {
                Ok(ns) => ns,
                Err(e) => {
                    return Ok(Self::error_payload(
                        400,
                        "INVALID_INPUT",
                        &format!("Invalid namespace_id: {}", e),
                        started_at,
                    ));
                },
            };
            exec_ctx = exec_ctx.with_namespace_id(namespace_id);
        }

        let mut scalar_params = Vec::with_capacity(req.params.len());
        for (idx, param) in req.params.iter().enumerate() {
            let scalar = match Self::forwarded_param_to_scalar(param) {
                Ok(value) => value,
                Err(e) => {
                    return Ok(Self::error_payload(
                        400,
                        "INVALID_PARAMETER",
                        &format!("Parameter ${} invalid: {}", idx + 1, e),
                        started_at,
                    ));
                },
            };
            scalar_params.push(scalar);
        }

        let statements = match kalamdb_sql::split_statements(&req.sql) {
            Ok(v) if !v.is_empty() => v,
            Ok(_) => {
                return Ok(Self::error_payload(
                    400,
                    "EMPTY_SQL",
                    "No SQL statements provided",
                    started_at,
                ));
            },
            Err(e) => {
                return Ok(Self::error_payload(
                    400,
                    "BATCH_PARSE_ERROR",
                    &format!("Failed to parse SQL batch: {}", e),
                    started_at,
                ));
            },
        };

        if !scalar_params.is_empty() && statements.len() > 1 {
            return Ok(Self::error_payload(
                400,
                "PARAMS_WITH_BATCH",
                "Parameters not supported with multi-statement batches",
                started_at,
            ));
        }

        let sql_executor = self.app_context.sql_executor();
        let impersonation_service = SqlImpersonationService::new(Arc::clone(&self.app_context));
        let mut results = Vec::with_capacity(statements.len());

        for (idx, statement) in statements.iter().enumerate() {
            let stmt_params = if idx + 1 == statements.len() {
                std::mem::take(&mut scalar_params)
            } else {
                scalar_params.clone()
            };

            let (prepared_statement, execute_as_username) = match self.prepare_forwarded_statement(
                statement,
                &exec_ctx.default_namespace(),
                exec_ctx.user_role(),
            ) {
                Ok(prepared) => prepared,
                Err(e) => {
                    return Ok(Self::error_payload(
                        400,
                        "INVALID_INPUT",
                        &format!("Statement {} failed: {}", idx + 1, e),
                        started_at,
                    ));
                },
            };

            let execute_as_user = match execute_as_username.as_ref() {
                Some(target_username) => match impersonation_service
                    .resolve_execute_as_user(
                        exec_ctx.user_id(),
                        exec_ctx.user_role(),
                        target_username.as_str(),
                    )
                    .await
                {
                    Ok(user_id) => Some(user_id),
                    Err(e) => {
                        return Ok(Self::error_payload(
                            400,
                            "SQL_EXECUTION_ERROR",
                            &format!("Statement {} failed: {}", idx + 1, e),
                            started_at,
                        ));
                    },
                },
                None => None,
            };

            if execute_as_user.is_some()
                && prepared_statement.table_type == Some(kalamdb_commons::schemas::TableType::Shared)
            {
                let table_name = prepared_statement
                    .table_id
                    .as_ref()
                    .map(|table_id| table_id.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                return Ok(Self::error_payload(
                    400,
                    "SQL_EXECUTION_ERROR",
                    &format!(
                        "Statement {} failed: EXECUTE AS USER is not allowed on SHARED tables (table '{}'). AS USER impersonation is only supported for USER tables.",
                        idx + 1,
                        table_name
                    ),
                    started_at,
                ));
            }

            let effective_ctx = match execute_as_user.as_ref() {
                Some(user_id) => exec_ctx.with_effective_identity(user_id.clone(), Role::User),
                None => exec_ctx.clone(),
            };

            let exec_result = match sql_executor
                .execute_with_metadata(&prepared_statement, &effective_ctx, stmt_params)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    return Ok(Self::error_payload(
                        400,
                        "SQL_EXECUTION_ERROR",
                        &format!("Statement {} failed: {}", idx + 1, e),
                        started_at,
                    ));
                },
            };
            let effective_username =
                Self::resolve_result_username(&authenticated_username, execute_as_username.as_ref());
            let effective_role = if execute_as_user.is_some() {
                Role::User
            } else {
                authenticated_role
            };
            let result = match Self::execution_result_to_forwarded(
                exec_result,
                &effective_username,
                effective_role,
            ) {
                Ok(r) => r,
                Err(err) => {
                    return Ok(Self::error_payload(
                        500,
                        "INTERNAL_ERROR",
                        &format!("Failed to serialize forwarded SQL result: {}", err),
                        started_at,
                    ));
                },
            };
            results.push(result);
        }

        let resp = ForwardedResponse {
            status: "success",
            results: &results,
            took: started_at.elapsed().as_secs_f64() * 1000.0,
            error: None,
        };
        let body = serde_json::to_vec(&resp)
            .map_err(|e| format!("Failed to serialize forwarded SQL success payload: {}", e))?;

        Ok(ForwardSqlResponsePayload {
            status_code: 200,
            body,
        })
    }

    async fn handle_ping(&self, req: PingRequest) -> Result<(), String> {
        log::trace!("Received cluster ping from node_id={}", req.from_node_id);
        Ok(())
    }

    async fn handle_get_node_info(
        &self,
        _req: GetNodeInfoRequest,
    ) -> Result<GetNodeInfoResponse, String> {
        let executor = self.app_context.executor();

        // Get local cluster info (synchronous, zero-network)
        let cluster_info = executor.get_cluster_info();
        let self_node = cluster_info
            .nodes
            .iter()
            .find(|n| n.is_self)
            .ok_or_else(|| "Self node not found in cluster_info".to_string())?;

        // Also fetch live groups_leading count from RaftExecutor directly
        let groups_leading =
            if let Some(raft_executor) = executor.as_any().downcast_ref::<RaftExecutor>() {
                let manager = raft_executor.manager();
                let all_groups = manager.all_group_ids();
                all_groups.iter().filter(|&&g| manager.is_leader(g)).count() as u32
            } else {
                self_node.groups_leading
            };

        Ok(GetNodeInfoResponse {
            success: true,
            error: String::new(),
            node_id: self_node.node_id.as_u64(),
            groups_leading,
            current_term: self_node.current_term,
            last_applied_log: self_node.last_applied_log,
            snapshot_index: self_node.snapshot_index,
            status: self_node.status.as_str().to_string(),
            hostname: self_node.hostname.clone(),
            version: self_node.version.clone(),
            memory_mb: self_node.memory_mb,
            memory_usage_mb: self_node.memory_usage_mb,
            cpu_usage_percent: self_node.cpu_usage_percent,
            uptime_seconds: self_node.uptime_seconds,
            uptime_human: self_node.uptime_human.clone(),
            os: self_node.os.clone(),
            arch: self_node.arch.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CoreClusterHandler;
    use crate::sql::ExecutionResult;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::conversions::with_kalam_data_type_metadata;
    use kalamdb_commons::models::datatypes::KalamDataType;
    use kalamdb_commons::models::Role;
    use kalamdb_commons::models::Username;
    use kalamdb_raft::ForwardSqlParam;
    use std::sync::Arc;

    #[test]
    fn forwarded_rows_include_schema_and_rows() {
        let schema = Arc::new(Schema::new(vec![with_kalam_data_type_metadata(
            Field::new("count", DataType::Int64, false),
            &KalamDataType::BigInt,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![0_i64]))],
        )
        .expect("build record batch");

        let result = CoreClusterHandler::execution_result_to_forwarded(
            ExecutionResult::Rows {
                batches: vec![batch],
                row_count: 1,
                schema: Some(schema),
            },
            &Username::from("root"),
            Role::System,
        )
        .expect("serialize rows result");

        assert_eq!(result.schema[0].name, "count");
        assert_eq!(result.row_count, 1);
        let rows = result.rows.unwrap();
        assert_eq!(rows[0][0].as_str(), Some("0"));
        assert_eq!(result.as_user.as_str(), "root");
    }

    #[test]
    fn forwarded_rows_mask_sensitive_fields_for_non_admins() {
        let schema = Arc::new(Schema::new(vec![with_kalam_data_type_metadata(
            Field::new("password_hash", DataType::Utf8, false),
            &KalamDataType::Text,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["secret-hash"]))],
        )
        .expect("build record batch");

        let result = CoreClusterHandler::execution_result_to_forwarded(
            ExecutionResult::Rows {
                batches: vec![batch],
                row_count: 1,
                schema: Some(schema),
            },
            &Username::from("alice"),
            Role::User,
        )
        .expect("serialize masked rows result");

        let rows = result.rows.unwrap();
        assert_eq!(rows[0][0].as_str(), Some("***"));
    }

    #[test]
    fn forwarded_typed_params_decode_to_scalars() {
        let null_scalar = CoreClusterHandler::forwarded_param_to_scalar(&ForwardSqlParam::null())
            .expect("decode null param");
        let bool_scalar =
            CoreClusterHandler::forwarded_param_to_scalar(&ForwardSqlParam::boolean(true))
                .expect("decode bool param");
        let int_scalar = CoreClusterHandler::forwarded_param_to_scalar(&ForwardSqlParam::int64(7))
            .expect("decode int param");
        let float_scalar =
            CoreClusterHandler::forwarded_param_to_scalar(&ForwardSqlParam::float64(2.5))
                .expect("decode float param");
        let text_scalar =
            CoreClusterHandler::forwarded_param_to_scalar(&ForwardSqlParam::text("abc"))
                .expect("decode string param");

        assert_eq!(null_scalar, ScalarValue::Utf8(None));
        assert_eq!(bool_scalar, ScalarValue::Boolean(Some(true)));
        assert_eq!(int_scalar, ScalarValue::Int64(Some(7)));
        assert_eq!(float_scalar, ScalarValue::Float64(Some(2.5)));
        assert_eq!(text_scalar, ScalarValue::Utf8(Some("abc".to_string())));
    }
}
