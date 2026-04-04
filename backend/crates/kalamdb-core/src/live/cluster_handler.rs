//! Core cluster message handler
//!
//! Handles incoming inter-node gRPC calls and dispatches them into core services.

use std::sync::Arc;
use std::time::Instant;

use kalamdb_auth::{authenticate, AuthRequest, CoreUsersRepo, UserRepository};
use kalamdb_commons::models::{ConnectionInfo, NamespaceId};
use kalamdb_raft::{
    ClusterMessageHandler, ForwardSqlRequest, ForwardSqlResponsePayload, GetNodeInfoRequest,
    GetNodeInfoResponse, PingRequest, RaftExecutor,
};
use kalamdb_session::{AuthMethod, AuthSession};
use serde_json::Value as JsonValue;

use crate::app_context::AppContext;
use crate::providers::arrow_json_conversion::json_value_to_scalar_strict;
use crate::sql::context::ExecutionContext;
use crate::sql::ExecutionResult;

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
        let body = serde_json::json!({
            "status": "error",
            "results": [],
            "took": started_at.elapsed().as_secs_f64() * 1000.0,
            "error": {
                "code": error_code,
                "message": message,
            }
        });
        let body = serde_json::to_vec(&body).unwrap_or_else(|_| {
            b"{\"status\":\"error\",\"results\":[],\"took\":0,\"error\":{\"code\":\"INTERNAL_ERROR\",\"message\":\"Failed to serialize error payload\"}}".to_vec()
        });

        ForwardSqlResponsePayload { status_code, body }
    }

    fn execution_result_to_json(result: ExecutionResult) -> serde_json::Value {
        match result {
            ExecutionResult::Success { message } => {
                serde_json::json!({"row_count": 0, "message": message})
            },
            ExecutionResult::Rows { row_count, .. } => serde_json::json!({"row_count": row_count}),
            ExecutionResult::Inserted { rows_affected } => serde_json::json!({
                "row_count": rows_affected,
                "message": format!("Inserted {} row(s)", rows_affected),
            }),
            ExecutionResult::Updated { rows_affected } => serde_json::json!({
                "row_count": rows_affected,
                "message": format!("Updated {} row(s)", rows_affected),
            }),
            ExecutionResult::Deleted { rows_affected } => serde_json::json!({
                "row_count": rows_affected,
                "message": format!("Deleted {} row(s)", rows_affected),
            }),
            ExecutionResult::Flushed {
                tables,
                bytes_written,
            } => serde_json::json!({
                "row_count": tables.len(),
                "message": format!("Flushed {} table(s), {} bytes written", tables.len(), bytes_written),
            }),
            ExecutionResult::Subscription {
                subscription_id,
                channel,
                select_query,
            } => serde_json::json!({
                "row_count": 1,
                "subscription": {
                    "id": subscription_id,
                    "channel": channel,
                    "sql": select_query,
                }
            }),
            ExecutionResult::JobKilled { job_id, status } => serde_json::json!({
                "row_count": 1,
                "message": format!("Job {} killed: {}", job_id, status),
            }),
        }
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

        let mut session = AuthSession::with_username_and_auth_details(
            auth_result.user.user_id,
            auth_result.user.username,
            auth_result.user.role,
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

        let parsed_params: Option<Vec<JsonValue>> = if req.params_json.is_empty() {
            None
        } else {
            match serde_json::from_slice(&req.params_json) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(Self::error_payload(
                        400,
                        "INVALID_INPUT",
                        &format!("Invalid params_json payload: {}", e),
                        started_at,
                    ));
                },
            }
        };

        let mut scalar_params = Vec::new();
        if let Some(params) = parsed_params.as_ref() {
            for (idx, param) in params.iter().enumerate() {
                let scalar = match json_value_to_scalar_strict(param) {
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
        let mut results = Vec::with_capacity(statements.len());

        for (idx, statement) in statements.iter().enumerate() {
            let stmt_params = if idx + 1 == statements.len() {
                std::mem::take(&mut scalar_params)
            } else {
                scalar_params.clone()
            };

            let exec_result = match sql_executor.execute(statement, &exec_ctx, stmt_params).await {
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
            results.push(Self::execution_result_to_json(exec_result));
        }

        let body = serde_json::json!({
            "status": "success",
            "results": results,
            "took": started_at.elapsed().as_secs_f64() * 1000.0,
        });
        let body = serde_json::to_vec(&body)
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
            os: self_node.os.clone(),
            arch: self_node.arch.clone(),
        })
    }
}
