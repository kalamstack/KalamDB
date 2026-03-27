//! SQL forwarding to leader node in cluster mode.

use actix_web::{HttpRequest, HttpResponse};
use kalamdb_commons::models::{NamespaceId, NodeId};
use kalamdb_commons::Role;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_raft::{ClusterClient, ForwardSqlRequest, GroupId, RaftExecutor};
use std::sync::Arc;
use std::time::Instant;

use super::models::{ErrorCode, QueryRequest, SqlResponse};

fn header_to_string(req: &HttpRequest, name: &str) -> Option<String> {
    req.headers().get(name).and_then(|v| v.to_str().ok()).map(|v| v.to_string())
}

fn normalize_addr(addr: &str) -> String {
    addr.trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_string()
}

fn resolve_node_id_from_cluster_addr(app_context: &AppContext, addr: &str) -> Option<NodeId> {
    let target = normalize_addr(addr);
    let cluster_info = app_context.executor().get_cluster_info();
    cluster_info
        .nodes
        .iter()
        .find(|n| normalize_addr(&n.api_addr) == target || normalize_addr(&n.rpc_addr) == target)
        .map(|n| n.node_id)
}

fn cluster_client_for(app_context: &AppContext) -> Result<ClusterClient, HttpResponse> {
    let executor = app_context.executor();
    let raft_executor = executor.as_any().downcast_ref::<RaftExecutor>().ok_or_else(|| {
        HttpResponse::ServiceUnavailable().json(SqlResponse::error(
            ErrorCode::ClusterUnavailable,
            "Cluster forwarding requires Raft executor",
            0.0,
        ))
    })?;
    Ok(ClusterClient::new(Arc::clone(raft_executor.manager())))
}

async fn forward_sql_to_leader_grpc(
    http_req: &HttpRequest,
    req: &QueryRequest,
    app_context: &AppContext,
    start_time: Instant,
) -> Option<HttpResponse> {
    let client = match cluster_client_for(app_context) {
        Ok(c) => c,
        Err(resp) => return Some(resp),
    };

    let params_json = match serde_json::to_vec(&req.params) {
        Ok(v) => v,
        Err(e) => {
            return Some(HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidParameter,
                &format!("Failed to encode query parameters: {}", e),
                start_time.elapsed().as_secs_f64() * 1000.0,
            )));
        },
    };

    let grpc_req = ForwardSqlRequest {
        sql: req.sql.clone(),
        namespace_id: req.namespace_id.as_ref().map(|ns| ns.to_string()),
        params_json,
        authorization_header: header_to_string(http_req, "Authorization"),
        request_id: header_to_string(http_req, "X-Request-ID"),
    };

    let response = match client.forward_sql_to_leader(grpc_req).await {
        Ok(resp) => resp,
        Err(err) => {
            log::warn!("Failed to forward SQL to leader over gRPC: {}", err);
            return Some(HttpResponse::ServiceUnavailable().json(SqlResponse::error(
                ErrorCode::ForwardFailed,
                "Failed to forward request to cluster leader",
                start_time.elapsed().as_secs_f64() * 1000.0,
            )));
        },
    };

    if !response.error.is_empty() && response.body.is_empty() {
        return Some(HttpResponse::BadGateway().json(SqlResponse::error(
            ErrorCode::ForwardFailed,
            &response.error,
            start_time.elapsed().as_secs_f64() * 1000.0,
        )));
    }

    let status = actix_web::http::StatusCode::from_u16(response.status_code as u16)
        .unwrap_or(actix_web::http::StatusCode::BAD_GATEWAY);
    Some(HttpResponse::build(status).content_type("application/json").body(response.body))
}

async fn forward_sql_to_node_grpc(
    target_node_id: NodeId,
    http_req: &HttpRequest,
    req: &QueryRequest,
    app_context: &AppContext,
    start_time: Instant,
) -> Option<HttpResponse> {
    let client = match cluster_client_for(app_context) {
        Ok(c) => c,
        Err(resp) => return Some(resp),
    };

    let params_json = match serde_json::to_vec(&req.params) {
        Ok(v) => v,
        Err(e) => {
            return Some(HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidParameter,
                &format!("Failed to encode query parameters: {}", e),
                start_time.elapsed().as_secs_f64() * 1000.0,
            )));
        },
    };

    let grpc_req = ForwardSqlRequest {
        sql: req.sql.clone(),
        namespace_id: req.namespace_id.as_ref().map(|ns| ns.to_string()),
        params_json,
        authorization_header: header_to_string(http_req, "Authorization"),
        request_id: header_to_string(http_req, "X-Request-ID"),
    };

    let response = match client.forward_sql_to_node(target_node_id, grpc_req).await {
        Ok(resp) => resp,
        Err(err) => {
            log::warn!("Failed to forward SQL to node {} over gRPC: {}", target_node_id, err);
            return Some(HttpResponse::ServiceUnavailable().json(SqlResponse::error(
                ErrorCode::ForwardFailed,
                "Failed to forward request to shard leader",
                start_time.elapsed().as_secs_f64() * 1000.0,
            )));
        },
    };

    if !response.error.is_empty() && response.body.is_empty() {
        return Some(HttpResponse::BadGateway().json(SqlResponse::error(
            ErrorCode::ForwardFailed,
            &response.error,
            start_time.elapsed().as_secs_f64() * 1000.0,
        )));
    }

    let status = actix_web::http::StatusCode::from_u16(response.status_code as u16)
        .unwrap_or(actix_web::http::StatusCode::BAD_GATEWAY);
    Some(HttpResponse::build(status).content_type("application/json").body(response.body))
}

/// Forwards write operations to the leader node in cluster mode.
pub async fn forward_sql_if_follower(
    http_req: &HttpRequest,
    req: &QueryRequest,
    app_context: &Arc<AppContext>,
    default_namespace: &NamespaceId,
) -> Option<HttpResponse> {
    let start_time = Instant::now();
    let executor = app_context.executor();

    if executor.is_leader(GroupId::Meta).await {
        return None;
    }

    let statements = match kalamdb_sql::split_statements(&req.sql) {
        Ok(stmts) => stmts,
        Err(_) => {
            return forward_sql_to_leader_grpc(http_req, req, app_context.as_ref(), start_time)
                .await
        },
    };

    let has_write = statements.iter().any(|sql| {
        let classify_sql =
            kalamdb_sql::execute_as::extract_inner_sql(sql).unwrap_or_else(|| sql.to_string());
        let stmt = kalamdb_sql::classifier::SqlStatement::classify_and_parse(
            &classify_sql,
            default_namespace,
            Role::System,
        )
        .unwrap_or_else(|_| {
            kalamdb_sql::classifier::SqlStatement::new(
                classify_sql,
                kalamdb_sql::classifier::SqlStatementKind::Unknown,
            )
        });
        stmt.is_write_operation()
    });

    if has_write {
        return forward_sql_to_leader_grpc(http_req, req, app_context.as_ref(), start_time).await;
    }

    None
}

/// Handle typed NOT_LEADER errors by forwarding to the known shard leader over gRPC.
pub async fn handle_not_leader_error(
    err: &KalamDbError,
    http_req: &HttpRequest,
    req: &QueryRequest,
    app_context: &AppContext,
    start_time: Instant,
) -> Option<HttpResponse> {
    if !app_context.is_cluster_mode() {
        return None;
    }

    let leader_addr = match err {
        KalamDbError::NotLeader { leader_addr } => leader_addr.as_ref(),
        _ => return None,
    };

    // If we have a specific leader address, try to forward directly to that node.
    if let Some(addr) = leader_addr {
        if let Some(target_node_id) = resolve_node_id_from_cluster_addr(app_context, addr) {
            return forward_sql_to_node_grpc(
                target_node_id,
                http_req,
                req,
                app_context,
                start_time,
            )
            .await;
        }
        // Leader address known but node not yet in cluster info — fall through to generic leader forward.
        log::debug!(
            target: "sql::forward",
            "NOT_LEADER: leader addr '{}' not found in cluster info, falling back to generic leader forward",
            addr
        );
    }

    // Leader unknown (election in progress) or not resolvable: forward to whoever the
    // Raft manager currently considers the leader for the Meta group.
    forward_sql_to_leader_grpc(http_req, req, app_context, start_time).await
}
