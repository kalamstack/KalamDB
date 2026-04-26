//! SQL forwarding to leader node in cluster mode.

use std::{sync::Arc, time::Instant};

use actix_web::{HttpRequest, HttpResponse};
use kalamdb_commons::{
    models::{NamespaceId, NodeId, UserId},
    schemas::TableType,
    Role,
};
use kalamdb_core::{app_context::AppContext, error::KalamDbError};
use kalamdb_raft::{ClusterClient, ForwardSqlRequest, GroupId, RaftExecutor, ShardRouter};

use super::{
    helpers::parse_forward_params,
    models::{ErrorCode, QueryRequest, SqlResponse},
};

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

/// Forward target: either the Meta leader or a specific node.
enum ForwardTarget {
    Leader,
    GroupLeader(GroupId),
    Node(NodeId),
}

fn data_group_for_table(
    app_context: &AppContext,
    table_id: &kalamdb_commons::TableId,
    user_id: &UserId,
) -> Option<GroupId> {
    let cached_table = app_context.schema_registry().get(table_id)?;
    let table_type: TableType = cached_table.table.table_type.into();

    let executor = app_context.executor();
    let raft_executor = executor.as_any().downcast_ref::<RaftExecutor>()?;
    let config = raft_executor.manager().config();
    let router = ShardRouter::new(config.user_shards, config.shared_shards);

    match table_type {
        TableType::User | TableType::Stream => {
            Some(GroupId::DataUserShard(router.user_shard_id(user_id)))
        },
        TableType::Shared => Some(GroupId::DataSharedShard(router.shared_shard_id())),
        TableType::System => Some(GroupId::Meta),
    }
}

fn write_statement_target_group(
    sql: &str,
    app_context: &AppContext,
    default_namespace: &NamespaceId,
    user_id: &UserId,
) -> Option<GroupId> {
    let classify_sql =
        kalamdb_sql::execute_as::extract_inner_sql(sql).unwrap_or_else(|| sql.to_string());
    let stmt = match kalamdb_sql::classifier::SqlStatement::classify_and_parse(
        &classify_sql,
        default_namespace,
        Role::System,
    ) {
        Ok(stmt) => stmt,
        Err(_) => return Some(GroupId::Meta),
    };

    if !stmt.is_write_operation() {
        return None;
    }

    match stmt.kind() {
        kalamdb_sql::classifier::SqlStatementKind::Insert(_)
        | kalamdb_sql::classifier::SqlStatementKind::Update(_)
        | kalamdb_sql::classifier::SqlStatementKind::Delete(_) => {
            let Some(table_id) = kalamdb_sql::parser::utils::extract_dml_table_id_fast(
                &classify_sql,
                default_namespace.as_str(),
            )
            .or_else(|| {
                kalamdb_sql::parser::utils::extract_dml_table_id(
                    &classify_sql,
                    default_namespace.as_str(),
                )
            }) else {
                return Some(GroupId::Meta);
            };
            Some(data_group_for_table(app_context, &table_id, user_id).unwrap_or(GroupId::Meta))
        },
        _ => Some(GroupId::Meta),
    }
}

async fn forward_sql_grpc(
    target: ForwardTarget,
    http_req: &HttpRequest,
    req: &QueryRequest,
    app_context: &AppContext,
    request_id: Option<&str>,
    start_time: Instant,
) -> Option<HttpResponse> {
    let client = match cluster_client_for(app_context) {
        Ok(c) => c,
        Err(resp) => return Some(resp),
    };

    let params = match parse_forward_params(&req.params) {
        Ok(v) => v,
        Err(e) => {
            return Some(HttpResponse::BadRequest().json(SqlResponse::error(
                ErrorCode::InvalidParameter,
                &e,
                start_time.elapsed().as_secs_f64() * 1000.0,
            )));
        },
    };

    let grpc_req = ForwardSqlRequest {
        sql: req.sql.clone(),
        namespace_id: req.namespace_id.as_ref().map(|ns| ns.to_string()),
        authorization_header: header_to_string(http_req, "Authorization"),
        request_id: header_to_string(http_req, "X-Request-ID")
            .or_else(|| request_id.map(ToOwned::to_owned)),
        params,
    };

    let response = match target {
        ForwardTarget::Leader => client.forward_sql_to_leader(grpc_req).await,
        ForwardTarget::GroupLeader(group_id) => {
            client.forward_sql_to_group_leader(group_id, grpc_req).await
        },
        ForwardTarget::Node(node_id) => client.forward_sql_to_node(node_id, grpc_req).await,
    };

    let response = match response {
        Ok(resp) => resp,
        Err(err) => {
            log::warn!("Failed to forward SQL over gRPC: {}", err);
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

/// Forwards write operations to the leader node in cluster mode.
pub async fn forward_sql_if_follower(
    http_req: &HttpRequest,
    req: &QueryRequest,
    app_context: &Arc<AppContext>,
    default_namespace: &NamespaceId,
    user_id: &UserId,
    request_id: Option<&str>,
) -> Option<HttpResponse> {
    let start_time = Instant::now();
    let executor = app_context.executor();

    let statements = match kalamdb_sql::split_statements(&req.sql) {
        Ok(stmts) => stmts,
        Err(_) => {
            return forward_sql_grpc(
                ForwardTarget::Leader,
                http_req,
                req,
                app_context.as_ref(),
                request_id,
                start_time,
            )
            .await
        },
    };

    let write_targets: Vec<GroupId> = statements
        .iter()
        .filter_map(|sql| {
            write_statement_target_group(sql, app_context.as_ref(), default_namespace, user_id)
        })
        .collect();

    if let Some(first_target) = write_targets.first().copied() {
        let target_group = if write_targets.iter().all(|target| *target == first_target) {
            first_target
        } else {
            GroupId::Meta
        };

        if executor.is_leader(target_group).await {
            return None;
        }

        return forward_sql_grpc(
            ForwardTarget::GroupLeader(target_group),
            http_req,
            req,
            app_context.as_ref(),
            request_id,
            start_time,
        )
        .await;
    }

    None
}

/// Handle typed NOT_LEADER errors by forwarding to the known shard leader over gRPC.
pub async fn handle_not_leader_error(
    err: &KalamDbError,
    http_req: &HttpRequest,
    req: &QueryRequest,
    app_context: &AppContext,
    request_id: Option<&str>,
    start_time: Instant,
) -> Option<HttpResponse> {
    if !app_context.is_cluster_mode() {
        return None;
    }

    let leader_addr = match err {
        KalamDbError::NotLeader { leader_addr } => leader_addr.as_ref(),
        _ => return None,
    };

    if let Some(addr) = leader_addr {
        if let Some(target_node_id) = resolve_node_id_from_cluster_addr(app_context, addr) {
            return forward_sql_grpc(
                ForwardTarget::Node(target_node_id),
                http_req,
                req,
                app_context,
                request_id,
                start_time,
            )
            .await;
        }
        log::debug!(
            target: "sql::forward",
            "NOT_LEADER: leader addr '{}' not found in cluster info, falling back to generic leader forward",
            addr
        );
    }

    forward_sql_grpc(ForwardTarget::Leader, http_req, req, app_context, request_id, start_time)
        .await
}
