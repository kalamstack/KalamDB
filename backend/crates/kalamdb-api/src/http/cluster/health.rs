//! Cluster health endpoint handler

use actix_web::{web, HttpRequest, HttpResponse};
use kalamdb_auth::extract_client_ip_secure;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::metrics::{BUILD_DATE, SERVER_VERSION};
use kalamdb_raft::NodeStatus;
use std::sync::Arc;

use super::models::{ClusterHealthResponse, NodeHealth};

/// Cluster health endpoint handler
///
/// Returns detailed cluster health information including:
/// - Node roles and status
/// - Replication metrics
/// - Catchup progress
///
/// Access restricted to:
/// - Localhost requests only (for container/liveness checks)
pub async fn cluster_health_handler(
    req: HttpRequest,
    ctx: web::Data<Arc<AppContext>>,
) -> HttpResponse {
    // Check access (localhost only)
    let connection_info = extract_client_ip_secure(&req);
    if !connection_info.is_localhost() {
        return HttpResponse::Forbidden().json(serde_json::json!({
            "error": "Access denied. Cluster health is localhost-only."
        }));
    }

    let cluster_info = ctx.executor().get_cluster_info();

    // Calculate overall health status
    let status = if cluster_info.is_cluster_mode {
        // In cluster mode, check if we have a leader
        let has_leader = cluster_info.nodes.iter().any(|n| n.is_leader);
        let self_node = cluster_info.nodes.iter().find(|n| n.is_self);
        let is_active = self_node.map(|n| n.status == NodeStatus::Active).unwrap_or(false);

        if has_leader && is_active {
            "healthy"
        } else if is_active {
            "degraded" // No leader known but node is active
        } else {
            "unhealthy"
        }
    } else {
        "healthy" // Standalone is always healthy if responding
    };

    // Convert nodes
    let nodes: Vec<NodeHealth> = cluster_info
        .nodes
        .iter()
        .map(|n| NodeHealth {
            node_id: n.node_id,
            role: n.role,
            status: n.status,
            api_addr: n.api_addr.clone(),
            is_self: n.is_self,
            is_leader: n.is_leader,
            replication_lag: n.replication_lag,
            catchup_progress_pct: n.catchup_progress_pct,
        })
        .collect();

    // Find self node for groups_leading
    let groups_leading = cluster_info
        .nodes
        .iter()
        .find(|n| n.is_self)
        .map(|n| n.groups_leading)
        .unwrap_or(0);

    let response = ClusterHealthResponse {
        status: status.to_string(),
        version: SERVER_VERSION.to_string(),
        build_date: BUILD_DATE.to_string(),
        is_cluster_mode: cluster_info.is_cluster_mode,
        cluster_id: cluster_info.cluster_id,
        node_id: cluster_info.current_node_id.as_u64(),
        is_leader: cluster_info.nodes.iter().any(|n| n.is_self && n.is_leader),
        total_groups: cluster_info.total_groups,
        groups_leading,
        current_term: cluster_info.current_term,
        last_applied: cluster_info.last_applied,
        millis_since_quorum_ack: cluster_info.millis_since_quorum_ack,
        nodes,
    };

    HttpResponse::Ok().json(response)
}
