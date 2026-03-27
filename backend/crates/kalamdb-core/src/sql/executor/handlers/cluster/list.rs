//! CLUSTER LIST handler
//!
//! Lists all nodes in the cluster with their groups and health status
//! Provides a formatted display for debugging and cluster overview

use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::sql::executor::handlers::{
    ExecutionContext, ExecutionResult, ScalarValue, StatementHandler,
};
use kalamdb_raft::{GroupId, NodeRole, RaftExecutor};
use kalamdb_sql::classifier::{SqlStatement, SqlStatementKind};
use std::sync::Arc;

pub struct ClusterListHandler {
    app_context: Arc<AppContext>,
}

impl ClusterListHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

#[async_trait::async_trait]
impl StatementHandler for ClusterListHandler {
    async fn execute(
        &self,
        statement: SqlStatement,
        _params: Vec<ScalarValue>,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        if !matches!(statement.kind(), SqlStatementKind::ClusterList) {
            return Err(KalamDbError::InvalidOperation(format!(
                "CLUSTER LIST handler received wrong statement type: {}",
                statement.name()
            )));
        }

        log::info!("CLUSTER LIST queried by user: {}", ctx.user_id());

        // Get the RaftExecutor to access cluster info
        let executor = self.app_context.executor();
        let Some(raft_executor) = executor.as_any().downcast_ref::<RaftExecutor>() else {
            return Err(KalamDbError::InvalidOperation(
                "CLUSTER LIST requires cluster mode (Raft executor not available)".to_string(),
            ));
        };

        // Fan-out GetNodeInfo to all peers so get_cluster_info() can return full data.
        // This is fire-and-update: each peer response populates the cache before
        // get_cluster_info() reads it below.
        raft_executor.refresh_peer_stats().await;

        let manager = raft_executor.manager();
        let cluster_info = executor.get_cluster_info();

        // Build formatted output
        let mut output = String::new();

        // Header with cluster info
        output.push_str(&format!(
            "╔══════════════════════════════════════════════════════════════════╗\n\
             ║                    CLUSTER OVERVIEW                              ║\n\
             ╠══════════════════════════════════════════════════════════════════╣\n\
             ║ Cluster ID: {:<54} ║\n\
             ║ Mode: {:<60} ║\n\
             ║ Current Node: {:<52} ║\n\
             ║ Total Groups: {:<52} ║\n\
             ║   • Meta: 1                                                      ║\n\
             ║   • User Shards: {:<49} ║\n\
             ║   • Shared Shards: {:<47} ║\n\
             ╠══════════════════════════════════════════════════════════════════╣\n",
            cluster_info.cluster_id,
            if cluster_info.is_cluster_mode {
                "Cluster"
            } else {
                "Single-Node"
            },
            cluster_info.current_node_id.as_u64(),
            cluster_info.total_groups,
            cluster_info.user_shards,
            cluster_info.shared_shards,
        ));

        // Cluster-wide metrics (if available)
        if let Some(term) = cluster_info.last_log_index {
            output.push_str(&format!("║ Raft Term: {:<55} ║\n", cluster_info.current_term));
            output.push_str(&format!("║ Last Log Index: {:<50} ║\n", term));
            if let Some(applied) = cluster_info.last_applied {
                output.push_str(&format!("║ Last Applied: {:<52} ║\n", applied));
            }
            if let Some(ms) = cluster_info.millis_since_quorum_ack {
                output.push_str(&format!("║ Quorum ACK: {:<54} ║\n", format!("{}ms ago", ms)));
            }
            output
                .push_str("╠══════════════════════════════════════════════════════════════════╣\n");
        }

        // Nodes section
        output.push_str("║                         NODES                                    ║\n");
        output.push_str("╠══════════════════════════════════════════════════════════════════╣\n");

        for node in &cluster_info.nodes {
            let self_marker = if node.is_self { " (this)" } else { "" };
            let leader_marker = if node.is_leader { " ★" } else { "" };

            // Status with color codes (ANSI escape sequences)
            let status_str = format!("{:?}", node.status);
            let role_str = match node.role {
                NodeRole::Leader => "LEADER",
                NodeRole::Follower => "FOLLOWER",
                NodeRole::Candidate => "CANDIDATE",
                NodeRole::Learner => "LEARNER",
                NodeRole::Shutdown => "SHUTDOWN",
            };

            output.push_str(&format!(
                "║ Node {}{}{:<57} ║\n",
                node.node_id.as_u64(),
                self_marker,
                leader_marker
            ));
            output.push_str(&format!("║   Role: {:<58} ║\n", role_str));
            output.push_str(&format!("║   Status: {:<56} ║\n", status_str));
            output.push_str(&format!("║   API: {:<59} ║\n", node.api_addr));
            output.push_str(&format!("║   RPC: {:<59} ║\n", node.rpc_addr));
            output.push_str(&format!(
                "║   Groups Leading: {:<48} ║\n",
                format!("{}/{}", node.groups_leading, node.total_groups)
            ));

            if let Some(term) = node.current_term {
                output.push_str(&format!("║   Term: {:<58} ║\n", term));
            }
            if let Some(applied) = node.last_applied_log {
                output.push_str(&format!("║   Last Applied: {:<50} ║\n", applied));
            }
            if let Some(lag) = node.replication_lag {
                output.push_str(&format!(
                    "║   Replication Lag: {:<47} ║\n",
                    format!("{} entries", lag)
                ));
            }
            if let Some(pct) = node.catchup_progress_pct {
                output.push_str(&format!("║   Catchup Progress: {:<46} ║\n", format!("{}%", pct)));
            }

            output
                .push_str("║                                                                  ║\n");
        }

        // Group status summary
        output.push_str("╠══════════════════════════════════════════════════════════════════╣\n");
        output.push_str("║                    GROUP STATUS SUMMARY                          ║\n");
        output.push_str("╠══════════════════════════════════════════════════════════════════╣\n");

        // Collect group metrics
        let all_groups = manager.all_group_ids();
        let mut leaders = 0;
        let mut followers = 0;
        let mut unknown = 0;

        for group_id in &all_groups {
            if manager.is_leader(*group_id) {
                leaders += 1;
            } else if manager.current_leader(*group_id).is_some() {
                followers += 1;
            } else {
                unknown += 1;
            }
        }

        output.push_str(&format!("║ Leading: {:<57} ║\n", leaders));
        output.push_str(&format!("║ Following: {:<55} ║\n", followers));
        if unknown > 0 {
            output.push_str(&format!("║ Unknown/Pending: {:<49} ║\n", unknown));
        }

        // Show sample of groups (first 5 of each type)
        output.push_str("╠══════════════════════════════════════════════════════════════════╣\n");
        output.push_str("║ Group │ Type        │ State    │ Leader │ Snapshot │ Applied    ║\n");
        output.push_str("╠══════════════════════════════════════════════════════════════════╣\n");

        // Meta group
        if let Some(metrics) = manager.group_metrics(GroupId::Meta) {
            let state = format!("{:?}", metrics.state);
            let leader = metrics
                .current_leader
                .map(|id| id.to_string())
                .unwrap_or_else(|| "-".to_string());
            let snapshot =
                metrics.snapshot.map(|l| l.index.to_string()).unwrap_or_else(|| "-".to_string());
            let applied = metrics
                .last_applied
                .map(|l| l.index.to_string())
                .unwrap_or_else(|| "-".to_string());
            output.push_str(&format!(
                "║ {:<5} │ {:<11} │ {:<8} │ {:<6} │ {:<8} │ {:<10} ║\n",
                "Meta",
                "meta",
                &state[..state.len().min(8)],
                leader,
                snapshot,
                applied
            ));
        }

        // Sample user shards (first 3)
        for shard in 0..cluster_info.user_shards.min(3) {
            let group_id = GroupId::DataUserShard(shard);
            if let Some(metrics) = manager.group_metrics(group_id) {
                let state = format!("{:?}", metrics.state);
                let leader = metrics
                    .current_leader
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let snapshot = metrics
                    .snapshot
                    .map(|l| l.index.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let applied = metrics
                    .last_applied
                    .map(|l| l.index.to_string())
                    .unwrap_or_else(|| "-".to_string());
                output.push_str(&format!(
                    "║ U{:<4} │ {:<11} │ {:<8} │ {:<6} │ {:<8} │ {:<10} ║\n",
                    shard,
                    "user_data",
                    &state[..state.len().min(8)],
                    leader,
                    snapshot,
                    applied
                ));
            }
        }
        if cluster_info.user_shards > 3 {
            output.push_str(&format!(
                "║ ...   │ ({} more user shards)                                       ║\n",
                cluster_info.user_shards - 3
            ));
        }

        // Sample shared shards (first 2)
        for shard in 0..cluster_info.shared_shards.min(2) {
            let group_id = GroupId::DataSharedShard(shard);
            if let Some(metrics) = manager.group_metrics(group_id) {
                let state = format!("{:?}", metrics.state);
                let leader = metrics
                    .current_leader
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let snapshot = metrics
                    .snapshot
                    .map(|l| l.index.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let applied = metrics
                    .last_applied
                    .map(|l| l.index.to_string())
                    .unwrap_or_else(|| "-".to_string());
                output.push_str(&format!(
                    "║ S{:<4} │ {:<11} │ {:<8} │ {:<6} │ {:<8} │ {:<10} ║\n",
                    shard,
                    "shared_data",
                    &state[..state.len().min(8)],
                    leader,
                    snapshot,
                    applied
                ));
            }
        }

        output.push_str("╚══════════════════════════════════════════════════════════════════╝\n");

        Ok(ExecutionResult::Success { message: output })
    }
}
