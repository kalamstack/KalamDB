//! Node health model

use kalamdb_commons::models::NodeId;
use kalamdb_raft::{NodeRole, NodeStatus};
use serde::Serialize;

/// Health status of a single node
#[derive(Serialize)]
pub struct NodeHealth {
    /// Node ID
    pub node_id: NodeId,
    /// Node role (leader, follower, learner, candidate)
    pub role: NodeRole,
    /// Node status (active, offline, joining, catching_up, unknown)
    pub status: NodeStatus,
    /// API address
    pub api_addr: String,
    /// Whether this is the current node
    pub is_self: bool,
    /// Whether this node is the leader
    pub is_leader: bool,
    /// Replication lag in log entries (only for leader viewing followers)
    pub replication_lag: Option<u64>,
    /// Catchup progress percentage (0-100), None if not catching up
    pub catchup_progress_pct: Option<u8>,
}
