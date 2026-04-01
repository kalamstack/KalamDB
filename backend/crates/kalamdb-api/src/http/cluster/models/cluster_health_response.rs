//! Cluster health response model

use super::NodeHealth;
use serde::Serialize;

/// Response for cluster health endpoint
#[derive(Serialize)]
pub struct ClusterHealthResponse {
    /// Overall health status
    pub status: String,
    /// Server version
    pub version: String,
    /// Build date
    pub build_date: String,
    /// Whether in cluster mode
    pub is_cluster_mode: bool,
    /// Cluster ID (empty for standalone)
    pub cluster_id: String,
    /// Current node ID (0 for standalone)
    pub node_id: u64,
    /// Whether this node is a leader for the Meta group
    pub is_leader: bool,
    /// Total number of Raft groups
    pub total_groups: u32,
    /// Number of groups this node leads
    pub groups_leading: u32,
    /// Current Raft term
    pub current_term: u64,
    /// Last applied log index
    pub last_applied: Option<u64>,
    /// Milliseconds since quorum acknowledgment (leader health indicator)
    pub millis_since_quorum_ack: Option<u64>,
    /// Nodes in the cluster with their status
    pub nodes: Vec<NodeHealth>,
}
