//! Node health model

use serde::Serialize;

/// Health status of a single node
#[derive(Serialize)]
pub struct NodeHealth {
    /// Node ID
    pub node_id: u64,
    /// Node role (leader, follower, learner, candidate)
    pub role: String,
    /// Node status (active, offline, joining, catching_up, unknown)
    pub status: String,
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
    /// Machine hostname for display
    pub hostname: Option<String>,
    /// Current KalamDB process memory usage in megabytes.
    /// On macOS this is physical footprint; elsewhere it is RSS.
    pub memory_usage_mb: Option<u64>,
    /// Current KalamDB process CPU usage percentage
    pub cpu_usage_percent: Option<f32>,
    /// KalamDB server uptime in seconds
    pub uptime_seconds: Option<u64>,
    /// KalamDB server uptime in compact human-readable form
    pub uptime_human: Option<String>,
}
