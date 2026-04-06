use serde::{Deserialize, Serialize};

/// Per-node health details from the cluster health endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct ClusterNodeHealth {
    pub node_id: u64,
    pub role: String,
    pub status: String,
    pub api_addr: String,
    pub is_self: bool,
    pub is_leader: bool,
    pub replication_lag: Option<u64>,
    pub catchup_progress_pct: Option<u8>,
    pub hostname: Option<String>,
    pub memory_usage_mb: Option<u64>,
    pub cpu_usage_percent: Option<f32>,
    pub uptime_seconds: Option<u64>,
    pub uptime_human: Option<String>,
}

/// Cluster health response from the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct ClusterHealthResponse {
    pub status: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub build_date: String,
    pub is_cluster_mode: bool,
    #[serde(default)]
    pub cluster_id: String,
    pub node_id: u64,
    pub is_leader: bool,
    pub total_groups: u32,
    pub groups_leading: u32,
    pub current_term: u64,
    pub last_applied: Option<u64>,
    pub millis_since_quorum_ack: Option<u64>,
    #[serde(default)]
    pub nodes: Vec<ClusterNodeHealth>,
}