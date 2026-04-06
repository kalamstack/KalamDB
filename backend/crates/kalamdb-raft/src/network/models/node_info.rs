//! GetNodeInfo request/response message types.
//!
//! Used to fetch live per-node statistics from a remote peer so that
//! `\cluster list` can show complete data for all nodes even when
//! queried from a follower that lacks real-time peer stats.

/// Request to fetch live node info from a peer.
///
/// The requesting node sends its own `node_id` so the receiver can include
/// it in the response for tracing purposes.
#[derive(Clone, PartialEq, prost::Message)]
pub struct GetNodeInfoRequest {
    /// Node ID of the sender.
    #[prost(uint64, tag = "1")]
    pub from_node_id: u64,
}

/// Live per-node statistics returned by `GetNodeInfo`.
///
/// Each field mirrors the corresponding column in `system.cluster`.
/// Optional fields use the prost `optional` attribute so that `None` is
/// distinguishable from a zero value (e.g., 0 applied-log vs unknown).
#[derive(Clone, PartialEq, prost::Message)]
pub struct GetNodeInfoResponse {
    /// Whether the responder considers itself healthy/active.
    #[prost(bool, tag = "1")]
    pub success: bool,

    /// Non-empty string if there was an error.
    #[prost(string, tag = "2")]
    pub error: String,

    // ── Identity ────────────────────────────────────────────────────────────
    /// Node ID of the responding peer.
    #[prost(uint64, tag = "3")]
    pub node_id: u64,

    // ── Raft metrics (from local OpenRaft metrics) ───────────────────────────
    /// Number of Raft groups this node is currently leading.
    #[prost(uint32, tag = "4")]
    pub groups_leading: u32,

    /// Current Raft term (from the node's own metrics).
    #[prost(uint64, optional, tag = "5")]
    pub current_term: Option<u64>,

    /// Last applied log index on this node.
    #[prost(uint64, optional, tag = "6")]
    pub last_applied_log: Option<u64>,

    /// Snapshot index on this node.
    #[prost(uint64, optional, tag = "7")]
    pub snapshot_index: Option<u64>,

    // ── Node status ──────────────────────────────────────────────────────────
    /// `"active"`, `"offline"`, `"joining"`, `"catching_up"`, or `"unknown"`.
    #[prost(string, tag = "8")]
    pub status: String,

    // ── System metadata (auto-detected at startup) ───────────────────────────
    /// Machine hostname.
    #[prost(string, optional, tag = "9")]
    pub hostname: Option<String>,

    /// KalamDB version string.
    #[prost(string, optional, tag = "10")]
    pub version: Option<String>,

    /// Total system memory in megabytes.
    #[prost(uint64, optional, tag = "11")]
    pub memory_mb: Option<u64>,

    /// Operating system (e.g., `"linux"`, `"macos"`).
    #[prost(string, optional, tag = "12")]
    pub os: Option<String>,

    /// CPU architecture (e.g., `"x86_64"`, `"aarch64"`).
    #[prost(string, optional, tag = "13")]
    pub arch: Option<String>,

    /// Current KalamDB process memory usage in megabytes.
    /// On macOS this is physical footprint; elsewhere it is RSS.
    #[prost(uint64, optional, tag = "14")]
    pub memory_usage_mb: Option<u64>,

    /// Current KalamDB process CPU usage percentage.
    #[prost(float, optional, tag = "15")]
    pub cpu_usage_percent: Option<f32>,

    /// KalamDB server uptime in seconds.
    #[prost(uint64, optional, tag = "16")]
    pub uptime_seconds: Option<u64>,

    /// KalamDB server uptime in compact human-readable form.
    #[prost(string, optional, tag = "17")]
    pub uptime_human: Option<String>,
}
