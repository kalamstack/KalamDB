//! Raft Manager Configuration
//!
//! This module contains configuration types for the RaftManager,
//! including runtime configuration and peer node definitions.

use std::time::Duration;

use kalamdb_commons::models::NodeId;
pub use kalamdb_configs::RpcTlsConfig;

/// Default number of user data shards
pub const DEFAULT_USER_DATA_SHARDS: u32 = 32;

/// Default number of shared data shards
pub const DEFAULT_SHARED_DATA_SHARDS: u32 = 1;

/// Runtime configuration for the Raft Manager
///
/// This is the internal runtime config used by RaftManager, distinct from the
/// TOML-parseable `kalamdb_configs::ClusterConfig` which uses primitives
/// like `u64` for TOML compatibility. This struct uses types like `Duration`
/// for runtime convenience.
///
/// Construct this from `kalamdb_configs::ClusterConfig` using `From` trait.
#[derive(Debug, Clone)]
pub struct RaftManagerConfig {
    /// Cluster identifier
    pub cluster_id: String,

    /// This node's ID (must be >= 1)
    pub node_id: NodeId,

    /// This node's RPC address for Raft communication
    pub rpc_addr: String,

    /// This node's API address for client requests
    pub api_addr: String,

    /// Peer nodes in the cluster
    pub peers: Vec<PeerNode>,

    /// TLS/mTLS settings for inter-node RPC.
    pub rpc_tls: kalamdb_configs::RpcTlsConfig,

    /// Number of user data shards (default: 32)
    pub user_shards: u32,

    /// Number of shared data shards (default: 1)
    pub shared_shards: u32,

    /// Raft heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,

    /// Raft election timeout range (min, max) in milliseconds
    pub election_timeout_ms: (u64, u64),

    /// Snapshot policy (default: "LogsSinceLast(1000)")
    pub snapshot_policy: String,

    /// Maximum number of snapshots to keep
    pub max_snapshots_to_keep: u32,

    /// Replication timeout for learner catchup during cluster membership changes
    pub replication_timeout_ms: u64,

    /// Timeout for waiting for learner catchup during cluster membership changes
    pub replication_timeout: Duration,

    /// Minimum interval between reconnect attempts to an unreachable peer
    pub reconnect_interval_ms: u64,

    /// Maximum number of retries when waiting for peer to come online during initialization
    pub peer_wait_max_retries: u32,

    /// Initial delay in milliseconds between peer availability checks
    pub peer_wait_initial_delay_ms: u64,

    /// Maximum delay in milliseconds between peer availability checks (exponential backoff cap)
    pub peer_wait_max_delay_ms: u64,
}

impl Default for RaftManagerConfig {
    fn default() -> Self {
        Self {
            cluster_id: "kalamdb".to_string(),
            node_id: NodeId::new(1),
            rpc_addr: "127.0.0.1:9188".to_string(),
            api_addr: "127.0.0.1:8080".to_string(),
            peers: vec![],
            rpc_tls: kalamdb_configs::RpcTlsConfig::default(),
            user_shards: DEFAULT_USER_DATA_SHARDS,
            shared_shards: DEFAULT_SHARED_DATA_SHARDS,
            heartbeat_interval_ms: 250,
            election_timeout_ms: (500, 1000),
            snapshot_policy: "LogsSinceLast(1000)".to_string(),
            max_snapshots_to_keep: 3,
            replication_timeout_ms: 5000,
            replication_timeout: Duration::from_secs(5),
            reconnect_interval_ms: 3000,
            peer_wait_max_retries: 60,
            peer_wait_initial_delay_ms: 500,
            peer_wait_max_delay_ms: 2000,
        }
    }
}

impl RaftManagerConfig {
    /// Create a single-node configuration for standalone mode
    ///
    /// In single-node mode:
    /// - Only this node participates in Raft consensus
    /// - Leader election is instant (self-vote)
    /// - No network overhead (all operations are local)
    /// - Same code path as cluster mode (no separate standalone logic)
    /// - Single shard for both user and shared data (no distribution needed)
    ///
    /// This ensures the same Raft-based execution path is used in both
    /// standalone and cluster deployments, simplifying testing and maintenance.
    pub fn for_single_node(cluster_id: String, api_addr: String) -> Self {
        Self {
            cluster_id,
            node_id: NodeId::new(1),
            rpc_addr: "127.0.0.1:0".to_string(), // Port 0 = OS assigns random available port
            api_addr,
            peers: vec![], // No peers - single node cluster
            rpc_tls: kalamdb_configs::RpcTlsConfig::default(),
            user_shards: 1,   // Single shard - no distribution needed
            shared_shards: 1, // Single shard - no distribution needed
            heartbeat_interval_ms: 250,
            election_timeout_ms: (500, 1000),
            snapshot_policy: "LogsSinceLast(1000)".to_string(),
            max_snapshots_to_keep: 1, // Keep only most recent snapshot for single-node
            replication_timeout_ms: 5000,
            replication_timeout: Duration::from_secs(5),
            reconnect_interval_ms: 3000,
            peer_wait_max_retries: 60,
            peer_wait_initial_delay_ms: 500,
            peer_wait_max_delay_ms: 2000,
        }
    }
}

/// Convert from the TOML-parseable ClusterConfig to runtime RaftManagerConfig
impl From<kalamdb_configs::ClusterConfig> for RaftManagerConfig {
    fn from(config: kalamdb_configs::ClusterConfig) -> Self {
        Self {
            cluster_id: config.cluster_id,
            node_id: NodeId::new(config.node_id),
            rpc_addr: config.rpc_addr,
            api_addr: config.api_addr,
            peers: config.peers.into_iter().map(PeerNode::from).collect(),
            rpc_tls: kalamdb_configs::RpcTlsConfig::default(),
            user_shards: config.user_shards,
            shared_shards: config.shared_shards,
            heartbeat_interval_ms: config.heartbeat_interval_ms,
            election_timeout_ms: config.election_timeout_ms,
            snapshot_policy: config.snapshot_policy,
            max_snapshots_to_keep: config.max_snapshots_to_keep,
            replication_timeout_ms: config.replication_timeout_ms,
            replication_timeout: Duration::from_millis(config.replication_timeout_ms),
            reconnect_interval_ms: config.reconnect_interval_ms,
            peer_wait_max_retries: config.peer_wait_max_retries.unwrap_or(60),
            peer_wait_initial_delay_ms: config.peer_wait_initial_delay_ms.unwrap_or(500),
            peer_wait_max_delay_ms: config.peer_wait_max_delay_ms.unwrap_or(2000),
        }
    }
}

/// Runtime configuration for a peer node
#[derive(Debug, Clone)]
pub struct PeerNode {
    /// Peer's node ID
    pub node_id: NodeId,

    /// Peer's RPC address for Raft communication
    pub rpc_addr: String,

    /// Peer's API address for client requests
    pub api_addr: String,

    /// Optional TLS server-name override for this peer.
    pub rpc_server_name: Option<String>,
}

impl From<kalamdb_configs::PeerConfig> for PeerNode {
    fn from(peer: kalamdb_configs::PeerConfig) -> Self {
        Self {
            node_id: NodeId::new(peer.node_id),
            rpc_addr: peer.rpc_addr,
            api_addr: peer.api_addr,
            rpc_server_name: peer.rpc_server_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RaftManagerConfig::default();

        assert_eq!(config.node_id, NodeId::new(1));
        assert_eq!(config.user_shards, DEFAULT_USER_DATA_SHARDS);
        assert_eq!(config.shared_shards, DEFAULT_SHARED_DATA_SHARDS);
        assert!(config.peers.is_empty());
    }

    #[test]
    fn test_peer_node_from() {
        let peer_config = kalamdb_configs::PeerConfig {
            node_id: 2,
            rpc_addr: "127.0.0.1:9101".to_string(),
            api_addr: "127.0.0.1:8081".to_string(),
            rpc_server_name: Some("node2.cluster.local".to_string()),
        };

        let peer = PeerNode::from(peer_config);

        assert_eq!(peer.node_id, NodeId::new(2));
        assert_eq!(peer.rpc_addr, "127.0.0.1:9101");
        assert_eq!(peer.api_addr, "127.0.0.1:8081");
        assert_eq!(peer.rpc_server_name.as_deref(), Some("node2.cluster.local"));
    }
}
