//! Cluster configuration types
//!
//! Configuration for Raft clustering, parsed from the `[cluster]` section
//! of server.toml. These types are shared across kalamdb-raft, kalamdb-core,
//! and other crates that need cluster configuration.
//!
//! The cluster section in server.toml has a FLAT structure (no nesting),
//! so these types reflect that flat structure for proper TOML deserialization.

use serde::{Deserialize, Serialize};

/// Complete cluster configuration (FLAT structure matching server.toml)
///
/// Parsed from the `[cluster]` section in server.toml.
/// If this section is absent, the server runs in standalone mode.
///
/// Example server.toml:
/// ```toml
/// [cluster]
/// cluster_id = "cluster"
/// node_id = 1
/// rpc_addr = "127.0.0.1:9188"
/// api_addr = "127.0.0.1:8080"
/// user_shards = 12
/// shared_shards = 1
/// heartbeat_interval_ms = 50
/// election_timeout_ms = [150, 300]
/// snapshot_threshold = 10000
/// replication_timeout_ms = 5000
/// reconnect_interval_ms = 3000
///
/// [[cluster.peers]]
/// node_id = 2
/// rpc_addr = "10.0.0.2:9188"
/// api_addr = "http://10.0.0.2:8080"
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterConfig {
    /// Unique identifier for this cluster (used for Raft group prefixes)
    #[serde(default = "default_cluster_id")]
    pub cluster_id: String,

    /// This node's unique ID within the cluster (must be >= 1)
    /// This is the authoritative node ID for the server.
    pub node_id: u64,

    /// RPC address advertised to peer nodes for Raft inter-node communication.
    ///
    /// This must be a reachable host/IP for other nodes, not a wildcard bind address.
    /// Examples: "127.0.0.1:9188" for local tests, "kalamdb-node1:9188" in Docker,
    /// or "10.0.0.1:9188" on a private network.
    #[serde(default = "default_rpc_addr")]
    pub rpc_addr: String,

    /// API address advertised to other nodes and clients (e.g., "127.0.0.1:8080")
    /// This should match the server.host:server.port
    #[serde(default = "default_api_addr")]
    pub api_addr: String,

    /// List of peer nodes in the cluster
    /// Each peer should have node_id, rpc_addr, and api_addr
    #[serde(default)]
    pub peers: Vec<PeerConfig>,

    /// Number of user data shards (default: 12)
    /// Each shard is a separate Raft group for user table data.
    #[serde(default = "default_user_shards")]
    pub user_shards: u32,

    /// Number of shared data shards (must be 1)
    /// Shared tables currently run in a single Raft group.
    #[serde(default = "default_shared_shards")]
    pub shared_shards: u32,

    /// Raft heartbeat interval in milliseconds (default: 50)
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,

    /// Raft election timeout range (min, max) in milliseconds (default: 150-300)
    /// Election timeout is randomly chosen from this range.
    #[serde(default = "default_election_timeout_ms")]
    pub election_timeout_ms: (u64, u64),

    /// Snapshot policy (default: "LogsSinceLast(1000)")
    /// Options:
    /// - "LogsSinceLast(N)" - Snapshot after N log entries since last snapshot
    /// - "Never" - Disable automatic snapshots (not recommended for production)
    ///
    /// Lower values (e.g., 100) create snapshots more frequently:
    /// - Faster follower catchup (smaller log to replay)
    /// - Smaller memory footprint
    /// - More disk I/O
    ///
    /// Higher values (e.g., 10000) reduce snapshot frequency:
    /// - Less disk I/O
    /// - Slower follower catchup
    /// - Larger memory footprint
    #[serde(default = "default_snapshot_policy")]
    pub snapshot_policy: String,

    /// Maximum number of snapshots to keep (default: 3)
    /// Older snapshots are automatically deleted. Set to 0 to keep all snapshots.
    /// For single-node, you may want to set this to 1 to minimize disk usage.
    #[serde(default = "default_max_snapshots_to_keep")]
    pub max_snapshots_to_keep: u32,

    /// Timeout in milliseconds to wait for learner catchup during cluster membership changes
    #[serde(default = "default_replication_timeout_ms")]
    pub replication_timeout_ms: u64,

    /// Minimum interval in milliseconds between reconnect attempts to an unreachable peer
    #[serde(default = "default_reconnect_interval_ms")]
    pub reconnect_interval_ms: u64,

    /// Maximum number of retries when waiting for peers to come online during initialization
    /// Default: 60 retries × 500ms = ~30s max wait
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_wait_max_retries: Option<u32>,

    /// Initial delay in milliseconds between peer availability checks
    /// Default: 500ms
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_wait_initial_delay_ms: Option<u64>,

    /// Maximum delay in milliseconds between peer availability checks (exponential backoff cap)
    /// Default: 2000ms
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_wait_max_delay_ms: Option<u64>,
}

/// Configuration for a peer node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    /// Peer's unique node ID (must be >= 1)
    pub node_id: u64,
    /// Peer's RPC address for Raft communication (e.g., "10.0.0.2:9188")
    pub rpc_addr: String,
    /// Peer's API address for client requests (e.g., "10.0.0.2:8080")
    pub api_addr: String,
    /// Optional TLS server name override for this peer's RPC endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_server_name: Option<String>,
}

// Default value functions for serde

fn default_cluster_id() -> String {
    "cluster".to_string()
}

fn default_rpc_addr() -> String {
    "127.0.0.1:9188".to_string()
}

fn default_api_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_user_shards() -> u32 {
    8 // Reduced from 12 for lower memory footprint (~5-8 MB savings)
}

fn default_shared_shards() -> u32 {
    1
}

fn default_heartbeat_interval_ms() -> u64 {
    250
}

fn default_election_timeout_ms() -> (u64, u64) {
    (500, 1000)
}

fn default_snapshot_policy() -> String {
    "LogsSinceLast(1000)".to_string()
}

fn default_max_snapshots_to_keep() -> u32 {
    3
}

fn default_replication_timeout_ms() -> u64 {
    5000 // 5 seconds for learner catchup
}

fn default_reconnect_interval_ms() -> u64 {
    3000 // 3 seconds between reconnect attempts
}

impl ClusterConfig {
    /// Parse snapshot policy string into OpenRaft SnapshotPolicy
    ///
    /// Supported formats:
    /// - "LogsSinceLast(N)" - Snapshot after N log entries (e.g., "LogsSinceLast(100)")
    /// - "Never" - Disable automatic snapshots
    pub fn parse_snapshot_policy(policy_str: &str) -> Result<openraft::SnapshotPolicy, String> {
        let trimmed = policy_str.trim();

        if trimmed.eq_ignore_ascii_case("never") {
            return Ok(openraft::SnapshotPolicy::Never);
        }

        // Parse LogsSinceLast(N)
        if let Some(inner) = trimmed.strip_prefix("LogsSinceLast(") {
            if let Some(num_str) = inner.strip_suffix(")") {
                let num = num_str
                    .trim()
                    .parse::<u64>()
                    .map_err(|e| format!("Invalid number in LogsSinceLast: {}", e))?;
                return Ok(openraft::SnapshotPolicy::LogsSinceLast(num));
            }
        }

        Err(format!(
            "Invalid snapshot policy: '{}'. Expected 'LogsSinceLast(N)' or 'Never'",
            policy_str
        ))
    }

    /// Check if this configuration is valid
    pub fn validate(&self) -> Result<(), String> {
        if self.cluster_id.is_empty() {
            return Err("cluster_id cannot be empty".to_string());
        }

        if self.node_id == 0 {
            return Err("node_id must be > 0".to_string());
        }

        let has_peers = !self.peers.is_empty();
        validate_advertised_address("cluster.rpc_addr", &self.rpc_addr, has_peers)?;
        validate_advertised_address("cluster.api_addr", &self.api_addr, has_peers)?;

        for peer in &self.peers {
            if peer.node_id == 0 {
                return Err("cluster.peers[].node_id must be > 0".to_string());
            }
            // Peer addresses must always be concrete (we need to reach them)
            validate_advertised_address(
                &format!("cluster.peers(node_id={}).rpc_addr", peer.node_id),
                &peer.rpc_addr,
                true,
            )?;
            validate_advertised_address(
                &format!("cluster.peers(node_id={}).api_addr", peer.node_id),
                &peer.api_addr,
                true,
            )?;
        }

        // Check election timeout > heartbeat
        if self.election_timeout_ms.0 <= self.heartbeat_interval_ms {
            return Err("election_timeout_min must be > heartbeat_interval".to_string());
        }

        if self.election_timeout_ms.1 <= self.election_timeout_ms.0 {
            return Err("election_timeout_max must be > election_timeout_min".to_string());
        }

        // Check shard counts
        if self.user_shards == 0 {
            return Err("user_shards must be > 0".to_string());
        }

        if self.shared_shards != 1 {
            return Err("shared_shards must be exactly 1".to_string());
        }

        if self.reconnect_interval_ms == 0 {
            return Err("reconnect_interval_ms must be > 0".to_string());
        }

        Ok(())
    }

    /// Get the total number of Raft groups
    pub fn total_groups(&self) -> usize {
        1 // unified metadata group
            + self.user_shards as usize
            + self.shared_shards as usize
    }

    /// Find a peer by node_id
    pub fn find_peer(&self, node_id: u64) -> Option<&PeerConfig> {
        self.peers.iter().find(|p| p.node_id == node_id)
    }
}

fn validate_advertised_address(
    field_name: &str,
    value: &str,
    has_peers: bool,
) -> Result<(), String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{} cannot be empty", field_name));
    }

    if let Ok(addr) = trimmed.parse::<std::net::SocketAddr>() {
        if addr.ip().is_unspecified() && has_peers {
            return Err(format!(
                "{} must not use an unspecified/wildcard address ({}) when peers are configured. Use a reachable hostname or IP instead",
                field_name, value
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> ClusterConfig {
        ClusterConfig {
            cluster_id: "test-cluster".to_string(),
            node_id: 1,
            rpc_addr: "127.0.0.1:9188".to_string(),
            api_addr: "127.0.0.1:8080".to_string(),
            peers: vec![PeerConfig {
                node_id: 2,
                rpc_addr: "127.0.0.2:9188".to_string(),
                api_addr: "127.0.0.2:8080".to_string(),
                rpc_server_name: None,
            }],
            user_shards: 12,
            shared_shards: 1,
            heartbeat_interval_ms: 250,
            election_timeout_ms: (500, 1000),
            snapshot_policy: "LogsSinceLast(1000)".to_string(),
            max_snapshots_to_keep: 3,
            replication_timeout_ms: 5000,
            reconnect_interval_ms: 3000,
            peer_wait_max_retries: None,
            peer_wait_initial_delay_ms: None,
            peer_wait_max_delay_ms: None,
        }
    }

    #[test]
    fn test_parse_snapshot_policy() {
        assert_eq!(
            ClusterConfig::parse_snapshot_policy("LogsSinceLast(100)"),
            Ok(openraft::SnapshotPolicy::LogsSinceLast(100))
        );
        assert_eq!(
            ClusterConfig::parse_snapshot_policy("Never"),
            Ok(openraft::SnapshotPolicy::Never)
        );
        assert!(ClusterConfig::parse_snapshot_policy("Invalid").is_err());
    }

    #[test]
    fn test_validate_valid_config() {
        let config = valid_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_node_id() {
        let mut config = valid_config();
        config.node_id = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_election_timeout() {
        let mut config = valid_config();
        config.election_timeout_ms = (200, 100);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_multiple_shared_shards() {
        let mut config = valid_config();
        config.shared_shards = 2;
        assert_eq!(
            config.validate().expect_err("shared shards > 1 must be rejected"),
            "shared_shards must be exactly 1"
        );
    }

    #[test]
    fn test_total_groups() {
        let config = valid_config();
        assert_eq!(config.total_groups(), 1 + 12 + 1);
    }

    #[test]
    fn test_validate_allows_wildcard_rpc_addr_without_peers() {
        let mut config = valid_config();
        config.rpc_addr = "0.0.0.0:9188".to_string();
        config.peers.clear();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_wildcard_rpc_addr_with_peers() {
        let mut config = valid_config();
        config.rpc_addr = "0.0.0.0:9188".to_string();

        let err = config
            .validate()
            .expect_err("wildcard advertise address must be rejected when peers exist");
        assert!(err.contains("cluster.rpc_addr"));
    }

    #[test]
    fn test_find_peer() {
        let config = valid_config();
        assert!(config.find_peer(2).is_some());
        assert!(config.find_peer(99).is_none());
    }
}
