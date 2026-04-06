//! CommandExecutor trait definition

use async_trait::async_trait;
use std::fmt::Debug;

use kalamdb_commons::models::{NodeId, UserId};

use crate::cluster_types::{NodeRole, NodeStatus};
use crate::commands::{
    DataResponse, MetaCommand, MetaResponse, SharedDataCommand, UserDataCommand,
};
use crate::error::Result;
use crate::GroupId;

/// Information about a cluster node
#[derive(Debug, Clone)]
pub struct ClusterNodeInfo {
    /// Node ID (1, 2, 3, ...)
    pub node_id: NodeId,
    /// Node role derived from OpenRaft ServerState
    pub role: NodeRole,
    /// Node status (active, offline, joining, unknown, catching_up)
    pub status: NodeStatus,
    /// RPC address for Raft communication
    pub rpc_addr: String,
    /// HTTP API address
    pub api_addr: String,
    /// Whether this is the current node
    pub is_self: bool,
    /// Whether this node is leader for Meta group
    pub is_leader: bool,
    /// Number of groups this node leads (for multi-group Raft)
    pub groups_leading: u32,
    /// Total number of Raft groups
    pub total_groups: u32,
    /// Current Raft term (from leader's perspective, if known)
    pub current_term: Option<u64>,
    /// Last applied log index (for this node if is_self, otherwise from replication metrics)
    pub last_applied_log: Option<u64>,
    /// Leader's last log index (target for catchup)
    pub leader_last_log_index: Option<u64>,
    /// Snapshot log index (used during snapshot transfer catchup)
    pub snapshot_index: Option<u64>,
    /// Catchup progress as percentage (0-100), None if not catching up
    pub catchup_progress_pct: Option<u8>,
    /// Milliseconds since last heartbeat response (only for leader viewing followers)
    pub millis_since_last_heartbeat: Option<u64>,
    /// Replication lag in log entries (only for leader viewing followers)
    pub replication_lag: Option<u64>,
    // --- Node metadata (replicated via OpenRaft membership) ---
    /// Machine hostname (e.g., "node-1.kalamdb.local")
    pub hostname: Option<String>,
    /// KalamDB version (e.g., "0.1.0")
    pub version: Option<String>,
    /// Total system memory in megabytes
    pub memory_mb: Option<u64>,
    /// Current KalamDB process memory usage in megabytes.
    /// On macOS this is physical footprint; elsewhere it is RSS.
    pub memory_usage_mb: Option<u64>,
    /// Current KalamDB process CPU usage percentage
    pub cpu_usage_percent: Option<f32>,
    /// KalamDB server uptime in seconds
    pub uptime_seconds: Option<u64>,
    /// KalamDB server uptime in compact human-readable form
    pub uptime_human: Option<String>,
    /// Operating system (e.g., "linux", "macos", "windows")
    pub os: Option<String>,
    /// CPU architecture (e.g., "x86_64", "aarch64")
    pub arch: Option<String>,
}

/// Cluster status information
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    /// Cluster ID
    pub cluster_id: String,
    /// Current node ID
    pub current_node_id: NodeId,
    /// Whether in cluster mode
    pub is_cluster_mode: bool,
    /// All nodes in the cluster
    pub nodes: Vec<ClusterNodeInfo>,
    /// Total number of Raft groups
    pub total_groups: u32,
    /// Number of user data shards
    pub user_shards: u32,
    /// Number of shared data shards  
    pub shared_shards: u32,
    /// Current Raft term (from MetaSystem group)
    pub current_term: u64,
    /// Last log index (from MetaSystem group)
    pub last_log_index: Option<u64>,
    /// Last applied log index (from MetaSystem group)
    pub last_applied: Option<u64>,
    /// Milliseconds since quorum acknowledgment (leader health indicator)
    pub millis_since_quorum_ack: Option<u64>,
}

/// Unified interface for executing commands (Phase 20 - Unified Raft Executor).
///
/// This trait provides a single interface for all command execution.
/// Both single-node and cluster modes use RaftExecutor, ensuring
/// consistent behavior and testing.
///
/// # Implementation
///
/// - [`RaftExecutor`]: The only implementation - handles both single-node and cluster modes
///
/// # Example
///
/// ```rust,ignore
/// // In a DDL handler:
/// async fn create_table(ctx: &AppContext, table: TableDefinition) -> Result<()> {
///     let cmd = MetaCommand::CreateTable {
///         table_id: table.id.clone(),
///         table_type: TableType::User,
///         schema_json: serde_json::to_string(&table)?,
///     };
///     ctx.executor().execute_meta(cmd).await?;
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait CommandExecutor: Send + Sync + Debug {
    /// Execute a unified metadata command (namespaces, tables, storages, users, jobs)
    async fn execute_meta(&self, cmd: MetaCommand) -> Result<MetaResponse>;

    /// Execute a user data command (routed by user_id to correct shard)
    async fn execute_user_data(
        &self,
        user_id: &UserId,
        cmd: UserDataCommand,
    ) -> Result<DataResponse>;

    /// Execute a shared data command (routed to shared shard)
    async fn execute_shared_data(&self, cmd: SharedDataCommand) -> Result<DataResponse>;

    /// Check if this node is the leader for a specific group
    ///
    /// In standalone mode, always returns true.
    /// In cluster mode, checks Raft leadership.
    async fn is_leader(&self, group: GroupId) -> bool;

    /// Get the leader node ID for a group, if known
    ///
    /// In standalone mode, returns None (there's only one node).
    /// In cluster mode, returns the leader's node_id.
    async fn get_leader(&self, group: GroupId) -> Option<NodeId>;

    /// Returns true if running in cluster mode
    fn is_cluster_mode(&self) -> bool;

    /// Get the current node's ID (NodeId::default() for standalone)
    fn node_id(&self) -> NodeId;

    /// Get cluster information including all nodes, their roles, and status
    ///
    /// In standalone mode, returns a single-node cluster info.
    /// In cluster mode, returns info about all configured nodes.
    fn get_cluster_info(&self) -> ClusterInfo;

    /// Start the executor (initialize Raft groups, begin consensus participation)
    ///
    /// In standalone mode, this is a no-op.
    /// In cluster mode, this starts all Raft groups.
    async fn start(&self) -> Result<()>;

    /// Initialize the cluster (bootstrap first node only)
    ///
    /// In standalone mode, this is a no-op.
    /// In cluster mode, this bootstraps all Raft groups with initial membership.
    /// Only call this on the first node when creating a new cluster.
    async fn initialize_cluster(&self) -> Result<()>;

    /// Gracefully shutdown the executor
    ///
    /// In standalone mode, this is a no-op.
    /// In cluster mode, this stops all Raft groups and optionally transfers leadership.
    async fn shutdown(&self) -> Result<()>;

    /// Downcast to concrete type (for accessing implementation-specific methods)
    ///
    /// This is used sparingly during initialization to wire up appliers.
    /// Regular operation should use trait methods only.
    fn as_any(&self) -> &dyn std::any::Any;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_node_info_construction() {
        let node_info = ClusterNodeInfo {
            node_id: NodeId::from(1),
            role: NodeRole::Leader,
            status: NodeStatus::Active,
            rpc_addr: "127.0.0.1:9081".to_string(),
            api_addr: "http://127.0.0.1:8081".to_string(),
            is_self: true,
            is_leader: true,
            groups_leading: 14,
            total_groups: 14,
            current_term: Some(1),
            last_applied_log: Some(100),
            leader_last_log_index: Some(100),
            snapshot_index: None,
            catchup_progress_pct: None,
            millis_since_last_heartbeat: None,
            replication_lag: None,
            hostname: Some("node-1.local".to_string()),
            version: Some("0.2.0".to_string()),
            memory_mb: Some(16384),
            memory_usage_mb: Some(256),
            cpu_usage_percent: Some(12.5),
            uptime_seconds: Some(7200),
            uptime_human: Some("2h 0m".to_string()),
            os: Some("linux".to_string()),
            arch: Some("x86_64".to_string()),
        };

        assert_eq!(node_info.node_id, NodeId::from(1));
        assert!(matches!(node_info.role, NodeRole::Leader));
        assert!(node_info.is_self);
        assert_eq!(node_info.groups_leading, 14);
        assert_eq!(node_info.hostname, Some("node-1.local".to_string()));
    }

    #[test]
    fn test_cluster_node_info_follower() {
        let follower = ClusterNodeInfo {
            node_id: NodeId::from(2),
            role: NodeRole::Follower,
            status: NodeStatus::Active,
            rpc_addr: "127.0.0.1:9082".to_string(),
            api_addr: "http://127.0.0.1:8082".to_string(),
            is_self: false,
            is_leader: false,
            groups_leading: 0,
            total_groups: 14,
            current_term: Some(1),
            last_applied_log: Some(95),
            leader_last_log_index: Some(100),
            snapshot_index: None,
            catchup_progress_pct: None,
            millis_since_last_heartbeat: Some(50),
            replication_lag: Some(5),
            hostname: Some("node-2.local".to_string()),
            version: Some("0.2.0".to_string()),
            memory_mb: Some(8192),
            memory_usage_mb: Some(192),
            cpu_usage_percent: Some(4.25),
            uptime_seconds: Some(3600),
            uptime_human: Some("1h 0m".to_string()),
            os: Some("linux".to_string()),
            arch: Some("aarch64".to_string()),
        };

        assert_eq!(follower.node_id, NodeId::from(2));
        assert!(matches!(follower.role, NodeRole::Follower));
        assert!(!follower.is_leader);
        assert_eq!(follower.replication_lag, Some(5));
        assert_eq!(follower.millis_since_last_heartbeat, Some(50));
    }

    #[test]
    fn test_cluster_info_construction() {
        let cluster_info = ClusterInfo {
            cluster_id: "test-cluster".to_string(),
            current_node_id: NodeId::from(1),
            is_cluster_mode: true,
            nodes: vec![],
            total_groups: 14,
            user_shards: 12,
            shared_shards: 1,
            current_term: 5,
            last_log_index: Some(250),
            last_applied: Some(250),
            millis_since_quorum_ack: Some(20),
        };

        assert_eq!(cluster_info.cluster_id, "test-cluster");
        assert_eq!(cluster_info.current_node_id, NodeId::from(1));
        assert!(cluster_info.is_cluster_mode);
        assert_eq!(cluster_info.total_groups, 14);
        assert_eq!(cluster_info.user_shards, 12);
        assert_eq!(cluster_info.shared_shards, 1);
    }

    #[test]
    fn test_cluster_info_standalone_mode() {
        let standalone = ClusterInfo {
            cluster_id: "standalone".to_string(),
            current_node_id: NodeId::from(0),
            is_cluster_mode: false,
            nodes: vec![],
            total_groups: 0,
            user_shards: 0,
            shared_shards: 0,
            current_term: 0,
            last_log_index: None,
            last_applied: None,
            millis_since_quorum_ack: None,
        };

        assert!(!standalone.is_cluster_mode);
        assert_eq!(standalone.total_groups, 0);
    }

    #[test]
    fn test_node_status_variants() {
        // Ensure all NodeStatus variants can be created
        let _active = NodeStatus::Active;
        let _offline = NodeStatus::Offline;
        let _joining = NodeStatus::Joining;
        let _catching_up = NodeStatus::CatchingUp;
        let _unknown = NodeStatus::Unknown;
    }

    #[test]
    fn test_node_role_variants() {
        // Ensure all NodeRole variants can be created
        let _leader = NodeRole::Leader;
        let _follower = NodeRole::Follower;
        let _candidate = NodeRole::Candidate;
        let _learner = NodeRole::Learner;
    }

    #[test]
    fn test_cluster_node_info_clone() {
        let original = ClusterNodeInfo {
            node_id: NodeId::from(3),
            role: NodeRole::Learner,
            status: NodeStatus::Joining,
            rpc_addr: "127.0.0.1:9083".to_string(),
            api_addr: "http://127.0.0.1:8083".to_string(),
            is_self: false,
            is_leader: false,
            groups_leading: 0,
            total_groups: 14,
            current_term: Some(2),
            last_applied_log: Some(10),
            leader_last_log_index: Some(100),
            snapshot_index: Some(50),
            catchup_progress_pct: Some(50),
            millis_since_last_heartbeat: Some(100),
            replication_lag: Some(90),
            hostname: None,
            version: None,
            memory_mb: None,
            memory_usage_mb: None,
            cpu_usage_percent: None,
            uptime_seconds: None,
            uptime_human: None,
            os: None,
            arch: None,
        };

        let cloned = original.clone();
        assert_eq!(cloned.node_id, original.node_id);
        assert_eq!(cloned.catchup_progress_pct, Some(50));
        assert_eq!(cloned.snapshot_index, Some(50));
    }

    #[test]
    fn test_cluster_info_with_multiple_nodes() {
        let nodes = vec![
            ClusterNodeInfo {
                node_id: NodeId::from(1),
                role: NodeRole::Leader,
                status: NodeStatus::Active,
                rpc_addr: "127.0.0.1:9081".to_string(),
                api_addr: "http://127.0.0.1:8081".to_string(),
                is_self: true,
                is_leader: true,
                groups_leading: 14,
                total_groups: 14,
                current_term: Some(3),
                last_applied_log: Some(200),
                leader_last_log_index: Some(200),
                snapshot_index: None,
                catchup_progress_pct: None,
                millis_since_last_heartbeat: None,
                replication_lag: None,
                hostname: Some("leader.local".to_string()),
                version: Some("0.2.0".to_string()),
                memory_mb: Some(32768),
                memory_usage_mb: Some(384),
                cpu_usage_percent: Some(8.75),
                uptime_seconds: Some(10_800),
                uptime_human: Some("3h 0m".to_string()),
                os: Some("linux".to_string()),
                arch: Some("x86_64".to_string()),
            },
            ClusterNodeInfo {
                node_id: NodeId::from(2),
                role: NodeRole::Follower,
                status: NodeStatus::Active,
                rpc_addr: "127.0.0.1:9082".to_string(),
                api_addr: "http://127.0.0.1:8082".to_string(),
                is_self: false,
                is_leader: false,
                groups_leading: 0,
                total_groups: 14,
                current_term: Some(3),
                last_applied_log: Some(198),
                leader_last_log_index: Some(200),
                snapshot_index: None,
                catchup_progress_pct: None,
                millis_since_last_heartbeat: Some(25),
                replication_lag: Some(2),
                hostname: Some("follower.local".to_string()),
                version: Some("0.2.0".to_string()),
                memory_mb: Some(16384),
                memory_usage_mb: Some(224),
                cpu_usage_percent: Some(2.5),
                uptime_seconds: Some(9_600),
                uptime_human: Some("2h 40m".to_string()),
                os: Some("linux".to_string()),
                arch: Some("x86_64".to_string()),
            },
        ];

        let cluster_info = ClusterInfo {
            cluster_id: "multi-node".to_string(),
            current_node_id: NodeId::from(1),
            is_cluster_mode: true,
            nodes,
            total_groups: 14,
            user_shards: 12,
            shared_shards: 1,
            current_term: 3,
            last_log_index: Some(200),
            last_applied: Some(200),
            millis_since_quorum_ack: Some(10),
        };

        assert_eq!(cluster_info.nodes.len(), 2);
        assert_eq!(cluster_info.nodes[0].node_id, NodeId::from(1));
        assert_eq!(cluster_info.nodes[1].node_id, NodeId::from(2));
        assert!(cluster_info.nodes[0].is_leader);
        assert!(!cluster_info.nodes[1].is_leader);
    }
}
