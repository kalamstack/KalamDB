//! KalamDB Raft Consensus Layer
//!
//! This crate provides Multi-Raft consensus for KalamDB, enabling multi-node
//! clustering with strong consistency guarantees.
//!
//! ## Architecture (Phase 20 - Unified Raft Executor)
//!
//! - **34 Raft Groups**: 1 unified metadata group + 32 user data shards + 1 shared data shard
//! - **Unified Code Path**: Both single-node and cluster modes use RaftExecutor
//! - **Leader-Only Jobs**: Background jobs (flush, compaction) run only on the leader
//! - **Meta→Data Watermarking**: Data commands carry `required_meta_index` for ordering
//!
//! ## Key Components
//!
//! - [`GroupId`]: Identifies which of the 34 Raft groups a command belongs to
//! - [`ShardRouter`]: Routes operations to the correct shard based on user_id
//! - [`MetaCommand`]: Unified metadata command (namespaces, tables, users, jobs)
//! - [`MetaStateMachine`]: Unified state machine for all metadata
//! - [`CommandExecutor`]: Generic trait for executing commands
//! - [`RaftExecutor`]: The only executor implementation (handles both single-node and cluster)
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Single-node mode (no [cluster] config):
//! let config = RaftManagerConfig::for_single_node("127.0.0.1:8080".to_string());
//! let manager = RaftManager::new(config);
//! let executor = RaftExecutor::new(manager, std::time::Instant::now());
//!
//! // Cluster mode:
//! let config = RaftManagerConfig::from(cluster_config);
//! let manager = RaftManager::new(config);
//! let executor = RaftExecutor::new(manager, std::time::Instant::now());
//!
//! // Same interface in both modes:
//! ctx.executor().execute_meta(MetaCommand::CreateTable { ... }).await?;
//! ```

pub mod applier;
pub mod cluster_types;
pub mod codec;
pub mod commands;
pub mod error;
pub mod executor;
pub mod manager;
pub mod network;
pub mod state_machine;
pub mod storage;

// Re-exports - Meta layer
pub use applier::{MetaApplier, NoOpMetaApplier};
// Re-exports - Data layer
pub use applier::{NoOpSharedDataApplier, NoOpUserDataApplier, SharedDataApplier, UserDataApplier};
// Re-exports - Core types
pub use cluster_types::{NodeRole, NodeStatus, ServerStateExt};
pub use commands::{
    commit_seq_from_log_position, DataResponse, MetaCommand, MetaResponse, RaftCommand,
    RaftResponse, SharedDataCommand, TransactionApplyResult, UserDataCommand,
};
pub use error::{RaftError, Result};
pub use executor::{ClusterInfo, ClusterNodeInfo, CommandExecutor, RaftExecutor};
pub use kalamdb_sharding::{ClusterConfig as RaftClusterConfig, GroupId, PeerConfig, ShardRouter};
pub use manager::{
    PeerNode, RaftGroup, RaftManager, RaftManagerConfig, SnapshotInfo, SnapshotsSummary,
    DEFAULT_SHARED_DATA_SHARDS, DEFAULT_USER_DATA_SHARDS,
};
pub use network::{
    forward_sql_param, start_rpc_server, ClusterClient, ClusterMessageHandler, ClusterServiceImpl,
    ForwardSqlParam, ForwardSqlRequest, ForwardSqlResponse, ForwardSqlResponsePayload,
    GetNodeInfoRequest, GetNodeInfoResponse, NoOpClusterHandler, PingRequest, PingResponse,
    RaftNetwork, RaftNetworkFactory, RaftService,
};
pub use state_machine::{
    serde_helpers, ApplyResult, KalamStateMachine, MetaStateMachine, SharedDataStateMachine,
    StateMachineSnapshot, UserDataStateMachine,
};
pub use storage::{KalamNode, KalamRaftStorage, KalamTypeConfig};
