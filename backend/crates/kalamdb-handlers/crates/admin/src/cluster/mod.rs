//! Cluster management command handlers
//!
//! Handles cluster-level operations:
//! - CLUSTER SNAPSHOT: Force logs to snapshot
//! - CLUSTER PURGE: Purge logs up to index
//! - CLUSTER TRIGGER ELECTION: Trigger leader election
//! - CLUSTER TRANSFER-LEADER: Transfer leadership
//! - CLUSTER STEPDOWN: Attempt leader stepdown
//! - CLUSTER CLEAR: Clear old snapshots
//! - CLUSTER LIST: List cluster nodes

pub mod clear;
pub mod list;
pub mod purge;
pub mod snapshot;
pub mod stepdown;
pub mod transfer_leader;
pub mod trigger_election;

pub use clear::ClusterClearHandler;
pub use list::ClusterListHandler;
pub use purge::ClusterPurgeHandler;
pub use snapshot::ClusterSnapshotHandler;
pub use stepdown::ClusterStepdownHandler;
pub use transfer_leader::ClusterTransferLeaderHandler;
pub use trigger_election::ClusterTriggerElectionHandler;
