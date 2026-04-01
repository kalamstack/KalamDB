//! Leader-Only Job Execution Guard
//!
//! Ensures that background jobs (flush, compaction, retention, etc.) are only
//! executed by the leader node in a cluster. This prevents duplicate job
//! execution and ensures consistency.
//!
//! ## Architecture
//!
//! In standalone mode, this node is always the leader.
//! In cluster mode, we check the Meta Raft group for leadership.
//!
//! ## Job Claiming Protocol
//!
//! 1. Check if this node is the leader for Meta group
//! 2. If leader, claim job via Raft (ClaimJob command)
//! 3. Execute job only if claim succeeds
//! 4. Release job on completion/failure
//!
//! This ensures that even with leadership changes mid-execution,
//! only one node executes each job.

use chrono::Utc;
use kalamdb_raft::{CommandExecutor, GroupId, MetaCommand, MetaResponse};
use std::sync::Arc;

use kalamdb_commons::models::{JobId, NodeId};
use kalamdb_core::error::KalamDbError;

/// Result of checking leadership status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipStatus {
    /// This node is the leader and can execute jobs
    IsLeader,
    /// This node is not the leader
    NotLeader {
        /// The leader's node ID, if known
        leader_node_id: Option<NodeId>,
    },
}

/// Guard that ensures only the leader executes jobs
///
/// This is a lightweight wrapper around the CommandExecutor that provides
/// job-specific leader checking and claiming.
#[derive(Clone)]
pub struct LeaderOnlyJobGuard {
    executor: Arc<dyn CommandExecutor>,
    node_id: NodeId,
}

impl LeaderOnlyJobGuard {
    /// Create a new LeaderOnlyJobGuard
    pub fn new(executor: Arc<dyn CommandExecutor>) -> Self {
        let node_id = executor.node_id();
        Self { executor, node_id }
    }

    /// Check if this node is the leader for job execution
    ///
    /// Jobs are managed by the Meta Raft group, so we check leadership there.
    pub async fn check_leadership(&self) -> LeadershipStatus {
        if self.executor.is_leader(GroupId::Meta).await {
            LeadershipStatus::IsLeader
        } else {
            let leader_node_id = self.executor.get_leader(GroupId::Meta).await;
            LeadershipStatus::NotLeader { leader_node_id }
        }
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        self.executor.is_leader(GroupId::Meta).await
    }

    /// Get the current node's ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the leader's node ID, if known
    pub async fn get_leader(&self) -> Option<NodeId> {
        self.executor.get_leader(GroupId::Meta).await
    }

    /// Claim a job for execution via Raft consensus
    ///
    /// This atomically claims the job on all nodes, preventing duplicate execution.
    /// Returns Ok(true) if claim succeeded, Ok(false) if already claimed.
    pub async fn claim_job(&self, job_id: &JobId) -> Result<bool, KalamDbError> {
        let now = Utc::now();

        let cmd = MetaCommand::ClaimJob {
            job_id: job_id.clone(),
            node_id: self.node_id,
            claimed_at: now,
        };

        match self.executor.execute_meta(cmd).await {
            Ok(MetaResponse::JobClaimed { .. }) => Ok(true),
            Ok(MetaResponse::Error { message }) if message.contains("already claimed") => Ok(false),
            Ok(MetaResponse::Error { message }) => Err(KalamDbError::InvalidOperation(message)),
            Ok(_) => Ok(true), // Unexpected but treat as success
            Err(e) => Err(KalamDbError::InvalidOperation(format!(
                "Failed to claim job {}: {}",
                job_id, e
            ))),
        }
    }

    /// Release a claimed job (on failure or leader change)
    ///
    /// This allows other nodes to pick up the job if this node fails.
    pub async fn release_job(&self, job_id: &JobId, reason: &str) -> Result<(), KalamDbError> {
        let now = Utc::now();

        let cmd = MetaCommand::ReleaseJob {
            job_id: job_id.clone(),
            reason: reason.to_string(),
            released_at: now,
        };

        match self.executor.execute_meta(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(KalamDbError::InvalidOperation(format!(
                "Failed to release job {}: {}",
                job_id, e
            ))),
        }
    }

    /// Complete a job via Raft consensus
    pub async fn complete_job(
        &self,
        job_id: &JobId,
        result_json: Option<String>,
    ) -> Result<(), KalamDbError> {
        let now = Utc::now();

        let cmd = MetaCommand::CompleteJob {
            job_id: job_id.clone(),
            result: result_json,
            completed_at: now,
        };

        match self.executor.execute_meta(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => Err(KalamDbError::InvalidOperation(format!(
                "Failed to complete job {}: {}",
                job_id, e
            ))),
        }
    }

    /// Fail a job via Raft consensus
    pub async fn fail_job(&self, job_id: &JobId, error_message: &str) -> Result<(), KalamDbError> {
        let now = Utc::now();

        let cmd = MetaCommand::FailJob {
            job_id: job_id.clone(),
            error_message: error_message.to_string(),
            failed_at: now,
        };

        match self.executor.execute_meta(cmd).await {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(KalamDbError::InvalidOperation(format!("Failed to fail job {}: {}", job_id, e)))
            },
        }
    }

    /// Check if running in cluster mode
    pub fn is_cluster_mode(&self) -> bool {
        self.executor.is_cluster_mode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use kalamdb_commons::models::UserId;
    use kalamdb_raft::{
        ClusterInfo, ClusterNodeInfo, DataResponse, NodeRole, NodeStatus, Result,
        SharedDataCommand, UserDataCommand,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Mock CommandExecutor for testing
    #[derive(Debug)]
    struct MockExecutor {
        is_leader: AtomicBool,
        is_cluster: bool,
        node_id: NodeId,
        leader_id: Option<NodeId>,
    }

    impl MockExecutor {
        fn new_leader() -> Self {
            Self {
                is_leader: AtomicBool::new(true),
                is_cluster: true,
                node_id: NodeId::from(1u64),
                leader_id: Some(NodeId::from(1u64)),
            }
        }

        fn new_follower() -> Self {
            Self {
                is_leader: AtomicBool::new(false),
                is_cluster: true,
                node_id: NodeId::from(2u64),
                leader_id: Some(NodeId::from(1u64)),
            }
        }

        fn new_standalone() -> Self {
            Self {
                is_leader: AtomicBool::new(true),
                is_cluster: false,
                node_id: NodeId::default(),
                leader_id: None,
            }
        }

        fn set_leader(&self, is_leader: bool) {
            self.is_leader.store(is_leader, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl CommandExecutor for MockExecutor {
        async fn execute_meta(&self, cmd: MetaCommand) -> Result<MetaResponse> {
            // Simulate successful job operations
            match cmd {
                MetaCommand::ClaimJob {
                    job_id, node_id, ..
                } => {
                    if self.is_leader.load(Ordering::SeqCst) {
                        Ok(MetaResponse::JobClaimed {
                            job_id,
                            node_id,
                            message: "Job claimed successfully".to_string(),
                        })
                    } else {
                        Ok(MetaResponse::Error {
                            message: "Not leader".to_string(),
                        })
                    }
                },
                MetaCommand::ReleaseJob { .. }
                | MetaCommand::CompleteJob { .. }
                | MetaCommand::FailJob { .. } => Ok(MetaResponse::Ok),
                _ => Ok(MetaResponse::Ok),
            }
        }

        async fn execute_user_data(
            &self,
            _user_id: &UserId,
            _cmd: UserDataCommand,
        ) -> Result<DataResponse> {
            Ok(DataResponse::Ok)
        }

        async fn execute_shared_data(&self, _cmd: SharedDataCommand) -> Result<DataResponse> {
            Ok(DataResponse::Ok)
        }

        async fn is_leader(&self, _group: GroupId) -> bool {
            self.is_leader.load(Ordering::SeqCst)
        }

        async fn get_leader(&self, _group: GroupId) -> Option<NodeId> {
            self.leader_id.clone()
        }

        fn is_cluster_mode(&self) -> bool {
            self.is_cluster
        }

        fn node_id(&self) -> NodeId {
            self.node_id.clone()
        }

        fn get_cluster_info(&self) -> ClusterInfo {
            ClusterInfo {
                cluster_id: "test-cluster".to_string(),
                current_node_id: self.node_id.clone(),
                is_cluster_mode: self.is_cluster,
                nodes: vec![ClusterNodeInfo {
                    node_id: self.node_id.clone(),
                    role: if self.is_leader.load(Ordering::SeqCst) {
                        NodeRole::Leader
                    } else {
                        NodeRole::Follower
                    },
                    status: NodeStatus::Active,
                    rpc_addr: "127.0.0.1:9081".to_string(),
                    api_addr: "http://127.0.0.1:8081".to_string(),
                    is_self: true,
                    is_leader: self.is_leader.load(Ordering::SeqCst),
                    groups_leading: if self.is_leader.load(Ordering::SeqCst) {
                        14
                    } else {
                        0
                    },
                    total_groups: 14,
                    current_term: Some(1),
                    last_applied_log: None,
                    leader_last_log_index: None,
                    snapshot_index: None,
                    catchup_progress_pct: None,
                    millis_since_last_heartbeat: None,
                    replication_lag: None,
                    hostname: Some("test-node".to_string()),
                    version: Some("0.2.0".to_string()),
                    memory_mb: Some(16384),
                    os: Some("test".to_string()),
                    arch: Some("x86_64".to_string()),
                }],
                total_groups: 14,
                user_shards: 12,
                shared_shards: 1,
                current_term: 1,
                last_log_index: None,
                last_applied: None,
                millis_since_quorum_ack: None,
            }
        }

        async fn start(&self) -> Result<()> {
            Ok(())
        }

        async fn initialize_cluster(&self) -> Result<()> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<()> {
            Ok(())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_leadership_status_display() {
        let is_leader = LeadershipStatus::IsLeader;
        assert_eq!(is_leader, LeadershipStatus::IsLeader);

        let not_leader = LeadershipStatus::NotLeader {
            leader_node_id: Some(NodeId::from(2u64)),
        };
        match not_leader {
            LeadershipStatus::NotLeader { leader_node_id } => {
                assert_eq!(leader_node_id, Some(NodeId::from(2u64)));
            },
            _ => panic!("Expected NotLeader"),
        }
    }

    #[tokio::test]
    async fn test_leader_guard_is_leader() {
        let executor = Arc::new(MockExecutor::new_leader());
        let guard = LeaderOnlyJobGuard::new(executor);

        assert!(guard.is_leader().await);
        assert!(guard.is_cluster_mode());
        assert_eq!(guard.node_id(), NodeId::from(1u64));
    }

    #[tokio::test]
    async fn test_leader_guard_not_leader() {
        let executor = Arc::new(MockExecutor::new_follower());
        let guard = LeaderOnlyJobGuard::new(executor);

        assert!(!guard.is_leader().await);
        assert_eq!(guard.get_leader().await, Some(NodeId::from(1u64)));
    }

    #[tokio::test]
    async fn test_leader_guard_standalone() {
        let executor = Arc::new(MockExecutor::new_standalone());
        let guard = LeaderOnlyJobGuard::new(executor);

        // In standalone mode, always leader
        assert!(guard.is_leader().await);
        assert!(!guard.is_cluster_mode());
    }

    #[tokio::test]
    async fn test_check_leadership_status() {
        let executor = Arc::new(MockExecutor::new_leader());
        let guard = LeaderOnlyJobGuard::new(executor.clone());

        // Initially leader
        assert_eq!(guard.check_leadership().await, LeadershipStatus::IsLeader);

        // Demote to follower
        executor.set_leader(false);
        match guard.check_leadership().await {
            LeadershipStatus::NotLeader { leader_node_id } => {
                assert_eq!(leader_node_id, Some(NodeId::from(1u64)));
            },
            _ => panic!("Expected NotLeader status"),
        }
    }

    #[tokio::test]
    async fn test_claim_job_as_leader() {
        let executor = Arc::new(MockExecutor::new_leader());
        let guard = LeaderOnlyJobGuard::new(executor);

        let job_id = JobId::new("FL-ns1-table1-20250101");
        let result = guard.claim_job(&job_id).await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_claim_job_as_follower() {
        let executor = Arc::new(MockExecutor::new_follower());
        let guard = LeaderOnlyJobGuard::new(executor);

        let job_id = JobId::new("FL-ns1-table1-20250101");
        let result = guard.claim_job(&job_id).await;
        // Follower gets an error response, not claim success
        assert!(result.is_err() || !result.unwrap());
    }

    #[tokio::test]
    async fn test_release_job() {
        let executor = Arc::new(MockExecutor::new_leader());
        let guard = LeaderOnlyJobGuard::new(executor);

        let job_id = JobId::new("FL-ns1-table1-20250101");
        let result = guard.release_job(&job_id, "Test release").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_complete_job() {
        let executor = Arc::new(MockExecutor::new_leader());
        let guard = LeaderOnlyJobGuard::new(executor);

        let job_id = JobId::new("FL-ns1-table1-20250101");
        let result = guard.complete_job(&job_id, Some("{}".to_string())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fail_job() {
        let executor = Arc::new(MockExecutor::new_leader());
        let guard = LeaderOnlyJobGuard::new(executor);

        let job_id = JobId::new("FL-ns1-table1-20250101");
        let result = guard.fail_job(&job_id, "Test error").await;
        assert!(result.is_ok());
    }
}
