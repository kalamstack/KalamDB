//! Individual Raft Group
//!
//! Represents a single Raft consensus group with its own log, state machine, and network.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kalamdb_commons::models::NodeId;
use kalamdb_store::StorageBackend;
use openraft::storage::Adaptor;
use openraft::{Config, Raft, RaftMetrics};
use parking_lot::RwLock;

use crate::network::RaftNetworkFactory;
use crate::state_machine::KalamStateMachine;
use crate::storage::{KalamNode, KalamRaftStorage, KalamTypeConfig};
use crate::{GroupId, RaftError};

/// Type alias for the openraft Raft instance
pub type RaftInstance = Raft<KalamTypeConfig>;

/// Type alias for the storage adaptor
pub type StorageAdaptor<SM> = Adaptor<KalamTypeConfig, Arc<KalamRaftStorage<SM>>>;

/// A single Raft consensus group
///
/// Each group has:
/// - Its own combined storage (log + state machine)
/// - Its own network connections to peers
pub struct RaftGroup<SM: KalamStateMachine + Send + Sync + 'static> {
    /// Group identifier
    group_id: GroupId,

    /// The Raft instance
    raft: RwLock<Option<RaftInstance>>,

    /// Combined storage for this group
    storage: Arc<KalamRaftStorage<SM>>,

    /// Network factory for this group
    network_factory: RaftNetworkFactory,
}

impl<SM: KalamStateMachine + Send + Sync + 'static> RaftGroup<SM> {
    /// Create a new Raft group with in-memory storage (not yet started)
    pub fn new(group_id: GroupId, state_machine: SM) -> Self {
        Self {
            group_id,
            raft: RwLock::new(None),
            storage: Arc::new(KalamRaftStorage::new(group_id, state_machine)),
            network_factory: RaftNetworkFactory::new(group_id),
        }
    }

    /// Create a new Raft group with persistent storage (not yet started)
    ///
    /// This mode persists Raft log entries, votes, and metadata to durable storage.
    /// On restart, state is recovered from the persistent store.
    pub fn new_persistent(
        group_id: GroupId,
        state_machine: SM,
        backend: Arc<dyn StorageBackend>,
        snapshots_dir: std::path::PathBuf,
    ) -> Result<Self, RaftError> {
        let storage =
            KalamRaftStorage::new_persistent(group_id, state_machine, backend, snapshots_dir)
                .map_err(|e| {
                    RaftError::Storage(format!("Failed to create persistent storage: {}", e))
                })?;

        Ok(Self {
            group_id,
            raft: RwLock::new(None),
            storage: Arc::new(storage),
            network_factory: RaftNetworkFactory::new(group_id),
        })
    }

    /// Check if this group is using persistent storage
    pub fn is_persistent(&self) -> bool {
        self.storage.is_persistent()
    }

    /// Restore state machine from persisted snapshot
    ///
    /// This should be called after the state machine's applier is configured.
    /// It restores the state machine's internal state from the persisted snapshot
    /// to ensure idempotency checks work correctly on restart.
    pub async fn restore_state_machine_from_snapshot(&self) -> Result<(), RaftError> {
        self.storage.restore_state_machine_from_snapshot().await
    }

    /// Check if there's a snapshot that can be restored
    pub fn has_snapshot(&self) -> bool {
        self.storage.has_snapshot()
    }

    /// Check if this group has persisted Raft state (indicating a restart)
    ///
    /// This is used to determine if initialize() should be skipped on restart.
    pub fn has_persisted_state(&self) -> bool {
        self.storage.has_persisted_state()
    }

    /// Get the last applied log ID from storage
    pub fn get_last_applied(&self) -> Option<openraft::LogId<u64>> {
        self.storage.get_last_applied()
    }

    /// Start the Raft group with the given node ID and configuration
    ///
    /// This initializes the Raft instance and begins participating in consensus.
    pub async fn start(
        &self,
        node_id: NodeId,
        config: &crate::manager::RaftManagerConfig,
    ) -> Result<(), RaftError> {
        // Check if already started
        if self.is_started() {
            return Ok(());
        }

        // Detect single-node mode (no peers configured)
        let is_single_node = config.peers.is_empty();

        self.network_factory
            .set_reconnect_interval(Duration::from_millis(config.reconnect_interval_ms));
        self.network_factory
            .configure_rpc_auth_identity(node_id, config.cluster_id.clone());
        self.network_factory.configure_rpc_tls(&config.rpc_tls, &config.peers)?;

        // Parse snapshot policy from configuration
        let snapshot_policy =
            kalamdb_configs::ClusterConfig::parse_snapshot_policy(&config.snapshot_policy)
                .map_err(|e| RaftError::Config(format!("Invalid snapshot policy: {}", e)))?;

        // Create Raft configuration with optimizations for single-node mode
        // Single-node optimizations (OpenRaft recommendations):
        // - Disable snapshot purging (max_in_snapshot_log_to_keep = 0)
        // - Reduce heartbeat/election timeouts
        // - Smaller purge batch to reduce CPU overhead
        // - Keep tick and elect enabled (required for Raft state machine to function)
        // - Snapshot policy from config (default: 1000 entries)
        let raft_config = if is_single_node {
            log::debug!(
                "[SINGLE-NODE] Applying lightweight Raft optimizations for {}",
                self.group_id
            );
            log::debug!("[SINGLE-NODE] Snapshot policy: {:?}", snapshot_policy);
            let mut cfg = Config {
                cluster_name: format!("kalamdb-{}", self.group_id),
                election_timeout_min: 150, // Faster elections (default 150-300ms)
                election_timeout_max: 300,
                heartbeat_interval: 50, // Faster heartbeats (default 50ms)
                install_snapshot_timeout: 5000,
                max_in_snapshot_log_to_keep: 0, // Disable log retention after snapshot
                purge_batch_size: 64,           // Smaller purge batches

                enable_heartbeat: false, // Disable heartbeats (no followers to send to)
                enable_elect: true,      // Keep elections enabled (needed to become leader)
                enable_tick: true,       // Keep tick enabled (REQUIRED for Raft to function)
                ..Default::default()
            };

            // Configure snapshot policy from config
            cfg.snapshot_policy = snapshot_policy;
            cfg
        } else {
            // Multi-node cluster: use conservative settings for network reliability
            log::debug!("[MULTI-NODE] Cluster Raft configuration for {}", self.group_id);
            log::debug!("[MULTI-NODE] Snapshot policy: {:?}", snapshot_policy);
            let mut cfg = Config {
                cluster_name: format!("kalamdb-{}", self.group_id),
                election_timeout_min: config.election_timeout_ms.0,
                election_timeout_max: config.election_timeout_ms.1,
                heartbeat_interval: config.heartbeat_interval_ms,
                install_snapshot_timeout: 10000,
                max_in_snapshot_log_to_keep: config.max_snapshots_to_keep as u64,
                purge_batch_size: 256,
                ..Default::default()
            };

            // Configure snapshot policy from config
            cfg.snapshot_policy = snapshot_policy;
            cfg
        };

        let config =
            Arc::new(raft_config.validate().map_err(|e| RaftError::Config(e.to_string()))?);

        // Create adaptor from combined storage
        let (log_store, state_machine): (StorageAdaptor<SM>, StorageAdaptor<SM>) =
            Adaptor::new(self.storage.clone());

        // Create the Raft instance
        let raft = Raft::new(
            node_id.as_u64(),
            config,
            self.network_factory.clone(),
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| RaftError::Internal(format!("Failed to create Raft: {:?}", e)))?;

        // Store the instance
        {
            let mut guard = self.raft.write();
            *guard = Some(raft);
        }

        log::debug!("Started Raft group {} on node {}", self.group_id, node_id);
        Ok(())
    }

    /// Initialize the cluster (call on first node only)
    ///
    /// This bootstraps the cluster with an initial membership containing only this node.
    /// If the cluster is already initialized (has existing log entries), this is a no-op.
    pub async fn initialize(&self, node_id: NodeId, node: KalamNode) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        // Create initial membership with just this node
        let mut members = std::collections::BTreeMap::new();
        members.insert(node_id.as_u64(), node);

        match raft.initialize(members).await {
            Ok(_) => {
                log::debug!(
                    "Initialized Raft group {} cluster with node {}",
                    self.group_id,
                    node_id
                );
                Ok(())
            },
            Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed(_),
            )) => {
                // Cluster already initialized (has log entries) - this is fine for restart
                log::debug!("Raft group {} already initialized, skipping", self.group_id);
                Ok(())
            },
            Err(e) => Err(RaftError::Internal(format!("Failed to initialize cluster: {:?}", e))),
        }
    }

    /// Add a learner (non-voting member) to the cluster
    pub async fn add_learner(&self, node_id: NodeId, node: KalamNode) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        raft.add_learner(node_id.as_u64(), node, true)
            .await
            .map_err(|e| RaftError::Internal(format!("Failed to add learner: {:?}", e)))?;

        Ok(())
    }

    /// Wait for a learner to catch up to the leader's commit index
    pub async fn wait_for_learner_catchup(
        &self,
        node_id: NodeId,
        timeout: Duration,
    ) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        let start = Instant::now();
        let poll_interval = Duration::from_millis(50);

        loop {
            if start.elapsed() > timeout {
                return Err(RaftError::ReplicationTimeout {
                    group: self.group_id.to_string(),
                    committed_log_id: "catchup".to_string(),
                    timeout_ms: timeout.as_millis() as u64,
                });
            }

            let metrics = raft.metrics().borrow().clone();
            if metrics.current_leader != Some(metrics.id) {
                return Err(RaftError::not_leader(
                    self.group_id.to_string(),
                    metrics.current_leader,
                ));
            }

            let last_log_index = metrics.last_log_index.unwrap_or(0);
            let snapshot_index = metrics.snapshot.map(|log_id| log_id.index).unwrap_or(0);
            let applied_index = metrics.last_applied.map(|log_id| log_id.index).unwrap_or(0);
            let target_index = last_log_index.max(snapshot_index).max(applied_index);

            if target_index == 0 {
                return Ok(());
            }

            if let Some(ref replication) = metrics.replication {
                if let Some(matched) = replication.get(&node_id.as_u64()) {
                    let matched_index = matched.map(|log_id| log_id.index).unwrap_or(0);
                    if matched_index >= target_index {
                        return Ok(());
                    }
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Change membership to include the given voters
    pub async fn change_membership(&self, members: Vec<NodeId>) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        let member_set: std::collections::BTreeSet<u64> =
            members.into_iter().map(|id| id.as_u64()).collect();
        raft.change_membership(member_set, false)
            .await
            .map_err(|e| RaftError::Internal(format!("Failed to change membership: {:?}", e)))?;

        Ok(())
    }

    /// Promote a learner to a voter using the current membership configuration
    pub async fn promote_learner(&self, node_id: NodeId) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        let metrics = raft.metrics().borrow().clone();
        if metrics.current_leader != Some(metrics.id) {
            return Err(RaftError::not_leader(self.group_id.to_string(), metrics.current_leader));
        }

        let mut voters: std::collections::BTreeSet<u64> =
            metrics.membership_config.voter_ids().collect();
        if voters.contains(&node_id.as_u64()) {
            return Ok(());
        }

        voters.insert(node_id.as_u64());
        raft.change_membership(voters, false)
            .await
            .map_err(|e| RaftError::Internal(format!("Failed to change membership: {:?}", e)))?;

        Ok(())
    }

    /// Get the Raft instance (if started)
    pub fn raft(&self) -> Option<RaftInstance> {
        self.raft.read().clone()
    }

    /// Get the group ID
    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Check if this group has been started
    pub fn is_started(&self) -> bool {
        self.raft.read().is_some()
    }

    /// Check if this node is the leader for this group
    pub fn is_leader(&self) -> bool {
        let raft = self.raft.read();
        match raft.as_ref() {
            Some(r) => {
                let metrics = r.metrics().borrow().clone();
                metrics.current_leader == Some(metrics.id)
            },
            None => false,
        }
    }

    /// Get the current leader node ID, if known
    pub fn current_leader(&self) -> Option<NodeId> {
        let raft = self.raft.read();
        raft.as_ref()
            .and_then(|r| r.metrics().borrow().current_leader.map(NodeId::from))
    }

    /// Get the latest OpenRaft metrics for this group, if started
    pub fn metrics(&self) -> Option<RaftMetrics<u64, KalamNode>> {
        let raft = self.raft.read();
        raft.as_ref().map(|r| r.metrics().borrow().clone())
    }

    /// Propose a command to this Raft group
    ///
    /// Returns the response after the command is committed and applied.
    /// Note: This method only works if this node is the leader.
    /// For automatic forwarding, use `propose_with_forward`.
    pub async fn propose(
        &self,
        command: crate::RaftCommand,
    ) -> Result<crate::RaftResponse, RaftError> {
        let (response, _log_index) = self.propose_with_index(command).await?;
        Ok(response)
    }

    /// Propose a command and return both response and log index
    ///
    /// Handles serialization of the command and deserialization of the response internally.
    /// Returns (response, log_index) after the command is committed and applied.
    /// The log_index is useful for read-your-writes consistency when forwarding.
    /// Note: This method only works if this node is the leader.
    pub async fn propose_with_index(
        &self,
        command: crate::RaftCommand,
    ) -> Result<(crate::RaftResponse, u64), RaftError> {
        // Serialize the INNER command (not the RaftCommand wrapper)
        // The state machine expects MetaCommand, UserDataCommand, or SharedDataCommand directly
        let command_bytes = match &command {
            crate::RaftCommand::Meta(cmd) => crate::codec::command_codec::encode_meta_command(cmd)?,
            crate::RaftCommand::UserData(cmd) => {
                crate::codec::command_codec::encode_user_data_command(cmd)?
            },
            crate::RaftCommand::SharedData(cmd) => {
                crate::codec::command_codec::encode_shared_data_command(cmd)?
            },
            crate::RaftCommand::TransactionCommit { .. } => {
                crate::codec::command_codec::encode_raft_command(&command)?
            },
        };

        // Clone Arc once outside the lock scope to avoid holding read lock
        let raft = self
            .raft
            .read()
            .as_ref()
            .ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
            .clone();

        // Submit the command and wait for commit
        let response = raft
            .client_write(command_bytes)
            .await
            .map_err(|e| RaftError::Proposal(format!("{:?}", e)))?;

        // Deserialize the response based on command type using centralized serde_helpers.
        // The state machine returns MetaResponse or DataResponse directly, not wrapped in RaftResponse.
        // If the state machine short-circuits with NoOp, response.data can be empty; treat as Ok.
        let response_obj = if response.data.is_empty() {
            match command {
                crate::RaftCommand::Meta(_) => crate::RaftResponse::Meta(crate::MetaResponse::Ok),
                crate::RaftCommand::UserData(_)
                | crate::RaftCommand::SharedData(_)
                | crate::RaftCommand::TransactionCommit { .. } => {
                    crate::RaftResponse::Data(crate::DataResponse::Ok)
                },
            }
        } else {
            match command {
                crate::RaftCommand::Meta(_) => {
                    let meta_response =
                        crate::codec::command_codec::decode_meta_response(&response.data)?;
                    crate::RaftResponse::Meta(meta_response)
                },
                crate::RaftCommand::UserData(_)
                | crate::RaftCommand::SharedData(_)
                | crate::RaftCommand::TransactionCommit { .. } => {
                    let data_response =
                        crate::codec::command_codec::decode_data_response(&response.data)?;
                    crate::RaftResponse::Data(data_response)
                },
            }
        };

        Ok((response_obj, response.log_id.index))
    }

    /// Propose a command with automatic leader forwarding
    ///
    /// If this node is the leader, proposes locally.
    /// If this node is a follower, forwards the proposal to the leader via gRPC.
    /// Includes retry logic for transient failures (e.g., leader unknown).
    ///
    /// For the Meta group, when forwarding, waits for the log entry to be applied
    /// locally to ensure read-your-writes consistency.
    pub async fn propose_with_forward(
        &self,
        command: crate::RaftCommand,
    ) -> Result<crate::RaftResponse, RaftError> {
        // Fast path: if we are the leader, propose locally
        if self.is_leader() {
            return self.propose(command).await;
        }

        // Serialize command once for forwarding using centralized serde_helpers
        let command_bytes = crate::codec::command_codec::encode_raft_command(&command)?;

        // We're not the leader - try to forward to the leader with retries
        // because the leader might not be known yet (during election)
        const MAX_RETRIES: u32 = 5;
        const INITIAL_BACKOFF_MS: u64 = 50;

        let mut last_error = None;

        for attempt in 0..MAX_RETRIES {
            // Get current leader
            match self.current_leader() {
                Some(leader_id) => {
                    // Get leader node info
                    match self.network_factory.get_node(leader_id) {
                        Some(leader_node) => {
                            log::debug!(
                                "Forwarding proposal for group {} to leader {} at {} (attempt {})",
                                self.group_id,
                                leader_id,
                                leader_node.rpc_addr,
                                attempt + 1
                            );

                            let cmd_bytes = command_bytes.clone();

                            // Try to forward
                            match self.forward_to_leader(leader_id, cmd_bytes).await {
                                Ok((response_bytes, log_index)) => {
                                    // Deserialize the response using serdes_helpers
                                    let response =
                                        crate::codec::command_codec::decode_raft_response(
                                            &response_bytes,
                                        )
                                        .map_err(|e| {
                                            RaftError::Internal(format!(
                                                "Failed to deserialize forwarded response: {}",
                                                e
                                            ))
                                        })?;

                                    // Wait for local apply to ensure read-your-writes consistency
                                    // This applies to ALL groups (Meta and Data shards)
                                    if log_index > 0 {
                                        log::debug!(
                                            "Waiting for {} log index {} to be applied locally for read-your-writes consistency",
                                            self.group_id, log_index
                                        );

                                        // Poll the state machine until the log is applied
                                        let start = Instant::now();
                                        let timeout = Duration::from_secs(10);
                                        let poll_interval = Duration::from_millis(5);

                                        loop {
                                            let applied =
                                                self.storage.state_machine().last_applied_index();
                                            if applied >= log_index {
                                                log::debug!(
                                                    "{} log index {} applied locally (current: {}), read-your-writes consistency achieved",
                                                    self.group_id, log_index, applied
                                                );
                                                break;
                                            }

                                            if start.elapsed() > timeout {
                                                log::warn!(
                                                    "Timeout waiting for {} log index {} to be applied locally (current: {}). \
                                                     Read-your-writes consistency may not be guaranteed.",
                                                    self.group_id, log_index, applied
                                                );
                                                break;
                                            }

                                            tokio::time::sleep(poll_interval).await;
                                        }
                                    }
                                    return Ok(response);
                                },
                                Err(e) => {
                                    log::debug!("Forward attempt {} failed: {}", attempt + 1, e);
                                    last_error = Some(e);
                                    // Continue to retry
                                },
                            }
                        },
                        None => {
                            log::debug!(
                                "Leader node {} for group {} not in registry (attempt {})",
                                leader_id,
                                self.group_id,
                                attempt + 1
                            );
                            last_error = Some(RaftError::Network(format!(
                                "Unknown leader node {} for group {}",
                                leader_id, self.group_id
                            )));
                        },
                    }
                },
                None => {
                    log::debug!(
                        "No leader known for group {} (attempt {}), waiting...",
                        self.group_id,
                        attempt + 1
                    );
                    last_error = Some(RaftError::not_leader(self.group_id.to_string(), None));
                },
            }

            // Wait before retry with exponential backoff
            if attempt + 1 < MAX_RETRIES {
                let backoff = INITIAL_BACKOFF_MS * (1 << attempt);
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| RaftError::not_leader(self.group_id.to_string(), None)))
    }

    /// Forward a proposal to the leader via gRPC
    ///
    /// Returns (payload, log_index) where log_index is the Raft log index
    /// of the applied entry (for read-your-writes consistency).
    async fn forward_to_leader(
        &self,
        leader_node_id: NodeId,
        command: Vec<u8>,
    ) -> Result<(Vec<u8>, u64), RaftError> {
        use crate::network::{ClientProposalRequest, RaftClient};
        let channel =
            self.network_factory.get_or_create_channel(leader_node_id).ok_or_else(|| {
                RaftError::Network(format!(
                    "No channel available for leader node {}",
                    leader_node_id
                ))
            })?;

        let mut client = RaftClient::new(channel);

        // Send the proposal
        let mut request = tonic::Request::new(ClientProposalRequest {
            group_id: self.group_id.to_string(),
            command,
        });
        self.network_factory
            .add_outgoing_rpc_metadata(&mut request)
            .map_err(|e| RaftError::Network(format!("Failed to add RPC metadata: {}", e)))?;

        let response = client
            .client_proposal(request)
            .await
            .map_err(|e| RaftError::Network(format!("gRPC error forwarding proposal: {}", e)))?;

        let inner = response.into_inner();

        if inner.success {
            Ok((inner.payload, inner.log_index))
        } else if let Some(leader_hint) = inner.leader_hint {
            // Leader might have changed - return not_leader error with hint
            Err(RaftError::not_leader(self.group_id.to_string(), Some(leader_hint)))
        } else {
            Err(RaftError::Proposal(inner.error))
        }
    }

    /// Register a peer node
    pub fn register_peer(&self, node_id: NodeId, node: KalamNode) {
        self.network_factory.register_node(node_id, node);
    }

    /// Get reference to the storage
    pub fn storage(&self) -> &Arc<KalamRaftStorage<SM>> {
        &self.storage
    }

    /// Get reference to the network factory
    pub fn network_factory(&self) -> &RaftNetworkFactory {
        &self.network_factory
    }

    /// Attempt to transfer leadership to another node
    ///
    /// In OpenRaft v0.9, we don't have explicit leadership transfer API.
    /// Instead, we trigger an election by sending heartbeats with a hint
    /// that another node should take over. If leadership transfer is not
    /// supported, we just log and continue - the cluster will re-elect.
    pub async fn transfer_leadership(&self, target_node_id: NodeId) -> Result<(), RaftError> {
        Err(RaftError::InvalidState(format!(
            "Leadership transfer is unsupported in current OpenRaft version for group {} (target node {})",
            self.group_id, target_node_id
        )))
    }

    /// Trigger election for this Raft group
    pub async fn trigger_election(&self) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        raft.trigger().elect().await.map_err(|e| {
            RaftError::Internal(format!(
                "Failed to trigger election for group {}: {:?}",
                self.group_id, e
            ))
        })?;

        Ok(())
    }

    /// Purge logs up to the given index for this Raft group
    pub async fn purge_log(&self, upto: u64) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        raft.trigger().purge_log(upto).await.map_err(|e| {
            RaftError::Internal(format!(
                "Failed to purge logs for group {}: {:?}",
                self.group_id, e
            ))
        })?;

        Ok(())
    }

    /// Attempt to step down the leader for this group
    pub async fn step_down(&self) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        if !self.is_leader() {
            return Ok(());
        }

        let cooldown_ms = raft.config().election_timeout_max * 2;
        let cooldown = Duration::from_millis(cooldown_ms);

        log::info!(
            "Stepdown requested for group {} - disabling heartbeats/election for {}ms",
            self.group_id,
            cooldown_ms
        );

        let runtime = raft.runtime_config();
        runtime.heartbeat(false);
        runtime.elect(false);

        let raft_clone = raft.clone();
        tokio::spawn(async move {
            tokio::time::sleep(cooldown).await;
            let runtime = raft_clone.runtime_config();
            runtime.heartbeat(true);
            runtime.elect(true);
        });

        Ok(())
    }

    /// Trigger a snapshot for this Raft group
    ///
    /// Forces OpenRaft to create a snapshot of the current state.
    /// This is useful for CLUSTER SNAPSHOT operations to ensure durability.
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        let raft = {
            let guard = self.raft.read();
            guard.clone().ok_or_else(|| RaftError::NotStarted(self.group_id.to_string()))?
        };

        let metrics = raft.metrics().borrow().clone();
        let snapshot_index = metrics.snapshot.map(|log_id| log_id.index);
        let last_log_index = metrics.last_log_index.unwrap_or(0);
        let last_applied_index = metrics.last_applied.map(|log_id| log_id.index).unwrap_or(0);
        let target_index = last_log_index.max(last_applied_index);

        if let Some(snapshot_index) = snapshot_index {
            if snapshot_index >= target_index {
                log::debug!(
                    "Skipping snapshot for Raft group {} (snapshot_index={}, target_index={})",
                    self.group_id,
                    snapshot_index,
                    target_index
                );
                return Ok(());
            }
        }

        raft.trigger().snapshot().await.map_err(|e| {
            RaftError::Internal(format!(
                "Failed to trigger snapshot for group {}: {:?}",
                self.group_id, e
            ))
        })?;

        log::debug!("Triggered snapshot for Raft group {}", self.group_id);
        Ok(())
    }

    /// Get the snapshot index for this group (if a snapshot exists)
    pub fn snapshot_index(&self) -> Option<u64> {
        self.metrics().and_then(|m| m.snapshot.map(|log_id| log_id.index))
    }

    /// Shutdown this Raft group
    ///
    /// Calls OpenRaft's Raft::shutdown() to cleanly terminate the internal tasks.
    /// This is important for test cleanup to prevent "Fatal(Stopped)" errors.
    pub async fn shutdown(&self) -> Result<(), RaftError> {
        let raft = {
            let mut guard = self.raft.write();
            guard.take() // Take ownership to drop after shutdown
        };

        if let Some(raft) = raft {
            log::debug!("Shutting down Raft group {}", self.group_id);
            raft.shutdown().await.map_err(|e| {
                RaftError::Internal(format!(
                    "Failed to shutdown Raft group {}: {:?}",
                    self.group_id, e
                ))
            })?;
            log::debug!("Raft group {} shutdown complete", self.group_id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::MetaStateMachine;

    #[test]
    fn test_raft_group_creation() {
        let sm = MetaStateMachine::new();
        let group = RaftGroup::new(GroupId::Meta, sm);

        assert_eq!(group.group_id(), GroupId::Meta);
        assert!(!group.is_started());
        assert!(!group.is_leader());
    }
}
