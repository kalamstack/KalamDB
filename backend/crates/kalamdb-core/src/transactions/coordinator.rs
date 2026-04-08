use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::runtime::Handle;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use kalamdb_commons::models::{NodeId, TableId, TransactionId, TransactionOrigin, TransactionState, UserId};
use kalamdb_commons::TableType;
use kalamdb_raft::RaftExecutor;
use kalamdb_sharding::{GroupId, ShardRouter};

use crate::app_context::AppContext;
use crate::error::KalamDbError;

use super::{
    ActiveTransactionMetric, CommitSequenceTracker, ExecutionOwnerKey, StagedMutation,
    TransactionCommitResult, TransactionHandle, TransactionOverlay, TransactionRaftBinding,
    TransactionWriteSet,
};

#[derive(Debug, Clone, Copy)]
struct TransactionAccessRoute {
    group_id: GroupId,
    current_leader_node_id: Option<NodeId>,
}

/// In-memory coordinator for active explicit transactions.
#[derive(Debug)]
pub struct TransactionCoordinator {
    app_context: Arc<AppContext>,
    commit_sequence_tracker: Arc<CommitSequenceTracker>,
    active_by_owner: DashMap<ExecutionOwnerKey, TransactionId>,
    active_by_id: DashMap<TransactionId, TransactionHandle>,
    write_sets: DashMap<TransactionId, TransactionWriteSet>,
}

impl TransactionCoordinator {
    pub fn new(
        app_context: Arc<AppContext>,
        commit_sequence_tracker: Arc<CommitSequenceTracker>,
    ) -> Self {
        Self {
            app_context,
            commit_sequence_tracker,
            active_by_owner: DashMap::new(),
            active_by_id: DashMap::new(),
            write_sets: DashMap::new(),
        }
    }

    pub fn start_timeout_sweeper(self: &Arc<Self>) {
        let timeout = self.timeout_duration();
        let sweep_interval = Self::timeout_sweep_interval(timeout);
        let coordinator = Arc::downgrade(self);

        match Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    Self::run_timeout_sweeper(coordinator, sweep_interval).await;
                });
            },
            Err(error) => {
                log::warn!(
                    "transaction timeout sweeper was not started because no Tokio runtime is active: {}",
                    error
                );
            },
        }
    }

    pub fn begin(
        &self,
        owner_key: ExecutionOwnerKey,
        owner_id: Arc<str>,
        origin: TransactionOrigin,
    ) -> Result<TransactionId, KalamDbError> {
        if self.active_by_owner.contains_key(&owner_key) {
            return Err(KalamDbError::Conflict(format!(
                "owner '{}' already has an active transaction",
                owner_id
            )));
        }

        let transaction_id = TransactionId::new(Uuid::now_v7().to_string());
        let snapshot_commit_seq = self.commit_sequence_tracker.current_committed();
        let handle = TransactionHandle::new(
            transaction_id.clone(),
            owner_key,
            Arc::clone(&owner_id),
            origin,
            self.initial_raft_binding(),
            snapshot_commit_seq,
            Instant::now(),
        );

        self.active_by_owner.insert(owner_key, transaction_id.clone());
        self.active_by_id.insert(transaction_id.clone(), handle);
        Ok(transaction_id)
    }

    pub fn stage(
        &self,
        transaction_id: &TransactionId,
        mutation: StagedMutation,
    ) -> Result<(), KalamDbError> {
        if mutation.table_type == TableType::Stream {
            return Err(KalamDbError::InvalidOperation(
                "stream tables are not supported inside explicit transactions".to_string(),
            ));
        }

        if mutation.table_type == TableType::System {
            return Err(KalamDbError::InvalidOperation(
                "system tables are not supported inside explicit transactions".to_string(),
            ));
        }

        if &mutation.transaction_id != transaction_id {
            return Err(KalamDbError::InvalidOperation(format!(
                "staged mutation transaction mismatch: expected '{}', got '{}'",
                transaction_id, mutation.transaction_id
            )));
        }

        let estimated_bytes = mutation.approximate_size_bytes();
        let table_id = mutation.table_id.clone();

        self.validate_table_access(
            transaction_id,
            &mutation.table_id,
            mutation.table_type,
            mutation.user_id.as_ref(),
        )?;

        let mut handle = self
            .active_by_id
            .get_mut(transaction_id)
            .ok_or_else(|| KalamDbError::NotFound(format!("transaction '{}' not found", transaction_id)))?;

        if !handle.state.is_open() {
            return Err(Self::state_error(transaction_id, handle.state, "stage writes in"));
        }

        let projected_buffer_bytes = handle.write_bytes.saturating_add(estimated_bytes);
        let max_transaction_buffer_bytes = self.app_context.config().max_transaction_buffer_bytes;
        if projected_buffer_bytes > max_transaction_buffer_bytes {
            handle.mark_state(TransactionState::Aborted);
            drop(handle);
            self.write_sets.remove(transaction_id);

            tracing::warn!(
                transaction_id = %transaction_id,
                projected_buffer_bytes,
                max_transaction_buffer_bytes,
                "transaction buffer limit exceeded"
            );

            return Err(KalamDbError::InvalidOperation(format!(
                "transaction '{}' exceeded buffer limit of {} bytes and was aborted",
                transaction_id, max_transaction_buffer_bytes
            )));
        }

        let mut write_set = self
            .write_sets
            .entry(transaction_id.clone())
            .or_insert_with(|| TransactionWriteSet::new(transaction_id.clone()));
        write_set.stage(mutation);

        handle.record_staged_write(table_id, write_set.affected_rows(), write_set.buffer_bytes);
        Ok(())
    }

    pub async fn commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<TransactionCommitResult, KalamDbError> {
        let owner_key = {
            let mut handle = self
                .active_by_id
                .get_mut(transaction_id)
                .ok_or_else(|| {
                    KalamDbError::NotFound(format!("transaction '{}' not found", transaction_id))
                })?;

            if !handle.state.is_open() {
                return Err(Self::state_error(transaction_id, handle.state, "commit"));
            }

            if let Err(error) = self.ensure_bound_leadership_is_current(transaction_id, &mut handle) {
                drop(handle);
                self.write_sets.remove(transaction_id);
                return Err(error);
            }

            handle.mark_state(TransactionState::Committing);
            handle.owner_key
        };

        let write_set = self.write_sets.remove(transaction_id).map(|(_, write_set)| write_set);

        let Some(write_set) = write_set else {
            self.active_by_owner.remove(&owner_key);

            let Some((_, mut sealed_handle)) = self.active_by_id.remove(transaction_id) else {
                return Err(KalamDbError::NotFound(format!(
                    "transaction '{}' not found during commit",
                    transaction_id
                )));
            };
            sealed_handle.mark_state(TransactionState::Committed);

            return Ok(TransactionCommitResult::committed(
                transaction_id.clone(),
                0,
                None,
                super::TransactionSideEffects::default(),
            ));
        };

        let affected_rows = write_set.affected_rows();
        let mutations = write_set.ordered_mutations.clone();

        let response = match self
            .app_context
            .applier()
            .commit_transaction(transaction_id.clone(), mutations)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.active_by_owner.remove(&owner_key);
                if let Some((_, mut sealed_handle)) = self.active_by_id.remove(transaction_id) {
                    sealed_handle.mark_state(TransactionState::Aborted);
                }
                return Err(KalamDbError::InvalidOperation(format!(
                    "failed to commit transaction '{}': {}",
                    transaction_id, error
                )));
            },
        };

        let committed_commit_seq = response.committed_commit_seq().ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "transaction '{}' commit response missing commit_seq",
                transaction_id
            ))
        })?;
        let (notifications_sent, manifest_updates, publisher_events) = response
            .committed_side_effect_counts()
            .unwrap_or((0, 0, 0));

        self.commit_sequence_tracker.observe_committed(committed_commit_seq);

        self.active_by_owner.remove(&owner_key);

        let Some((_, mut sealed_handle)) = self.active_by_id.remove(transaction_id) else {
            return Err(KalamDbError::NotFound(format!(
                "transaction '{}' not found during commit",
                transaction_id
            )));
        };
        sealed_handle.mark_state(TransactionState::Committed);

        Ok(TransactionCommitResult::committed(
            transaction_id.clone(),
            affected_rows,
            Some(committed_commit_seq),
            super::TransactionSideEffects {
                notifications_sent,
                manifest_updates,
                publisher_events,
            },
        ))
    }

    pub fn rollback(&self, transaction_id: &TransactionId) -> Result<(), KalamDbError> {
        let owner_key = {
            let mut handle = self
                .active_by_id
                .get_mut(transaction_id)
                .ok_or_else(|| {
                    KalamDbError::NotFound(format!("transaction '{}' not found", transaction_id))
                })?;

            if matches!(
                handle.state,
                TransactionState::TimedOut | TransactionState::Aborted | TransactionState::RolledBack
            ) {
                let owner_key = handle.owner_key;
                drop(handle);
                self.write_sets.remove(transaction_id);
                self.active_by_owner.remove(&owner_key);
                self.active_by_id.remove(transaction_id);
                return Ok(());
            }

            if handle.state.is_terminal() {
                return Err(Self::state_error(transaction_id, handle.state, "rollback"));
            }

            handle.mark_state(TransactionState::RollingBack);
            handle.owner_key
        };

        self.write_sets.remove(transaction_id);
        self.active_by_owner.remove(&owner_key);
        let Some((_, mut sealed_handle)) = self.active_by_id.remove(transaction_id) else {
            return Err(KalamDbError::NotFound(format!(
                "transaction '{}' not found during rollback",
                transaction_id
            )));
        };
        sealed_handle.mark_state(TransactionState::RolledBack);

        Ok(())
    }

    pub fn get_overlay(&self, transaction_id: &TransactionId) -> Option<TransactionOverlay> {
        self.write_sets.get(transaction_id).map(|write_set| write_set.merged_overlay())
    }

    pub fn get_handle(&self, transaction_id: &TransactionId) -> Option<TransactionHandle> {
        self.active_by_id.get(transaction_id).map(|handle| handle.clone())
    }

    #[doc(hidden)]
    pub fn force_raft_binding_for_test(
        &self,
        transaction_id: &TransactionId,
        raft_binding: TransactionRaftBinding,
    ) -> Result<(), KalamDbError> {
        let mut handle = self
            .active_by_id
            .get_mut(transaction_id)
            .ok_or_else(|| KalamDbError::NotFound(format!("transaction '{}' not found", transaction_id)))?;
        handle.raft_binding = raft_binding;
        Ok(())
    }

    pub fn validate_table_access(
        &self,
        transaction_id: &TransactionId,
        table_id: &TableId,
        table_type: TableType,
        user_id: Option<&UserId>,
    ) -> Result<(), KalamDbError> {
        let mut handle = self
            .active_by_id
            .get_mut(transaction_id)
            .ok_or_else(|| KalamDbError::NotFound(format!("transaction '{}' not found", transaction_id)))?;

        if !handle.state.is_open() {
            return Err(Self::state_error(transaction_id, handle.state, "access"));
        }

        let Some(route) = self.resolve_access_route(table_id, table_type, user_id)? else {
            handle.touch();
            return Ok(());
        };

        let current_node_id = self.app_context.executor().node_id();
        let outcome = match handle.raft_binding {
            TransactionRaftBinding::LocalSingleNode => TransactionAccessOutcome::Allow,
            TransactionRaftBinding::UnboundCluster => {
                if route.current_leader_node_id == Some(current_node_id) {
                    handle.raft_binding = TransactionRaftBinding::BoundCluster {
                        group_id: route.group_id,
                        leader_node_id: current_node_id,
                    };
                    TransactionAccessOutcome::Allow
                } else {
                    TransactionAccessOutcome::Error(KalamDbError::NotLeader {
                        leader_addr: route
                            .current_leader_node_id
                            .and_then(|node_id| self.leader_addr_for_node(node_id)),
                    })
                }
            },
            TransactionRaftBinding::BoundCluster {
                group_id,
                leader_node_id,
            } => {
                if group_id != route.group_id {
                    TransactionAccessOutcome::Error(KalamDbError::InvalidOperation(format!(
                        "explicit transaction '{}' is already bound to data raft group '{}' and cannot access table '{}' in group '{}'",
                        transaction_id, group_id, table_id, route.group_id
                    )))
                } else if route.current_leader_node_id != Some(leader_node_id) {
                    handle.mark_state(TransactionState::Aborted);
                    TransactionAccessOutcome::Abort(Self::leader_change_error(
                        transaction_id,
                        group_id,
                        leader_node_id,
                        route.current_leader_node_id,
                    ))
                } else {
                    TransactionAccessOutcome::Allow
                }
            },
        };

        match outcome {
            TransactionAccessOutcome::Allow => {
                handle.touch();
                Ok(())
            },
            TransactionAccessOutcome::Error(error) => Err(error),
            TransactionAccessOutcome::Abort(error) => {
                drop(handle);
                self.write_sets.remove(transaction_id);
                Err(error)
            },
        }
    }

    pub fn active_metrics(&self) -> Vec<ActiveTransactionMetric> {
        let now = Instant::now();
        let mut metrics = self
            .active_by_id
            .iter()
            .filter_map(|entry| {
                let handle = entry.value();
                if handle.state.is_terminal() {
                    return None;
                }

                Some(ActiveTransactionMetric {
                    transaction_id: handle.transaction_id.clone(),
                    owner_id: Arc::clone(&handle.owner_id),
                    state: handle.state,
                    age_ms: now.duration_since(handle.started_at).as_millis() as u64,
                    idle_ms: now.duration_since(handle.last_activity_at).as_millis() as u64,
                    write_count: handle.write_count,
                    write_bytes: handle.write_bytes,
                    touched_tables_count: handle.touched_tables.len(),
                    snapshot_commit_seq: handle.snapshot_commit_seq,
                    origin: handle.origin,
                })
            })
            .collect::<Vec<_>>();

        metrics.sort_by(|left, right| {
            left.origin
                .as_str()
                .cmp(right.origin.as_str())
                .then_with(|| left.transaction_id.as_str().cmp(right.transaction_id.as_str()))
        });
        metrics
    }

    pub fn active_for_owner(&self, owner_key: &ExecutionOwnerKey) -> Option<TransactionId> {
        self.active_by_owner.get(owner_key).map(|transaction_id| transaction_id.clone())
    }

    pub fn reject_ddl_in_transaction(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<(), KalamDbError> {
        if let Some(handle) = self.active_by_id.get(transaction_id) {
            if !matches!(handle.state, TransactionState::Committed | TransactionState::RolledBack) {
                return Err(KalamDbError::InvalidOperation(format!(
                    "DDL is not allowed inside explicit transaction '{}'",
                    transaction_id
                )));
            }
        }

        Ok(())
    }

    fn timeout_duration(&self) -> Duration {
        Duration::from_secs(self.app_context.config().transaction_timeout_secs.max(1))
    }

    fn timeout_sweep_interval(timeout: Duration) -> Duration {
        let timeout_secs = timeout.as_secs().max(1);
        Duration::from_secs((timeout_secs / 2).clamp(1, 5))
    }

    async fn run_timeout_sweeper(
        coordinator: Weak<Self>,
        sweep_interval: Duration,
    ) {
        let mut interval = tokio::time::interval(sweep_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            interval.tick().await;

            let Some(coordinator) = coordinator.upgrade() else {
                break;
            };

            coordinator.sweep_timed_out_transactions();
        }
    }

    fn sweep_timed_out_transactions(&self) {
        let timeout = self.timeout_duration();
        let now = Instant::now();
        let expired_transactions = self
            .active_by_id
            .iter()
            .filter_map(|entry| {
                let handle = entry.value();
                if handle.state.is_open() && now.duration_since(handle.last_activity_at) >= timeout {
                    Some((
                        handle.transaction_id.clone(),
                        now.duration_since(handle.started_at),
                        now.duration_since(handle.last_activity_at),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for (transaction_id, age, idle) in expired_transactions {
            self.timeout_transaction(&transaction_id, age, idle);
        }
    }

    fn timeout_transaction(
        &self,
        transaction_id: &TransactionId,
        age: Duration,
        idle: Duration,
    ) {
        let Some(mut handle) = self.active_by_id.get_mut(transaction_id) else {
            return;
        };

        if !handle.state.is_open() {
            return;
        }

        handle.mark_state(TransactionState::TimedOut);
        drop(handle);
        self.write_sets.remove(transaction_id);

        tracing::warn!(
            transaction_id = %transaction_id,
            age_ms = age.as_millis() as u64,
            idle_ms = idle.as_millis() as u64,
            "transaction timed out"
        );
    }

    fn state_error(
        transaction_id: &TransactionId,
        state: TransactionState,
        action: &str,
    ) -> KalamDbError {
        match state {
            TransactionState::TimedOut => KalamDbError::InvalidOperation(format!(
                "cannot {} transaction '{}' because it timed out and was aborted",
                action, transaction_id
            )),
            TransactionState::Aborted => KalamDbError::InvalidOperation(format!(
                "cannot {} transaction '{}' because it was aborted",
                action, transaction_id
            )),
            _ => KalamDbError::InvalidOperation(format!(
                "cannot {} transaction '{}' while it is {}",
                action, transaction_id, state
            )),
        }
    }

    fn initial_raft_binding(&self) -> TransactionRaftBinding {
        if self.app_context.is_cluster_mode() {
            TransactionRaftBinding::UnboundCluster
        } else {
            TransactionRaftBinding::LocalSingleNode
        }
    }

    fn resolve_access_route(
        &self,
        table_id: &TableId,
        table_type: TableType,
        user_id: Option<&UserId>,
    ) -> Result<Option<TransactionAccessRoute>, KalamDbError> {
        if !self.app_context.is_cluster_mode() {
            return Ok(None);
        }

        let router = ShardRouter::from_optional_cluster_config(self.app_context.config().cluster.as_ref());
        let group_id = match table_type {
            TableType::User => router.user_group_id(user_id.ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "transactional access to user table '{}' requires user_id",
                    table_id
                ))
            })?),
            TableType::Shared => router.shared_group_id(),
            TableType::Stream => {
                return Err(KalamDbError::InvalidOperation(
                    "stream tables are not supported inside explicit transactions".to_string(),
                ));
            },
            TableType::System => return Ok(None),
        };

        Ok(Some(TransactionAccessRoute {
            group_id,
            current_leader_node_id: self.current_leader_for_group(group_id),
        }))
    }

    fn current_leader_for_group(&self, group_id: GroupId) -> Option<NodeId> {
        if !self.app_context.is_cluster_mode() {
            return Some(self.app_context.executor().node_id());
        }

        let executor = self.app_context.executor();
        let raft_executor = executor.as_any().downcast_ref::<RaftExecutor>()?;
        raft_executor.manager().current_leader(group_id)
    }

    fn leader_addr_for_node(&self, node_id: NodeId) -> Option<String> {
        self.app_context
            .executor()
            .get_cluster_info()
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(|node| node.api_addr.clone())
    }

    fn ensure_bound_leadership_is_current(
        &self,
        transaction_id: &TransactionId,
        handle: &mut TransactionHandle,
    ) -> Result<(), KalamDbError> {
        let TransactionRaftBinding::BoundCluster {
            group_id,
            leader_node_id,
        } = handle.raft_binding else {
            return Ok(());
        };

        let current_leader_node_id = self.current_leader_for_group(group_id);
        if current_leader_node_id == Some(leader_node_id) {
            return Ok(());
        }

        handle.mark_state(TransactionState::Aborted);
        Err(Self::leader_change_error(
            transaction_id,
            group_id,
            leader_node_id,
            current_leader_node_id,
        ))
    }

    fn leader_change_error(
        transaction_id: &TransactionId,
        group_id: GroupId,
        prior_leader_node_id: NodeId,
        current_leader_node_id: Option<NodeId>,
    ) -> KalamDbError {
        let current_leader = current_leader_node_id
            .map(|node_id| node_id.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        KalamDbError::InvalidOperation(format!(
            "transaction '{}' was aborted because leader for bound raft group '{}' changed from node '{}' to '{}'; retry in a new transaction",
            transaction_id, group_id, prior_leader_node_id, current_leader
        ))
    }
}

enum TransactionAccessOutcome {
    Allow,
    Error(KalamDbError),
    Abort(KalamDbError),
}