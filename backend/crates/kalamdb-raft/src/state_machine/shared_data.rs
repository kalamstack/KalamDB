//! SharedDataStateMachine - Handles shared table operations
//!
//! This state machine manages:
//! - Shared table INSERT/UPDATE/DELETE
//!
//! Runs in the DataSharedShard(0) Raft group.
//! Shared tables are not sharded by user - all operations go to shard 0.
//!
//! ## Watermark Synchronization
//!
//! Commands with `required_meta_index > current_meta_index` wait for Meta
//! to catch up before applying. This ensures data operations don't run before
//! their dependent metadata (tables) is applied locally.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use kalamdb_commons::{
    models::{OperationKind, TransactionId},
    TableId, TableType,
};
use kalamdb_transactions::StagedMutation;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::{
    decode as bincode_decode, encode as bincode_encode, get_coordinator, ApplyResult,
    KalamStateMachine, PendingBuffer, PendingCommand, StateMachineSnapshot,
};
use crate::{
    applier::SharedDataApplier, commit_seq_from_log_position, DataResponse, GroupId, RaftCommand,
    RaftError, SharedDataCommand,
};

/// Row operation tracking (for metrics)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedOperation {
    table_id: TableId,
    operation: OperationKind,
    row_count: u64,
}

/// Snapshot data for SharedDataStateMachine
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SharedDataSnapshot {
    /// Total operations count
    total_operations: u64,
    /// Recent operations for metrics
    recent_operations: Vec<SharedOperation>,
    /// Pending commands waiting for Meta to catch up (for crash recovery)
    #[serde(default)]
    pending_commands: Vec<PendingCommand>,
}

enum SharedApplyCommand {
    Shared(SharedDataCommand),
    TransactionCommit {
        transaction_id: TransactionId,
        mutations: Vec<StagedMutation>,
    },
}

impl SharedApplyCommand {
    fn required_meta_index(&self) -> u64 {
        match self {
            Self::Shared(command) => command.required_meta_index(),
            Self::TransactionCommit { .. } => 0,
        }
    }
}

/// State machine for shared table operations
///
/// Handles commands in DataSharedShard(0) Raft group:
/// - Insert, Update, Delete (shared table data)
///
/// Note: Row data is persisted via the SharedDataApplier after Raft consensus.
/// All nodes (leader and followers) call the applier, ensuring consistent data.
///
/// ## Watermark Synchronization
///
/// Commands with `required_meta_index > current_meta_index` wait for Meta
/// to catch up before applying. Pending commands restored from snapshots
/// are drained once Meta is satisfied.
pub struct SharedDataStateMachine {
    /// Shard number (always 0 for shared tables in Phase 1)
    shard: u32,
    /// Last applied log index (for idempotency)
    last_applied_index: AtomicU64,
    /// Last applied log term
    last_applied_term: AtomicU64,
    /// Notifies waiters when the applied index advances.
    last_applied_tx: tokio::sync::watch::Sender<u64>,
    /// Approximate data size in bytes
    approximate_size: AtomicU64,
    /// Total operations processed
    total_operations: AtomicU64,
    /// Recent operations for metrics
    recent_operations: RwLock<Vec<SharedOperation>>,
    /// Optional applier for persisting data to providers
    applier: RwLock<Option<Arc<dyn SharedDataApplier>>>,
    /// Buffer for commands waiting for Meta to catch up
    pending_buffer: PendingBuffer,
}

impl std::fmt::Debug for SharedDataStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedDataStateMachine")
            .field("shard", &self.shard)
            .field("last_applied_index", &self.last_applied_index.load(Ordering::Relaxed))
            .field("has_applier", &self.applier.read().is_some())
            .field("pending_count", &self.pending_buffer.len())
            .finish()
    }
}

impl SharedDataStateMachine {
    /// Create a new SharedDataStateMachine
    pub fn new(shard: u32) -> Self {
        let (last_applied_tx, _) = tokio::sync::watch::channel(0);
        Self {
            shard,
            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),
            last_applied_tx,
            approximate_size: AtomicU64::new(0),
            total_operations: AtomicU64::new(0),
            recent_operations: RwLock::new(Vec::new()),
            applier: RwLock::new(None),
            pending_buffer: PendingBuffer::new(),
        }
    }

    /// Create a new SharedDataStateMachine with an applier
    pub fn with_applier(shard: u32, applier: Arc<dyn SharedDataApplier>) -> Self {
        let (last_applied_tx, _) = tokio::sync::watch::channel(0);
        Self {
            shard,
            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),
            last_applied_tx,
            approximate_size: AtomicU64::new(0),
            total_operations: AtomicU64::new(0),
            recent_operations: RwLock::new(Vec::new()),
            applier: RwLock::new(Some(applier)),
            pending_buffer: PendingBuffer::new(),
        }
    }

    /// Create the default shared shard (shard 0)
    pub fn default_shard() -> Self {
        Self::new(0)
    }

    /// Set the applier for persisting data to providers
    ///
    /// This is called after RaftManager creation once providers are available.
    pub fn set_applier(&self, applier: Arc<dyn SharedDataApplier>) {
        let mut guard = self.applier.write();
        *guard = Some(applier);
        log::debug!(
            "SharedDataStateMachine[{}]: Applier registered for data persistence",
            self.shard
        );
    }

    /// Drain and apply any buffered commands that are now satisfied
    async fn drain_pending(&self) -> Result<(), RaftError> {
        let current_meta = get_coordinator().current_index();
        let drained = self.pending_buffer.drain_satisfied(current_meta);

        if !drained.is_empty() {
            log::info!(
                "SharedDataStateMachine[{}]: Draining {} buffered commands (meta_index={})",
                self.shard,
                drained.len(),
                current_meta
            );
        }

        for pending in drained {
            let cmd = Self::decode_apply_command(&pending.command_bytes)?;
            let commit_seq = commit_seq_from_log_position(self.group_id(), pending.log_index);
            let _ = self.apply_decoded_command(cmd, commit_seq).await?;
            log::debug!(
                "SharedDataStateMachine[{}]: Applied buffered command log_index={}",
                self.shard,
                pending.log_index
            );
        }

        Ok(())
    }

    /// Get the number of pending commands
    pub fn pending_count(&self) -> usize {
        self.pending_buffer.len()
    }

    fn publish_last_applied(&self, index: u64) {
        self.last_applied_tx.send_replace(index);
    }

    fn record_operation(&self, table_id: TableId, operation: OperationKind, row_count: usize) {
        let mut ops = self.recent_operations.write();
        ops.push(SharedOperation {
            table_id,
            operation,
            row_count: row_count as u64,
        });
        if ops.len() > 100 {
            ops.remove(0);
        }
    }

    /// Apply a shared data command
    async fn apply_command(
        &self,
        cmd: SharedDataCommand,
        commit_seq: u64,
    ) -> Result<DataResponse, RaftError> {
        // Get applier reference
        let applier = {
            let guard = self.applier.read();
            guard.clone()
        };

        match cmd {
            SharedDataCommand::Insert { table_id, rows, .. } => {
                log::debug!(
                    "SharedDataStateMachine[{}]: Insert into {:?} ({} rows)",
                    self.shard,
                    table_id,
                    rows.len()
                );

                // Persist data via applier if available
                let rows_affected = if let Some(ref a) = applier {
                    match a.insert(&table_id, &rows, commit_seq).await {
                        Ok(count) => count,
                        Err(e) => {
                            log::warn!(
                                "SharedDataStateMachine[{}]: Insert failed: {}",
                                self.shard,
                                e
                            );
                            return Ok(DataResponse::error(e.to_string()));
                        },
                    }
                } else {
                    log::warn!(
                        "SharedDataStateMachine[{}]: No applier set, data not persisted!",
                        self.shard
                    );
                    0
                };

                self.record_operation(table_id, OperationKind::Insert, rows_affected);

                self.total_operations.fetch_add(1, Ordering::Relaxed);
                self.approximate_size.fetch_add(rows.len() as u64, Ordering::Relaxed);

                Ok(DataResponse::RowsAffected(rows_affected))
            },

            SharedDataCommand::Update {
                table_id,
                updates,
                filter,
                ..
            } => {
                log::debug!("SharedDataStateMachine[{}]: Update {:?}", self.shard, table_id);

                let rows_affected = if let Some(ref a) = applier {
                    match a.update(&table_id, &updates, filter.as_deref(), commit_seq).await {
                        Ok(count) => count,
                        Err(e) => {
                            log::warn!(
                                "SharedDataStateMachine[{}]: Update failed: {}",
                                self.shard,
                                e
                            );
                            return Ok(DataResponse::error(e.to_string()));
                        },
                    }
                } else {
                    log::warn!(
                        "SharedDataStateMachine[{}]: No applier set, update not persisted!",
                        self.shard
                    );
                    0
                };

                self.record_operation(table_id, OperationKind::Update, rows_affected);

                self.total_operations.fetch_add(1, Ordering::Relaxed);
                Ok(DataResponse::RowsAffected(rows_affected))
            },

            SharedDataCommand::Delete {
                table_id,
                pk_values,
                ..
            } => {
                log::debug!("SharedDataStateMachine[{}]: Delete from {:?}", self.shard, table_id);

                let rows_affected = if let Some(ref a) = applier {
                    match a.delete(&table_id, pk_values.as_deref(), commit_seq).await {
                        Ok(count) => count,
                        Err(e) => {
                            log::warn!(
                                "SharedDataStateMachine[{}]: Delete failed: {}",
                                self.shard,
                                e
                            );
                            return Ok(DataResponse::error(e.to_string()));
                        },
                    }
                } else {
                    log::warn!(
                        "SharedDataStateMachine[{}]: No applier set, delete not persisted!",
                        self.shard
                    );
                    0
                };

                self.record_operation(table_id, OperationKind::Delete, rows_affected);

                self.total_operations.fetch_add(1, Ordering::Relaxed);
                Ok(DataResponse::RowsAffected(rows_affected))
            },
        }
    }

    fn decode_apply_command(command: &[u8]) -> Result<SharedApplyCommand, RaftError> {
        if let Ok(cmd) = crate::codec::command_codec::decode_shared_data_command(command) {
            return Ok(SharedApplyCommand::Shared(cmd));
        }

        match crate::codec::command_codec::decode_raft_command(command)? {
            RaftCommand::TransactionCommit {
                transaction_id,
                mutations,
            } => Ok(SharedApplyCommand::TransactionCommit {
                transaction_id,
                mutations,
            }),
            other => Err(RaftError::Serialization(format!(
                "Unexpected raft command for shared data state machine: {:?}",
                other
            ))),
        }
    }

    async fn apply_decoded_command(
        &self,
        cmd: SharedApplyCommand,
        commit_seq: u64,
    ) -> Result<DataResponse, RaftError> {
        match cmd {
            SharedApplyCommand::Shared(command) => self.apply_command(command, commit_seq).await,
            SharedApplyCommand::TransactionCommit {
                transaction_id,
                mutations,
            } => self.apply_transaction_commit(transaction_id, mutations, commit_seq).await,
        }
    }

    async fn apply_transaction_commit(
        &self,
        transaction_id: TransactionId,
        mutations: Vec<StagedMutation>,
        commit_seq: u64,
    ) -> Result<DataResponse, RaftError> {
        if mutations.iter().any(|mutation| mutation.table_type != TableType::Shared) {
            return Ok(DataResponse::error(
                "TransactionCommit contained non-shared mutation for shared data shard",
            ));
        }

        let applier = {
            let guard = self.applier.read();
            guard.clone()
        };

        let Some(applier) = applier else {
            return Ok(DataResponse::error("No applier set, transaction commit not persisted"));
        };

        match applier.apply_transaction_batch(&transaction_id, &mutations, commit_seq).await {
            Ok(result) => {
                self.total_operations.fetch_add(1, Ordering::Relaxed);
                Ok(DataResponse::TransactionCommitted(result))
            },
            Err(error) => Ok(DataResponse::error(error.to_string())),
        }
    }
}

impl Default for SharedDataStateMachine {
    fn default() -> Self {
        Self::default_shard()
    }
}

#[async_trait]
impl KalamStateMachine for SharedDataStateMachine {
    fn group_id(&self) -> GroupId {
        GroupId::DataSharedShard(self.shard)
    }

    async fn apply(&self, index: u64, term: u64, command: &[u8]) -> Result<ApplyResult, RaftError> {
        // Idempotency check
        let last_applied = self.last_applied_index.load(Ordering::Acquire);
        if index <= last_applied {
            log::debug!(
                "SharedDataStateMachine[{}]: Skipping already applied entry {} (last_applied={})",
                self.shard,
                index,
                last_applied
            );
            return Ok(ApplyResult::NoOp);
        }

        // Deserialize command to check watermark
        let cmd = Self::decode_apply_command(command)?;
        let required_meta = cmd.required_meta_index();

        // Check watermark: if Meta is behind AND required_meta > 0, wait for it to catch up.
        // DML commands (INSERT/UPDATE/DELETE) set required_meta_index=0 to skip waiting
        // since the table's existence was validated before the command was built.
        // See spec 021 section 5.4.1 "Watermark Nuance" for detailed analysis.
        if required_meta > 0 {
            let current_meta = get_coordinator().current_index();
            if required_meta > current_meta {
                log::debug!(
                    "SharedDataStateMachine[{}]: Buffering command (required_meta={} > \
                     current_meta={})",
                    self.shard,
                    required_meta,
                    current_meta
                );
                self.pending_buffer.add(PendingCommand {
                    log_index: index,
                    log_term: term,
                    required_meta_index: required_meta,
                    command_bytes: command.to_vec(),
                });

                // Mark as applied (buffered) to satisfy Raft log progress
                self.last_applied_index.store(index, Ordering::Release);
                self.last_applied_term.store(term, Ordering::Release);
                self.publish_last_applied(index);

                return Ok(ApplyResult::NoOp);
            }
        }

        // Drain any pending commands that are now satisfied (only if buffer non-empty)
        if !self.pending_buffer.is_empty() {
            self.drain_pending().await?;
        }

        // Apply current command
        let commit_seq = commit_seq_from_log_position(self.group_id(), index);
        let response = self.apply_decoded_command(cmd, commit_seq).await?;

        // Update last applied
        self.last_applied_index.store(index, Ordering::Release);
        self.last_applied_term.store(term, Ordering::Release);
        self.publish_last_applied(index);

        // Serialize response
        let response_data = crate::codec::command_codec::encode_data_response(&response)?;

        Ok(ApplyResult::ok_with_data(response_data))
    }

    fn last_applied_index(&self) -> u64 {
        self.last_applied_index.load(Ordering::Acquire)
    }

    fn subscribe_last_applied(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.last_applied_tx.subscribe())
    }

    fn mark_applied_index(&self, index: u64, term: u64) {
        let last_applied = self.last_applied_index.load(Ordering::Acquire);
        if index <= last_applied {
            return;
        }

        self.last_applied_index.store(index, Ordering::Release);
        self.last_applied_term.store(term, Ordering::Release);
        self.publish_last_applied(index);
    }

    fn last_applied_term(&self) -> u64 {
        self.last_applied_term.load(Ordering::Acquire)
    }

    async fn snapshot(&self) -> Result<StateMachineSnapshot, RaftError> {
        let recent_operations = self.recent_operations.read().clone();
        let pending_commands = self.pending_buffer.get_all();

        let snapshot = SharedDataSnapshot {
            total_operations: self.total_operations.load(Ordering::Relaxed),
            recent_operations,
            pending_commands,
        };

        let data = bincode_encode(&snapshot)?;

        Ok(StateMachineSnapshot::new(
            self.group_id(),
            self.last_applied_index(),
            self.last_applied_term(),
            data,
        ))
    }

    async fn restore(&self, snapshot: StateMachineSnapshot) -> Result<(), RaftError> {
        let data: SharedDataSnapshot = bincode_decode(&snapshot.data)?;

        {
            let mut ops = self.recent_operations.write();
            *ops = data.recent_operations;
        }

        // Restore pending buffer from snapshot
        self.pending_buffer.load_from(data.pending_commands);

        self.total_operations.store(data.total_operations, Ordering::Release);
        self.last_applied_index.store(snapshot.last_applied_index, Ordering::Release);
        self.last_applied_term.store(snapshot.last_applied_term, Ordering::Release);
        self.publish_last_applied(snapshot.last_applied_index);

        log::info!(
            "SharedDataStateMachine[{}]: Restored from snapshot at index {}, term {}, {} pending \
             commands",
            self.shard,
            snapshot.last_applied_index,
            snapshot.last_applied_term,
            self.pending_buffer.len()
        );

        Ok(())
    }

    fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed) as usize
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use kalamdb_commons::{
        models::{rows::Row, NamespaceId, OperationKind},
        TableId,
    };

    use super::*;

    struct TransactionBatchSharedApplier;

    #[async_trait]
    impl SharedDataApplier for TransactionBatchSharedApplier {
        async fn insert(
            &self,
            _table_id: &TableId,
            rows: &[Row],
            _commit_seq: u64,
        ) -> Result<usize, RaftError> {
            Ok(rows.len())
        }

        async fn update(
            &self,
            _table_id: &TableId,
            _updates: &[Row],
            _filter: Option<&str>,
            _commit_seq: u64,
        ) -> Result<usize, RaftError> {
            Ok(1)
        }

        async fn delete(
            &self,
            _table_id: &TableId,
            _pk_values: Option<&[String]>,
            _commit_seq: u64,
        ) -> Result<usize, RaftError> {
            Ok(1)
        }

        async fn apply_transaction_batch(
            &self,
            _transaction_id: &TransactionId,
            mutations: &[StagedMutation],
            commit_seq: u64,
        ) -> Result<crate::TransactionApplyResult, RaftError> {
            Ok(crate::TransactionApplyResult {
                rows_affected: mutations.len(),
                commit_seq,
                notifications_sent: 0,
                manifest_updates: 0,
                publisher_events: 0,
            })
        }
    }

    #[tokio::test]
    async fn test_shared_data_state_machine_insert() {
        let sm = SharedDataStateMachine::default();

        let cmd = SharedDataCommand::Insert {
            table_id: TableId::new(NamespaceId::default(), "config".into()),
            rows: vec![],
            required_meta_index: 0,
            transaction_id: None,
        };
        let cmd_bytes = crate::codec::command_codec::encode_shared_data_command(&cmd).unwrap();

        let result = sm.apply(1, 1, &cmd_bytes).await.unwrap();
        assert!(result.is_ok());
        assert_eq!(sm.last_applied_index(), 1);
        assert_eq!(sm.total_operations.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_shared_data_state_machine_multiple_ops() {
        let sm = SharedDataStateMachine::default();

        // Insert
        let insert = SharedDataCommand::Insert {
            table_id: TableId::new(NamespaceId::default(), "settings".into()),
            rows: vec![],
            required_meta_index: 0,
            transaction_id: None,
        };
        sm.apply(1, 1, &crate::codec::command_codec::encode_shared_data_command(&insert).unwrap())
            .await
            .unwrap();

        // Update
        let update = SharedDataCommand::Update {
            table_id: TableId::new(NamespaceId::default(), "settings".into()),
            updates: vec![],
            filter: None,
            required_meta_index: 0,
            transaction_id: None,
        };
        sm.apply(2, 1, &crate::codec::command_codec::encode_shared_data_command(&update).unwrap())
            .await
            .unwrap();

        // Delete
        let delete = SharedDataCommand::Delete {
            table_id: TableId::new(NamespaceId::default(), "settings".into()),
            pk_values: None,
            required_meta_index: 0,
            transaction_id: None,
        };
        sm.apply(3, 1, &crate::codec::command_codec::encode_shared_data_command(&delete).unwrap())
            .await
            .unwrap();

        assert_eq!(sm.total_operations.load(Ordering::Relaxed), 3);
        assert_eq!(sm.last_applied_index(), 3);

        // Check recent operations
        {
            let ops = sm.recent_operations.read();
            assert_eq!(ops.len(), 3);
            assert_eq!(ops[0].operation, OperationKind::Insert);
            assert_eq!(ops[1].operation, OperationKind::Update);
            assert_eq!(ops[2].operation, OperationKind::Delete);
            assert_eq!(ops[0].table_id, TableId::new(NamespaceId::default(), "settings".into()));
        }
    }

    #[tokio::test]
    async fn test_shared_data_state_machine_transaction_commit() {
        let sm = SharedDataStateMachine::with_applier(0, Arc::new(TransactionBatchSharedApplier));
        let transaction_id = TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
        let table_id = TableId::new(NamespaceId::default(), "config".into());

        let cmd = RaftCommand::TransactionCommit {
            transaction_id: transaction_id.clone(),
            mutations: vec![StagedMutation::new(
                transaction_id,
                table_id,
                TableType::Shared,
                None,
                OperationKind::Insert,
                "1",
                Row::new(BTreeMap::new()),
                false,
            )],
        };

        let payload = crate::codec::command_codec::encode_raft_command(&cmd).unwrap();
        let result = sm.apply(1, 1, &payload).await.unwrap();

        match result {
            ApplyResult::Ok(data) => {
                let response = crate::codec::command_codec::decode_data_response(&data).unwrap();
                match response {
                    DataResponse::TransactionCommitted(result) => {
                        assert_eq!(result.rows_affected, 1);
                        assert_eq!(
                            result.commit_seq,
                            commit_seq_from_log_position(GroupId::DataSharedShard(0), 1)
                        );
                    },
                    other => panic!("unexpected response: {:?}", other),
                }
            },
            other => panic!("unexpected apply result: {:?}", other),
        }
    }
}
