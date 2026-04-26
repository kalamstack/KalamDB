//! KalamStateMachine trait definition
//!
//! Defines the interface for all Raft state machines in KalamDB.
//! Each state machine type handles a specific subset of commands.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{GroupId, RaftError};

/// Result of applying a command to the state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyResult {
    /// Command applied successfully with optional response data
    Ok(Vec<u8>),
    /// Command was a no-op (already applied - idempotency)
    NoOp,
    /// Command failed with an error
    Error(String),
}

impl ApplyResult {
    /// Create a successful result with no data
    pub fn ok() -> Self {
        ApplyResult::Ok(Vec::new())
    }

    /// Create a successful result with serialized data
    pub fn ok_with_data(data: Vec<u8>) -> Self {
        ApplyResult::Ok(data)
    }

    /// Check if the result is successful
    pub fn is_ok(&self) -> bool {
        matches!(self, ApplyResult::Ok(_) | ApplyResult::NoOp)
    }
}

/// Snapshot data for a state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    /// The group this snapshot belongs to
    pub group_id: GroupId,
    /// Last applied log index
    pub last_applied_index: u64,
    /// Last applied log term
    pub last_applied_term: u64,
    /// Serialized state data
    pub data: Vec<u8>,
}

impl StateMachineSnapshot {
    /// Create a new snapshot
    pub fn new(
        group_id: GroupId,
        last_applied_index: u64,
        last_applied_term: u64,
        data: Vec<u8>,
    ) -> Self {
        Self {
            group_id,
            last_applied_index,
            last_applied_term,
            data,
        }
    }
}

/// Common trait for all Raft state machines
///
/// State machines are responsible for:
/// 1. Applying committed log entries to local state
/// 2. Creating snapshots for log compaction
/// 3. Restoring state from snapshots
/// 4. Tracking last applied index for idempotency
///
/// # Example
///
/// ```rust,ignore
/// struct MyStateMachine { /* ... */ }
///
/// #[async_trait]
/// impl KalamStateMachine for MyStateMachine {
///     fn group_id(&self) -> GroupId {
///         GroupId::MetaSystem
///     }
///
///     async fn apply(&self, index: u64, term: u64, command: &[u8]) -> Result<ApplyResult, RaftError> {
///         // Deserialize and apply command
///         Ok(ApplyResult::ok())
///     }
///
///     // ... other methods
/// }
/// ```
#[async_trait]
pub trait KalamStateMachine: Send + Sync {
    /// Get the Raft group this state machine belongs to
    fn group_id(&self) -> GroupId;

    /// Apply a committed log entry to the state machine
    ///
    /// # Arguments
    /// * `index` - The log index of this entry
    /// * `term` - The term when this entry was created
    /// * `command` - Serialized command data
    ///
    /// # Returns
    /// * `ApplyResult::Ok` - Command applied successfully
    /// * `ApplyResult::NoOp` - Command was already applied (idempotency check)
    /// * `ApplyResult::Error` - Command failed
    ///
    /// # Idempotency
    /// Implementations MUST track `last_applied_index` and skip entries
    /// that have already been applied.
    async fn apply(&self, index: u64, term: u64, command: &[u8]) -> Result<ApplyResult, RaftError>;

    /// Get the last applied log index
    ///
    /// Used for:
    /// - Idempotency checks during apply
    /// - Snapshot creation (include all entries up to this index)
    /// - Recovery (start applying from this index + 1)
    fn last_applied_index(&self) -> u64;

    /// Subscribe to last-applied index changes when the implementation supports it.
    fn subscribe_last_applied(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }

    /// Mark a non-command Raft entry as applied in the state-machine watermark.
    ///
    /// Blank and membership entries do not call [`Self::apply`], but apply barriers still need the
    /// state-machine watermark to advance past them.
    fn mark_applied_index(&self, _index: u64, _term: u64) {}

    /// Get the last applied log term
    fn last_applied_term(&self) -> u64;

    /// Create a snapshot of the current state
    ///
    /// The snapshot should capture all state up to `last_applied_index`.
    /// This allows the Raft log to be compacted (entries before the snapshot
    /// can be discarded).
    ///
    /// # Returns
    /// A `StateMachineSnapshot` containing serialized state
    async fn snapshot(&self) -> Result<StateMachineSnapshot, RaftError>;

    /// Restore state from a snapshot
    ///
    /// Called when:
    /// - A new node joins and receives a snapshot from the leader
    /// - A node is far behind and receives InstallSnapshot RPC
    ///
    /// # Arguments
    /// * `snapshot` - The snapshot to restore from
    ///
    /// # Post-conditions
    /// - State machine state matches the snapshot
    /// - `last_applied_index` and `last_applied_term` are updated
    async fn restore(&self, snapshot: StateMachineSnapshot) -> Result<(), RaftError>;

    /// Get approximate size of state machine data in bytes
    ///
    /// Used to determine when to trigger snapshot creation.
    fn approximate_size(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_result_is_ok() {
        assert!(ApplyResult::ok().is_ok());
        assert!(ApplyResult::ok_with_data(vec![1, 2, 3]).is_ok());
        assert!(ApplyResult::NoOp.is_ok());
        assert!(!ApplyResult::Error("failed".to_string()).is_ok());
    }

    #[test]
    fn test_snapshot_creation() {
        let snapshot = StateMachineSnapshot::new(GroupId::Meta, 100, 5, vec![1, 2, 3, 4]);

        assert_eq!(snapshot.group_id, GroupId::Meta);
        assert_eq!(snapshot.last_applied_index, 100);
        assert_eq!(snapshot.last_applied_term, 5);
        assert_eq!(snapshot.data, vec![1, 2, 3, 4]);
    }
}
