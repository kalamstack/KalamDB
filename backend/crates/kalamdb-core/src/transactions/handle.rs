use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use kalamdb_commons::models::{TableId, TransactionId, TransactionOrigin, TransactionState};

use super::binding::TransactionRaftBinding;
use super::ExecutionOwnerKey;

/// Hot transaction metadata kept separate from the staged write buffer.
#[derive(Debug, Clone)]
pub struct TransactionHandle {
    pub transaction_id: TransactionId,
    pub owner_key: ExecutionOwnerKey,
    pub owner_id: Arc<str>,
    pub origin: TransactionOrigin,
    pub state: TransactionState,
    pub raft_binding: TransactionRaftBinding,
    pub snapshot_commit_seq: u64,
    pub started_at: Instant,
    pub last_activity_at: Instant,
    pub write_count: usize,
    pub write_bytes: usize,
    pub touched_tables: HashSet<TableId>,
    pub has_write_set: bool,
}

impl TransactionHandle {
    pub fn new(
        transaction_id: TransactionId,
        owner_key: ExecutionOwnerKey,
        owner_id: Arc<str>,
        origin: TransactionOrigin,
        raft_binding: TransactionRaftBinding,
        snapshot_commit_seq: u64,
        now: Instant,
    ) -> Self {
        Self {
            transaction_id,
            owner_key,
            owner_id,
            origin,
            state: TransactionState::OpenRead,
            raft_binding,
            snapshot_commit_seq,
            started_at: now,
            last_activity_at: now,
            write_count: 0,
            write_bytes: 0,
            touched_tables: HashSet::new(),
            has_write_set: false,
        }
    }

    #[inline]
    pub fn touch(&mut self) {
        self.last_activity_at = Instant::now();
    }

    pub fn record_staged_write(
        &mut self,
        table_id: TableId,
        write_count: usize,
        write_bytes: usize,
    ) {
        self.last_activity_at = Instant::now();
        self.state = TransactionState::OpenWrite;
        self.write_count = write_count;
        self.write_bytes = write_bytes;
        self.has_write_set = true;
        self.touched_tables.insert(table_id);
    }

    pub fn record_staged_write_batch<I>(
        &mut self,
        table_ids: I,
        write_count: usize,
        write_bytes: usize,
    ) where
        I: IntoIterator<Item = TableId>,
    {
        self.last_activity_at = Instant::now();
        self.state = TransactionState::OpenWrite;
        self.write_count = write_count;
        self.write_bytes = write_bytes;
        self.has_write_set = true;
        for table_id in table_ids {
            self.touched_tables.insert(table_id);
        }
    }

    pub fn mark_state(&mut self, state: TransactionState) {
        self.state = state;
        self.last_activity_at = Instant::now();
    }
}
