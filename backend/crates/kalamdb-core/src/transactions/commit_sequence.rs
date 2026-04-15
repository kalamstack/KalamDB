use std::sync::atomic::{AtomicU64, Ordering};

use kalamdb_transactions::CommitSequenceSource;

/// Shared in-process tracker for the latest committed snapshot boundary.
pub struct CommitSequenceTracker {
    current_committed: AtomicU64,
    durable_high_watermark: u64,
}

impl std::fmt::Debug for CommitSequenceTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitSequenceTracker")
            .field("current_committed", &self.current_committed())
            .field("durable_high_watermark", &self.durable_high_watermark)
            .finish()
    }
}

impl CommitSequenceTracker {
    #[inline]
    pub fn new(durable_high_watermark: u64) -> Self {
        Self {
            current_committed: AtomicU64::new(durable_high_watermark),
            durable_high_watermark,
        }
    }

    #[inline]
    pub fn current_committed(&self) -> u64 {
        self.current_committed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn durable_high_watermark(&self) -> u64 {
        self.durable_high_watermark
    }

    pub fn observe_committed(&self, commit_seq: u64) {
        let mut observed = self.current_committed();
        while commit_seq > observed {
            match self.current_committed.compare_exchange(
                observed,
                commit_seq,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => observed = current,
            }
        }
    }

    #[inline]
    pub fn allocate_next(&self) -> u64 {
        self.current_committed.fetch_add(1, Ordering::AcqRel) + 1
    }
}

impl CommitSequenceSource for CommitSequenceTracker {
    fn current_committed(&self) -> u64 {
        self.current_committed()
    }

    fn allocate_next(&self) -> u64 {
        self.allocate_next()
    }
}
