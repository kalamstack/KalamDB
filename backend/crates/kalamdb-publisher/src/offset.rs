//! Atomic offset allocation for topic partitions.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use dashmap::DashMap;
use kalamdb_commons::models::TopicId;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TopicPartitionKey {
    topic_id: TopicId,
    partition_id: u32,
}

impl TopicPartitionKey {
    #[inline]
    fn new(topic_id: &TopicId, partition_id: u32) -> Self {
        Self {
            topic_id: topic_id.clone(),
            partition_id,
        }
    }
}

/// Manages per-topic-partition offset counters using atomic operations.
///
/// Each counter is an `AtomicU64` behind an `Arc`, so concurrent callers
/// on different tokio tasks can increment without holding a DashMap shard lock
/// during the actual increment.
pub(crate) struct OffsetAllocator {
    /// Key: (topic_id, partition_id) → atomic counter
    counters: DashMap<TopicPartitionKey, Arc<AtomicU64>>,
}

impl OffsetAllocator {
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }

    /// Get the next offset for a topic-partition and atomically increment.
    pub fn next_offset(&self, topic_id: &TopicId, partition_id: u32) -> u64 {
        let key = TopicPartitionKey::new(topic_id, partition_id);

        // Get or insert an AtomicU64 counter.
        // The DashMap shard lock is held only for the lookup/insert, not the increment.
        let counter =
            self.counters.entry(key).or_insert_with(|| Arc::new(AtomicU64::new(0))).clone();

        counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Atomically allocate a contiguous range of `count` offsets.
    ///
    /// Returns the start of the range. The allocated range is `[start, start + count)`.
    /// This is more efficient than calling `next_offset()` in a loop because it
    /// requires only a single atomic operation.
    pub fn next_n_offsets(&self, topic_id: &TopicId, partition_id: u32, count: u64) -> u64 {
        let key = TopicPartitionKey::new(topic_id, partition_id);
        let counter =
            self.counters.entry(key).or_insert_with(|| Arc::new(AtomicU64::new(0))).clone();
        counter.fetch_add(count, Ordering::Relaxed)
    }

    /// Seed a counter to a specific value (used during restore from persisted messages).
    pub fn seed(&self, topic_id: &TopicId, partition_id: u32, next_offset: u64) {
        let key = TopicPartitionKey::new(topic_id, partition_id);
        self.counters.insert(key, Arc::new(AtomicU64::new(next_offset)));
    }

    /// Peek the current next offset without incrementing.
    ///
    /// Returns `None` when the topic-partition counter is not initialized.
    pub fn peek_next_offset(&self, topic_id: &TopicId, partition_id: u32) -> Option<u64> {
        let key = TopicPartitionKey::new(topic_id, partition_id);
        self.counters.get(&key).map(|counter| counter.load(Ordering::Relaxed))
    }

    /// Clear all counters.
    pub fn clear(&self) {
        self.counters.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_offsets() {
        let alloc = OffsetAllocator::new();
        let topic = TopicId::new("topic1");
        assert_eq!(alloc.next_offset(&topic, 0), 0);
        assert_eq!(alloc.next_offset(&topic, 0), 1);
        assert_eq!(alloc.next_offset(&topic, 0), 2);
    }

    #[test]
    fn test_independent_partitions() {
        let alloc = OffsetAllocator::new();
        let topic = TopicId::new("topic1");
        assert_eq!(alloc.next_offset(&topic, 0), 0);
        assert_eq!(alloc.next_offset(&topic, 1), 0);
        assert_eq!(alloc.next_offset(&topic, 0), 1);
        assert_eq!(alloc.next_offset(&topic, 1), 1);
    }

    #[test]
    fn test_seed() {
        let alloc = OffsetAllocator::new();
        let topic = TopicId::new("topic1");
        alloc.seed(&topic, 0, 100);
        assert_eq!(alloc.next_offset(&topic, 0), 100);
        assert_eq!(alloc.next_offset(&topic, 0), 101);
    }

    #[test]
    fn test_peek_next_offset() {
        let alloc = OffsetAllocator::new();
        let topic = TopicId::new("topic1");
        assert_eq!(alloc.peek_next_offset(&topic, 0), None);

        alloc.seed(&topic, 0, 7);
        assert_eq!(alloc.peek_next_offset(&topic, 0), Some(7));

        assert_eq!(alloc.next_offset(&topic, 0), 7);
        assert_eq!(alloc.peek_next_offset(&topic, 0), Some(8));
    }

    #[test]
    fn test_concurrent_offsets() {
        use std::thread;

        let alloc = Arc::new(OffsetAllocator::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let alloc = Arc::clone(&alloc);
            handles.push(thread::spawn(move || {
                let mut offsets = vec![];
                let topic = TopicId::new("topic1");
                for _ in 0..100 {
                    offsets.push(alloc.next_offset(&topic, 0));
                }
                offsets
            }));
        }

        let mut all_offsets: Vec<u64> = vec![];
        for handle in handles {
            all_offsets.extend(handle.join().unwrap());
        }

        // All 1000 offsets should be unique
        all_offsets.sort();
        all_offsets.dedup();
        assert_eq!(all_offsets.len(), 1000);
        // Should cover 0..1000
        assert_eq!(*all_offsets.first().unwrap(), 0);
        assert_eq!(*all_offsets.last().unwrap(), 999);
    }
}
