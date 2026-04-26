//! PendingBuffer - Buffers data commands waiting for Meta to catch up
//!
//! When a data shard receives a command with `required_meta_index > current_meta_index`,
//! the command is buffered here until the local Meta group catches up.
//!
//! This ensures correct ordering: data commands are not applied until all
//! dependent metadata (tables, users, storages) has been applied locally.

use std::collections::BTreeMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// A command waiting to be applied once Meta catches up
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingCommand {
    /// Raft log index of this command
    pub log_index: u64,
    /// Raft log term of this command
    pub log_term: u64,
    /// The Meta index required before this command can be applied
    pub required_meta_index: u64,
    /// Serialized command bytes
    pub command_bytes: Vec<u8>,
}

/// Buffer for pending data commands
///
/// Commands are keyed by `required_meta_index` for efficient draining
/// when Meta advances. Within each key, commands are stored in order.
///
/// When draining, commands are sorted by `log_index` to preserve
/// the original Raft log order for correctness.
pub struct PendingBuffer {
    /// Commands grouped by required_meta_index
    /// Key: required_meta_index, Value: list of pending commands
    by_required_meta: RwLock<BTreeMap<u64, Vec<PendingCommand>>>,

    /// Total number of pending commands
    pending_count: std::sync::atomic::AtomicU64,
}

impl Default for PendingBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingBuffer {
    /// Create a new empty pending buffer
    pub fn new() -> Self {
        Self {
            by_required_meta: RwLock::new(BTreeMap::new()),
            pending_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Add a command to the pending buffer
    ///
    /// The command will be stored until `drain_satisfied` is called
    /// with a `current_meta_index >= required_meta_index`.
    pub fn add(&self, cmd: PendingCommand) {
        let required = cmd.required_meta_index;
        let mut guard = self.by_required_meta.write();
        guard.entry(required).or_default().push(cmd);
        self.pending_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Drain all commands whose required_meta_index <= current_meta_index
    ///
    /// Returns commands sorted by log_index (ascending) to preserve
    /// the original Raft log order for correctness.
    pub fn drain_satisfied(&self, current_meta_index: u64) -> Vec<PendingCommand> {
        let mut guard = self.by_required_meta.write();

        // Collect all keys that are satisfied
        let satisfied_keys: Vec<u64> =
            guard.range(..=current_meta_index).map(|(k, _)| *k).collect();

        if satisfied_keys.is_empty() {
            return Vec::new();
        }

        // Extract all commands from satisfied keys
        let mut commands = Vec::new();
        for key in satisfied_keys {
            if let Some(cmds) = guard.remove(&key) {
                commands.extend(cmds);
            }
        }

        // Update pending count
        let drained_count = commands.len() as u64;
        self.pending_count
            .fetch_sub(drained_count, std::sync::atomic::Ordering::Relaxed);

        // Sort by log_index to preserve Raft order
        commands.sort_by_key(|c| c.log_index);

        commands
    }

    /// Get the number of pending commands
    pub fn len(&self) -> usize {
        self.pending_count.load(std::sync::atomic::Ordering::Relaxed) as usize
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get all pending commands (for persistence/snapshot)
    pub fn get_all(&self) -> Vec<PendingCommand> {
        let guard = self.by_required_meta.read();
        guard.values().flatten().cloned().collect()
    }

    /// Load commands from persistence (on startup)
    pub fn load_from(&self, commands: Vec<PendingCommand>) {
        let mut guard = self.by_required_meta.write();
        for cmd in commands {
            let required = cmd.required_meta_index;
            guard.entry(required).or_default().push(cmd);
            self.pending_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Clear all pending commands
    pub fn clear(&self) {
        let mut guard = self.by_required_meta.write();
        guard.clear();
        self.pending_count.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get the highest required_meta_index in the buffer
    ///
    /// Returns None if buffer is empty.
    pub fn max_required_meta_index(&self) -> Option<u64> {
        let guard = self.by_required_meta.read();
        guard.keys().next_back().copied()
    }

    /// Get the lowest required_meta_index in the buffer
    ///
    /// Returns None if buffer is empty.
    pub fn min_required_meta_index(&self) -> Option<u64> {
        let guard = self.by_required_meta.read();
        guard.keys().next().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_drain() {
        let buffer = PendingBuffer::new();

        // Add commands with different required_meta_index values
        buffer.add(PendingCommand {
            log_index: 10,
            log_term: 1,
            required_meta_index: 5,
            command_bytes: vec![1, 2, 3],
        });
        buffer.add(PendingCommand {
            log_index: 11,
            log_term: 1,
            required_meta_index: 10,
            command_bytes: vec![4, 5, 6],
        });
        buffer.add(PendingCommand {
            log_index: 12,
            log_term: 1,
            required_meta_index: 5,
            command_bytes: vec![7, 8, 9],
        });

        assert_eq!(buffer.len(), 3);

        // Drain with meta index 5 - should get 2 commands
        let drained = buffer.drain_satisfied(5);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].log_index, 10); // sorted by log_index
        assert_eq!(drained[1].log_index, 12);

        assert_eq!(buffer.len(), 1);

        // Drain with meta index 10 - should get the remaining command
        let drained = buffer.drain_satisfied(10);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].log_index, 11);

        assert!(buffer.is_empty());
    }

    #[test]
    fn test_drain_preserves_log_order() {
        let buffer = PendingBuffer::new();

        // Add commands out of log order but same required_meta_index
        buffer.add(PendingCommand {
            log_index: 15,
            log_term: 1,
            required_meta_index: 5,
            command_bytes: vec![],
        });
        buffer.add(PendingCommand {
            log_index: 13,
            log_term: 1,
            required_meta_index: 5,
            command_bytes: vec![],
        });
        buffer.add(PendingCommand {
            log_index: 14,
            log_term: 1,
            required_meta_index: 5,
            command_bytes: vec![],
        });

        let drained = buffer.drain_satisfied(5);
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].log_index, 13);
        assert_eq!(drained[1].log_index, 14);
        assert_eq!(drained[2].log_index, 15);
    }
}
