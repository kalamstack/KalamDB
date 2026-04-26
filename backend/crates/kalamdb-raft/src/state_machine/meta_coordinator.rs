//! MetadataCoordinator - Coordinates meta→data watermark synchronization
//!
//! When the Meta group applies an entry, data shards need to be notified
//! so they can check if any buffered commands can now be drained.
//!
//! This coordinator provides:
//! - Atomic tracking of current meta index
//! - Notification mechanism for data shards to wake up and drain

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::sync::Notify;

/// Coordinates meta index updates and notifies data shards
///
/// Used by:
/// - MetaStateMachine: calls `advance()` after applying each entry
/// - Data state machines: call `current_index()` to check watermark, and `subscribe()` to get
///   notified when meta advances
#[derive(Debug)]
pub struct MetadataCoordinator {
    /// Current meta group last_applied_index
    current_index: AtomicU64,

    /// Notification channel for meta advances
    notify: Notify,
}

impl Default for MetadataCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataCoordinator {
    /// Create a new MetadataCoordinator
    pub fn new() -> Self {
        Self {
            current_index: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    /// Get the current meta index
    pub fn current_index(&self) -> u64 {
        self.current_index.load(Ordering::Acquire)
    }

    /// Advance the meta index and notify waiters
    ///
    /// Called by MetaStateMachine after applying an entry.
    /// This wakes up all data shards waiting for meta to catch up.
    pub fn advance(&self, new_index: u64) {
        let old = self.current_index.fetch_max(new_index, Ordering::Release);
        if new_index > old {
            log::trace!("MetadataCoordinator: Meta advanced {} -> {}", old, new_index);
            self.notify.notify_waiters();
        }
    }

    /// Wait for meta index to advance
    ///
    /// Returns immediately if current_index >= required_index.
    /// Otherwise, waits until notified and checks again.
    pub async fn wait_for(&self, required_index: u64) {
        loop {
            let current = self.current_index.load(Ordering::Acquire);
            if current >= required_index {
                return;
            }

            // Wait for notification
            self.notify.notified().await;
        }
    }

    /// Check if a required_meta_index is satisfied
    pub fn is_satisfied(&self, required_meta_index: u64) -> bool {
        self.current_index.load(Ordering::Acquire) >= required_meta_index
    }

    /// Get a future that completes when meta advances past the current index
    ///
    /// Useful for periodic drain checks without blocking.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}

/// Global metadata coordinator instance
///
/// Shared across all Raft groups on this node.
static COORDINATOR: std::sync::OnceLock<Arc<MetadataCoordinator>> = std::sync::OnceLock::new();

/// Get the global MetadataCoordinator instance
pub fn get_coordinator() -> Arc<MetadataCoordinator> {
    COORDINATOR.get_or_init(|| Arc::new(MetadataCoordinator::new())).clone()
}

/// Initialize the global coordinator with a specific initial index
///
/// Should be called during startup after loading persisted state.
pub fn init_coordinator(initial_index: u64) -> Arc<MetadataCoordinator> {
    let coordinator = get_coordinator();
    coordinator.advance(initial_index);
    coordinator
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_advance_and_check() {
        let coordinator = MetadataCoordinator::new();

        assert_eq!(coordinator.current_index(), 0);
        assert!(!coordinator.is_satisfied(5));

        coordinator.advance(5);
        assert_eq!(coordinator.current_index(), 5);
        assert!(coordinator.is_satisfied(5));
        assert!(!coordinator.is_satisfied(6));

        // Advance should not go backwards
        coordinator.advance(3);
        assert_eq!(coordinator.current_index(), 5);
    }

    #[tokio::test]
    async fn test_wait_for_already_satisfied() {
        let coordinator = MetadataCoordinator::new();
        coordinator.advance(10);

        // Should return immediately
        coordinator.wait_for(5).await;
        coordinator.wait_for(10).await;
    }
}
