use crate::consumer::models::ConsumerOffsets;

#[derive(Debug, Clone, Default)]
pub struct OffsetManager {
    position: Option<u64>,
    highest_processed: Option<u64>,
    last_committed: Option<u64>,
}

impl OffsetManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn position(&self) -> Option<u64> {
        self.position
    }

    pub fn set_position(&mut self, position: u64) {
        self.position = Some(position);
    }

    pub fn mark_processed(&mut self, offset: u64) {
        self.highest_processed = Some(match self.highest_processed {
            Some(current) => current.max(offset),
            None => offset,
        });
    }

    pub fn highest_processed(&self) -> Option<u64> {
        self.highest_processed
    }

    pub fn set_last_committed(&mut self, offset: u64) {
        self.last_committed = Some(offset);
    }

    pub fn commit_offset(&self) -> Option<u64> {
        match (self.highest_processed, self.last_committed) {
            (Some(processed), Some(committed)) if processed > committed => Some(processed),
            (Some(processed), None) => Some(processed),
            _ => None,
        }
    }

    pub fn snapshot(&self) -> ConsumerOffsets {
        ConsumerOffsets {
            position: self.position.unwrap_or(0),
            last_committed: self.last_committed,
            highest_processed: self.highest_processed,
        }
    }

    pub fn reset_position(&mut self, position: u64) {
        self.position = Some(position);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_offset_manager_has_no_position() {
        let mgr = OffsetManager::new();
        assert!(mgr.position().is_none());
        assert!(mgr.highest_processed().is_none());
        assert!(mgr.commit_offset().is_none());
    }

    #[test]
    fn test_set_position() {
        let mut mgr = OffsetManager::new();
        mgr.set_position(100);
        assert_eq!(mgr.position(), Some(100));
    }

    #[test]
    fn test_mark_processed_tracks_highest() {
        let mut mgr = OffsetManager::new();
        mgr.mark_processed(10);
        assert_eq!(mgr.highest_processed(), Some(10));

        mgr.mark_processed(5);
        assert_eq!(mgr.highest_processed(), Some(10));

        mgr.mark_processed(15);
        assert_eq!(mgr.highest_processed(), Some(15));
    }

    #[test]
    fn test_commit_offset_requires_processed() {
        let mut mgr = OffsetManager::new();
        assert!(mgr.commit_offset().is_none());

        mgr.mark_processed(50);
        assert_eq!(mgr.commit_offset(), Some(50));
    }

    #[test]
    fn test_commit_offset_only_if_ahead_of_last_committed() {
        let mut mgr = OffsetManager::new();
        mgr.mark_processed(50);
        mgr.set_last_committed(50);
        assert!(mgr.commit_offset().is_none());

        mgr.mark_processed(60);
        assert_eq!(mgr.commit_offset(), Some(60));
    }

    #[test]
    fn test_reset_position_changes_next_fetch() {
        let mut mgr = OffsetManager::new();
        mgr.set_position(100);
        mgr.mark_processed(110);

        mgr.reset_position(50);
        assert_eq!(mgr.position(), Some(50));
        assert_eq!(mgr.highest_processed(), Some(110));
    }

    #[test]
    fn test_snapshot() {
        let mut mgr = OffsetManager::new();
        mgr.set_position(100);
        mgr.mark_processed(105);
        mgr.set_last_committed(102);

        let snap = mgr.snapshot();
        assert_eq!(snap.position, 100);
        assert_eq!(snap.highest_processed, Some(105));
        assert_eq!(snap.last_committed, Some(102));
    }
}
