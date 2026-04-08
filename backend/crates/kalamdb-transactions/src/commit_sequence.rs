/// Shared read-only contract for the latest committed snapshot boundary.
pub trait CommitSequenceSource: std::fmt::Debug + Send + Sync {
    fn current_committed(&self) -> u64;
}