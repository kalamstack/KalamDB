use serde::{Deserialize, Serialize};

use crate::seq_id::SeqId;

/// Status of the initial data loading process
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    /// Initial batch being loaded
    Loading,

    /// Subsequent batches being loaded
    LoadingBatch,

    /// All initial data has been loaded, live updates active
    Ready,
}

/// Batch control metadata for paginated initial data loading
///
/// Note: We don't include total_batches because we can't know it upfront
/// without counting all rows first (expensive). The `has_more` field is
/// sufficient for clients to know whether to request more batches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchControl {
    /// Current batch number (0-indexed)
    pub batch_num: u32,

    /// Whether more batches are available to fetch
    pub has_more: bool,

    /// Loading status for the subscription
    pub status: BatchStatus,

    /// The SeqId of the last row in this batch (used for subsequent requests)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seq_id: Option<SeqId>,
}
