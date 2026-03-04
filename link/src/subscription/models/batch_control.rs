use crate::seq_id::SeqId;
use serde::{Deserialize, Serialize};

use super::batch_status::BatchStatus;

/// Batch control metadata for paginated initial data loading
///
/// Note: We don't include total_batches because we can't know it upfront
/// without counting all rows first (expensive). The `has_more` field is
/// sufficient for clients to know whether to request more batches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
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

    /// Snapshot boundary SeqId captured at subscription time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_end_seq: Option<SeqId>,
}
