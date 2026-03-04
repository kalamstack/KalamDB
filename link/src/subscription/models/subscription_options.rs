use serde::{Deserialize, Serialize};

use crate::seq_id::SeqId;

/// Subscription options for a live query.
///
/// These options control individual subscription behavior including:
/// - Initial data loading (batch_size, last_rows)
/// - Data resumption after reconnection (from_seq_id)
///
/// Aligned with backend's SubscriptionOptions in kalamdb-commons/websocket.rs.
///
/// # Example
///
/// ```rust
/// use kalam_link::{SeqId, SubscriptionOptions};
///
/// // Fetch last 100 rows with batch size of 50
/// let options = SubscriptionOptions::default()
///     .with_batch_size(50)
///     .with_last_rows(100);
///
/// // Resume from a specific sequence ID after reconnection
/// let some_seq_id = SeqId::new(123);
/// let options = SubscriptionOptions::default()
///     .with_from_seq_id(some_seq_id);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct SubscriptionOptions {
    /// Hint for server-side batch sizing during initial data load.
    /// Default: server-configured (typically 1000 rows per batch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,

    /// Number of last (newest) rows to fetch for initial data.
    /// Default: None (fetch all matching rows).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rows: Option<u32>,

    /// Resume subscription from a specific sequence ID.
    /// When set, the server will only send changes after this seq_id.
    /// Typically set automatically during reconnection to resume from last received event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_seq_id: Option<SeqId>,
}

impl SubscriptionOptions {
    /// Create new subscription options with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the batch size for initial data loading.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Set the number of last rows to fetch.
    pub fn with_last_rows(mut self, count: u32) -> Self {
        self.last_rows = Some(count);
        self
    }

    /// Resume from a specific sequence ID.
    /// Used during reconnection to continue from where we left off.
    pub fn with_from_seq_id(mut self, seq_id: SeqId) -> Self {
        self.from_seq_id = Some(seq_id);
        self
    }

    /// Check if this has a resume seq_id set.
    pub fn has_resume_seq_id(&self) -> bool {
        self.from_seq_id.is_some()
    }
}
