use serde::{Deserialize, Serialize};

/// Result of acknowledging consumed messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckResponse {
    /// Whether the acknowledgment was successful
    pub success: bool,

    /// The offset that was acknowledged
    pub acknowledged_offset: u64,
}
