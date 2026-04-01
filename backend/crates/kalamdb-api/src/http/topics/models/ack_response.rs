//! Ack response model

use serde::Serialize;

/// Response for POST /api/topics/ack
#[derive(Debug, Serialize)]
pub struct AckResponse {
    /// Whether acknowledgment was successful
    pub success: bool,
    /// The offset that was acknowledged
    pub acknowledged_offset: u64,
}
