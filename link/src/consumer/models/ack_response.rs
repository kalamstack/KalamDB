use serde::{Deserialize, Serialize};

/// Result of acknowledging consumed messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct AckResponse {
    /// Whether the acknowledgment was successful
    pub success: bool,

    /// The offset that was acknowledged
    pub acknowledged_offset: u64,
}
