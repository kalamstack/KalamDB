use serde::{Deserialize, Serialize};

/// Error details for failed SQL execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct ErrorDetail {
    /// Error code
    pub code: String,

    /// Human-readable error message
    pub message: String,

    /// Optional additional details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}
