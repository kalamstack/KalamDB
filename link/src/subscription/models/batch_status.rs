use serde::{Deserialize, Serialize};

/// Status of the initial data loading process
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    /// Initial batch being loaded
    Loading,

    /// Subsequent batches being loaded
    LoadingBatch,

    /// All initial data has been loaded, live updates active
    Ready,
}
