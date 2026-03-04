use serde::{Deserialize, Serialize};

/// Type of change that occurred in the database
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
#[serde(rename_all = "lowercase")]
pub enum ChangeTypeRaw {
    /// New row(s) inserted
    Insert,

    /// Existing row(s) updated
    Update,

    /// Row(s) deleted
    Delete,
}
