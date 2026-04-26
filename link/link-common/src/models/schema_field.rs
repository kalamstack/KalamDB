use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::kalam_data_type::KalamDataType;

pub type FieldFlags = BTreeSet<FieldFlag>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldFlag {
    #[serde(rename = "pk")]
    PrimaryKey,
    #[serde(rename = "nn")]
    NonNull,
    #[serde(rename = "uq")]
    Unique,
}

/// A field in the result schema returned by SQL queries
///
/// Contains all the information a client needs to properly interpret
/// column data, including the name, data type, and index.
///
/// # Example (JSON representation)
///
/// ```json
/// {
///   "name": "user_id",
///   "data_type": "BigInt",
///   "index": 0,
///   "flags": ["pk", "nn", "uq"]
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    /// Column name
    pub name: String,

    /// Data type using KalamDB's unified type system
    pub data_type: KalamDataType,

    /// Column position (0-indexed) in the result set
    pub index: usize,

    /// Structured field flags (e.g. ["pk", "nn", "uq"]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flags: Option<FieldFlags>,
}
