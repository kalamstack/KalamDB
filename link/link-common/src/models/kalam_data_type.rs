use serde::{Deserialize, Serialize};

/// Data type for schema fields in query results
///
/// Represents the KalamDB data type system. Each variant maps to
/// an underlying storage representation.
///
/// # Example JSON
///
/// ```json
/// "BigInt"           // Simple type
/// {"Embedding": 384} // Parameterized type
/// {"Decimal": {"precision": 10, "scale": 2}} // Complex type
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum KalamDataType {
    /// Boolean type
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    BigInt,
    /// 64-bit floating point
    Double,
    /// 32-bit floating point
    Float,
    /// UTF-8 string
    Text,
    /// Timestamp with microsecond precision
    Timestamp,
    /// Date (days since epoch)
    Date,
    /// DateTime with timezone
    DateTime,
    /// Time of day
    Time,
    /// JSON document
    Json,
    /// Binary data
    Bytes,
    /// Fixed-size float32 vector for embeddings
    Embedding(usize),
    /// UUID (128-bit universally unique identifier)
    Uuid,
    /// Fixed-point decimal with precision and scale
    Decimal { precision: u8, scale: u8 },
    /// 16-bit signed integer
    SmallInt,
    /// As json representation
    File,
}
