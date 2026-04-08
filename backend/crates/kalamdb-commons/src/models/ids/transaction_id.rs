use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::StorageKey;

/// Type-safe wrapper for explicit transaction identifiers.
///
/// Transaction IDs use the canonical UUID v7 text format to preserve
/// monotonic creation ordering while keeping a stable string form for logs,
/// RPC payloads, and future storage keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionId(Arc<str>);

/// Validation error returned when a transaction ID is not a canonical UUID v7.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionIdValidationError(pub String);

impl fmt::Display for TransactionIdValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TransactionIdValidationError {}

impl TransactionId {
    /// Create a new transaction identifier, panicking on invalid input.
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self::try_new(id).expect("transaction id must be a canonical UUID v7 string")
    }

    /// Create a validated transaction identifier.
    pub fn try_new(id: impl Into<String>) -> Result<Self, TransactionIdValidationError> {
        let id = id.into();
        validate_uuid_v7(id.as_str())?;
        Ok(Self(Arc::<str>::from(id)))
    }

    /// Return the canonical string representation.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Return the transaction identifier as raw UTF-8 bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Consume the wrapper and return the owned string.
    #[inline]
    pub fn into_string(self) -> String {
        String::from(&*self.0)
    }
}

fn validate_uuid_v7(id: &str) -> Result<(), TransactionIdValidationError> {
    const HYPHEN_POSITIONS: [usize; 4] = [8, 13, 18, 23];

    let bytes = id.as_bytes();
    if bytes.len() != 36 {
        return Err(TransactionIdValidationError(
            "transaction id must be a 36-character UUID string".to_string(),
        ));
    }

    for (index, byte) in bytes.iter().enumerate() {
        if HYPHEN_POSITIONS.contains(&index) {
            if *byte != b'-' {
                return Err(TransactionIdValidationError(
                    "transaction id must use canonical UUID hyphen positions".to_string(),
                ));
            }
            continue;
        }

        if !byte.is_ascii_hexdigit() {
            return Err(TransactionIdValidationError(
                "transaction id must contain only hexadecimal digits".to_string(),
            ));
        }
    }

    if bytes[14] != b'7' {
        return Err(TransactionIdValidationError(
            "transaction id must be a UUID v7".to_string(),
        ));
    }

    let variant = bytes[19].to_ascii_lowercase();
    if !matches!(variant, b'8' | b'9' | b'a' | b'b') {
        return Err(TransactionIdValidationError(
            "transaction id must use an RFC 4122 variant".to_string(),
        ));
    }

    Ok(())
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TransactionId {
    type Err = TransactionIdValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_new(s)
    }
}

impl From<String> for TransactionId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for TransactionId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<TransactionId> for String {
    fn from(value: TransactionId) -> Self {
        value.into_string()
    }
}

impl AsRef<str> for TransactionId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for TransactionId {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl StorageKey for TransactionId {
    fn storage_key(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let id = String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())?;
        Self::try_new(id).map_err(|e| e.to_string())
    }
}

#[cfg(feature = "serde")]
impl Serialize for TransactionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for TransactionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        TransactionId::try_new(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_TX_ID: &str = "01960f7b-3d15-7d6d-b26c-7e4db6f25f8d";

    #[test]
    fn accepts_valid_uuid_v7() {
        let tx_id = TransactionId::try_new(VALID_TX_ID).unwrap();
        assert_eq!(tx_id.as_str(), VALID_TX_ID);
    }

    #[test]
    fn rejects_non_v7_uuid() {
        let err = TransactionId::try_new("01960f7b-3d15-4d6d-b26c-7e4db6f25f8d").unwrap_err();
        assert!(err.0.contains("UUID v7"));
    }

    #[test]
    fn rejects_non_canonical_uuid_shape() {
        let err = TransactionId::try_new("01960f7b3d157d6db26c7e4db6f25f8d").unwrap_err();
        assert!(err.0.contains("36-character UUID"));
    }

    #[test]
    fn storage_round_trip_preserves_value() {
        let tx_id = TransactionId::new(VALID_TX_ID);
        let restored = TransactionId::from_storage_key(&tx_id.storage_key()).unwrap();
        assert_eq!(restored, tx_id);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_round_trip_validates_input() {
        let tx_id = TransactionId::new(VALID_TX_ID);
        let json = serde_json::to_string(&tx_id).unwrap();
        let restored: TransactionId = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, tx_id);
        assert!(serde_json::from_str::<TransactionId>("\"not-a-uuid\"").is_err());
    }
}