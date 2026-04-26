use serde::{Deserialize, Serialize};

use super::Row;

/// Generic persisted row representation for system tables.
///
/// System providers can keep typed models for business logic and convert them
/// to/from this row shape at the storage boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SystemTableRow {
    pub fields: Row,
}

#[cfg(feature = "serialization")]
impl crate::serialization::KSerializable for SystemTableRow {
    fn encode(&self) -> Result<Vec<u8>, crate::storage::StorageError> {
        crate::serialization::row_codec::encode_system_table_row(&self.fields)
    }

    fn decode(bytes: &[u8]) -> Result<Self, crate::storage::StorageError>
    where
        Self: Sized,
    {
        let fields = crate::serialization::row_codec::decode_system_table_row(bytes)?;
        Ok(Self { fields })
    }
}
