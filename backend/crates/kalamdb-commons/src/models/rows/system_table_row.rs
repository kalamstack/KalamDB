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
impl crate::serialization::KSerializable for SystemTableRow {}
