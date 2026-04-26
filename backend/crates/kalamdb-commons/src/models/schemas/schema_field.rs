//! Schema field for API response serialization
//!
//! This module defines the `SchemaField` struct used in REST API responses
//! to provide type-safe schema information to clients.

use arrow_schema::Field;
use serde::{Deserialize, Serialize};

use crate::{
    conversions::read_kalam_column_flags_metadata,
    models::datatypes::KalamDataType,
    schemas::{ColumnDefinition, FieldFlag, FieldFlags},
};

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
///
/// # Example (with parameterized type)
///
/// ```json
/// {
///   "name": "vector",
///   "data_type": { "Embedding": 384 },
///   "index": 5
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
    ///
    /// Omitted when there are no notable flags to reduce response size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flags: Option<FieldFlags>,
}

impl SchemaField {
    /// Create a new schema field
    pub fn new(name: impl Into<String>, data_type: KalamDataType, index: usize) -> Self {
        Self {
            name: name.into(),
            data_type,
            index,
            flags: None,
        }
    }

    pub fn from_column_definition(column: &ColumnDefinition, index: usize) -> Self {
        Self {
            name: column.column_name.clone(),
            data_type: column.data_type.clone(),
            index,
            flags: Self::flags_for_column(column.is_primary_key, column.is_nullable),
        }
    }

    pub fn from_arrow_field(field: &Field, data_type: KalamDataType, index: usize) -> Self {
        let flags = read_kalam_column_flags_metadata(field);

        Self {
            name: field.name().clone(),
            data_type,
            index,
            flags,
        }
    }

    pub fn with_flags(mut self, flags: FieldFlags) -> Self {
        if !flags.is_empty() {
            self.flags = Some(flags);
        }
        self
    }

    pub fn flags_for_column(is_primary_key: bool, is_nullable: bool) -> Option<FieldFlags> {
        let mut flags = FieldFlags::new();
        if is_primary_key {
            flags.insert(FieldFlag::PrimaryKey);
            flags.insert(FieldFlag::Unique);
        }
        if !is_nullable {
            flags.insert(FieldFlag::NonNull);
        }

        if flags.is_empty() {
            None
        } else {
            Some(flags)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_schema_field_serialization() {
        let field = SchemaField::new("user_id", KalamDataType::BigInt, 0);
        let json = serde_json::to_string(&field).unwrap();
        assert!(json.contains("\"name\":\"user_id\""));
        assert!(json.contains("\"data_type\":\"BigInt\""));
        assert!(json.contains("\"index\":0"));
        assert!(!json.contains("\"flags\":"));
    }

    #[test]
    fn test_schema_field_deserialization() {
        let json = r#"{"name":"email","data_type":"Text","index":1}"#;
        let field: SchemaField = serde_json::from_str(json).unwrap();
        assert_eq!(field.name, "email");
        assert_eq!(field.data_type, KalamDataType::Text);
        assert_eq!(field.index, 1);
        assert_eq!(field.flags, None);
    }

    #[test]
    fn test_schema_field_with_flags() {
        let mut flags = FieldFlags::new();
        flags.insert(FieldFlag::PrimaryKey);
        flags.insert(FieldFlag::NonNull);
        flags.insert(FieldFlag::Unique);
        let field = SchemaField::new("id", KalamDataType::Uuid, 0).with_flags(flags);
        let json = serde_json::to_string(&field).unwrap();
        assert!(json.contains("\"flags\":["));
        assert!(json.contains("\"pk\""));
        assert!(json.contains("\"nn\""));
        assert!(json.contains("\"uq\""));
    }

    #[test]
    fn test_schema_field_deserializes_flags_array() {
        let json = r#"{"name":"id","data_type":"Uuid","index":0,"flags":["pk","nn","uq"]}"#;
        let field: SchemaField = serde_json::from_str(json).unwrap();
        assert!(matches!(
            field.flags,
            Some(flags)
                if flags.contains(&FieldFlag::PrimaryKey)
                    && flags.contains(&FieldFlag::NonNull)
                    && flags.contains(&FieldFlag::Unique)
        ));
    }

    #[test]
    fn test_schema_field_with_embedding() {
        let field = SchemaField::new("vector", KalamDataType::Embedding(384), 5);
        let json = serde_json::to_string(&field).unwrap();
        assert!(json.contains("\"Embedding\":384"));
    }

    #[test]
    fn test_schema_field_with_decimal() {
        let field = SchemaField::new(
            "price",
            KalamDataType::Decimal {
                precision: 10,
                scale: 2,
            },
            2,
        );
        let json = serde_json::to_string(&field).unwrap();
        assert!(json.contains("\"Decimal\""));
    }
}
