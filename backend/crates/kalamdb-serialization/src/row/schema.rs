//! Schema-known nested storage types for ordinal row encoding.

/// One field in a storage schema. Ordinal is the vec index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageField {
    pub name:      String,
    pub data_type: StorageDataType,
    /// Dropped physical slot. Encoded as NULL; decoded values are discarded.
    pub dropped:   bool,
}

impl StorageField {
    pub fn new(name: impl Into<String>, data_type: StorageDataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            dropped: false,
        }
    }

    /// Placeholder for a stable column_id hole (dropped or system column).
    pub fn dropped_slot() -> Self {
        Self {
            name:      String::new(),
            data_type: StorageDataType::Boolean,
            dropped:   true,
        }
    }
}

/// Physical nested type used by the row codec. Named `CREATE TYPE` identity lives in catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageDataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date32,
    Time64Microsecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
    Decimal {
        precision: u8,
        scale:     i8,
    },
    Embedding {
        dimension: i32,
    },
    /// Nested struct. Field slots are stable ordinals.
    Struct(Vec<StorageField>),
    /// One-dimensional list of the inner type.
    List(Box<StorageDataType>),
}

/// Schema used to encode/decode a table row by ordinal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageSchema {
    pub version: u16,
    pub fields:  Vec<StorageField>,
}

impl StorageSchema {
    pub fn new(version: u16, fields: Vec<StorageField>) -> Self {
        Self { version, fields }
    }

    pub fn field(&self, index: usize) -> Option<&StorageField> {
        self.fields.get(index)
    }
}
