//! Unified Data Type System - Single Source of Truth for KalamDB Types
//!
//! This module provides the consolidated type system for KalamDB, implementing
//! **Phase 4 (Unified Types)** from the 008-schema-consolidation feature.
//!
//! # Architecture
//!
//! The type system provides bidirectional conversion between KalamDB's unified
//! type representation and Apache Arrow's type system:
//!
//! ```text
//! KalamDataType (16 variants)
//!       ↕
//! Arrow DataType
//!       ↕
//! Physical Storage (Parquet, Arrow IPC)
//! ```
//!
//! # Core Types
//!
//! - **`KalamDataType`**: Unified enum representing all supported data types
//!   - Primitive: Boolean, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64
//!   - Numeric: Float32, Float64, Decimal(precision, scale)
//!   - String: Utf8, LargeUtf8
//!   - Binary: Binary, LargeBinary
//!   - Temporal: Date32, Date64, Timestamp(unit, tz), Time32(unit), Time64(unit), Duration(unit), Interval(unit)
//!   - Complex: List(inner), LargeList(inner), Struct(fields), Map(key, value, sorted)
//!   - Special: Uuid, Json
//!
//! - **`ToArrowType`**: Convert KalamDataType → Arrow DataType
//! - **`FromArrowType`**: Convert Arrow DataType → KalamDataType
//! - **`WireFormat`**: JSON wire format for client-server communication
//!
//! # Key Features
//!
//! - **Type Safety**: Compile-time type checking prevents runtime errors
//! - **Bidirectional Conversion**: Lossless KalamDataType ↔ Arrow DataType
//! - **Validation**: Enforce constraints (e.g., BINARY max 8192 bytes, DECIMAL max precision 38)
//! - **JSON Serialization**: Human-readable wire format for SDKs
//! - **Apache Arrow Integration**: Zero-copy data processing with DataFusion
//!
//! # Type Mappings
//!
//! | KalamDataType | Arrow DataType | Parquet Type | JSON Wire Format |
//! |---------------|----------------|--------------|------------------|
//! | Boolean       | Boolean        | BOOLEAN      | "boolean"        |
//! | Int32         | Int32          | INT32        | "int32"          |
//! | Int64         | Int64          | INT64        | "int64"          |
//! | Float64       | Float64        | DOUBLE       | "float64"        |
//! | Utf8          | Utf8           | BYTE_ARRAY   | "utf8"           |
//! | Uuid          | FixedSizeBinary(16) | FIXED_LEN_BYTE_ARRAY | "uuid" |
//! | Json          | Utf8           | BYTE_ARRAY   | "json"           |
//! | Decimal       | Decimal128     | DECIMAL      | "decimal(p,s)"   |
//! | Timestamp     | Timestamp      | INT64        | "timestamp_ms"   |
//!
//! # Usage Example
//!
//! ```rust,ignore
//! use kalamdb_commons::models::datatypes::{KalamDataType, ToArrowType, FromArrowType};
//! use arrow_schema::DataType as ArrowDataType;
//!
//! // Define a KalamDB type
//! let kalam_type = KalamDataType::Decimal { precision: 10, scale: 2 };
//!
//! // Convert to Arrow for DataFusion execution
//! let arrow_type = kalam_type.to_arrow_type();
//! assert_eq!(arrow_type, ArrowDataType::Decimal128(10, 2));
//!
//! // Convert back from Arrow (round-trip)
//! let recovered = KalamDataType::from_arrow_type(&arrow_type);
//! assert_eq!(recovered, kalam_type);
//! ```
//!
//! # Validation Rules
//!
//! - **BINARY(n)**: `1 ≤ n ≤ 8192` (8KB limit)
//! - **DECIMAL(p,s)**: `1 ≤ p ≤ 38`, `0 ≤ s ≤ p`
//! - **VARCHAR(n)**: No length limit (uses Utf8 with validation in application layer)
//! - **UUID**: Always 16 bytes (FixedSizeBinary(16) in Arrow)
//! - **JSON**: Stored as Utf8, validated as JSON on write
//!
//! # Wire Format
//!
//! The `WireFormat` trait provides JSON serialization for client SDKs:
//!
//! ```json
//! {
//!   "type": "decimal",
//!   "precision": 10,
//!   "scale": 2
//! }
//! ```
//!
//! This enables TypeScript/JavaScript SDKs to understand column types.
//!
//! # Migration Path
//!
//! - **Phase 3**: Introduced `KalamDataType` enum (completed)
//! - **Phase 4**: Replaced scattered type strings with enum (completed)
//! - **Phase 5**: Added Arrow ↔ KalamDataType conversion (completed)
//! - **Phase 6**: Integrated with SchemaCache (completed)
//! - **Phase 7**: Documentation and polish (current)
//!
//! # Related Modules
//!
//! - `kalamdb_commons::schemas` - Table and column definitions
//! - `kalamdb_sql::ddl` - SQL DDL parsing and type inference
//! - `kalamdb_core::schema_cache` - Cached type lookups
//! - `link/sdks/typescript/client` - Client SDK type definitions

pub mod kalam_data_type;
pub mod wire_format;

pub use kalam_data_type::KalamDataType;
pub use wire_format::{WireFormat, WireFormatError};

// Re-export arrow_conversion from conversions module
#[cfg(feature = "arrow-conversion")]
pub use crate::conversions::arrow_conversion::{ArrowConversionError, FromArrowType, ToArrowType};
