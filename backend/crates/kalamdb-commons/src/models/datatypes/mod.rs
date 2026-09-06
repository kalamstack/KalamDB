//! Canonical catalog type system.
//!
//! [`KalamDataType`] is the only place to add a builtin. Table columns, CREATE
//! TYPE fields, routine parameters/returns, and typed defaults all store this
//! enum. Named `CREATE TYPE` values are [`crate::models::TypeId`], not variants.
//!
//! ```text
//! SQL name / sqlparser  →  KalamDataType  →  Arrow DataType
//!                                       ↘  WireFormat (RocksDB / catalogs)
//!                                       ↘  StorageDataType (row codec)
//! ```

pub mod kalam_data_type;
pub mod wire_format;

pub use kalam_data_type::KalamDataType;
pub use wire_format::{WireFormat, WireFormatError};

#[cfg(feature = "arrow-conversion")]
pub use crate::conversions::arrow_conversion::{ArrowConversionError, FromArrowType, ToArrowType};
