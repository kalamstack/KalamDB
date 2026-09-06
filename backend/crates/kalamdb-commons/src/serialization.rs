//! Marker trait for values stored through an entity store.
//!
//! Generic catalog/system objects are encoded by `kalamdb-serialization`
//! (`encode_object`). USER/SHARED/STREAM rows use a schema-aware store
//! codec and do not go through this trait.

use serde::{Deserialize, Serialize};

/// Marker bound for values stored through an entity store.
///
/// Persistence encode/decode lives in `kalamdb-serialization`. This trait only
/// names types that may be stored as generic objects.
///
/// ## Example
///
/// ```rust
/// use kalamdb_commons::serialization::KSerializable;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyEntity {
///     id:    String,
///     value: i64,
/// }
///
/// impl KSerializable for MyEntity {}
/// ```
pub trait KSerializable: Serialize + for<'de> Deserialize<'de> + Send + Sync {}

impl KSerializable for String {}
