//! Persist entities through `kalamdb-serialization`.
//!
//! Generic catalog/system objects use the object envelope. Row stores override
//! [`EntityCodec`] with schema-aware ordinal encoding.

use std::sync::Arc;

use kalamdb_commons::KSerializable;
use kalamdb_serialization::{decode_object, encode_object};

use crate::storage_trait::{Result, StorageError};

fn map_ser(err: kalamdb_serialization::SerializationError) -> StorageError {
    StorageError::SerializationError(err.to_string())
}

/// Encode an entity immediately before writing it to storage.
pub fn encode_entity<V: KSerializable>(entity: &V) -> Result<Vec<u8>> {
    encode_object(entity).map(|encoded| encoded.into_bytes()).map_err(map_ser)
}

/// Decode an entity immediately after reading persisted bytes.
pub fn decode_entity<V: KSerializable>(bytes: &[u8]) -> Result<V> {
    decode_object(bytes).map_err(map_ser)
}

/// Schema-aware (or object) codec used by entity stores.
pub trait EntityCodec<K, V>: Send + Sync {
    fn encode(&self, key: &K, entity: &V) -> Result<Vec<u8>>;
    fn decode(&self, key: &K, bytes: &[u8]) -> Result<V>;
}

/// Default codec: generic object envelope.
#[derive(Debug, Default, Clone, Copy)]
pub struct ObjectEntityCodec;

impl<K, V: KSerializable> EntityCodec<K, V> for ObjectEntityCodec {
    fn encode(&self, _key: &K, entity: &V) -> Result<Vec<u8>> {
        encode_entity(entity)
    }

    fn decode(&self, _key: &K, bytes: &[u8]) -> Result<V> {
        decode_entity(bytes)
    }
}

/// Shared default object codec.
pub fn object_entity_codec<K, V: KSerializable>() -> Arc<dyn EntityCodec<K, V>> {
    Arc::new(ObjectEntityCodec)
}
