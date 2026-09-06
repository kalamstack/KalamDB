//! Central persisted-object codec for KalamDB.
//!
//! Callers choose a semantic object (generic, row, protocol, stream, or a
//! schema-guided catalog/system model). They never choose FlatBuffers,
//! FlexBuffers, JSON, or MessagePack for RocksDB payloads.

mod error;
mod model;
mod object;
mod protocol;
pub mod row;
mod stream_frame;
mod version;

pub use error::{Result, SerializationError};
pub use model::{
    model_ms_to_storage_micros, model_to_row, row_to_model, storage_micros_to_model_ms,
};
pub use object::{
    decode_object, decode_string_list, encode_object, encode_object_versioned, encode_string_list,
    has_object_magic, EncodedObject, ObjectKind,
};
pub use protocol::{decode_protocol, encode_protocol, ProtocolKind};
pub use row::{
    decode_row_fields, decode_row_metadata, decode_shared_row, decode_stream_row, decode_user_row,
    encode_row_fields, encode_shared_row, encode_stream_row, encode_user_row,
    storage_data_type_from_arrow, storage_data_type_from_kalam, storage_schema_from_table,
    RowMetadata, StorageDataType, StorageField, StorageSchema,
};
pub use stream_frame::{
    decode_stream, decode_stream_frame_payload, encode_stream, encode_stream_frame,
};
pub use version::{MAGIC, PROTOCOL_VERSION};

/// Reusable encoder for batch hot paths. Buffers are kept inside this crate.
#[derive(Debug, Default)]
pub struct ObjectEncoder {
    _private: (),
}

impl ObjectEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn encode_object<T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<EncodedObject> {
        encode_object(value)
    }

    pub fn encode_user_row(
        &mut self,
        row: &kalamdb_commons::models::rows::UserTableRow,
        schema: &StorageSchema,
    ) -> Result<EncodedObject> {
        encode_user_row(row, schema)
    }

    pub fn encode_shared_row(
        &mut self,
        commit_seq: u64,
        deleted: bool,
        fields: &kalamdb_commons::models::rows::Row,
        schema: &StorageSchema,
    ) -> Result<EncodedObject> {
        encode_shared_row(commit_seq, deleted, fields, schema)
    }

    pub fn encode_stream_row(
        &mut self,
        row: &kalamdb_commons::models::rows::StreamTableRow,
        schema: &StorageSchema,
    ) -> Result<EncodedObject> {
        encode_stream_row(row, schema)
    }
}
