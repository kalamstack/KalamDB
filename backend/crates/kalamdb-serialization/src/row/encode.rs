//! Encode table rows as ordinal nested values inside a KOBJ row envelope.
//!
//! Identity (`user_id`, `_seq`) is not written. Reconstruct it from the RocksDB key.

use datafusion_common::ScalarValue;
use kalamdb_commons::models::rows::{Row, StreamTableRow, UserTableRow};

use super::{
    scalar::TAG_NULL,
    schema::StorageSchema,
    value::{encode_value, write_u16, write_u8},
};
use crate::{
    error::{Result, SerializationError},
    object::{encode_envelope, EncodedObject, ObjectKind},
};

/// Encode a user-table row using schema ordinals. Nested STRUCT/List recurse in the value codec.
pub fn encode_user_row(row: &UserTableRow, schema: &StorageSchema) -> Result<EncodedObject> {
    encode_row_body(row._commit_seq, row._deleted, &row.fields, schema)
}

/// Encode a shared-table row (identity lives on the SeqId key).
pub fn encode_shared_row(
    commit_seq: u64,
    deleted: bool,
    fields: &Row,
    schema: &StorageSchema,
) -> Result<EncodedObject> {
    encode_row_body(commit_seq, deleted, fields, schema)
}

/// Encode a stream-table row. Streams have no `_commit_seq` / `_deleted`; both are stored as 0.
pub fn encode_stream_row(row: &StreamTableRow, schema: &StorageSchema) -> Result<EncodedObject> {
    encode_row_body(0, false, &row.fields, schema)
}

/// Encode only ordinal field values (no KOBJ envelope, no commit metadata).
///
/// Raft DML commands store this blob instead of FlexBuffering `ScalarValue` maps.
pub fn encode_row_fields(fields: &Row, schema: &StorageSchema) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    encode_fields(&mut buf, fields, schema)?;
    Ok(buf)
}

fn encode_row_body(
    commit_seq: u64,
    deleted: bool,
    fields: &Row,
    schema: &StorageSchema,
) -> Result<EncodedObject> {
    let mut payload = Vec::new();
    write_u16(&mut payload, schema.version);
    payload.extend_from_slice(&commit_seq.to_le_bytes());
    write_u8(&mut payload, u8::from(deleted));
    encode_fields(&mut payload, fields, schema)?;
    encode_envelope(ObjectKind::Row, schema.version, &payload)
}

fn encode_fields(buf: &mut Vec<u8>, fields: &Row, schema: &StorageSchema) -> Result<()> {
    let count = u16::try_from(schema.fields.len())
        .map_err(|_| SerializationError::Encode("too many row fields".to_string()))?;
    write_u16(buf, count);
    for field in &schema.fields {
        if field.dropped {
            write_u8(buf, TAG_NULL);
            continue;
        }
        let value = fields.values.get(&field.name).cloned().unwrap_or(ScalarValue::Null);
        encode_value(buf, &value, &field.data_type)?;
    }
    Ok(())
}
