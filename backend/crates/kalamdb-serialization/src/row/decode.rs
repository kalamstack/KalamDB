//! Decode ordinal nested row payloads.

use std::collections::BTreeMap;

use kalamdb_commons::{
    ids::SeqId,
    models::{
        rows::{Row, StreamTableRow, UserTableRow},
        UserId,
    },
};

use super::{
    schema::StorageSchema,
    value::{decode_value, skip_value, Reader},
};
use crate::{
    error::{Result, SerializationError},
    object::{decode_envelope, ObjectKind},
};

/// Decode a user-table row. Identity comes from the storage key.
///
/// Missing trailing schema fields become NULL. Extra stored ordinals (dropped columns) are skipped.
pub fn decode_user_row(
    bytes: &[u8],
    schema: &StorageSchema,
    user_id: UserId,
    seq: SeqId,
) -> Result<UserTableRow> {
    let decoded = decode_row_body(bytes, schema)?;
    Ok(UserTableRow {
        user_id,
        _seq: seq,
        _commit_seq: decoded.commit_seq,
        _deleted: decoded.deleted,
        fields: decoded.fields,
    })
}

/// Decode a shared-table row body. `seq` is reconstructed from the storage key.
pub fn decode_shared_row(
    bytes: &[u8],
    schema: &StorageSchema,
    seq: SeqId,
) -> Result<(SeqId, u64, bool, Row)> {
    let decoded = decode_row_body(bytes, schema)?;
    Ok((seq, decoded.commit_seq, decoded.deleted, decoded.fields))
}

/// Decode a stream-table row. Identity comes from the storage key.
pub fn decode_stream_row(
    bytes: &[u8],
    schema: &StorageSchema,
    user_id: UserId,
    seq: SeqId,
) -> Result<StreamTableRow> {
    let decoded = decode_row_body(bytes, schema)?;
    Ok(StreamTableRow {
        user_id,
        _seq: seq,
        fields: decoded.fields,
    })
}

pub(crate) struct DecodedRow {
    pub commit_seq: u64,
    pub deleted:    bool,
    pub fields:     Row,
}

/// Decode ordinal field payloads produced by [`super::encode::encode_row_fields`].
pub fn decode_row_fields(bytes: &[u8], schema: &StorageSchema) -> Result<Row> {
    let mut reader = Reader::new(bytes);
    let fields = decode_fields(&mut reader, schema)?;
    if !reader.is_empty() {
        return Err(SerializationError::Decode("trailing bytes after row fields".to_string()));
    }
    Ok(fields)
}

pub(crate) fn decode_row_body(bytes: &[u8], schema: &StorageSchema) -> Result<DecodedRow> {
    let (_header, payload) = decode_envelope(bytes, ObjectKind::Row)?;
    let mut reader = Reader::new(payload);
    let stored_version = reader.u16()?;
    if stored_version > schema.version {
        return Err(SerializationError::Decode(format!(
            "row schema version {stored_version} is newer than {}",
            schema.version
        )));
    }
    let commit_bytes = [
        reader.u8()?,
        reader.u8()?,
        reader.u8()?,
        reader.u8()?,
        reader.u8()?,
        reader.u8()?,
        reader.u8()?,
        reader.u8()?,
    ];
    let commit_seq = u64::from_le_bytes(commit_bytes);
    let deleted = reader.u8()? != 0;
    let fields = decode_fields(&mut reader, schema)?;
    if !reader.is_empty() {
        return Err(SerializationError::Decode("trailing bytes after row payload".to_string()));
    }
    Ok(DecodedRow {
        commit_seq,
        deleted,
        fields,
    })
}

fn decode_fields(reader: &mut Reader<'_>, schema: &StorageSchema) -> Result<Row> {
    let stored_field_count = reader.u16()? as usize;
    let mut values = BTreeMap::new();
    let live_count = stored_field_count.min(schema.fields.len());
    for (index, field) in schema.fields.iter().enumerate() {
        if index < live_count {
            if field.dropped {
                skip_value(reader)?;
                continue;
            }
            let value = decode_value(reader, &field.data_type)?;
            values.insert(field.name.clone(), value);
        } else if !field.dropped {
            values.insert(field.name.clone(), datafusion_common::ScalarValue::Null);
        }
    }
    for _ in live_count..stored_field_count {
        skip_value(reader)?;
    }
    Ok(Row { values })
}
