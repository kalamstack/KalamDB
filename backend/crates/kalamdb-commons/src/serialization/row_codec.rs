use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeListArray, Float32Array};
use arrow::datatypes::Float32Type;
use datafusion_common::ScalarValue;

use crate::ids::SeqId;
use crate::models::rows::{Row, UserTableRow};
use crate::models::UserId;
use crate::serialization::generated::row_models_generated::kalamdb::serialization::row as fb_row;
use crate::serialization::schema::ROW_SCHEMA_VERSION;
use crate::serialization::{decode_enveloped, encode_envelope_inline, CodecKind};
use crate::storage::StorageError;

type Result<T> = std::result::Result<T, StorageError>;

pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(256);

    let mut column_offsets = Vec::with_capacity(row.values.len());
    for (name, value) in row.values.iter() {
        let scalar = encode_scalar_payload(&mut builder, value)?;
        let name_offset = builder.create_string(name);
        let col = fb_row::ColumnValue::create(
            &mut builder,
            &fb_row::ColumnValueArgs {
                name: Some(name_offset),
                value: Some(scalar),
            },
        );
        column_offsets.push(col);
    }

    let columns = builder.create_vector(&column_offsets);
    let row_payload = fb_row::RowPayload::create(
        &mut builder,
        &fb_row::RowPayloadArgs {
            columns: Some(columns),
        },
    );

    fb_row::finish_row_payload_buffer(&mut builder, row_payload);
    // Zero-copy: pass builder's finished slice directly — no intermediate Vec<u8>
    encode_envelope_inline(CodecKind::FlatBuffers, ROW_SCHEMA_VERSION, builder.finished_data())
}

pub fn decode_row(bytes: &[u8]) -> Result<Row> {
    let envelope = decode_enveloped(bytes, ROW_SCHEMA_VERSION)?;
    let payload = envelope.payload;

    if !fb_row::row_payload_buffer_has_identifier(&payload) {
        return Err(StorageError::SerializationError(
            "row decode failed: invalid file identifier (expected KROW)".to_string(),
        ));
    }

    let row = fb_row::root_as_row_payload(&payload)
        .map_err(|e| StorageError::SerializationError(format!("row decode failed: {e}")))?;

    decode_row_payload(row)
}

pub fn encode_user_table_row(row: &UserTableRow) -> Result<Vec<u8>> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(256);
    let user_id = builder.create_string(row.user_id.as_str());
    let fields = encode_row_payload_table(&mut builder, &row.fields)?;

    let payload = fb_row::UserTableRowPayload::create(
        &mut builder,
        &fb_row::UserTableRowPayloadArgs {
            user_id: Some(user_id),
            seq: row._seq.as_i64(),
            commit_seq: row._commit_seq,
            deleted: row._deleted,
            fields: Some(fields),
        },
    );
    builder.finish(payload, None);
    // Zero-copy: pass builder's finished slice directly — no intermediate Vec<u8>
    encode_envelope_inline(CodecKind::FlatBuffers, ROW_SCHEMA_VERSION, builder.finished_data())
}

pub fn decode_user_table_row(bytes: &[u8]) -> Result<UserTableRow> {
    let envelope = decode_enveloped(bytes, ROW_SCHEMA_VERSION)?;
    let payload =
        flatbuffers::root::<fb_row::UserTableRowPayload>(&envelope.payload).map_err(|e| {
            StorageError::SerializationError(format!("user table row decode failed: {e}"))
        })?;

    let user_id = payload.user_id().ok_or_else(|| {
        StorageError::SerializationError(
            "user table row decode failed: missing user_id".to_string(),
        )
    })?;
    let fields = payload.fields().ok_or_else(|| {
        StorageError::SerializationError("user table row decode failed: missing fields".to_string())
    })?;

    Ok(UserTableRow {
        user_id: UserId::from(user_id),
        _seq: SeqId::new(payload.seq()),
        _commit_seq: payload.commit_seq(),
        _deleted: payload.deleted(),
        fields: decode_row_payload(fields)?,
    })
}

pub fn encode_shared_table_row(
    seq: SeqId,
    commit_seq: u64,
    deleted: bool,
    fields: &Row,
) -> Result<Vec<u8>> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(256);
    let fields = encode_row_payload_table(&mut builder, fields)?;

    let payload = fb_row::SharedTableRowPayload::create(
        &mut builder,
        &fb_row::SharedTableRowPayloadArgs {
            seq: seq.as_i64(),
            commit_seq,
            deleted,
            fields: Some(fields),
        },
    );
    builder.finish(payload, None);
    // Zero-copy: pass builder's finished slice directly — no intermediate Vec<u8>
    encode_envelope_inline(CodecKind::FlatBuffers, ROW_SCHEMA_VERSION, builder.finished_data())
}

pub fn decode_shared_table_row(bytes: &[u8]) -> Result<(SeqId, u64, bool, Row)> {
    let envelope = decode_enveloped(bytes, ROW_SCHEMA_VERSION)?;
    let payload =
        flatbuffers::root::<fb_row::SharedTableRowPayload>(&envelope.payload).map_err(|e| {
            StorageError::SerializationError(format!("shared table row decode failed: {e}"))
        })?;
    let fields = payload.fields().ok_or_else(|| {
        StorageError::SerializationError(
            "shared table row decode failed: missing fields".to_string(),
        )
    })?;

    Ok((
        SeqId::new(payload.seq()),
        payload.commit_seq(),
        payload.deleted(),
        decode_row_payload(fields)?,
    ))
}

/// Lightweight metadata extracted from a shared/user table row without full field deserialization.
///
/// Used for count-only scan paths (COUNT(*) queries) where we need version resolution
/// (PK dedup + tombstone filtering) but don't need the full row data.
/// Saves ~600-800 bytes per row by skipping the full `Row` HashMap allocation.
#[derive(Debug, Clone)]
pub struct RowMetadata {
    pub seq: SeqId,
    pub commit_seq: u64,
    pub deleted: bool,
    pub pk_value: Option<String>,
}

/// Decode only metadata (seq, deleted, pk_value) from a shared table row's serialized bytes.
///
/// This avoids deserializing the full `fields` HashMap, saving significant memory
/// for count-only queries (COUNT(*)) that only need version resolution data.
pub fn decode_shared_table_row_metadata(bytes: &[u8], pk_name: &str) -> Result<RowMetadata> {
    let envelope = decode_enveloped(bytes, ROW_SCHEMA_VERSION)?;
    let payload =
        flatbuffers::root::<fb_row::SharedTableRowPayload>(&envelope.payload).map_err(|e| {
            StorageError::SerializationError(format!(
                "shared table row metadata decode failed: {e}"
            ))
        })?;

    let pk_value = extract_pk_from_payload(payload.fields(), pk_name);

    Ok(RowMetadata {
        seq: SeqId::new(payload.seq()),
        commit_seq: payload.commit_seq(),
        deleted: payload.deleted(),
        pk_value,
    })
}

/// Decode only metadata (seq, deleted, pk_value) from a user table row's serialized bytes.
pub fn decode_user_table_row_metadata(
    bytes: &[u8],
    pk_name: &str,
) -> Result<(UserId, RowMetadata)> {
    let envelope = decode_enveloped(bytes, ROW_SCHEMA_VERSION)?;
    let payload =
        flatbuffers::root::<fb_row::UserTableRowPayload>(&envelope.payload).map_err(|e| {
            StorageError::SerializationError(format!("user table row metadata decode failed: {e}"))
        })?;

    let user_id = payload.user_id().ok_or_else(|| {
        StorageError::SerializationError(
            "user table row metadata decode failed: missing user_id".to_string(),
        )
    })?;

    let pk_value = extract_pk_from_payload(payload.fields(), pk_name);

    Ok((
        UserId::from(user_id),
        RowMetadata {
            seq: SeqId::new(payload.seq()),
            commit_seq: payload.commit_seq(),
            deleted: payload.deleted(),
            pk_value,
        },
    ))
}

/// Extract a single named field value from a FlatBuffers RowPayload without full deserialization.
fn extract_pk_from_payload(
    fields: Option<fb_row::RowPayload<'_>>,
    pk_name: &str,
) -> Option<String> {
    let fields = fields?;
    let columns = fields.columns()?;
    for col in columns.iter() {
        if let Some(name) = col.name() {
            if name == pk_name {
                return col.value().and_then(|scalar| {
                    decode_scalar_payload(scalar).ok().map(|sv| match &sv {
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                        _ => sv.to_string(),
                    })
                });
            }
        }
    }
    None
}

pub fn encode_system_table_row(row: &Row) -> Result<Vec<u8>> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(256);
    let fields = encode_row_payload_table(&mut builder, row)?;

    let payload = fb_row::SystemTableRowPayload::create(
        &mut builder,
        &fb_row::SystemTableRowPayloadArgs {
            fields: Some(fields),
        },
    );
    builder.finish(payload, None);
    encode_envelope_inline(CodecKind::FlatBuffers, ROW_SCHEMA_VERSION, builder.finished_data())
}

pub fn decode_system_table_row(bytes: &[u8]) -> Result<Row> {
    let envelope = decode_enveloped(bytes, ROW_SCHEMA_VERSION)?;
    let payload =
        flatbuffers::root::<fb_row::SystemTableRowPayload>(&envelope.payload).map_err(|e| {
            StorageError::SerializationError(format!("system table row decode failed: {e}"))
        })?;
    let fields = payload.fields().ok_or_else(|| {
        StorageError::SerializationError(
            "system table row decode failed: missing fields".to_string(),
        )
    })?;

    decode_row_payload(fields)
}

fn encode_row_payload_table<'a>(
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    row: &Row,
) -> Result<flatbuffers::WIPOffset<fb_row::RowPayload<'a>>> {
    let mut column_offsets = Vec::with_capacity(row.values.len());
    for (name, value) in row.values.iter() {
        let scalar = encode_scalar_payload(builder, value)?;
        let name_offset = builder.create_string(name);
        let col = fb_row::ColumnValue::create(
            builder,
            &fb_row::ColumnValueArgs {
                name: Some(name_offset),
                value: Some(scalar),
            },
        );
        column_offsets.push(col);
    }

    let columns = builder.create_vector(&column_offsets);
    Ok(fb_row::RowPayload::create(
        builder,
        &fb_row::RowPayloadArgs {
            columns: Some(columns),
        },
    ))
}

fn decode_row_payload(payload: fb_row::RowPayload<'_>) -> Result<Row> {
    let mut values = BTreeMap::<String, ScalarValue>::new();
    if let Some(columns) = payload.columns() {
        for col in columns.iter() {
            let name = col.name().ok_or_else(|| {
                StorageError::SerializationError(
                    "row decode failed: column missing name".to_string(),
                )
            })?;
            let scalar = col.value().ok_or_else(|| {
                StorageError::SerializationError(
                    "row decode failed: column missing scalar value".to_string(),
                )
            })?;

            values.insert(name.to_string(), decode_scalar_payload(scalar)?);
        }
    }
    Ok(Row { values })
}

fn to_scalar_tag(value: &ScalarValue) -> fb_row::ScalarTag {
    match value {
        ScalarValue::Null => fb_row::ScalarTag::Null,
        ScalarValue::Boolean(_) => fb_row::ScalarTag::Boolean,
        ScalarValue::Float32(_) => fb_row::ScalarTag::Float32,
        ScalarValue::Float64(_) => fb_row::ScalarTag::Float64,
        ScalarValue::Int8(_) => fb_row::ScalarTag::Int8,
        ScalarValue::Int16(_) => fb_row::ScalarTag::Int16,
        ScalarValue::Int32(_) => fb_row::ScalarTag::Int32,
        ScalarValue::Int64(_) => fb_row::ScalarTag::Int64,
        ScalarValue::UInt8(_) => fb_row::ScalarTag::UInt8,
        ScalarValue::UInt16(_) => fb_row::ScalarTag::UInt16,
        ScalarValue::UInt32(_) => fb_row::ScalarTag::UInt32,
        ScalarValue::UInt64(_) => fb_row::ScalarTag::UInt64,
        ScalarValue::Utf8(_) => fb_row::ScalarTag::Utf8,
        ScalarValue::LargeUtf8(_) => fb_row::ScalarTag::LargeUtf8,
        ScalarValue::Binary(_) => fb_row::ScalarTag::Binary,
        ScalarValue::LargeBinary(_) => fb_row::ScalarTag::LargeBinary,
        ScalarValue::FixedSizeBinary(_, _) => fb_row::ScalarTag::FixedSizeBinary,
        ScalarValue::Date32(_) => fb_row::ScalarTag::Date32,
        ScalarValue::Time64Microsecond(_) => fb_row::ScalarTag::Time64Microsecond,
        ScalarValue::TimestampMillisecond(_, _) => fb_row::ScalarTag::TimestampMillisecond,
        ScalarValue::TimestampMicrosecond(_, _) => fb_row::ScalarTag::TimestampMicrosecond,
        ScalarValue::TimestampNanosecond(_, _) => fb_row::ScalarTag::TimestampNanosecond,
        ScalarValue::Decimal128(_, _, _) => fb_row::ScalarTag::Decimal128,
        ScalarValue::FixedSizeList(_) => fb_row::ScalarTag::Embedding,
        _ => fb_row::ScalarTag::Fallback,
    }
}

fn encode_scalar_payload<'a>(
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    value: &ScalarValue,
) -> Result<flatbuffers::WIPOffset<fb_row::ScalarValuePayload<'a>>> {
    let mut args = fb_row::ScalarValuePayloadArgs {
        tag: to_scalar_tag(value),
        ..Default::default()
    };

    match value {
        ScalarValue::Null => {
            args.is_null = true;
        },
        ScalarValue::Boolean(v) => {
            args.is_null = v.is_none();
            args.bool_value = v.unwrap_or(false);
        },
        ScalarValue::Float32(v) => {
            args.is_null = v.is_none();
            args.f32_value = v.unwrap_or(0.0);
        },
        ScalarValue::Float64(v) => {
            args.is_null = v.is_none();
            args.f64_value = v.unwrap_or(0.0);
        },
        ScalarValue::Int8(v) => {
            args.is_null = v.is_none();
            args.i8_value = v.unwrap_or(0);
        },
        ScalarValue::Int16(v) => {
            args.is_null = v.is_none();
            args.i16_value = v.unwrap_or(0);
        },
        ScalarValue::Int32(v) => {
            args.is_null = v.is_none();
            args.i32_value = v.unwrap_or(0);
        },
        ScalarValue::Int64(v) => {
            args.is_null = v.is_none();
            args.i64_value = v.unwrap_or(0);
        },
        ScalarValue::UInt8(v) => {
            args.is_null = v.is_none();
            args.u8_value = v.unwrap_or(0);
        },
        ScalarValue::UInt16(v) => {
            args.is_null = v.is_none();
            args.u16_value = v.unwrap_or(0);
        },
        ScalarValue::UInt32(v) => {
            args.is_null = v.is_none();
            args.u32_value = v.unwrap_or(0);
        },
        ScalarValue::UInt64(v) => {
            args.is_null = v.is_none();
            args.u64_value = v.unwrap_or(0);
        },
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
            args.is_null = v.is_none();
            if let Some(text) = v {
                args.text_value = Some(builder.create_string(text));
            }
        },
        ScalarValue::Binary(v) | ScalarValue::LargeBinary(v) => {
            args.is_null = v.is_none();
            if let Some(bytes) = v {
                args.bytes_value = Some(builder.create_vector(bytes));
            }
        },
        ScalarValue::FixedSizeBinary(size, v) => {
            args.is_null = v.is_none();
            args.fixed_size = *size;
            if let Some(bytes) = v {
                args.bytes_value = Some(builder.create_vector(bytes));
            }
        },
        ScalarValue::Date32(v) => {
            args.is_null = v.is_none();
            args.i32_value = v.unwrap_or(0);
        },
        ScalarValue::Time64Microsecond(v) => {
            args.is_null = v.is_none();
            args.i64_value = v.unwrap_or(0);
        },
        ScalarValue::TimestampMillisecond(v, tz)
        | ScalarValue::TimestampMicrosecond(v, tz)
        | ScalarValue::TimestampNanosecond(v, tz) => {
            args.is_null = v.is_none();
            args.i64_value = v.unwrap_or(0);
            if let Some(tz) = tz {
                args.timezone = Some(builder.create_string(tz));
            }
        },
        ScalarValue::Decimal128(v, precision, scale) => {
            args.is_null = v.is_none();
            args.decimal_precision = *precision;
            args.decimal_scale = *scale;
            if let Some(decimal) = v {
                args.bytes_value = Some(builder.create_vector(&decimal.to_le_bytes()));
            }
        },
        ScalarValue::FixedSizeList(array) => {
            args.embedding_size = array.value_length();
            if array.len() == 0 || array.is_null(0) {
                args.is_null = true;
            } else {
                let values = array.value(0);
                let float_array =
                    values.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                        StorageError::SerializationError(
                            "row scalar encode failed: only FixedSizeList<Float32> is supported"
                                .to_string(),
                        )
                    })?;

                let len = args.embedding_size as usize;
                let mut embedding_values = Vec::with_capacity(len);
                let mut embedding_valid = Vec::with_capacity(len);
                for i in 0..len {
                    if float_array.is_null(i) {
                        embedding_values.push(0.0);
                        embedding_valid.push(false);
                    } else {
                        embedding_values.push(float_array.value(i));
                        embedding_valid.push(true);
                    }
                }

                args.embedding_values = Some(builder.create_vector(&embedding_values));
                args.embedding_valid = Some(builder.create_vector(&embedding_valid));
            }
        },
        other => {
            args.tag = fb_row::ScalarTag::Fallback;
            args.is_null = false;
            args.text_value = Some(builder.create_string(&other.to_string()));
        },
    }

    Ok(fb_row::ScalarValuePayload::create(builder, &args))
}

fn decode_scalar_payload(payload: fb_row::ScalarValuePayload<'_>) -> Result<ScalarValue> {
    let is_null = payload.is_null();

    match payload.tag() {
        fb_row::ScalarTag::Null => Ok(ScalarValue::Null),
        fb_row::ScalarTag::Boolean => Ok(ScalarValue::Boolean(if is_null {
            None
        } else {
            Some(payload.bool_value())
        })),
        fb_row::ScalarTag::Float32 => Ok(ScalarValue::Float32(if is_null {
            None
        } else {
            Some(payload.f32_value())
        })),
        fb_row::ScalarTag::Float64 => Ok(ScalarValue::Float64(if is_null {
            None
        } else {
            Some(payload.f64_value())
        })),
        fb_row::ScalarTag::Int8 => Ok(ScalarValue::Int8(if is_null {
            None
        } else {
            Some(payload.i8_value())
        })),
        fb_row::ScalarTag::Int16 => Ok(ScalarValue::Int16(if is_null {
            None
        } else {
            Some(payload.i16_value())
        })),
        fb_row::ScalarTag::Int32 => Ok(ScalarValue::Int32(if is_null {
            None
        } else {
            Some(payload.i32_value())
        })),
        fb_row::ScalarTag::Int64 => Ok(ScalarValue::Int64(if is_null {
            None
        } else {
            Some(payload.i64_value())
        })),
        fb_row::ScalarTag::UInt8 => Ok(ScalarValue::UInt8(if is_null {
            None
        } else {
            Some(payload.u8_value())
        })),
        fb_row::ScalarTag::UInt16 => Ok(ScalarValue::UInt16(if is_null {
            None
        } else {
            Some(payload.u16_value())
        })),
        fb_row::ScalarTag::UInt32 => Ok(ScalarValue::UInt32(if is_null {
            None
        } else {
            Some(payload.u32_value())
        })),
        fb_row::ScalarTag::UInt64 => Ok(ScalarValue::UInt64(if is_null {
            None
        } else {
            Some(payload.u64_value())
        })),
        fb_row::ScalarTag::Utf8 => Ok(ScalarValue::Utf8(if is_null {
            None
        } else {
            Some(decode_required_text(&payload, "Utf8")?)
        })),
        fb_row::ScalarTag::LargeUtf8 => Ok(ScalarValue::LargeUtf8(if is_null {
            None
        } else {
            Some(decode_required_text(&payload, "LargeUtf8")?)
        })),
        fb_row::ScalarTag::Binary => Ok(ScalarValue::Binary(if is_null {
            None
        } else {
            Some(decode_required_bytes(&payload, "Binary")?)
        })),
        fb_row::ScalarTag::LargeBinary => Ok(ScalarValue::LargeBinary(if is_null {
            None
        } else {
            Some(decode_required_bytes(&payload, "LargeBinary")?)
        })),
        fb_row::ScalarTag::FixedSizeBinary => Ok(ScalarValue::FixedSizeBinary(
            payload.fixed_size(),
            if is_null {
                None
            } else {
                Some(decode_required_bytes(&payload, "FixedSizeBinary")?)
            },
        )),
        fb_row::ScalarTag::Date32 => Ok(ScalarValue::Date32(if is_null {
            None
        } else {
            Some(payload.i32_value())
        })),
        fb_row::ScalarTag::Time64Microsecond => Ok(ScalarValue::Time64Microsecond(if is_null {
            None
        } else {
            Some(payload.i64_value())
        })),
        fb_row::ScalarTag::TimestampMillisecond => Ok(ScalarValue::TimestampMillisecond(
            if is_null {
                None
            } else {
                Some(payload.i64_value())
            },
            payload.timezone().map(Arc::<str>::from),
        )),
        fb_row::ScalarTag::TimestampMicrosecond => Ok(ScalarValue::TimestampMicrosecond(
            if is_null {
                None
            } else {
                Some(payload.i64_value())
            },
            payload.timezone().map(Arc::<str>::from),
        )),
        fb_row::ScalarTag::TimestampNanosecond => Ok(ScalarValue::TimestampNanosecond(
            if is_null {
                None
            } else {
                Some(payload.i64_value())
            },
            payload.timezone().map(Arc::<str>::from),
        )),
        fb_row::ScalarTag::Decimal128 => {
            let precision = payload.decimal_precision();
            let scale = payload.decimal_scale();
            if is_null {
                Ok(ScalarValue::Decimal128(None, precision, scale))
            } else {
                let bytes = decode_required_bytes(&payload, "Decimal128")?;
                if bytes.len() != 16 {
                    return Err(StorageError::SerializationError(format!(
                        "row decode failed: decimal128 bytes length must be 16, got {}",
                        bytes.len()
                    )));
                }
                let mut raw = [0_u8; 16];
                raw.copy_from_slice(&bytes);
                Ok(ScalarValue::Decimal128(Some(i128::from_le_bytes(raw)), precision, scale))
            }
        },
        fb_row::ScalarTag::Embedding => decode_embedding_payload(payload),
        fb_row::ScalarTag::Fallback => Ok(ScalarValue::Utf8(if is_null {
            None
        } else {
            Some(decode_required_text(&payload, "Fallback")?)
        })),
        other => Err(StorageError::SerializationError(format!(
            "row decode failed: unsupported scalar tag {:?}",
            other
        ))),
    }
}

fn decode_required_text(payload: &fb_row::ScalarValuePayload<'_>, kind: &str) -> Result<String> {
    payload.text_value().map(|s| s.to_string()).ok_or_else(|| {
        StorageError::SerializationError(format!(
            "row decode failed: {kind} scalar missing text_value"
        ))
    })
}

fn decode_required_bytes(payload: &fb_row::ScalarValuePayload<'_>, kind: &str) -> Result<Vec<u8>> {
    payload.bytes_value().map(|bytes| bytes.bytes().to_vec()).ok_or_else(|| {
        StorageError::SerializationError(format!(
            "row decode failed: {kind} scalar missing bytes_value"
        ))
    })
}

fn decode_embedding_payload(payload: fb_row::ScalarValuePayload<'_>) -> Result<ScalarValue> {
    if payload.is_null() {
        return Ok(ScalarValue::Null);
    }

    let size = payload.embedding_size();
    if size <= 0 {
        return Err(StorageError::SerializationError(
            "row decode failed: embedding_size must be > 0".to_string(),
        ));
    }

    let values_vec = payload.embedding_values().ok_or_else(|| {
        StorageError::SerializationError(
            "row decode failed: embedding scalar missing embedding_values".to_string(),
        )
    })?;

    let valid_vec = payload.embedding_valid();
    let len = size as usize;
    if values_vec.len() < len {
        return Err(StorageError::SerializationError(format!(
            "row decode failed: embedding_values too short (expected {len}, got {})",
            values_vec.len()
        )));
    }

    let mut values = Vec::with_capacity(len);
    for i in 0..len {
        let is_valid = valid_vec.as_ref().map(|v| v.get(i)).unwrap_or(true);
        if is_valid {
            values.push(Some(values_vec.get(i)));
        } else {
            values.push(None);
        }
    }

    let array = Float32Array::from(values);
    let list = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        std::iter::once(Some(array.iter())),
        size,
    );

    Ok(ScalarValue::FixedSizeList(Arc::new(list)))
}

// ─── Batch encoding with FlatBufferBuilder reuse ───────────────────────────
//
// For batch inserts (100+ rows), reusing the FlatBufferBuilder across rows
// avoids O(N) heap allocations for the builder's internal buffer. The builder
// is `reset()` between iterations, retaining its capacity.

/// Batch-encode multiple `UserTableRow`s, reusing internal FlatBufferBuilders.
///
/// For N rows this saves N–1 FlatBufferBuilder allocations on the inner
/// builder side compared to calling `encode_user_table_row` in a loop.
pub fn batch_encode_user_table_rows(rows: &[UserTableRow]) -> Result<Vec<Vec<u8>>> {
    batch_encode_user_table_rows_iter(rows.iter(), rows.len())
}

/// Batch-encode borrowed `UserTableRow`s without cloning owned row payloads.
pub fn batch_encode_user_table_row_refs(rows: &[&UserTableRow]) -> Result<Vec<Vec<u8>>> {
    batch_encode_user_table_rows_iter(rows.iter().copied(), rows.len())
}

fn batch_encode_user_table_rows_iter<'a, I>(rows: I, row_count: usize) -> Result<Vec<Vec<u8>>>
where
    I: IntoIterator<Item = &'a UserTableRow>,
{
    let _span = tracing::info_span!("batch_encode_user_table_rows", count = row_count).entered();
    let mut results = Vec::with_capacity(row_count);
    // Reuse inner builder across rows — reset() keeps the allocated capacity
    let mut inner_builder = flatbuffers::FlatBufferBuilder::with_capacity(512);

    for row in rows {
        inner_builder.reset();

        let user_id = inner_builder.create_string(row.user_id.as_str());
        let fields = encode_row_payload_table(&mut inner_builder, &row.fields)?;

        let payload = fb_row::UserTableRowPayload::create(
            &mut inner_builder,
            &fb_row::UserTableRowPayloadArgs {
                user_id: Some(user_id),
                seq: row._seq.as_i64(),
                commit_seq: row._commit_seq,
                deleted: row._deleted,
                fields: Some(fields),
            },
        );
        inner_builder.finish(payload, None);

        let encoded = encode_envelope_inline(
            CodecKind::FlatBuffers,
            ROW_SCHEMA_VERSION,
            inner_builder.finished_data(),
        )?;
        results.push(encoded);
    }

    Ok(results)
}

/// Batch-encode multiple shared table rows, reusing internal FlatBufferBuilders.
pub fn batch_encode_shared_table_rows(rows: &[(SeqId, u64, bool, &Row)]) -> Result<Vec<Vec<u8>>> {
    let _span = tracing::info_span!("batch_encode_shared_table_rows", count = rows.len()).entered();
    let mut results = Vec::with_capacity(rows.len());
    let mut inner_builder = flatbuffers::FlatBufferBuilder::with_capacity(512);

    for (seq, commit_seq, deleted, fields) in rows {
        inner_builder.reset();

        let fields_offset = encode_row_payload_table(&mut inner_builder, fields)?;
        let payload = fb_row::SharedTableRowPayload::create(
            &mut inner_builder,
            &fb_row::SharedTableRowPayloadArgs {
                seq: seq.as_i64(),
                commit_seq: *commit_seq,
                deleted: *deleted,
                fields: Some(fields_offset),
            },
        );
        inner_builder.finish(payload, None);

        let encoded = encode_envelope_inline(
            CodecKind::FlatBuffers,
            ROW_SCHEMA_VERSION,
            inner_builder.finished_data(),
        )?;
        results.push(encoded);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::rows::UserTableRow;

    #[test]
    fn row_roundtrip_mixed_types() {
        let mut values = BTreeMap::new();
        values.insert("id".to_string(), ScalarValue::Int64(Some(42)));
        values.insert("name".to_string(), ScalarValue::Utf8(Some("alice".to_string())));
        values.insert("active".to_string(), ScalarValue::Boolean(Some(true)));
        values.insert("notes".to_string(), ScalarValue::Utf8(None));
        values.insert(
            "ts".to_string(),
            ScalarValue::TimestampMillisecond(Some(1000), Some(Arc::<str>::from("UTC"))),
        );

        let row = Row { values };
        let encoded = encode_row(&row).expect("encode row");
        let decoded = decode_row(&encoded).expect("decode row");
        assert_eq!(decoded, row);
    }

    #[test]
    fn row_roundtrip_decimal() {
        let mut values = BTreeMap::new();
        values.insert("amount".to_string(), ScalarValue::Decimal128(Some(20075), 10, 2));

        let row = Row { values };
        let encoded = encode_row(&row).expect("encode row");
        let decoded = decode_row(&encoded).expect("decode row");
        assert_eq!(decoded, row);
    }

    #[test]
    fn user_table_row_roundtrip() {
        let mut values = BTreeMap::new();
        values.insert("pk".to_string(), ScalarValue::Int64(Some(7)));
        let fields = Row { values };

        let row = UserTableRow {
            user_id: UserId::from("user1"),
            _seq: SeqId::new(123),
            _commit_seq: 55,
            _deleted: false,
            fields,
        };

        let encoded = encode_user_table_row(&row).expect("encode user row");
        let decoded = decode_user_table_row(&encoded).expect("decode user row");
        assert_eq!(decoded, row);
    }

    #[test]
    fn shared_table_row_roundtrip() {
        let mut values = BTreeMap::new();
        values.insert("pk".to_string(), ScalarValue::Int64(Some(11)));
        values.insert("name".to_string(), ScalarValue::Utf8(Some("shared".to_string())));
        let fields = Row { values };

        let encoded =
            encode_shared_table_row(SeqId::new(456), 77, true, &fields).expect("encode shared row");
        let (seq, commit_seq, deleted, decoded_fields) =
            decode_shared_table_row(&encoded).expect("decode shared row");
        assert_eq!(seq, SeqId::new(456));
        assert_eq!(commit_seq, 77);
        assert!(deleted);
        assert_eq!(decoded_fields, fields);
    }

    #[test]
    fn system_table_row_roundtrip() {
        let mut values = BTreeMap::new();
        values.insert("user_id".to_string(), ScalarValue::Utf8(Some("admin".to_string())));
        values.insert("role".to_string(), ScalarValue::Utf8(Some("dba".to_string())));
        values.insert(
            "created_at".to_string(),
            ScalarValue::TimestampMillisecond(Some(1000000), None),
        );
        let row = Row { values };

        let encoded = encode_system_table_row(&row).expect("encode system row");
        let decoded = decode_system_table_row(&encoded).expect("decode system row");
        assert_eq!(decoded, row);
    }
}
