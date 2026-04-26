use serde::{Deserialize, Serialize};

use crate::{
    serialization::generated::entity_envelope_generated::kalamdb::serialization as fb,
    storage::StorageError,
};

type Result<T> = std::result::Result<T, StorageError>;

/// Storage codec identifier carried in the entity envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CodecKind {
    FlatBuffers = 0,
    FlexBuffers = 1,
}

/// Versioned envelope around persisted values.
///
/// This envelope enables codec/schema migration without requiring a full data
/// rewrite of all persisted models at once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityEnvelope {
    pub codec_kind: CodecKind,
    pub schema_version: u16,
    pub payload: Vec<u8>,
}

impl EntityEnvelope {
    pub fn new(codec_kind: CodecKind, schema_version: u16, payload: Vec<u8>) -> Self {
        Self {
            codec_kind,
            schema_version,
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let payload = builder.create_vector(&self.payload);

        let args = fb::EntityEnvelopeArgs {
            codec_kind: to_fb_codec_kind(self.codec_kind),
            schema_version: self.schema_version,
            payload: Some(payload),
        };

        let envelope = fb::EntityEnvelope::create(&mut builder, &args);
        fb::finish_entity_envelope_buffer(&mut builder, envelope);
        Ok(builder.finished_data().to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if !fb::entity_envelope_buffer_has_identifier(bytes) {
            return Err(StorageError::SerializationError(
                "entity envelope decode failed: invalid file identifier (expected KENV)"
                    .to_string(),
            ));
        }

        let envelope = fb::root_as_entity_envelope(bytes).map_err(|e| {
            StorageError::SerializationError(format!("entity envelope decode failed: {e}"))
        })?;

        let payload = envelope.payload().ok_or_else(|| {
            StorageError::SerializationError(
                "entity envelope decode failed: missing payload".to_string(),
            )
        })?;

        Ok(Self {
            codec_kind: from_fb_codec_kind(envelope.codec_kind())?,
            schema_version: envelope.schema_version(),
            payload: payload.bytes().to_vec(),
        })
    }

    pub fn validate(&self, expected_schema_version: u16) -> Result<()> {
        if self.schema_version != expected_schema_version {
            return Err(StorageError::SerializationError(format!(
                "schema_version mismatch: expected {}, got {}",
                expected_schema_version, self.schema_version
            )));
        }

        Ok(())
    }
}

fn to_fb_codec_kind(value: CodecKind) -> fb::CodecKind {
    match value {
        CodecKind::FlatBuffers => fb::CodecKind::FlatBuffers,
        CodecKind::FlexBuffers => fb::CodecKind::FlexBuffers,
    }
}

fn from_fb_codec_kind(value: fb::CodecKind) -> Result<CodecKind> {
    match value {
        fb::CodecKind::FlatBuffers => Ok(CodecKind::FlatBuffers),
        fb::CodecKind::FlexBuffers => Ok(CodecKind::FlexBuffers),
        _ => Err(StorageError::SerializationError(format!(
            "entity envelope decode failed: unknown codec kind {}",
            value.0
        ))),
    }
}

/// High-performance inline envelope encoder for the write hot path.
///
/// Unlike [`encode_enveloped`] which creates an intermediate `EntityEnvelope`
/// struct and copies the payload into a `Vec<u8>`, this function:
/// 1. Takes the inner payload as `&[u8]` (e.g. from `builder.finished_data()`) — no `.to_vec()`
/// 2. Builds the envelope FlatBuffer directly — no intermediate struct allocation
/// 3. Pre-allocates the builder based on payload size — fewer internal resizes
///
/// This eliminates one heap allocation and one full-payload copy per row,
/// which is critical for batch insert throughput.
pub fn encode_envelope_inline(
    codec_kind: CodecKind,
    schema_version: u16,
    inner_payload: &[u8],
) -> Result<Vec<u8>> {
    // Pre-allocate: payload + FlatBuffer overhead (~64 bytes for
    // vtable, file identifier, root offset, field metadata).
    let estimated_size = inner_payload.len() + 64;
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(estimated_size);

    let payload_offset = builder.create_vector(inner_payload);

    let envelope = fb::EntityEnvelope::create(
        &mut builder,
        &fb::EntityEnvelopeArgs {
            codec_kind: to_fb_codec_kind(codec_kind),
            schema_version,
            payload: Some(payload_offset),
        },
    );
    fb::finish_entity_envelope_buffer(&mut builder, envelope);

    // finished_data() returns &[u8] pointing into the builder's internal buffer.
    // to_vec() creates a owned copy. This is a single allocation + memcpy.
    Ok(builder.finished_data().to_vec())
}

/// Inline envelope encoder that writes into a caller-provided
/// [`FlatBufferBuilder`], allowing builder reuse across multiple rows in a
/// batch encode loop (call `builder.reset()` between iterations).
///
/// Returns the finished envelope bytes **by reference** from the builder.
/// The caller must consume the slice before the builder is reset or dropped.
pub fn encode_envelope_into<'a>(
    builder: &'a mut flatbuffers::FlatBufferBuilder<'a>,
    codec_kind: CodecKind,
    schema_version: u16,
    inner_payload: &[u8],
) {
    let payload_offset = builder.create_vector(inner_payload);

    let envelope = fb::EntityEnvelope::create(
        builder,
        &fb::EntityEnvelopeArgs {
            codec_kind: to_fb_codec_kind(codec_kind),
            schema_version,
            payload: Some(payload_offset),
        },
    );
    fb::finish_entity_envelope_buffer(builder, envelope);
}
