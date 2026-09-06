//! Metadata-only row decode used by count/version-resolution scans.

use super::value::Reader;
use crate::{
    error::Result,
    object::{decode_envelope, ObjectKind},
};

/// Visibility fields without decoding nested user columns.
///
/// Identity is not stored in the payload; reconstruct it from the RocksDB key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowMetadata {
    pub commit_seq: u64,
    pub deleted:    bool,
}

/// Decode commit_seq / deleted without walking nested STRUCT/List columns.
pub fn decode_row_metadata(bytes: &[u8]) -> Result<RowMetadata> {
    let (_header, payload) = decode_envelope(bytes, ObjectKind::Row)?;
    let mut reader = Reader::new(payload);
    let _version = reader.u16()?;
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
    Ok(RowMetadata {
        commit_seq,
        deleted,
    })
}
