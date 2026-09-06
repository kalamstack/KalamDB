//! Visibility metadata for count/version-resolution scans.

use crate::{ids::SeqId, PkBucketKey};

/// Lightweight metadata extracted from a table row without full field data.
///
/// Used for count-only scan paths (`COUNT(*)`) where version resolution
/// (PK dedup + tombstone filtering) does not need the full row map.
#[derive(Debug, Clone)]
pub struct RowMetadata {
    pub seq:        SeqId,
    pub commit_seq: u64,
    pub deleted:    bool,
    pub pk_bucket:  PkBucketKey,
}
