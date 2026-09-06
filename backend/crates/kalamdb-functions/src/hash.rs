//! Content hash for function artifacts.

use kalamdb_commons::ArtifactId;
use sha2::{Digest, Sha256};

/// Hex-encoded SHA-256 of artifact bytes.
pub fn hash_artifact_bytes(bytes: &[u8]) -> ArtifactId {
    ArtifactId::new(hex::encode(Sha256::digest(bytes)))
}
