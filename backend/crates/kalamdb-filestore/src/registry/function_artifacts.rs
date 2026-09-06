//! Content-addressed function module artifacts.
//!
//! Bytes live at `{storage}/functions/artifacts/{artifact_id}/module.js`.
//! RocksDB keys are unchanged; this path stores values only.

use bytes::Bytes;
use kalamdb_commons::ArtifactId;
use object_store::{path::Path as ObjectPath, ObjectStoreExt};
use sha2::{Digest, Sha256};

use super::{
    operations::{GetResult, PutResult},
    storage_cached::StorageCached,
};
use crate::error::{FilestoreError, Result};

const ARTIFACT_DIR: &str = "functions/artifacts";
const MODULE_FILE: &str = "module.js";

/// SHA-256 hex of artifact bytes. Identity of the blob, not a RocksDB key.
pub fn hash_function_artifact(bytes: &[u8]) -> ArtifactId {
    ArtifactId::new(hex::encode(Sha256::digest(bytes)))
}

impl StorageCached {
    /// Object-store path for a function artifact's `module.js`.
    pub fn function_artifact_object_path(artifact_id: &ArtifactId) -> Result<String> {
        validate_artifact_id(artifact_id.as_str())?;
        Ok(format!("{ARTIFACT_DIR}/{}/{MODULE_FILE}", artifact_id.as_str()))
    }

    /// Write content-addressed function bytes. Idempotent for the same hash.
    pub async fn put_function_artifact(&self, bytes: Bytes) -> Result<(ArtifactId, PutResult)> {
        let artifact_id = hash_function_artifact(&bytes);
        let relative = Self::function_artifact_object_path(&artifact_id)?;
        let size = bytes.len();
        let store = self.object_store_internal()?;
        let object_path = ObjectPath::parse(&relative)
            .map_err(|error| FilestoreError::Path(format!("Invalid object path: {error}")))?;
        store
            .put(&object_path, bytes.into())
            .await
            .map_err(|error| FilestoreError::ObjectStore(error.to_string()))?;
        Ok((artifact_id, PutResult::new(relative, size)))
    }

    /// Read previously uploaded function artifact bytes.
    pub async fn get_function_artifact(&self, artifact_id: &ArtifactId) -> Result<GetResult> {
        let relative = Self::function_artifact_object_path(artifact_id)?;
        let store = self.object_store_internal()?;
        let object_path = ObjectPath::parse(&relative)
            .map_err(|error| FilestoreError::Path(format!("Invalid object path: {error}")))?;
        let result = store.get(&object_path).await.map_err(|error| match error {
            object_store::Error::NotFound { .. } => FilestoreError::NotFound(relative.clone()),
            other => FilestoreError::ObjectStore(other.to_string()),
        })?;
        let bytes = result
            .bytes()
            .await
            .map_err(|error| FilestoreError::ObjectStore(error.to_string()))?;
        Ok(GetResult::new(bytes, relative))
    }
}

fn validate_artifact_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.contains('/')
        || id.contains('\\')
        || id.contains("..")
        || id.contains('\0')
    {
        return Err(FilestoreError::PathTraversal(format!("invalid function artifact id: {id}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::env;

    use kalamdb_commons::models::ids::StorageId;
    use kalamdb_system::{providers::storages::models::StorageType, Storage};

    use super::*;

    fn test_storage() -> Storage {
        let unique_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let temp_dir = env::temp_dir().join(format!("function_artifact_test_{unique_id}"));
        std::fs::create_dir_all(&temp_dir).ok();
        let now = chrono::Utc::now().timestamp_millis();
        Storage {
            storage_id:             StorageId::from("function_artifacts"),
            storage_name:           "function_artifacts".to_string(),
            description:            None,
            storage_type:           StorageType::Filesystem,
            base_directory:         temp_dir.to_string_lossy().to_string(),
            credentials:            None,
            config_json:            None,
            shared_tables_template: "{namespace}/{tableName}".to_string(),
            user_tables_template:   "{namespace}/{tableName}/{userId}".to_string(),
            created_at:             now,
            updated_at:             now,
        }
    }

    #[tokio::test]
    async fn put_get_function_artifact_is_content_addressed() {
        let cached = StorageCached::with_default_timeouts(test_storage());
        let bytes = Bytes::from_static(b"function kalamInvoke() { return 1; }");
        let expected = hash_function_artifact(&bytes);
        let (artifact_id, put) = cached.put_function_artifact(bytes.clone()).await.unwrap();
        assert_eq!(artifact_id, expected);
        assert!(put.path.contains(artifact_id.as_str()));
        let loaded = cached.get_function_artifact(&artifact_id).await.unwrap();
        assert_eq!(loaded.data, bytes);

        let (again, _) = cached.put_function_artifact(bytes.clone()).await.unwrap();
        assert_eq!(again, artifact_id);
    }

    #[test]
    fn rejects_path_traversal_artifact_ids() {
        assert!(validate_artifact_id("../etc").is_err());
        assert!(validate_artifact_id("abc/def").is_err());
        assert!(validate_artifact_id("deadbeef").is_ok());
    }
}
