//! Manifest file operations using StorageCached.
//!
//! Provides read/write operations for manifest.json files with atomic writes.
//! Supports both local and remote storage backends.

use bytes::Bytes;
use kalamdb_commons::{
    models::{TableId, UserId},
    schemas::TableType,
};

use crate::{
    error::{FilestoreError, Result},
    registry::StorageCached,
};

/// Read manifest.json from storage.
///
/// Returns the raw JSON string.
/// TODO: Return JsonValue instead of raw string?
pub fn read_manifest_json(
    storage_cached: &StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
) -> Result<String> {
    let bytes = storage_cached.get_sync(table_type, table_id, user_id, "manifest.json")?;
    String::from_utf8(bytes.data.to_vec())
        .map_err(|e| FilestoreError::Other(format!("Invalid UTF-8 in manifest: {}", e)))
}

/// Write manifest.json to storage atomically.
///
/// For local storage, this uses temp file + rename for atomicity.
/// For object_store backends, PUT operations are atomic by design.
pub fn write_manifest_json(
    storage_cached: &StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    json_content: &str,
) -> Result<()> {
    let span = tracing::info_span!(
        "manifest.write",
        table_type = ?table_type,
        table_id = %table_id,
        has_user_id = user_id.is_some(),
        bytes = json_content.len()
    );
    let _span_guard = span.entered();

    let bytes = Bytes::from(json_content.to_string());
    storage_cached
        .put_sync(table_type, table_id, user_id, "manifest.json", bytes)
        .map(|_| {
            tracing::debug!("Manifest write completed");
        })
}

/// Check if manifest.json exists (async).
pub async fn manifest_exists(
    storage_cached: &StorageCached,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
) -> Result<bool> {
    match storage_cached.exists(table_type, table_id, user_id, "manifest.json").await {
        Ok(result) => Ok(result.exists),
        Err(FilestoreError::ObjectStore(ref e)) if e.contains("not found") || e.contains("404") => {
            Ok(false)
        },
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs};

    use kalamdb_commons::{
        models::{ids::StorageId, TableId},
        schemas::TableType,
    };
    use kalamdb_system::{providers::storages::models::StorageType, Storage};

    use super::*;
    use crate::registry::StorageCached;

    fn create_test_storage(temp_dir: &std::path::Path) -> Storage {
        let now = chrono::Utc::now().timestamp_millis();
        Storage {
            storage_id: StorageId::from("test_manifest"),
            storage_name: "test_manifest".to_string(),
            description: None,
            storage_type: StorageType::Filesystem,
            base_directory: temp_dir.to_string_lossy().to_string(),
            credentials: None,
            config_json: None,
            shared_tables_template: "{namespace}/{tableName}".to_string(),
            user_tables_template: "{namespace}/{tableName}/{userId}".to_string(),
            created_at: now,
            updated_at: now,
        }
    }

    fn make_table_id() -> TableId {
        TableId::from_strings("test_namespace", "test_table")
    }

    #[test]
    fn test_write_and_read_manifest_json() {
        let temp_dir = env::temp_dir().join("kalamdb_test_manifest_write_read");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = make_table_id();
        let test_json = r#"{\"version\":1,\"segments\":[]}"#;

        // Write manifest
        let write_result =
            write_manifest_json(&storage_cached, TableType::Shared, &table_id, None, test_json);
        assert!(write_result.is_ok(), "Failed to write manifest");

        // Read manifest back
        let read_result = read_manifest_json(&storage_cached, TableType::Shared, &table_id, None);
        assert!(read_result.is_ok(), "Failed to read manifest");
        assert_eq!(read_result.unwrap(), test_json);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_manifest_exists_after_write() {
        let temp_dir = env::temp_dir().join("kalamdb_test_manifest_exists");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = make_table_id();

        // Check doesn't exist initially
        let exists_before =
            manifest_exists(&storage_cached, TableType::Shared, &table_id, None).await;
        assert!(exists_before.is_ok());
        assert!(!exists_before.unwrap(), "Manifest should not exist yet");

        // Write manifest
        let test_json = r#"{\"version\":1}"#;
        write_manifest_json(&storage_cached, TableType::Shared, &table_id, None, test_json)
            .unwrap();

        // Check exists after write
        let exists_after =
            manifest_exists(&storage_cached, TableType::Shared, &table_id, None).await;
        assert!(exists_after.is_ok());
        assert!(exists_after.unwrap(), "Manifest should exist after write");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_write_manifest_creates_directories() {
        let temp_dir = env::temp_dir().join("kalamdb_test_manifest_mkdir");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("ns1", "ns2");

        // Deep nested path
        let test_json = r#"{\"version\":1,\"segments\":[]}"#;

        // Should create all intermediate directories
        let result =
            write_manifest_json(&storage_cached, TableType::Shared, &table_id, None, test_json);
        assert!(result.is_ok(), "Should create nested directories");

        // Verify can read it back
        let read_result = read_manifest_json(&storage_cached, TableType::Shared, &table_id, None);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), test_json);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_read_nonexistent_manifest() {
        let temp_dir = env::temp_dir().join("kalamdb_test_manifest_nonexistent");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = TableId::from_strings("nonexistent", "table");

        let result = read_manifest_json(&storage_cached, TableType::Shared, &table_id, None);

        assert!(result.is_err(), "Should fail for nonexistent file");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_manifest_json_with_special_characters() {
        let temp_dir = env::temp_dir().join("kalamdb_test_manifest_special");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = make_table_id();

        // JSON with unicode, escapes, etc.
        let test_json = r#"{\"version\":1,\"name\":\"Test 🚀\",\"segments\":[\"seg-1\",\"seg-2\"],\"metadata\":{\"key\":\"value with \\\"quotes\\\"\"}}"#;

        write_manifest_json(&storage_cached, TableType::Shared, &table_id, None, test_json)
            .unwrap();
        let read_back =
            read_manifest_json(&storage_cached, TableType::Shared, &table_id, None).unwrap();

        assert_eq!(read_back, test_json);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_manifest_overwrite() {
        let temp_dir = env::temp_dir().join("kalamdb_test_manifest_overwrite");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = make_table_id();

        // Write first version
        let v1_json = r#"{\"version\":1}"#;
        write_manifest_json(&storage_cached, TableType::Shared, &table_id, None, v1_json).unwrap();

        let read_v1 =
            read_manifest_json(&storage_cached, TableType::Shared, &table_id, None).unwrap();
        assert_eq!(read_v1, v1_json);

        // Overwrite with second version
        let v2_json = r#"{\"version\":2}"#;
        write_manifest_json(&storage_cached, TableType::Shared, &table_id, None, v2_json).unwrap();

        let read_v2 =
            read_manifest_json(&storage_cached, TableType::Shared, &table_id, None).unwrap();
        assert_eq!(read_v2, v2_json, "Should read updated version");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_empty_manifest_json() {
        let temp_dir = env::temp_dir().join("kalamdb_test_manifest_empty");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = make_table_id();
        let empty_json = "{}";

        write_manifest_json(&storage_cached, TableType::Shared, &table_id, None, empty_json)
            .unwrap();
        let read_back =
            read_manifest_json(&storage_cached, TableType::Shared, &table_id, None).unwrap();

        assert_eq!(read_back, empty_json);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_large_manifest_json() {
        let temp_dir = env::temp_dir().join("kalamdb_test_manifest_large");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let storage = create_test_storage(&temp_dir);
        let storage_cached = StorageCached::with_default_timeouts(storage);
        let table_id = make_table_id();

        // Generate large JSON with many segments
        let mut segments = Vec::new();
        for i in 0..1000 {
            segments.push(format!(r#"{{\"id\":\"seg-{}\",\"size\":{}}}"#, i, i * 1024));
        }
        let large_json = format!(r#"{{\"version\":1,\"segments\":[{}]}}"#, segments.join(","));

        write_manifest_json(&storage_cached, TableType::Shared, &table_id, None, &large_json)
            .unwrap();
        let read_back =
            read_manifest_json(&storage_cached, TableType::Shared, &table_id, None).unwrap();

        assert_eq!(read_back, large_json);
        assert!(read_back.len() > 10000, "Should be a large manifest");

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
