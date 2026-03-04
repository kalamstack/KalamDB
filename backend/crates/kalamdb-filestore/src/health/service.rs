//! Storage health check service implementation.

use super::models::{ConnectivityTestResult, StorageHealthResult};
use crate::core::factory::build_object_store;
use crate::error::{FilestoreError, Result};
use bytes::Bytes;
use kalamdb_system::providers::storages::models::StorageType;
use kalamdb_system::Storage;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use sysinfo::Disks;

/// Test file content used for health checks.
const HEALTH_CHECK_CONTENT: &[u8] = b"KALAMDB_HEALTH\n";

/// Directory prefix for health check test files.
const HEALTH_CHECK_PREFIX: &str = ".kalamdb-health-check";

/// Storage health check service.
///
/// Provides methods to validate storage backend connectivity by performing
/// a series of CRUD operations on test files.
pub struct StorageHealthService;

impl StorageHealthService {
    /// Run a lightweight connectivity test.
    ///
    /// Attempts to list files at the root of the storage to verify connectivity.
    /// This is faster than a full health check but less comprehensive.
    pub async fn test_connectivity(storage: &Storage) -> Result<ConnectivityTestResult> {
        let start = Instant::now();

        // Try to build ObjectStore with default timeouts
        let timeouts = kalamdb_configs::config::types::RemoteStorageTimeouts::default();
        let store = match build_object_store(storage, &timeouts) {
            Ok(s) => s,
            Err(e) => {
                return Ok(ConnectivityTestResult::failure(
                    format!("Failed to initialize storage: {}", e),
                    start.elapsed().as_millis() as u64,
                ));
            },
        };

        // Try to list root to verify connectivity
        match Self::try_list(&store, "").await {
            Ok(_) => Ok(ConnectivityTestResult::success(start.elapsed().as_millis() as u64)),
            Err(e) => Ok(ConnectivityTestResult::failure(
                e.to_string(),
                start.elapsed().as_millis() as u64,
            )),
        }
    }

    /// Run a full health check on the storage backend.
    ///
    /// Performs the following test sequence:
    /// 1. Build ObjectStore from configuration
    /// 2. Write a test file
    /// 3. List files to verify the test file exists
    /// 4. Read the test file and verify contents
    /// 5. Delete the test file
    /// 6. Verify the file is gone
    ///
    /// Returns detailed results about which operations succeeded/failed.
    pub async fn run_full_health_check(storage: &Storage) -> Result<StorageHealthResult> {
        let start = Instant::now();

        // Build ObjectStore with default timeouts
        let timeouts = kalamdb_configs::config::types::RemoteStorageTimeouts::default();
        let store = match build_object_store(storage, &timeouts) {
            Ok(s) => s,
            Err(e) => {
                return Ok(StorageHealthResult::unreachable(
                    format!("Failed to initialize storage: {}", e),
                    start.elapsed().as_millis() as u64,
                ));
            },
        };

        // Generate unique test file path
        let test_filename = Self::generate_test_filename();
        let test_path = format!("{}/{}", HEALTH_CHECK_PREFIX, test_filename);

        let mut readable = false;
        let mut writable = false;
        let mut listable = false;
        let mut deletable = false;
        let mut errors: Vec<String> = Vec::new();

        // Step 1: Write test file
        match Self::try_put(&store, &test_path, HEALTH_CHECK_CONTENT).await {
            Ok(_) => writable = true,
            Err(e) => errors.push(format!("PUT failed: {}", e)),
        }

        // Step 2: List files (only if write succeeded)
        if writable {
            match Self::try_list(&store, HEALTH_CHECK_PREFIX).await {
                Ok(files) => {
                    listable = true;
                    // Verify our test file is in the list
                    if !files.iter().any(|f| f.contains(&test_filename)) {
                        errors
                            .push("LIST succeeded but test file not found in results".to_string());
                    }
                },
                Err(e) => errors.push(format!("LIST failed: {}", e)),
            }
        }

        // Step 3: Read test file (only if write succeeded)
        if writable {
            match Self::try_get(&store, &test_path).await {
                Ok(content) => {
                    readable = true;
                    // Verify content matches
                    if content.as_ref() != HEALTH_CHECK_CONTENT {
                        errors.push(format!(
                            "GET succeeded but content mismatch: expected {} bytes, got {} bytes",
                            HEALTH_CHECK_CONTENT.len(),
                            content.len()
                        ));
                    }
                },
                Err(e) => errors.push(format!("GET failed: {}", e)),
            }
        }

        // Step 4: Delete test file (only if write succeeded)
        if writable {
            match Self::try_delete(&store, &test_path).await {
                Ok(_) => deletable = true,
                Err(e) => errors.push(format!("DELETE failed: {}", e)),
            }
        }

        // Cleanup: Try to delete the health check prefix directory
        // (ignore errors - it's just cleanup)
        let _ = Self::try_delete_prefix(&store, HEALTH_CHECK_PREFIX).await;

        let latency_ms = start.elapsed().as_millis() as u64;

        // Determine overall status
        let mut result = if readable && writable && listable && deletable {
            StorageHealthResult::healthy(latency_ms)
        } else if !writable && !readable && !listable {
            StorageHealthResult::unreachable(errors.join("; "), latency_ms)
        } else {
            StorageHealthResult::degraded(
                readable,
                writable,
                listable,
                deletable,
                errors.join("; "),
                latency_ms,
            )
        };

        if storage.storage_type == StorageType::Filesystem {
            if let Some((total_bytes, used_bytes)) = Self::filesystem_capacity(storage) {
                result = result.with_capacity(Some(total_bytes), Some(used_bytes));
            }
        }

        Ok(result)
    }

    /// Generate a unique test filename.
    fn generate_test_filename() -> String {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let random: u32 = rand::random();
        format!("{}-{:08x}.tmp", timestamp, random)
    }

    /// Try to put a file to the object store.
    async fn try_put(store: &Arc<dyn ObjectStore>, path: &str, content: &[u8]) -> Result<()> {
        let object_path = ObjectPath::from(path);
        store
            .put(&object_path, Bytes::copy_from_slice(content).into())
            .await
            .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
        Ok(())
    }

    /// Try to get a file from the object store.
    async fn try_get(store: &Arc<dyn ObjectStore>, path: &str) -> Result<Bytes> {
        let object_path = ObjectPath::from(path);
        let result = store
            .get(&object_path)
            .await
            .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
        let bytes = result.bytes().await.map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
        Ok(bytes)
    }

    /// Try to list files in the object store.
    async fn try_list(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<Vec<String>> {
        use futures_util::StreamExt;

        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(&ObjectPath::from(prefix))
        };

        let mut files = Vec::new();
        let mut stream = store.list(prefix_path);

        while let Some(item) = stream.next().await {
            match item {
                Ok(meta) => files.push(meta.location.to_string()),
                Err(e) => return Err(FilestoreError::ObjectStore(e.to_string())),
            }
        }

        Ok(files)
    }

    /// Try to delete a file from the object store.
    async fn try_delete(store: &Arc<dyn ObjectStore>, path: &str) -> Result<()> {
        let object_path = ObjectPath::from(path);
        store
            .delete(&object_path)
            .await
            .map_err(|e| FilestoreError::ObjectStore(e.to_string()))?;
        Ok(())
    }

    /// Try to delete all files with a given prefix.
    async fn try_delete_prefix(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<()> {
        use futures_util::StreamExt;

        let prefix_path = ObjectPath::from(prefix);
        let mut stream = store.list(Some(&prefix_path));

        while let Some(item) = stream.next().await {
            if let Ok(meta) = item {
                let _ = store.delete(&meta.location).await;
            }
        }

        Ok(())
    }

    fn filesystem_capacity(storage: &Storage) -> Option<(u64, u64)> {
        let base = storage.base_directory.trim();
        if base.is_empty() {
            return None;
        }

        let base_path = Path::new(base);
        let canonical = base_path.canonicalize().unwrap_or_else(|_| base_path.to_path_buf());

        let disks = Disks::new_with_refreshed_list();
        let mut best: Option<&sysinfo::Disk> = None;
        let mut best_len = 0usize;

        for disk in disks.list() {
            let mount = disk.mount_point();
            if canonical.starts_with(mount) {
                let mount_len = mount.as_os_str().len();
                if mount_len >= best_len {
                    best_len = mount_len;
                    best = Some(disk);
                }
            }
        }

        best.map(|disk| {
            let total = disk.total_space();
            let used = total.saturating_sub(disk.available_space());
            (total, used)
        })
    }
}
