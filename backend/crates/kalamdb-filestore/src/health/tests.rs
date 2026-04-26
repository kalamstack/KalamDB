//! Tests for the storage health service.

use std::env;

use kalamdb_commons::models::StorageId;
use kalamdb_system::{providers::storages::models::StorageType, Storage};

use super::{
    models::{HealthStatus, StorageHealthResult},
    service::StorageHealthService,
};

fn create_test_storage(base_directory: &str) -> Storage {
    let now = chrono::Utc::now().timestamp();
    Storage {
        storage_id: StorageId::from("test_health"),
        storage_name: "Test Health Storage".to_string(),
        description: None,
        storage_type: StorageType::Filesystem,
        base_directory: base_directory.to_string(),
        credentials: None,
        config_json: None,
        shared_tables_template: "{namespace}/{tableName}".to_string(),
        user_tables_template: "{namespace}/{userId}/{tableName}".to_string(),
        created_at: now,
        updated_at: now,
    }
}

#[tokio::test]
async fn test_health_check_local_filesystem() {
    // Create a temp directory for testing
    let temp_dir = env::temp_dir().join("kalamdb_health_test");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let storage = create_test_storage(&temp_dir.to_string_lossy());

    let result = StorageHealthService::run_full_health_check(&storage).await.unwrap();

    assert_eq!(result.status, HealthStatus::Healthy);
    assert!(result.readable);
    assert!(result.writable);
    assert!(result.listable);
    assert!(result.deletable);
    assert!(result.error.is_none());
    assert!(result.latency_ms > 0);

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_connectivity_test_local_filesystem() {
    let temp_dir = env::temp_dir().join("kalamdb_connectivity_test");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let storage = create_test_storage(&temp_dir.to_string_lossy());

    let result = StorageHealthService::test_connectivity(&storage).await.unwrap();

    assert!(result.connected);
    assert!(result.error.is_none());

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_health_check_nonexistent_directory() {
    // Use a path that doesn't exist and shouldn't be createable
    // On most systems, trying to write to root-level directories fails
    #[cfg(windows)]
    let bad_path = "Z:\\nonexistent\\impossible\\path\\that\\should\\fail";
    #[cfg(not(windows))]
    let bad_path = "/nonexistent/impossible/path/that/should/fail";

    let storage = create_test_storage(bad_path);

    let result = StorageHealthService::run_full_health_check(&storage).await.unwrap();

    // Should fail to initialize or write
    assert!(result.has_issues());
    assert!(result.error.is_some());
}

#[tokio::test]
async fn test_health_result_structure() {
    let result = StorageHealthResult::healthy(100);

    assert_eq!(result.status, HealthStatus::Healthy);
    assert!(result.readable);
    assert!(result.writable);
    assert!(result.listable);
    assert!(result.deletable);
    assert_eq!(result.latency_ms, 100);
    assert!(result.total_bytes.is_none());
    assert!(result.used_bytes.is_none());
    assert!(result.error.is_none());
    assert!(result.tested_at > 0);
}

#[tokio::test]
async fn test_health_result_with_capacity() {
    let result =
        StorageHealthResult::healthy(50).with_capacity(Some(1_000_000_000), Some(500_000_000));

    assert_eq!(result.total_bytes, Some(1_000_000_000));
    assert_eq!(result.used_bytes, Some(500_000_000));
}

#[tokio::test]
async fn test_health_result_degraded() {
    let result = StorageHealthResult::degraded(
        true,  // readable
        false, // writable
        true,  // listable
        false, // deletable
        "Write permission denied".to_string(),
        200,
    );

    assert_eq!(result.status, HealthStatus::Degraded);
    assert!(result.readable);
    assert!(!result.writable);
    assert!(result.listable);
    assert!(!result.deletable);
    assert_eq!(result.error, Some("Write permission denied".to_string()));
}

#[tokio::test]
async fn test_health_result_unreachable() {
    let result = StorageHealthResult::unreachable("Connection refused".to_string(), 5000);

    assert_eq!(result.status, HealthStatus::Unreachable);
    assert!(!result.readable);
    assert!(!result.writable);
    assert!(!result.listable);
    assert!(!result.deletable);
    assert_eq!(result.error, Some("Connection refused".to_string()));
}
