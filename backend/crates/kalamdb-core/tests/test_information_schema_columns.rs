//! Test for information_schema.columns implementation
//!
//! This test verifies that the information_schema.columns table is properly
//! registered and can be queried via SQL.

use std::sync::Arc;

use kalamdb_commons::NodeId;
use kalamdb_configs::ServerConfig;
use kalamdb_core::app_context::AppContext;
use kalamdb_store::test_utils::TestDb;

/// Helper to create AppContext with temporary RocksDB for testing
async fn create_test_app_context() -> (Arc<AppContext>, TestDb) {
    let test_db = TestDb::with_system_tables().expect("Failed to create test database");
    let storage_base_path = test_db.storage_dir().expect("Failed to create storage directory");
    let backend = test_db.backend();
    let app_context = AppContext::create_isolated(
        backend,
        NodeId::new(1),
        storage_base_path.to_string_lossy().into_owned(),
        ServerConfig::default(),
    );

    (app_context, test_db)
}

#[tokio::test]
async fn test_information_schema_columns_query() {
    // Initialize AppContext (which registers information_schema.columns)
    let (app_ctx, _test_db) = create_test_app_context().await;

    // Get the base session context
    let session = app_ctx.base_session_context();

    // Query information_schema.columns
    let sql = "SELECT table_catalog, table_schema, table_name, column_name FROM \
               information_schema.columns WHERE table_name = 'jobs' ORDER BY ordinal_position \
               LIMIT 5";

    let result = session.sql(sql).await;

    // Should not return an error
    assert!(result.is_ok(), "Query failed with error: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.expect("Failed to collect batches");

    // Verify we got results
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Expected at least 1 row from information_schema.columns, got 0");

    println!("✅ information_schema.columns query succeeded with {} rows", total_rows);
}

#[tokio::test]
async fn test_information_schema_columns_shows_system_jobs() {
    // Initialize AppContext
    let (app_ctx, _temp_dir) = create_test_app_context().await;

    // Get the base session context
    let session = app_ctx.base_session_context();

    // Query for system.jobs columns specifically
    let sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE \
               table_schema = 'system' AND table_name = 'jobs' ORDER BY ordinal_position";

    let result = session.sql(sql).await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.expect("Failed to collect batches");

    // Should have at least the job_id, job_type, status columns
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows >= 3,
        "Expected at least 3 columns for system.jobs, got {}",
        total_rows
    );

    println!("✅ system.jobs has {} columns in information_schema.columns", total_rows);
}
