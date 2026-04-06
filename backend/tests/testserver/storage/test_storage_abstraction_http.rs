//! Integration tests for User Story 7: Storage Backend Abstraction and Architecture Cleanup
//!
//! These tests intentionally go through the real HTTP API (`/v1/api/sql`) using the
//! near-production server wiring from `tests/testserver/commons`.

use kalam_client::models::ResponseStatus;
use std::path::PathBuf;

fn file_uri(path: PathBuf) -> String {
    #[cfg(unix)]
    {
        // Unix: file:// + absolute path yields file:///...
        format!("file://{}", path.to_string_lossy())
    }

    #[cfg(windows)]
    {
        // Windows: Use forward slashes and don't add file:// prefix
        // Just return the path with forward slashes
        path.to_string_lossy().replace('\\', "/")
    }
}

#[tokio::test]
async fn test_storage_abstraction_over_http() -> anyhow::Result<()> {
    let server = super::test_support::http_server::get_global_server().await;
    // T420: StorageBackend trait interface exists (smoke via CREATE NAMESPACE + CREATE STORAGE)
    let response = server.execute_sql("CREATE NAMESPACE IF NOT EXISTS test_storage_trait").await?;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        response.error
    );

    let storage_path = server.data_path().join("test_storage");
    let create_storage_sql = format!(
        "CREATE STORAGE test_local TYPE filesystem NAME 'Test Local' PATH '{}'",
        file_uri(storage_path)
    );
    let response = server.execute_sql(&create_storage_sql).await?;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "CREATE STORAGE failed: {:?}",
        response.error
    );

    // T421: RocksDB implements storage trait (CRUD on a user table)
    let response = server.execute_sql("CREATE NAMESPACE IF NOT EXISTS test_rocksdb_trait").await?;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        response.error
    );

    let response = server
            .execute_sql(
                "CREATE TABLE test_rocksdb_trait.test_table (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE = 'USER')",
            )
            .await?;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        response.error
    );

    let response = server
        .execute_sql("INSERT INTO test_rocksdb_trait.test_table (id, name) VALUES (1, 'test')")
        .await?;
    assert_eq!(response.status, ResponseStatus::Success, "INSERT failed: {:?}", response.error);

    let response = server.execute_sql("SELECT * FROM test_rocksdb_trait.test_table").await?;
    assert_eq!(response.status, ResponseStatus::Success, "SELECT failed: {:?}", response.error);
    let rows = response
        .results
        .first()
        .and_then(|r| r.rows.as_ref())
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(rows, 1);

    // T422: system.storages rename is complete
    let response = server.execute_sql("SELECT * FROM system.storages").await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let response = server.execute_sql("SELECT * FROM system.storage_locations").await?;
    assert_eq!(response.status, ResponseStatus::Error);

    // T423: Storage CRUD through abstraction
    let storage_path = server.data_path().join("test_crud");
    let create_storage_sql = format!(
        "CREATE STORAGE test_crud_storage TYPE filesystem NAME 'CRUD Storage' PATH '{}'",
        file_uri(storage_path)
    );
    let response = server.execute_sql(&create_storage_sql).await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let response = server
        .execute_sql("SELECT * FROM system.storages WHERE storage_id = 'test_crud_storage'")
        .await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let response = server.execute_sql("DROP STORAGE test_crud_storage").await?;
    assert_eq!(response.status, ResponseStatus::Success);

    // T424: Column family (partition) abstraction smoke via multi-table isolation
    let response = server.execute_sql("CREATE NAMESPACE IF NOT EXISTS test_partition_ns").await?;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        response.error
    );

    let response = server.execute_sql("CREATE TABLE test_partition_ns.table1 (id BIGINT PRIMARY KEY, data TEXT) WITH (TYPE = 'USER')").await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let response = server.execute_sql("CREATE TABLE test_partition_ns.table2 (id BIGINT PRIMARY KEY, value BIGINT) WITH (TYPE = 'USER')").await?;
    assert_eq!(response.status, ResponseStatus::Success);

    let _ = server
        .execute_sql("INSERT INTO test_partition_ns.table1 (id, data) VALUES (1, 'test')")
        .await?;
    let _ = server
        .execute_sql("INSERT INTO test_partition_ns.table2 (id, value) VALUES (1, 42)")
        .await?;

    let response = server.execute_sql("SELECT * FROM test_partition_ns.table1").await?;
    assert_eq!(response.status, ResponseStatus::Success);
    let rows = response
        .results
        .first()
        .and_then(|r| r.rows.as_ref())
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(rows, 1);

    let response = server.execute_sql("SELECT * FROM test_partition_ns.table2").await?;
    assert_eq!(response.status, ResponseStatus::Success);
    let rows = response
        .results
        .first()
        .and_then(|r| r.rows.as_ref())
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(rows, 1);

    // T425: Error handling
    let response = server
        .execute_sql("CREATE STORAGE bad_storage TYPE filesystem NAME 'Bad' PATH ''")
        .await?;
    assert_eq!(response.status, ResponseStatus::Error);
    let response = server.execute_sql("SELECT * FROM nonexistent.table").await?;
    assert_eq!(response.status, ResponseStatus::Error);
    let response = server.execute_sql("INSERT INTO nonexistent.table (id) VALUES (1)").await?;
    assert_eq!(response.status, ResponseStatus::Error);

    // Cleanup best-effort: keep the test idempotent.
    let _ = server.execute_sql("DROP NAMESPACE test_storage_trait").await;
    let _ = server.execute_sql("DROP NAMESPACE test_rocksdb_trait").await;
    let _ = server.execute_sql("DROP NAMESPACE test_partition_ns").await;
    let _ = server.execute_sql("DROP STORAGE test_local").await;
    Ok(())
}
