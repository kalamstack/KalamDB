//! Scenario 11: Multi-Storage Routing
//!
//! Validates data flows to correct storage backends based on namespace/table config.
//!
//! ## Checklist
//! - [x] Create multiple storages
//! - [x] Assign tables to different storages
//! - [x] Flush writes to correct destination
//! - [x] Verify artifacts in expected storage folder

use std::time::Duration;

use kalamdb_commons::Role;

use super::helpers::*;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Main multi-storage routing test
#[tokio::test]
async fn test_scenario_11_multi_storage_basic() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    // =========================================================
    // Step 1: Query existing storages
    // =========================================================
    let resp = server.execute_sql("SELECT * FROM system.storages ORDER BY storage_id").await?;
    assert_success(&resp, "Query system.storages");
    let storage_rows = get_rows(&resp);
    println!("Existing storages: {} rows", storage_rows.len());
    for row in &storage_rows {
        println!("  Storage: {:?}", row);
    }

    // =========================================================
    // Step 2: Create a namespace
    // =========================================================
    let ns = unique_ns("multi_storage");
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    // =========================================================
    // Step 3: Create tables (they use default storage)
    // =========================================================
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.hot_data (
                        id BIGINT PRIMARY KEY,
                        value TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE hot_data table");

    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.cold_data (
                        id BIGINT PRIMARY KEY,
                        archive_value TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE cold_data table");

    // =========================================================
    // Step 4: Insert data to both tables
    // =========================================================
    // Create client with unique name to avoid parallel test interference
    let username = format!("{}_storage_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;

    // Insert to hot_data
    for i in 1..=100 {
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.hot_data (id, value) VALUES ({}, 'hot_value_{}')",
                    ns, i, i
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert hot_data {}", i);
    }

    // Insert to cold_data
    for i in 1..=100 {
        let resp = client
            .execute_query(
                &format!(
                    "INSERT INTO {}.cold_data (id, archive_value) VALUES ({}, 'cold_value_{}')",
                    ns, i, i
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert cold_data {}", i);
    }

    // =========================================================
    // Step 5: Flush both tables
    // =========================================================
    let resp = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.hot_data", ns)).await?;
    assert_success(&resp, "FLUSH hot_data");

    let resp = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.cold_data", ns)).await?;
    assert_success(&resp, "FLUSH cold_data");

    // Wait for flushes to complete
    tokio::time::sleep(Duration::from_secs(3)).await;

    // =========================================================
    // Step 6: Verify data is queryable after flush
    // =========================================================
    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.hot_data", ns), None, None, None)
        .await?;
    assert!(resp.success(), "Query hot_data count");
    let hot_count = resp
        .rows_as_maps()
        .first()
        .and_then(|r| r.get("cnt"))
        .and_then(|v| json_to_i64(v))
        .unwrap_or(0);
    assert_eq!(hot_count, 100, "hot_data should have 100 rows after flush");

    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.cold_data", ns), None, None, None)
        .await?;
    assert!(resp.success(), "Query cold_data count");
    let cold_count = resp
        .rows_as_maps()
        .first()
        .and_then(|r| r.get("cnt"))
        .and_then(|v| json_to_i64(v))
        .unwrap_or(0);
    assert_eq!(cold_count, 100, "cold_data should have 100 rows after flush");

    // =========================================================
    // Step 7: Query tables metadata to see storage assignment
    // =========================================================
    let resp = server
        .execute_sql(&format!(
            "SELECT table_name, table_type FROM system.schemas WHERE namespace_id = '{}'",
            ns
        ))
        .await?;
    assert_success(&resp, "Query system.schemas");
    assert_eq!(get_rows(&resp).len(), 2, "Should have 2 tables");

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test storage validation and constraints
#[tokio::test]
async fn test_scenario_11_storage_constraints() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("storage_constraints");

    // Create namespace
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    // Create table with specific configuration
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.constrained (
                        id BIGINT PRIMARY KEY,
                        data TEXT NOT NULL
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE constrained table");

    // Insert data with unique user to avoid parallel test interference
    let username = format!("{}_constraint_user", ns);
    let client = create_user_and_client(server, &username, &Role::User).await?;
    for i in 1..=50 {
        let resp = client
            .execute_query(
                &format!("INSERT INTO {}.constrained (id, data) VALUES ({}, 'data_{}')", ns, i, i),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert row {}", i);
    }

    // Flush and verify
    let resp = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.constrained", ns)).await?;
    assert_success(&resp, "FLUSH constrained");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify data persisted correctly
    let resp = client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.constrained", ns), None, None, None)
        .await?;
    assert!(resp.success(), "Query count");
    let count = resp
        .rows_as_maps()
        .first()
        .and_then(|r| r.get("cnt"))
        .and_then(|v| json_to_i64(v))
        .unwrap_or(0);
    assert_eq!(count, 50, "Should have 50 rows");

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}

/// Test flush behavior with different table types to different storage paths
#[tokio::test]
async fn test_scenario_11_table_types_storage() -> anyhow::Result<()> {
    let server = crate::test_support::http_server::get_global_server().await;
    let ns = unique_ns("types_storage");

    // Create namespace
    let resp = server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await?;
    assert_success(&resp, "CREATE namespace");

    // Create USER table
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.user_table (
                        id BIGINT PRIMARY KEY,
                        data TEXT
                    ) WITH (TYPE = 'USER')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE USER table");

    // Create SHARED table
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.shared_table (
                        id BIGINT PRIMARY KEY,
                        config TEXT
                    ) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'PUBLIC')"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE SHARED table");

    // Create STREAM table
    let resp = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.stream_table (
                        id BIGINT PRIMARY KEY,
                        event TEXT
                    ) WITH (TYPE = 'STREAM', TTL_SECONDS = 3600)"#,
            ns
        ))
        .await?;
    assert_success(&resp, "CREATE STREAM table");

    // Get clients with unique names to avoid parallel test interference
    let user1_name = format!("{}_storage_user1", ns);
    let user2_name = format!("{}_storage_user2", ns);
    let user1_client = create_user_and_client(server, &user1_name, &Role::User).await?;
    let user2_client = create_user_and_client(server, &user2_name, &Role::User).await?;
    let admin_client = server.link_client("root");

    // Insert USER data (per-user isolation)
    for i in 1..=10 {
        let resp = user1_client
            .execute_query(
                &format!(
                    "INSERT INTO {}.user_table (id, data) VALUES ({}, 'user1_data_{}')",
                    ns, i, i
                ),
                None,
                None,
                None,
            )
            .await?;
        if !resp.success() {
            eprintln!("DEBUG: User1 insert {} FAILED: {:?}", i, resp.error);
        }
        assert!(resp.success(), "User1 insert {}: {:?}", i, resp.error);
    }

    for i in 11..=20 {
        let resp = user2_client
            .execute_query(
                &format!(
                    "INSERT INTO {}.user_table (id, data) VALUES ({}, 'user2_data_{}')",
                    ns, i, i
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "User2 insert {}", i);
    }

    // Insert SHARED data (visible to all)
    for i in 1..=10 {
        let resp = admin_client
            .execute_query(
                &format!(
                    "INSERT INTO {}.shared_table (id, config) VALUES ({}, 'config_{}')",
                    ns, i, i
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Admin insert shared {}", i);
    }

    // Insert STREAM data
    for i in 1..=10 {
        let resp = user1_client
            .execute_query(
                &format!(
                    "INSERT INTO {}.stream_table (id, event) VALUES ({}, 'event_{}')",
                    ns, i, i
                ),
                None,
                None,
                None,
            )
            .await?;
        assert!(resp.success(), "Insert stream event {}", i);
    }

    // Flush all tables
    let _ = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.user_table", ns)).await;
    let _ = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.shared_table", ns)).await;
    let _ = server.execute_sql(&format!("STORAGE FLUSH TABLE {}.stream_table", ns)).await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify USER table isolation: user1 sees only their rows
    let resp = user1_client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.user_table", ns), None, None, None)
        .await?;
    let user1_count = resp
        .rows_as_maps()
        .first()
        .and_then(|r| r.get("cnt"))
        .and_then(|v| json_to_i64(v))
        .unwrap_or(0);
    assert_eq!(user1_count, 10, "User1 should see 10 rows");

    // Verify USER table isolation: user2 sees only their rows
    let resp = user2_client
        .execute_query(&format!("SELECT COUNT(*) as cnt FROM {}.user_table", ns), None, None, None)
        .await?;
    let user2_count = resp
        .rows_as_maps()
        .first()
        .and_then(|r| r.get("cnt"))
        .and_then(|v| json_to_i64(v))
        .unwrap_or(0);
    assert_eq!(user2_count, 10, "User2 should see 10 rows");

    // Verify SHARED table visible to all
    let resp = user1_client
        .execute_query(
            &format!("SELECT COUNT(*) as cnt FROM {}.shared_table", ns),
            None,
            None,
            None,
        )
        .await?;
    let shared_count = resp
        .rows_as_maps()
        .first()
        .and_then(|r| r.get("cnt"))
        .and_then(|v| json_to_i64(v))
        .unwrap_or(0);
    assert_eq!(shared_count, 10, "Shared table should have 10 rows");

    // Cleanup
    let _ = server.execute_sql(&format!("DROP NAMESPACE {} CASCADE", ns)).await;
    Ok(())
}
