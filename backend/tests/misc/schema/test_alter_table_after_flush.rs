//! Integration test for ALTER TABLE after flush operations
//!
//! This test verifies that schema evolution works correctly when:
//! 1. Data has been flushed to cold storage (Parquet files)
//! 2. Schema is altered (new columns added)
//! 3. Queries span both old (flushed) and new (hot) data
//!
//! This is a critical scenario because it tests schema compatibility
//! between hot storage (RocksDB) and cold storage (Parquet files).

use super::test_support::{consolidated_helpers, fixtures, flush_helpers, TestServer};
use kalam_client::models::ResponseStatus;
use kalam_client::parse_i64;

/// Test ALTER TABLE ADD COLUMN after flushing data to cold storage
///
/// Scenario:
/// 1. Create messages table with 3 columns (id, content, timestamp)
/// 2. Insert initial rows
/// 3. Flush to cold storage
/// 4. Alter table to add new column (priority)
/// 5. Query table (should work with old and new schema)
/// 6. Insert new rows with new column
/// 7. Query again and verify new column exists in results
#[actix_web::test]
async fn test_alter_table_add_column_after_flush() {
    let server = TestServer::new_shared().await;

    // Use unique namespace to avoid test conflicts
    let ns = consolidated_helpers::unique_namespace("alter_flush");

    // Step 1: Create namespace and messages table
    fixtures::create_namespace(&server, &ns).await;

    let create_table_sql = format!(
        r#"CREATE TABLE {}.messages (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            timestamp BIGINT
        ) WITH (
            TYPE = 'USER',
            STORAGE_ID = 'local'
        )"#,
        ns
    );

    let resp = server.execute_sql_as_user(&create_table_sql, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to create table: {:?}", resp.error);

    // Step 2: Insert initial rows (before alter)
    let insert_sql = format!(
        r#"INSERT INTO {}.messages (id, content, timestamp) VALUES
            ('msg1', 'Hello World', 1000),
            ('msg2', 'Test Message', 2000),
            ('msg3', 'Another Message', 3000)"#,
        ns
    );

    let resp = server.execute_sql_as_user(&insert_sql, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to insert initial rows: {:?}",
        resp.error
    );

    // Step 3: Flush data to cold storage
    let flush_result = flush_helpers::execute_flush_synchronously(&server, &ns, "messages")
        .await
        .expect("Flush should succeed");

    println!("✅ Flushed {} rows to cold storage", flush_result.rows_flushed);
    assert_eq!(flush_result.rows_flushed, 3, "Expected to flush 3 rows");

    // Step 4: Alter table to add new column
    let alter_sql = format!(r#"ALTER TABLE {}.messages ADD COLUMN priority INT"#, ns);

    let resp = server.execute_sql_as_user(&alter_sql, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "ALTER TABLE after flush failed: {:?}",
        resp.error
    );

    println!("✅ ALTER TABLE added 'priority' column after flush");

    // Step 5: Query table with * (should include new column)
    let select_sql = format!("SELECT * FROM {}.messages ORDER BY id", ns);

    let resp = server.execute_sql_as_user(&select_sql, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "SELECT after ALTER failed: {:?}",
        resp.error
    );

    // Verify we can read flushed data with new schema
    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 3, "Should read all 3 flushed rows");

    // Check first row (old data from cold storage)
    assert_eq!(rows[0].get("id").unwrap().as_str().unwrap(), "msg1");
    assert_eq!(rows[0].get("content").unwrap().as_str().unwrap(), "Hello World");
    assert_eq!(rows[0].get("timestamp").map(parse_i64).unwrap(), 1000);

    // New column should be NULL for old data
    assert!(
        rows[0].get("priority").unwrap().is_null(),
        "Priority should be NULL for flushed data"
    );

    println!("✅ Successfully queried flushed data with new schema");

    // Step 6: Insert new rows with new column
    let insert_new_sql = format!(
        r#"INSERT INTO {}.messages (id, content, timestamp, priority) VALUES
            ('msg4', 'High Priority', 4000, 1),
            ('msg5', 'Low Priority', 5000, 3)"#,
        ns
    );

    let resp = server.execute_sql_as_user(&insert_new_sql, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to insert rows with new column: {:?}",
        resp.error
    );

    println!("✅ Inserted new rows with priority column");

    // Step 7: Query again with * and verify new column appears
    let resp = server.execute_sql_as_user(&select_sql, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Final SELECT failed: {:?}", resp.error);

    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 5, "Should have 5 total rows (3 old + 2 new)");

    // Verify old rows still have NULL priority
    for (i, row) in rows.iter().enumerate().take(3) {
        assert!(
            row.get("priority").unwrap().is_null(),
            "Old row {} should have NULL priority",
            i
        );
    }

    // Verify new rows have priority values
    assert_eq!(rows[3].get("id").unwrap().as_str().unwrap(), "msg4", "Row 4 should be msg4");
    assert_eq!(
        rows[3].get("priority").map(parse_i64).unwrap(),
        1,
        "msg4 should have priority 1"
    );

    assert_eq!(rows[4].get("id").unwrap().as_str().unwrap(), "msg5", "Row 5 should be msg5");
    assert_eq!(
        rows[4].get("priority").map(parse_i64).unwrap(),
        3,
        "msg5 should have priority 3"
    );

    println!("✅ All rows returned with correct schema including new column");

    // Bonus: Verify explicit column selection works
    let explicit_select = format!(
        "SELECT id, content, timestamp, priority FROM {}.messages WHERE priority IS NOT NULL",
        ns
    );

    let resp = server.execute_sql_as_user(&explicit_select, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 2, "Should find 2 rows with non-NULL priority");

    println!("✅ Test passed: ALTER TABLE after flush with mixed schema data");
}

/// Test multiple ALTER operations after flush
///
/// Scenario:
/// 1. Create table, insert, flush
/// 2. Add column1
/// 3. Insert more data
/// 4. Flush again
/// 5. Add column2
/// 6. Query and verify all schema versions are handled correctly
#[actix_web::test]
async fn test_multiple_alter_operations_with_flushes() {
    let server = TestServer::new_shared().await;

    let ns = consolidated_helpers::unique_namespace("multi_alter");

    // Setup
    fixtures::create_namespace(&server, &ns).await;

    let create_sql = format!(
        r#"CREATE TABLE {}.events (
            id TEXT PRIMARY KEY,
            event_type TEXT
        ) WITH (TYPE = 'USER', STORAGE_ID = 'local')"#,
        ns
    );

    server.execute_sql_as_user(&create_sql, "user1").await;

    // First batch of data
    server
        .execute_sql_as_user(
            &format!("INSERT INTO {}.events (id, event_type) VALUES ('e1', 'login')", ns),
            "user1",
        )
        .await;

    // First flush
    flush_helpers::execute_flush_synchronously(&server, &ns, "events")
        .await
        .expect("First flush failed");

    println!("✅ First flush completed");

    // First ALTER
    server
        .execute_sql_as_user(&format!("ALTER TABLE {}.events ADD COLUMN user_id TEXT", ns), "user1")
        .await;

    // Second batch with first new column
    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.events (id, event_type, user_id) VALUES ('e2', 'logout', 'user123')",
                ns
            ),
            "user1",
        )
        .await;

    // Second flush
    flush_helpers::execute_flush_synchronously(&server, &ns, "events")
        .await
        .expect("Second flush failed");

    println!("✅ Second flush completed");

    // Second ALTER
    server
        .execute_sql_as_user(
            &format!("ALTER TABLE {}.events ADD COLUMN timestamp BIGINT", ns),
            "user1",
        )
        .await;

    // Third batch with both new columns
    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.events (id, event_type, user_id, timestamp) 
                 VALUES ('e3', 'action', 'user456', 9999)",
                ns
            ),
            "user1",
        )
        .await;

    // Query all data spanning three schema versions
    let resp = server
        .execute_sql_as_user(
            &format!("SELECT id, event_type, user_id, timestamp FROM {}.events ORDER BY id", ns),
            "user1",
        )
        .await;

    assert_eq!(resp.status, ResponseStatus::Success);

    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 3, "Should have 3 events across schema versions");

    // e1: Original schema (both new columns NULL)
    assert!(rows[0].get("user_id").unwrap().is_null());
    assert!(rows[0].get("timestamp").unwrap().is_null());

    // e2: First ALTER (user_id present, timestamp NULL)
    assert_eq!(rows[1].get("user_id").unwrap().as_str().unwrap(), "user123");
    assert!(rows[1].get("timestamp").unwrap().is_null());

    // e3: Second ALTER (both columns present)
    assert_eq!(rows[2].get("user_id").unwrap().as_str().unwrap(), "user456");
    assert_eq!(rows[2].get("timestamp").map(parse_i64).unwrap(), 9999);

    println!("✅ Successfully queried data across 3 schema versions");

    println!("✅ Test passed: Multiple ALTER operations with flushes");
}
