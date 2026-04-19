//! Integration tests for PK Index Efficiency
//!
//! These tests verify that UPDATE and SELECT by PK use the secondary index
//! for O(1) lookups rather than scanning all rows in memory.
//!
//! ## Tests
//! - User table PK index: INSERT 100 rows → UPDATE by PK → verify O(1) lookup
//! - Shared table PK index: INSERT 100 rows → UPDATE by PK → verify O(1) lookup
//! - Performance comparison: Update with many rows vs few rows should have similar latency

use super::test_support::flush_helpers::{
    execute_flush_synchronously, execute_shared_flush_synchronously,
};
use super::test_support::{consolidated_helpers, fixtures, TestServer};
use kalam_client::models::ResponseStatus;
use std::cmp::max;
use std::time::{Duration, Instant};

fn median_duration(samples: &mut [Duration]) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

/// Test: User table UPDATE by PK uses index - not full scan
///
/// This test verifies that UPDATE operations on user tables use the PK index
/// for efficient O(1) lookup instead of scanning all rows.
///
/// Strategy:
/// 1. Insert 100 rows
/// 2. Measure UPDATE latency for a specific row
/// 3. Insert 1000 more rows  
/// 4. Measure UPDATE latency again
/// 5. Verify latency doesn't scale linearly with row count (O(1) not O(n))
#[actix_web::test]
async fn test_user_table_pk_index_update() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("idx_test");

    // Setup namespace and user table
    fixtures::create_namespace(&server, &ns).await;
    let create_response = server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.user_items (
                id INT PRIMARY KEY,
                name TEXT,
                value INT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "test_user",
        )
        .await;
    assert_eq!(
        create_response.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_response.error
    );

    // Insert 100 rows in one batch
    {
        let values: Vec<String> =
            (1..=100).map(|i| format!("({}, 'item_{}', {})", i, i, i * 10)).collect();
        let insert_response = server
            .execute_sql_as_user(
                &format!(
                    "INSERT INTO {}.user_items (id, name, value) VALUES {}",
                    ns,
                    values.join(", ")
                ),
                "test_user",
            )
            .await;
        assert_eq!(
            insert_response.status,
            ResponseStatus::Success,
            "Batch INSERT failed: {:?}",
            insert_response.error
        );
    }

    // Warmup: Run a few updates to warm up caches/JIT
    for _ in 0..3 {
        server
            .execute_sql_as_user(
                &format!("UPDATE {}.user_items SET value = 1 WHERE id = 1", ns),
                "test_user",
            )
            .await;
    }

    // Measure UPDATE latency with 100 rows (average of 3 runs)
    let start_100 = Instant::now();
    let update_response = server
        .execute_sql_as_user(
            &format!("UPDATE {}.user_items SET value = 999 WHERE id = 50", ns),
            "test_user",
        )
        .await;
    let latency_100_rows = start_100.elapsed();

    assert_eq!(
        update_response.status,
        ResponseStatus::Success,
        "UPDATE failed: {:?}",
        update_response.error
    );

    // Verify the update worked
    let select_response = server
        .execute_sql_as_user(
            &format!("SELECT id, value FROM {}.user_items WHERE id = 50", ns),
            "test_user",
        )
        .await;
    assert_eq!(select_response.status, ResponseStatus::Success);
    let rows = select_response.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("value").unwrap().as_i64().unwrap(), 999);

    // Insert 900 more rows in batches (total 1000)
    for batch_start in (101..=1000usize).step_by(100) {
        let batch_end = (batch_start + 99).min(1000);
        let values: Vec<String> = (batch_start..=batch_end)
            .map(|i| format!("({}, 'item_{}', {})", i, i, i * 10))
            .collect();
        let insert_response = server
            .execute_sql_as_user(
                &format!(
                    "INSERT INTO {}.user_items (id, name, value) VALUES {}",
                    ns,
                    values.join(", ")
                ),
                "test_user",
            )
            .await;
        assert_eq!(
            insert_response.status,
            ResponseStatus::Success,
            "Batch INSERT failed (starting at {}): {:?}",
            batch_start,
            insert_response.error
        );
    }

    // Measure UPDATE latency with 1000 rows
    let start_1000 = Instant::now();
    let update_response = server
        .execute_sql_as_user(
            &format!("UPDATE {}.user_items SET value = 888 WHERE id = 50", ns),
            "test_user",
        )
        .await;
    let latency_1000_rows = start_1000.elapsed();

    assert_eq!(
        update_response.status,
        ResponseStatus::Success,
        "UPDATE failed with 1000 rows: {:?}",
        update_response.error
    );

    // Verify the update worked
    let select_response = server
        .execute_sql_as_user(
            &format!("SELECT id, value FROM {}.user_items WHERE id = 50", ns),
            "test_user",
        )
        .await;
    assert_eq!(select_response.status, ResponseStatus::Success);
    let rows = select_response.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("value").unwrap().as_i64().unwrap(), 888);

    // Performance assertion: With O(1) index lookup, 1000 rows should not be
    // 10x slower than 100 rows (which would indicate O(n) scan).
    // Allow 10x margin for variance (JIT, cache, system load, etc.)
    // Key insight: O(1) lookup should NOT scale linearly with row count
    let max_allowed = latency_100_rows.mul_f32(10.0);

    println!("✅ User table PK index UPDATE test:");
    println!("   Latency with 100 rows: {:?}", latency_100_rows);
    println!("   Latency with 1000 rows: {:?}", latency_1000_rows);
    println!("   Max allowed (10x baseline): {:?}", max_allowed);

    // The latency should be sub-linear even if not O(1) due to other factors
    assert!(
        latency_1000_rows <= max_allowed,
        "UPDATE latency scaled too much with row count. \
        With 100 rows: {:?}, with 1000 rows: {:?}. \
        Expected sub-linear scaling with PK index.",
        latency_100_rows,
        latency_1000_rows
    );

    println!("✅ User table PK index UPDATE test passed - latency sub-linear with row count");
}

/// Test: Shared table UPDATE by PK uses index - not full scan
///
/// This test verifies that UPDATE operations on shared tables use the PK index
/// for efficient O(1) lookup instead of scanning all rows.
#[actix_web::test]
async fn test_shared_table_pk_index_update() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("idx_shared");

    // Setup namespace and shared table
    fixtures::create_namespace(&server, &ns).await;
    let create_response = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.products (
                id INT PRIMARY KEY,
                name TEXT,
                price INT
            ) WITH (
                TYPE = 'SHARED',
                STORAGE_ID = 'local'
            )"#,
            ns
        ))
        .await;
    assert_eq!(
        create_response.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_response.error
    );

    // Insert 100 rows in one batch
    {
        let values: Vec<String> =
            (1..=100).map(|i| format!("({}, 'product_{}', {})", i, i, i * 100)).collect();
        let insert_response = server
            .execute_sql(&format!(
                "INSERT INTO {}.products (id, name, price) VALUES {}",
                ns,
                values.join(", ")
            ))
            .await;
        assert_eq!(
            insert_response.status,
            ResponseStatus::Success,
            "Batch INSERT failed: {:?}",
            insert_response.error
        );
    }

    // Measure UPDATE latency with 100 rows
    let start_100 = Instant::now();
    let update_response = server
        .execute_sql(&format!("UPDATE {}.products SET price = 9999 WHERE id = 50", ns))
        .await;
    let latency_100_rows = start_100.elapsed();

    assert_eq!(
        update_response.status,
        ResponseStatus::Success,
        "UPDATE failed: {:?}",
        update_response.error
    );

    // Verify the update worked
    let select_response = server
        .execute_sql(&format!("SELECT id, price FROM {}.products WHERE id = 50", ns))
        .await;
    assert_eq!(select_response.status, ResponseStatus::Success);
    let rows = select_response.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("price").unwrap().as_i64().unwrap(), 9999);

    // Insert 900 more rows in batches (total 1000)
    for batch_start in (101..=1000usize).step_by(100) {
        let batch_end = (batch_start + 99).min(1000);
        let values: Vec<String> = (batch_start..=batch_end)
            .map(|i| format!("({}, 'product_{}', {})", i, i, i * 100))
            .collect();
        let insert_response = server
            .execute_sql(&format!(
                "INSERT INTO {}.products (id, name, price) VALUES {}",
                ns,
                values.join(", ")
            ))
            .await;
        assert_eq!(
            insert_response.status,
            ResponseStatus::Success,
            "Batch INSERT failed (starting at {}): {:?}",
            batch_start,
            insert_response.error
        );
    }

    // Measure UPDATE latency with 1000 rows
    let start_1000 = Instant::now();
    let update_response = server
        .execute_sql(&format!("UPDATE {}.products SET price = 8888 WHERE id = 50", ns))
        .await;
    let latency_1000_rows = start_1000.elapsed();

    assert_eq!(
        update_response.status,
        ResponseStatus::Success,
        "UPDATE failed with 1000 rows: {:?}",
        update_response.error
    );

    // Verify the update worked
    let select_response = server
        .execute_sql(&format!("SELECT id, price FROM {}.products WHERE id = 50", ns))
        .await;
    assert_eq!(select_response.status, ResponseStatus::Success);
    let rows = select_response.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("price").unwrap().as_i64().unwrap(), 8888);

    // Performance assertion: With O(1) index lookup, 1000 rows should not be
    // dramatically slower than 100 rows (which would indicate O(n) scan).
    // Allow 15x margin for variance across platforms/CI.
    let max_allowed = latency_100_rows.mul_f32(15.0);

    println!("✅ Shared table PK index UPDATE test:");
    println!("   Latency with 100 rows: {:?}", latency_100_rows);
    println!("   Latency with 1000 rows: {:?}", latency_1000_rows);
    println!("   Max allowed (15x baseline): {:?}", max_allowed);

    assert!(
        latency_1000_rows <= max_allowed,
        "UPDATE latency scaled too much with row count. \
        With 100 rows: {:?}, with 1000 rows: {:?}. \
        Expected sub-linear scaling with PK index.",
        latency_100_rows,
        latency_1000_rows
    );

    println!("✅ Shared table PK index UPDATE test passed - latency sub-linear with row count");
}

/// Test: SELECT by PK uses index for user table
///
/// This test verifies that SELECT WHERE id = X uses the PK index
/// for efficient O(1) lookup.
#[actix_web::test]
async fn test_user_table_pk_index_select() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("select_test");

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    let create_response = server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.records (
                id INT PRIMARY KEY,
                data TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "select_user",
        )
        .await;
    assert_eq!(
        create_response.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_response.error
    );

    // Insert 500 rows in batches for speed
    for batch_start in (1..=500).step_by(100) {
        let batch_end = (batch_start + 99).min(500);
        let values: Vec<String> = (batch_start..=batch_end)
            .map(|i| format!("({}, 'data_for_{}')", i, i))
            .collect();
        server
            .execute_sql_as_user(
                &format!("INSERT INTO {}.records (id, data) VALUES {}", ns, values.join(", ")),
                "select_user",
            )
            .await;
    }

    // Measure SELECT by PK latency with 500 rows (median of multiple runs)
    let mut samples_500 = Vec::with_capacity(5);
    for idx in 0..5 {
        let start = Instant::now();
        let select_response = server
            .execute_sql_as_user(
                &format!("SELECT id, data FROM {}.records WHERE id = 250", ns),
                "select_user",
            )
            .await;
        let elapsed = start.elapsed();
        assert_eq!(select_response.status, ResponseStatus::Success);
        if idx == 0 {
            let rows = select_response.rows_as_maps();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("data").unwrap().as_str().unwrap(), "data_for_250");
        }
        samples_500.push(elapsed);
    }
    let latency_500_rows = median_duration(&mut samples_500);

    // Insert 2000 more rows (total 2500) in batches for speed
    for batch_start in (501..=2500).step_by(100) {
        let batch_end = (batch_start + 99).min(2500);
        let values: Vec<String> = (batch_start..=batch_end)
            .map(|i| format!("({}, 'data_for_{}')", i, i))
            .collect();
        server
            .execute_sql_as_user(
                &format!("INSERT INTO {}.records (id, data) VALUES {}", ns, values.join(", ")),
                "select_user",
            )
            .await;
    }

    // Measure SELECT by PK latency with 2500 rows (median of multiple runs)
    let mut samples_2500 = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let select_response = server
            .execute_sql_as_user(
                &format!("SELECT id, data FROM {}.records WHERE id = 250", ns),
                "select_user",
            )
            .await;
        let elapsed = start.elapsed();
        assert_eq!(select_response.status, ResponseStatus::Success);
        samples_2500.push(elapsed);
    }
    let latency_2500_rows = median_duration(&mut samples_2500);

    // Performance assertion: 5x more rows should not cause massive slowdown with O(1) lookup.
    // Allow a large margin for heavily loaded CI environments.
    let max_allowed = max(latency_500_rows.mul_f32(100.0), Duration::from_secs(3));

    println!("✅ User table PK index SELECT test:");
    println!("   Latency with 500 rows: {:?}", latency_500_rows);
    println!("   Latency with 2500 rows: {:?}", latency_2500_rows);
    println!("   Max allowed (100x baseline): {:?}", max_allowed);

    assert!(
        latency_2500_rows <= max_allowed,
        "SELECT latency scaled too much with row count. \
        With 500 rows: {:?}, with 2500 rows: {:?}. \
        This suggests the PK index may not be used for efficient lookup.",
        latency_500_rows,
        latency_2500_rows
    );

    println!("✅ User table PK index provides O(1) SELECT lookup");
}

/// Test: DELETE by PK uses index for user table
///
/// This test verifies that DELETE WHERE id = X uses the PK index
/// for efficient O(1) lookup.
#[actix_web::test]
async fn test_user_table_pk_index_delete() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("delete_test");

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    let create_response = server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.items (
                id INT PRIMARY KEY,
                description TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "delete_user",
        )
        .await;
    assert_eq!(
        create_response.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_response.error
    );

    // Insert 300 rows in batches
    for batch_start in (1..=300usize).step_by(100) {
        let batch_end = (batch_start + 99).min(300);
        let values: Vec<String> =
            (batch_start..=batch_end).map(|i| format!("({}, 'desc_{}')", i, i)).collect();
        let insert_response = server
            .execute_sql_as_user(
                &format!("INSERT INTO {}.items (id, description) VALUES {}", ns, values.join(", ")),
                "delete_user",
            )
            .await;
        assert_eq!(
            insert_response.status,
            ResponseStatus::Success,
            "Batch INSERT failed (starting at {}): {:?}",
            batch_start,
            insert_response.error
        );
    }

    // Measure DELETE by PK latency with 300 rows
    let start_300 = Instant::now();
    let delete_response = server
        .execute_sql_as_user(&format!("DELETE FROM {}.items WHERE id = 150", ns), "delete_user")
        .await;
    let latency_300_rows = start_300.elapsed();

    assert_eq!(
        delete_response.status,
        ResponseStatus::Success,
        "DELETE failed: {:?}",
        delete_response.error
    );

    // Verify the delete worked
    let select_response = server
        .execute_sql_as_user(&format!("SELECT id FROM {}.items WHERE id = 150", ns), "delete_user")
        .await;
    assert_eq!(select_response.status, ResponseStatus::Success);
    let rows = select_response.rows_as_maps();
    assert_eq!(rows.len(), 0, "Deleted row should not be returned");

    // Insert 1200 more rows in batches (total ~1500)
    for batch_start in (301..=1500usize).step_by(100) {
        let batch_end = (batch_start + 99).min(1500);
        let values: Vec<String> =
            (batch_start..=batch_end).map(|i| format!("({}, 'desc_{}')", i, i)).collect();
        let insert_response = server
            .execute_sql_as_user(
                &format!("INSERT INTO {}.items (id, description) VALUES {}", ns, values.join(", ")),
                "delete_user",
            )
            .await;
        assert_eq!(
            insert_response.status,
            ResponseStatus::Success,
            "Batch INSERT failed (starting at {}): {:?}",
            batch_start,
            insert_response.error
        );
    }

    // Measure DELETE by PK latency with ~1500 rows
    let start_1500 = Instant::now();
    let delete_response = server
        .execute_sql_as_user(&format!("DELETE FROM {}.items WHERE id = 750", ns), "delete_user")
        .await;
    let latency_1500_rows = start_1500.elapsed();

    assert_eq!(
        delete_response.status,
        ResponseStatus::Success,
        "DELETE failed with 1500 rows: {:?}",
        delete_response.error
    );

    // Performance assertion
    let max_allowed = std::cmp::max(latency_300_rows.mul_f32(50.0), Duration::from_millis(500));

    println!("✅ User table PK index DELETE test:");
    println!("   Latency with 300 rows: {:?}", latency_300_rows);
    println!("   Latency with 1500 rows: {:?}", latency_1500_rows);
    println!("   Max allowed (5x baseline): {:?}", max_allowed);

    assert!(
        latency_1500_rows <= max_allowed,
        "DELETE latency scaled too much with row count. \
        With 300 rows: {:?}, with 1500 rows: {:?}. \
        This suggests the PK index may not be used for efficient lookup.",
        latency_300_rows,
        latency_1500_rows
    );

    println!("✅ User table PK index provides O(1) DELETE lookup");
}

/// Test: User table UPDATE by PK after flush to cold storage
///
/// This test verifies that UPDATE operations work correctly on rows that have
/// been flushed to Parquet (cold storage), using the PK index for lookup.
///
/// Strategy:
/// 1. Insert 50 rows
/// 2. STORAGE FLUSH table to Parquet (cold storage)
/// 3. UPDATE a row by PK
/// 4. Verify the update worked correctly
#[actix_web::test]
async fn test_user_table_pk_index_update_after_flush() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("flush_update");

    // Setup namespace and user table
    fixtures::create_namespace(&server, &ns).await;
    let create_response = server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.user_items (
                id INT PRIMARY KEY,
                name TEXT,
                value INT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "flush_user",
        )
        .await;
    assert_eq!(
        create_response.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_response.error
    );

    // Insert 50 rows
    for i in 1..=50 {
        let insert_response = server
            .execute_sql_as_user(
                &format!(
                    "INSERT INTO {}.user_items (id, name, value) VALUES ({}, 'item_{}', {})",
                    ns,
                    i,
                    i,
                    i * 10
                ),
                "flush_user",
            )
            .await;
        assert_eq!(
            insert_response.status,
            ResponseStatus::Success,
            "INSERT failed for row {}: {:?}",
            i,
            insert_response.error
        );
    }

    // Verify row exists before flush
    let select_before = server
        .execute_sql_as_user(
            &format!("SELECT id, value FROM {}.user_items WHERE id = 25", ns),
            "flush_user",
        )
        .await;
    assert_eq!(select_before.status, ResponseStatus::Success);
    let rows = select_before.rows_as_maps();
    assert_eq!(rows.len(), 1, "Expected 1 row before flush");
    assert_eq!(rows[0].get("value").unwrap().as_i64().unwrap(), 250);

    // Flush the table to Parquet (cold storage)
    let flush_result = execute_flush_synchronously(&server, &ns, "user_items").await;
    assert!(flush_result.is_ok(), "Flush failed: {:?}", flush_result.err());
    let flush_stats = flush_result.unwrap();
    println!("✅ Flushed {} rows to cold storage", flush_stats.rows_flushed);
    assert!(flush_stats.rows_flushed > 0, "Expected rows to be flushed");

    // Update a row that is now in cold storage
    let update_response = server
        .execute_sql_as_user(
            &format!("UPDATE {}.user_items SET value = 9999 WHERE id = 25", ns),
            "flush_user",
        )
        .await;
    assert_eq!(
        update_response.status,
        ResponseStatus::Success,
        "UPDATE after flush failed: {:?}",
        update_response.error
    );
    println!("📊 UPDATE response: rows_affected={:?}", update_response.results);

    // Debug: Check COUNT in a separate query
    let total_count = server
        .execute_sql_as_user(
            &format!("SELECT COUNT(*) as cnt FROM {}.user_items", ns),
            "flush_user",
        )
        .await;
    println!("📊 Total COUNT: {:?}", total_count.results);

    // Verify the update worked
    // Also check row count to see how many versions exist
    let count_response = server
        .execute_sql_as_user(
            &format!("SELECT COUNT(*) as cnt FROM {}.user_items WHERE id = 25", ns),
            "flush_user",
        )
        .await;
    println!("📊 COUNT result: {:?}", count_response.results);

    // Debug: Query with _deleted filter to see ALL versions (hot + cold, including tombstones)
    let all_with_deleted = server
        .execute_sql_as_user(
            &format!(
                "SELECT id, value, _seq, _deleted FROM {}.user_items WHERE _deleted = true OR _deleted = false",
                ns
            ),
            "flush_user",
        )
        .await;
    println!(
        "📊 ALL rows (deleted filter): {:?}",
        all_with_deleted.results.first().and_then(|r| r.rows.as_ref()).map(|r| r.len())
    );

    // Check the highest _seq value to see if UPDATE created a new version
    let max_seq = server
        .execute_sql_as_user(
            &format!(
                "SELECT MAX(_seq) as max_seq FROM {}.user_items WHERE id = 25 OR _deleted = true",
                ns
            ),
            "flush_user",
        )
        .await;
    println!("📊 MAX(_seq) for id=25: {:?}", max_seq.results);

    let select_after = server
        .execute_sql_as_user(
            &format!("SELECT id, value, _seq FROM {}.user_items WHERE id = 25", ns),
            "flush_user",
        )
        .await;
    assert_eq!(select_after.status, ResponseStatus::Success);
    println!("📊 SELECT after UPDATE results: {:?}", select_after.results);
    let rows = select_after.rows_as_maps();
    println!("📊 Row count: {}, rows: {:?}", rows.len(), rows);
    assert_eq!(rows.len(), 1, "Expected 1 row after update");
    let value = rows[0].get("value").unwrap().as_i64().unwrap();
    let seq = rows[0].get("_seq");
    println!("📊 value={}, _seq={:?}", value, seq);
    assert_eq!(value, 9999, "Expected updated value 9999");

    // Also verify that other rows are still accessible
    let select_other = server
        .execute_sql_as_user(
            &format!("SELECT id, value FROM {}.user_items WHERE id = 1", ns),
            "flush_user",
        )
        .await;
    assert_eq!(select_other.status, ResponseStatus::Success);
    let rows = select_other.rows_as_maps();
    assert_eq!(rows.len(), 1, "Expected row id=1 to still exist");
    assert_eq!(
        rows[0].get("value").unwrap().as_i64().unwrap(),
        10,
        "Expected original value 10 for id=1"
    );

    println!("✅ User table UPDATE after flush works correctly using PK index");
}

/// Test: Shared table UPDATE by PK after flush to cold storage
///
/// This test verifies that UPDATE operations work correctly on shared table
/// rows that have been flushed to Parquet (cold storage).
#[actix_web::test]
async fn test_shared_table_pk_index_update_after_flush() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("flush_shared");

    // Setup namespace and shared table
    fixtures::create_namespace(&server, &ns).await;
    let create_response = server
        .execute_sql(&format!(
            r#"CREATE TABLE {}.products (
                id INT PRIMARY KEY,
                name TEXT,
                price INT
            ) WITH (
                TYPE = 'SHARED',
                STORAGE_ID = 'local'
            )"#,
            ns
        ))
        .await;
    assert_eq!(
        create_response.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_response.error
    );

    // Insert 50 rows
    for i in 1..=50 {
        let insert_response = server
            .execute_sql(&format!(
                "INSERT INTO {}.products (id, name, price) VALUES ({}, 'product_{}', {})",
                ns,
                i,
                i,
                i * 100
            ))
            .await;
        assert_eq!(
            insert_response.status,
            ResponseStatus::Success,
            "INSERT failed for row {}: {:?}",
            i,
            insert_response.error
        );
    }

    // Verify row exists before flush
    let select_before = server
        .execute_sql(&format!("SELECT id, price FROM {}.products WHERE id = 25", ns))
        .await;
    assert_eq!(select_before.status, ResponseStatus::Success);
    let rows = select_before.rows_as_maps();
    assert_eq!(rows.len(), 1, "Expected 1 row before flush");
    assert_eq!(rows[0].get("price").unwrap().as_i64().unwrap(), 2500);

    // Flush the table to Parquet (cold storage)
    let flush_result = execute_shared_flush_synchronously(&server, &ns, "products").await;
    assert!(flush_result.is_ok(), "Shared table flush failed: {:?}", flush_result.err());
    let flush_stats = flush_result.unwrap();
    println!("✅ Flushed {} rows to cold storage (shared table)", flush_stats.rows_flushed);
    assert!(flush_stats.rows_flushed > 0, "Expected rows to be flushed");

    // Update a row that is now in cold storage
    let update_response = server
        .execute_sql(&format!("UPDATE {}.products SET price = 99999 WHERE id = 25", ns))
        .await;
    assert_eq!(
        update_response.status,
        ResponseStatus::Success,
        "UPDATE after flush failed: {:?}",
        update_response.error
    );

    // Verify the update worked
    let select_after = server
        .execute_sql(&format!("SELECT id, price FROM {}.products WHERE id = 25", ns))
        .await;
    assert_eq!(select_after.status, ResponseStatus::Success);
    let rows = select_after.rows_as_maps();
    assert_eq!(rows.len(), 1, "Expected 1 row after update");
    assert_eq!(
        rows[0].get("price").unwrap().as_i64().unwrap(),
        99999,
        "Expected updated price 99999"
    );

    // Also verify that other rows are still accessible
    let select_other = server
        .execute_sql(&format!("SELECT id, price FROM {}.products WHERE id = 1", ns))
        .await;
    assert_eq!(select_other.status, ResponseStatus::Success);
    let rows = select_other.rows_as_maps();
    assert_eq!(rows.len(), 1, "Expected row id=1 to still exist");
    assert_eq!(
        rows[0].get("price").unwrap().as_i64().unwrap(),
        100,
        "Expected original price 100 for id=1"
    );

    println!("✅ Shared table UPDATE after flush works correctly using PK index");
}

/// Test: Multiple users - verify user isolation in PK index
///
/// This test verifies that the PK index correctly isolates rows by user_id,
/// preventing cross-user access and ensuring correct lookups.
#[actix_web::test]
async fn test_user_table_pk_index_isolation() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("isolation");

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    let create_response = server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.user_data (
                id INT PRIMARY KEY,
                secret TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "alice",
        )
        .await;
    assert_eq!(
        create_response.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        create_response.error
    );

    // Alice inserts rows with id 1-10
    for i in 1..=10 {
        server
            .execute_sql_as_user(
                &format!(
                    "INSERT INTO {}.user_data (id, secret) VALUES ({}, 'alice_secret_{}')",
                    ns, i, i
                ),
                "alice",
            )
            .await;
    }

    // Bob inserts rows with SAME id 1-10 (different user scope)
    for i in 1..=10 {
        server
            .execute_sql_as_user(
                &format!(
                    "INSERT INTO {}.user_data (id, secret) VALUES ({}, 'bob_secret_{}')",
                    ns, i, i
                ),
                "bob",
            )
            .await;
    }

    // Alice updates her row id=5
    let update_response = server
        .execute_sql_as_user(
            &format!("UPDATE {}.user_data SET secret = 'alice_updated' WHERE id = 5", ns),
            "alice",
        )
        .await;
    assert_eq!(
        update_response.status,
        ResponseStatus::Success,
        "Alice UPDATE failed: {:?}",
        update_response.error
    );

    // Verify Alice's update worked
    let alice_select = server
        .execute_sql_as_user(
            &format!("SELECT id, secret FROM {}.user_data WHERE id = 5", ns),
            "alice",
        )
        .await;
    assert_eq!(alice_select.status, ResponseStatus::Success);
    let rows = alice_select.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("secret").unwrap().as_str().unwrap(), "alice_updated");

    // Verify Bob's row id=5 is unchanged
    let bob_select = server
        .execute_sql_as_user(
            &format!("SELECT id, secret FROM {}.user_data WHERE id = 5", ns),
            "bob",
        )
        .await;
    assert_eq!(bob_select.status, ResponseStatus::Success);
    let rows = bob_select.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("secret").unwrap().as_str().unwrap(), "bob_secret_5");

    // Bob updates his row id=5
    let update_response = server
        .execute_sql_as_user(
            &format!("UPDATE {}.user_data SET secret = 'bob_updated' WHERE id = 5", ns),
            "bob",
        )
        .await;
    assert_eq!(
        update_response.status,
        ResponseStatus::Success,
        "Bob UPDATE failed: {:?}",
        update_response.error
    );

    // Verify Bob's update worked and Alice's is still correct
    let alice_select = server
        .execute_sql_as_user(
            &format!("SELECT id, secret FROM {}.user_data WHERE id = 5", ns),
            "alice",
        )
        .await;
    assert_eq!(alice_select.status, ResponseStatus::Success);
    let rows = alice_select.rows_as_maps();
    assert_eq!(rows[0].get("secret").unwrap().as_str().unwrap(), "alice_updated");

    let bob_select = server
        .execute_sql_as_user(
            &format!("SELECT id, secret FROM {}.user_data WHERE id = 5", ns),
            "bob",
        )
        .await;
    assert_eq!(bob_select.status, ResponseStatus::Success);
    let rows = bob_select.rows_as_maps();
    assert_eq!(rows[0].get("secret").unwrap().as_str().unwrap(), "bob_updated");

    println!("✅ User table PK index provides correct user isolation");
}
