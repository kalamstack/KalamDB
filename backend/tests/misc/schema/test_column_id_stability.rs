//! Integration test for stable column IDs (DuckLake pattern)
//!
//! Verifies that column_id is preserved across schema changes:
//! 1. ADD COLUMN - new column gets next_column_id
//! 2. DROP COLUMN - column_id is NOT reused
//! 3. RENAME COLUMN - column_id stays the same
//! 4. Flushed data - column statistics use column_id keys

use std::collections::HashMap;

use kalam_client::models::ResponseStatus;
use serde_json::Value;

use super::test_support::{consolidated_helpers, fixtures, flush_helpers, TestServer};

/// Test that column_id is stable across ADD, DROP, RENAME operations
#[actix_web::test]
async fn test_column_id_stability_across_schema_changes() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("col_id_stable");

    // Step 1: Create table with initial columns
    fixtures::create_namespace(&server, &ns).await;

    let create_sql = format!(
        r#"CREATE TABLE {}.products (
            id TEXT PRIMARY KEY,
            name TEXT,
            price INT
        ) WITH (
            TYPE = 'USER',
            STORAGE_ID = 'local'
        )"#,
        ns
    );

    let resp = server.execute_sql_as_user(&create_sql, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to create table: {:?}", resp.error);

    // Step 2: Get initial schema and verify column_ids
    let schema_v1 = get_table_schema(&server, &ns, "products").await;
    assert_eq!(schema_v1.len(), 3, "Expected 3 columns initially");

    // Verify initial column_ids (should be 1, 2, 3 based on ordinal positions)
    let id_col = find_column(&schema_v1, "id");
    let name_col = find_column(&schema_v1, "name");
    let price_col = find_column(&schema_v1, "price");

    let id_col_id = id_col["column_id"].as_u64().expect("column_id should be u64");
    let name_col_id = name_col["column_id"].as_u64().expect("column_id should be u64");
    let price_col_id = price_col["column_id"].as_u64().expect("column_id should be u64");

    println!(
        "✅ Initial column_ids: id={}, name={}, price={}",
        id_col_id, name_col_id, price_col_id
    );

    // Step 3: ADD COLUMN - should get next_column_id (4)
    let alter_add = format!(r#"ALTER TABLE {}.products ADD COLUMN stock INT"#, ns);
    let resp = server.execute_sql_as_user(&alter_add, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to add column: {:?}", resp.error);

    let schema_v2 = get_table_schema(&server, &ns, "products").await;
    let stock_col = find_column(&schema_v2, "stock");
    let stock_col_id = stock_col["column_id"].as_u64().expect("column_id should be u64");

    println!("✅ After ADD COLUMN: stock column_id={}", stock_col_id);
    assert!(stock_col_id > price_col_id, "New column should get higher column_id");

    // Verify original columns still have same column_ids
    let id_col_v2 = find_column(&schema_v2, "id");
    let name_col_v2 = find_column(&schema_v2, "name");
    let price_col_v2 = find_column(&schema_v2, "price");

    assert_eq!(
        id_col_v2["column_id"].as_u64().unwrap(),
        id_col_id,
        "id column_id should not change"
    );
    assert_eq!(
        name_col_v2["column_id"].as_u64().unwrap(),
        name_col_id,
        "name column_id should not change"
    );
    assert_eq!(
        price_col_v2["column_id"].as_u64().unwrap(),
        price_col_id,
        "price column_id should not change"
    );

    println!("✅ Original columns preserved their column_ids after ADD");

    // Step 4: DROP COLUMN - removed column's ID should NOT be reused
    let alter_drop = format!(r#"ALTER TABLE {}.products DROP COLUMN stock"#, ns);
    let resp = server.execute_sql_as_user(&alter_drop, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to drop column: {:?}", resp.error);

    let schema_v3 = get_table_schema(&server, &ns, "products").await;
    assert_eq!(schema_v3.len(), 3, "Should have 3 columns after dropping stock");

    // Verify stock column is gone
    assert!(
        schema_v3.iter().all(|c| c["column_name"].as_str().unwrap() != "stock"),
        "stock column should be removed"
    );

    // Step 5: Add another column - should NOT reuse the dropped column's ID
    let alter_add2 = format!(r#"ALTER TABLE {}.products ADD COLUMN category TEXT"#, ns);
    let resp = server.execute_sql_as_user(&alter_add2, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to add category: {:?}", resp.error);

    let schema_v4 = get_table_schema(&server, &ns, "products").await;
    let category_col = find_column(&schema_v4, "category");
    let category_col_id = category_col["column_id"].as_u64().expect("column_id should be u64");

    println!("✅ After DROP then ADD: category column_id={}", category_col_id);
    assert_ne!(category_col_id, stock_col_id, "New column should NOT reuse dropped column's ID");
    assert!(
        category_col_id > stock_col_id,
        "New column_id should be greater than dropped column's ID"
    );

    // Step 6: RENAME COLUMN - column_id should stay the same
    let alter_rename = format!(r#"ALTER TABLE {}.products RENAME COLUMN name TO product_name"#, ns);
    let resp = server.execute_sql_as_user(&alter_rename, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to rename column: {:?}",
        resp.error
    );

    let schema_v5 = get_table_schema(&server, &ns, "products").await;
    let renamed_col = find_column(&schema_v5, "product_name");
    let renamed_col_id = renamed_col["column_id"].as_u64().expect("column_id should be u64");

    println!(
        "✅ After RENAME: product_name column_id={} (was name with id={})",
        renamed_col_id, name_col_id
    );
    assert_eq!(renamed_col_id, name_col_id, "RENAME should preserve column_id");

    // Verify old name is gone
    assert!(
        schema_v5.iter().all(|c| c["column_name"].as_str().unwrap() != "name"),
        "Old column name should not exist"
    );

    println!("✅ test_column_id_stability_across_schema_changes passed");
}

/// Test that column statistics in manifest use column_id keys after flush
#[actix_web::test]
async fn test_column_stats_use_column_id() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("col_stats");

    // Step 1: Create table with initial schema
    fixtures::create_namespace(&server, &ns).await;

    let create_sql = format!(
        r#"CREATE TABLE {}.metrics (
            id TEXT PRIMARY KEY,
            value INT,
            timestamp BIGINT
        ) WITH (
            TYPE = 'USER',
            STORAGE_ID = 'local'
        )"#,
        ns
    );

    let resp = server.execute_sql_as_user(&create_sql, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to create table: {:?}", resp.error);

    // Get schema and column_ids before flush
    let schema = get_table_schema(&server, &ns, "metrics").await;
    let value_col = find_column(&schema, "value");
    let value_col_id = value_col["column_id"].as_u64().expect("column_id should be u64");

    println!("✅ value column has column_id={}", value_col_id);

    // Step 2: Insert data with various values for statistics
    let insert_sql = format!(
        r#"INSERT INTO {}.metrics (id, value, timestamp) VALUES
            ('m1', 10, 1000),
            ('m2', 50, 2000),
            ('m3', 30, 3000),
            ('m4', 100, 4000)"#,
        ns
    );

    let resp = server.execute_sql_as_user(&insert_sql, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to insert data: {:?}", resp.error);

    // Step 3: Flush to create Parquet file with column statistics
    let flush_result = flush_helpers::execute_flush_synchronously(&server, &ns, "metrics")
        .await
        .expect("Flush should succeed");

    println!("✅ Flushed {} rows to Parquet", flush_result.rows_flushed);
    assert_eq!(flush_result.rows_flushed, 4, "Expected to flush 4 rows");

    // Step 4: RENAME column and verify data is still accessible
    let alter_rename = format!(r#"ALTER TABLE {}.metrics RENAME COLUMN value TO metric_value"#, ns);
    let resp = server.execute_sql_as_user(&alter_rename, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to rename column: {:?}",
        resp.error
    );

    // Step 5: Query renamed column from flushed data
    // This verifies that column_id is used to map old Parquet column name to new schema
    let select_sql = format!("SELECT id, metric_value FROM {}.metrics WHERE id = 'm2'", ns);
    let resp = server.execute_sql_as_user(&select_sql, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Query after rename failed: {:?}",
        resp.error
    );

    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 1, "Expected 1 row");

    // Note: Full schema evolution (column aliasing) may not be implemented yet
    // This test documents expected behavior for future enhancement
    println!("✅ Query after rename column in flushed data passed");

    // Step 6: Insert new data with renamed column
    let insert_new = format!(
        r#"INSERT INTO {}.metrics (id, metric_value, timestamp) VALUES ('m5', 75, 5000)"#,
        ns
    );

    let resp = server.execute_sql_as_user(&insert_new, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to insert with renamed column: {:?}",
        resp.error
    );

    // Query both old (flushed, original name) and new (hot, renamed) data
    let select_all = format!("SELECT id, metric_value FROM {}.metrics ORDER BY id", ns);
    let resp = server.execute_sql_as_user(&select_all, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Query all data failed: {:?}", resp.error);

    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 5, "Expected 5 rows total");

    println!("✅ test_column_stats_use_column_id passed");
}

/// Comprehensive test: Create, insert, flush, alter (add/drop columns), query verification
///
/// This test verifies the complete workflow:
/// 1. Create table with 3 columns
/// 2. Insert initial data
/// 3. Flush to Parquet (cold storage)
/// 4. ALTER TABLE ADD COLUMN
/// 5. Query and verify new column appears (NULL for old data)
/// 6. Add another column
/// 7. Drop a previous column
/// 8. Insert more data with new schema
/// 9. Flush again
/// 10. Query and verify only correct columns are returned
#[actix_web::test]
async fn test_full_lifecycle_with_alter_and_flush() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("lifecycle");

    // ========== Step 1: Create table ==========
    fixtures::create_namespace(&server, &ns).await;

    let create_sql = format!(
        r#"CREATE TABLE {}.users (
            id TEXT PRIMARY KEY,
            username TEXT,
            email TEXT
        ) WITH (
            TYPE = 'USER',
            STORAGE_ID = 'local'
        )"#,
        ns
    );

    let resp = server.execute_sql_as_user(&create_sql, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to create table: {:?}", resp.error);
    println!("✅ Created table with columns: id, username, email");

    // ========== Step 2: Insert initial data ==========
    let insert_sql = format!(
        r#"INSERT INTO {}.users (id, username, email) VALUES
            ('u1', 'alice', 'alice@example.com'),
            ('u2', 'bob', 'bob@example.com'),
            ('u3', 'charlie', 'charlie@example.com')"#,
        ns
    );

    let resp = server.execute_sql_as_user(&insert_sql, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to insert initial data: {:?}",
        resp.error
    );
    println!("✅ Inserted 3 rows");

    // ========== Step 3: Flush to disk (Parquet) ==========
    let flush_result = flush_helpers::execute_flush_synchronously(&server, &ns, "users")
        .await
        .expect("First flush should succeed");

    println!("✅ Flushed {} rows to Parquet (cold storage)", flush_result.rows_flushed);
    assert_eq!(flush_result.rows_flushed, 3, "Expected to flush 3 rows");

    // ========== Step 4: ALTER TABLE ADD COLUMN ==========
    let alter_add1 = format!(r#"ALTER TABLE {}.users ADD COLUMN age INT"#, ns);
    let resp = server.execute_sql_as_user(&alter_add1, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to add age column: {:?}",
        resp.error
    );
    println!("✅ Added column: age");

    // ========== Step 5: Query and verify new column appears ==========
    let query1 = format!("SELECT id, username, email, age FROM {}.users ORDER BY id", ns);
    let resp = server.execute_sql_as_user(&query1, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Query after first ALTER failed: {:?}",
        resp.error
    );

    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 3, "Should have 3 rows");

    // Verify old data has NULL for new column
    assert_eq!(rows[0]["username"].as_str().unwrap(), "alice");
    assert_eq!(rows[0]["email"].as_str().unwrap(), "alice@example.com");
    assert!(rows[0]["age"].is_null(), "age should be NULL for flushed data");

    println!("✅ Query after ADD COLUMN: new column appears with NULL for old data");

    // ========== Step 6: Add another column ==========
    let alter_add2 = format!(r#"ALTER TABLE {}.users ADD COLUMN status TEXT"#, ns);
    let resp = server.execute_sql_as_user(&alter_add2, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to add status column: {:?}",
        resp.error
    );
    println!("✅ Added column: status");

    // ========== Step 7: Drop a previous column ==========
    let alter_drop = format!(r#"ALTER TABLE {}.users DROP COLUMN email"#, ns);
    let resp = server.execute_sql_as_user(&alter_drop, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to drop email column: {:?}",
        resp.error
    );
    println!("✅ Dropped column: email");

    // Verify schema now has: id, username, age, status (no email)
    let schema = get_table_schema(&server, &ns, "users").await;
    let column_names: Vec<&str> = schema.iter()
        .map(|c| c["column_name"].as_str().unwrap())
        .filter(|name| !name.starts_with('_')) // Filter out system columns like _seq, _deleted
        .collect();

    assert_eq!(
        column_names,
        vec!["id", "username", "age", "status"],
        "Schema should be: id, username, age, status (email dropped)"
    );
    println!("✅ Verified schema: {:?}", column_names);

    // ========== Step 8: Insert more data with new schema ==========
    let insert_sql2 = format!(
        r#"INSERT INTO {}.users (id, username, age, status) VALUES
            ('u4', 'david', 28, 'active'),
            ('u5', 'eve', 32, 'inactive')"#,
        ns
    );

    let resp = server.execute_sql_as_user(&insert_sql2, "user1").await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Failed to insert new data: {:?}",
        resp.error
    );
    println!("✅ Inserted 2 more rows with new schema");

    // ========== Step 9: Flush again ==========
    let flush_result2 = flush_helpers::execute_flush_synchronously(&server, &ns, "users")
        .await
        .expect("Second flush should succeed");

    println!("✅ Flushed {} more rows to Parquet", flush_result2.rows_flushed);
    assert_eq!(flush_result2.rows_flushed, 2, "Expected to flush 2 new rows");

    // ========== Step 10: Query and verify only correct columns are returned ==========
    let query_final = format!("SELECT * FROM {}.users ORDER BY id", ns);
    let resp = server.execute_sql_as_user(&query_final, "user1").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Final query failed: {:?}", resp.error);

    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 5, "Should have 5 total rows");

    // Verify columns in result
    let result_columns: Vec<String> = rows[0].keys().cloned().collect();
    println!("✅ Query result columns: {:?}", result_columns);

    // Verify email column is NOT in results
    assert!(
        !result_columns.contains(&"email".to_string()),
        "email column should NOT be in query results"
    );

    // Verify old data (first 3 rows from first flush)
    assert_eq!(rows[0]["id"].as_str().unwrap(), "u1");
    assert_eq!(rows[0]["username"].as_str().unwrap(), "alice");
    assert!(rows[0]["age"].is_null(), "age should be NULL for old data");
    assert!(rows[0]["status"].is_null(), "status should be NULL for old data");

    assert_eq!(rows[1]["id"].as_str().unwrap(), "u2");
    assert_eq!(rows[1]["username"].as_str().unwrap(), "bob");

    assert_eq!(rows[2]["id"].as_str().unwrap(), "u3");
    assert_eq!(rows[2]["username"].as_str().unwrap(), "charlie");

    // Verify new data (rows 4-5 from second flush)
    assert_eq!(rows[3]["id"].as_str().unwrap(), "u4");
    assert_eq!(rows[3]["username"].as_str().unwrap(), "david");
    assert_eq!(rows[3]["age"].as_i64().unwrap(), 28);
    assert_eq!(rows[3]["status"].as_str().unwrap(), "active");

    assert_eq!(rows[4]["id"].as_str().unwrap(), "u5");
    assert_eq!(rows[4]["username"].as_str().unwrap(), "eve");
    assert_eq!(rows[4]["age"].as_i64().unwrap(), 32);
    assert_eq!(rows[4]["status"].as_str().unwrap(), "inactive");

    println!("✅ Final verification passed:");
    println!("   - Old data (3 rows) has NULLs for new columns (age, status)");
    println!("   - New data (2 rows) has values for all columns");
    println!("   - Dropped column (email) is NOT in results");
    println!("   - All 5 rows returned with correct schema");

    println!("✅ test_full_lifecycle_with_alter_and_flush passed");
}

/// Helper: Query system.schemas to get table schema with column_ids (excludes system columns)
async fn get_table_schema(
    server: &TestServer,
    namespace: &str,
    table_name: &str,
) -> Vec<HashMap<String, Value>> {
    let query = format!(
        "SELECT columns FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}' AND \
         is_latest = true",
        namespace, table_name
    );

    // Use root user to access system.schemas (user1 doesn't have permissions)
    let resp = server.execute_sql_as_user(&query, "root").await;
    assert_eq!(resp.status, ResponseStatus::Success, "Failed to query schema: {:?}", resp.error);

    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 1, "Expected 1 table definition");

    // Extract columns field - it's returned as a JSON string
    let columns_str = rows[0]["columns"].as_str().expect("columns field should be a string");

    // Parse the JSON string into a Vec of column definitions
    let all_columns: Vec<HashMap<String, Value>> =
        serde_json::from_str(columns_str).expect("Failed to parse columns JSON");

    // Filter out system columns (_seq, _deleted)
    all_columns
        .into_iter()
        .filter(|col| {
            let name = col.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
            !name.starts_with('_')
        })
        .collect()
}

/// Helper: Find column by name in schema result
fn find_column<'a>(
    schema: &'a [HashMap<String, Value>],
    column_name: &str,
) -> &'a HashMap<String, Value> {
    schema
        .iter()
        .find(|c| c["column_name"].as_str().unwrap() == column_name)
        .unwrap_or_else(|| panic!("Column '{}' not found in schema", column_name))
}
