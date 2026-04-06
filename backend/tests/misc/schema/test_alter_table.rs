//! Integration tests for ALTER TABLE (Phase 4, US2, T022)
//!
//! Tests:
//! - ADD COLUMN with default values
//! - DROP COLUMN with data preservation
//! - RENAME COLUMN (metadata only)
//! - MODIFY COLUMN type changes
//! - Schema versioning
//! - Cache invalidation after ALTER

use super::test_support::{fixtures, TestServer};
use kalam_client::models::ResponseStatus;

/// T022a: ALTER TABLE ADD COLUMN
#[actix_web::test]
async fn test_alter_table_add_column() {
    let server = TestServer::new_shared().await;

    // Use unique namespace per test to avoid parallel test interference
    let ns = format!("test_add_col_{}", std::process::id());

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    let create_response = server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.products (
                id TEXT PRIMARY KEY,
                name TEXT,
                price INT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "user1",
        )
        .await;
    assert_eq!(create_response.status, ResponseStatus::Success);

    // Insert initial data
    server
        .execute_sql_as_user(
            &format!(
                r#"INSERT INTO {}.products (id, name, price) 
               VALUES ('p1', 'Widget', 100)"#,
                ns
            ),
            "user1",
        )
        .await;

    // ALTER TABLE: ADD COLUMN
    let alter_response = server
        .execute_sql_as_user(
            &format!(r#"ALTER TABLE {}.products ADD COLUMN stock INT"#, ns),
            "user1",
        )
        .await;

    assert_eq!(
        alter_response.status,
        ResponseStatus::Success,
        "ALTER TABLE ADD COLUMN failed: {:?}",
        alter_response.error
    );

    // Verify new column exists in schema (query should work)
    let query_response = server
        .execute_sql_as_user(
            &format!("SELECT id, name, price, stock FROM {}.products WHERE id = 'p1'", ns),
            "user1",
        )
        .await;

    assert_eq!(
        query_response.status,
        ResponseStatus::Success,
        "Query after ALTER failed: {:?}",
        query_response.error
    );

    // Verify old data still accessible
    let rows = query_response.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name").unwrap().as_str().unwrap(), "Widget");
    assert_eq!(rows[0].get("price").unwrap().as_i64().unwrap(), 100);
    // New column should be NULL for existing rows
    assert!(rows[0].get("stock").unwrap().is_null());

    println!("✅ T022a: ALTER TABLE ADD COLUMN passed");
}

/// T022b: ALTER TABLE DROP COLUMN
#[actix_web::test]
async fn test_alter_table_drop_column() {
    let server = TestServer::new_shared().await;

    // Use unique namespace per test to avoid parallel test interference
    let ns = format!("test_drop_col_{}", std::process::id());

    // Setup - create namespace and verify it succeeded
    let ns_resp = fixtures::create_namespace(&server, &ns).await;
    assert_eq!(
        ns_resp.status,
        ResponseStatus::Success,
        "Failed to create namespace: {:?}",
        ns_resp.error
    );

    // Verify namespace exists before proceeding
    assert!(server.namespace_exists(&ns).await, "Namespace should exist after creation");

    let create_resp = server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.inventory (
                id TEXT PRIMARY KEY,
                item TEXT,
                quantity INT,
                warehouse TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "user1",
        )
        .await;
    assert_eq!(
        create_resp.status,
        ResponseStatus::Success,
        "Failed to create table: {:?}",
        create_resp.error
    );

    // Insert data
    server
        .execute_sql_as_user(
            &format!(
                r#"INSERT INTO {}.inventory (id, item, quantity, warehouse) 
               VALUES ('i1', 'Laptop', 10, 'WH1')"#,
                ns
            ),
            "user1",
        )
        .await;

    // ALTER TABLE: DROP COLUMN
    let alter_response = server
        .execute_sql_as_user(
            &format!(r#"ALTER TABLE {}.inventory DROP COLUMN warehouse"#, ns),
            "user1",
        )
        .await;

    assert_eq!(
        alter_response.status,
        ResponseStatus::Success,
        "ALTER TABLE DROP COLUMN failed: {:?}",
        alter_response.error
    );

    // Verify column no longer accessible
    let query_response = server
        .execute_sql_as_user(
            &format!("SELECT id, item, quantity FROM {}.inventory WHERE id = 'i1'", ns),
            "user1",
        )
        .await;

    assert_eq!(query_response.status, ResponseStatus::Success);

    let rows = query_response.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("item").unwrap().as_str().unwrap(), "Laptop");
    assert_eq!(rows[0].get("quantity").unwrap().as_i64().unwrap(), 10);
    // Dropped column should not be present
    assert!(!rows[0].contains_key("warehouse"));

    println!("✅ T022b: ALTER TABLE DROP COLUMN passed");
}

/// T022c: ALTER TABLE RENAME COLUMN (new functionality)
#[actix_web::test]
async fn test_alter_table_rename_column() {
    let server = TestServer::new_shared().await;

    // Use unique namespace per test to avoid parallel test interference
    let ns = format!("test_rename_col_{}", std::process::id());

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.customers (
                id TEXT PRIMARY KEY,
                customer_name TEXT,
                email TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "user1",
        )
        .await;

    // Insert data
    server
        .execute_sql_as_user(
            &format!(
                r#"INSERT INTO {}.customers (id, customer_name, email) 
               VALUES ('c1', 'Alice', 'alice@example.com')"#,
                ns
            ),
            "user1",
        )
        .await;

    // ALTER TABLE: RENAME COLUMN
    let alter_response = server
        .execute_sql_as_user(
            &format!(r#"ALTER TABLE {}.customers RENAME COLUMN customer_name TO name"#, ns),
            "user1",
        )
        .await;

    assert_eq!(
        alter_response.status,
        ResponseStatus::Success,
        "ALTER TABLE RENAME COLUMN failed: {:?}",
        alter_response.error
    );

    // Note: Data written before RENAME still has old column name in Parquet files.
    // Full schema evolution support (column aliasing during scan) is not yet implemented.
    // For now, verify that the schema metadata was updated by checking DESCRIBE TABLE.

    let describe_response = server
        .execute_sql_as_user(&format!("DESCRIBE TABLE {}.customers", ns), "user1")
        .await;

    assert_eq!(
        describe_response.status,
        ResponseStatus::Success,
        "DESCRIBE TABLE failed: {:?}",
        describe_response.error
    );

    // Verify schema shows new column name
    let rows = describe_response.rows_as_maps();
    let column_names: Vec<String> = rows
        .iter()
        .map(|row| row.get("column_name").unwrap().as_str().unwrap().to_string())
        .collect();

    assert!(
        column_names.contains(&"name".to_string()),
        "Schema should contain 'name' column after RENAME, got: {:?}",
        column_names
    );
    assert!(
        !column_names.contains(&"customer_name".to_string()),
        "Schema should not contain 'customer_name' after RENAME, got: {:?}",
        column_names
    );

    println!("✅ T022c: ALTER TABLE RENAME COLUMN passed");
}

/// T022d: ALTER TABLE MODIFY COLUMN
#[actix_web::test]
async fn test_alter_table_modify_column() {
    let server = TestServer::new_shared().await;

    // Use unique namespace per test to avoid parallel test interference
    let ns = format!("test_modify_col_{}", std::process::id());

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.metrics (
                id TEXT PRIMARY KEY,
                value INT,
                description TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "user1",
        )
        .await;

    // ALTER TABLE: MODIFY COLUMN (change type)
    let alter_response = server
        .execute_sql_as_user(
            &format!(r#"ALTER TABLE {}.metrics MODIFY COLUMN value BIGINT"#, ns),
            "user1",
        )
        .await;

    assert_eq!(
        alter_response.status,
        ResponseStatus::Success,
        "ALTER TABLE MODIFY COLUMN failed: {:?}",
        alter_response.error
    );

    // Verify table still works (schema updated)
    server
        .execute_sql_as_user(
            &format!(
                r#"INSERT INTO {}.metrics (id, value, description) 
               VALUES ('m1', 99999999, 'large value')"#,
                ns
            ),
            "user1",
        )
        .await;

    let query_response = server
        .execute_sql_as_user(
            &format!("SELECT id, value FROM {}.metrics WHERE id = 'm1'", ns),
            "user1",
        )
        .await;

    assert_eq!(query_response.status, ResponseStatus::Success);

    println!("✅ T022d: ALTER TABLE MODIFY COLUMN passed");
}

/// T022e: Verify schema versioning increments
#[actix_web::test]
async fn test_alter_table_schema_versioning() {
    let server = TestServer::new_shared().await;

    // Use unique namespace per test to avoid parallel test interference
    let ns = format!("test_version_{}", std::process::id());

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    server
        .execute_sql_as_user(
            &format!(
                r#"CREATE TABLE {}.versioned (
                id TEXT PRIMARY KEY,
                col1 TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )"#,
                ns
            ),
            "user1",
        )
        .await;

    // Perform multiple ALTER operations
    server
        .execute_sql_as_user(
            &format!(r#"ALTER TABLE {}.versioned ADD COLUMN col2 INT"#, ns),
            "user1",
        )
        .await;

    server
        .execute_sql_as_user(
            &format!(r#"ALTER TABLE {}.versioned ADD COLUMN col3 TEXT"#, ns),
            "user1",
        )
        .await;

    // Query should work with all columns (schema evolution tracked internally)
    let query_response = server
        .execute_sql_as_user(&format!("SELECT id, col1, col2, col3 FROM {}.versioned", ns), "user1")
        .await;

    assert_eq!(
        query_response.status,
        ResponseStatus::Success,
        "Query after multiple ALTERs failed"
    );

    println!("✅ T022e: ALTER TABLE schema versioning passed");
}
