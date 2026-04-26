mod common;
use std::time::Duration;

use common::*;

async fn execute_sql(sql: &str) -> Result<String, String> {
    execute_sql_as_root_via_client(sql).map_err(|e| e.to_string())
}

#[tokio::test]
async fn repro_duplicate_column_error() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("repro_dupe_ns");
    let table = generate_unique_table("dupe_table");
    let full_table = format!("{}.{}", namespace, table);

    // Setup
    let _ = execute_sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace)).await;
    execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await.unwrap();

    execute_sql(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='USER')",
        full_table
    ))
    .await
    .unwrap();

    // Add COL1
    println!("Adding COL1...");
    let res1 = execute_sql(&format!("ALTER TABLE {} ADD COLUMN COL1 TEXT", full_table)).await;
    println!("Add COL1 result: {:?}", res1);
    assert!(res1.is_ok(), "First ADD COLUMN should succeed");

    // Sleep to ensure propagation locally if needed
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Add COL1 again - should fail
    println!("Adding COL1 again...");
    let res2 = execute_sql(&format!("ALTER TABLE {} ADD COLUMN COL1 TEXT", full_table)).await;
    println!("Add COL1 again result: {:?}", res2);

    // We expect this to FAIL
    if res2.is_ok() {
        println!("❌ BUG REPRODUCED: Second ADD COLUMN succeeded but should have failed.");

        // Inspect schema
        let select = execute_sql(&format!("SELECT * FROM {}", full_table)).await;
        println!("Select result after adding duplicate: {:?}", select);

        panic!("Duplicate column addition was allowed!");
    } else {
        println!("✅ Check passed: Duplicate column addition failed as expected.");
    }
}
