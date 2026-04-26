#![allow(unused_imports, dead_code)]

use std::time::Duration;

// (apply_patch sanity check)
use serde_json::Value;

use crate::{common, common::*};

#[test]
fn test_datatypes_json_preservation() {
    if !common::is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = common::generate_unique_table("datatypes_test");
    let namespace = common::generate_unique_namespace("test_datatypes");

    // Create namespace first
    let _ = common::execute_sql_as_root_via_cli(&format!(
        "CREATE NAMESPACE IF NOT EXISTS {}",
        namespace
    ));

    // Create test table with multiple datatypes
    let create_sql = format!(
        r#"CREATE TABLE {}.{} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            col_string VARCHAR,
            col_int INT,
            col_float FLOAT,
            col_bool BOOLEAN,
            col_timestamp TIMESTAMP
        ) WITH (TYPE = 'USER')"#,
        namespace, table_name
    );

    let result = common::execute_sql_as_root_via_cli(&create_sql);
    assert!(result.is_ok(), "Should create table successfully: {:?}", result.err());

    // Insert test data
    // Note: Using a fixed timestamp for easier verification
    let timestamp_str = "2023-01-01 12:00:00";
    let insert_sql = format!(
        "INSERT INTO {}.{} (col_string, col_int, col_float, col_bool, col_timestamp) VALUES \
         ('test_string', 123, 45.67, true, '{}')",
        namespace, table_name, timestamp_str
    );
    let result = common::execute_sql_as_root_via_cli(&insert_sql);
    assert!(result.is_ok(), "Should insert data successfully: {:?}", result.err());

    // Query the data using JSON output format
    let select_sql = format!("SELECT * FROM {}.{}", namespace, table_name);
    let output = common::wait_for_query_contains_with(
        &select_sql,
        "test_string",
        Duration::from_secs(5),
        common::execute_sql_as_root_via_cli_json,
    )
    .expect("Should query data successfully");
    println!("Query output: {}", output);

    // Parse JSON output
    let json: Value = parse_cli_json_output(&output).expect("Failed to parse JSON output");

    // Navigate to results[0].rows[0] using the new schema-based format
    let rows = get_rows_as_hashmaps(&json).expect("Failed to get rows from JSON response");

    assert!(!rows.is_empty(), "Should return at least one row");
    let row = rows.first().expect("Should have a first row");

    // Verify values and types (using Arrow JSON format extraction)

    // String
    let col_string = row.get("col_string").expect("Missing col_string");
    let col_string = extract_arrow_value(col_string).expect("Failed to extract col_string");
    assert!(col_string.is_string(), "col_string should be a string");
    assert_eq!(col_string.as_str().unwrap(), "test_string");

    // Int
    let col_int = row.get("col_int").expect("Missing col_int");
    let col_int = extract_arrow_value(col_int).expect("Failed to extract col_int");
    assert!(col_int.is_number(), "col_int should be a number");
    assert_eq!(col_int.as_i64().unwrap(), 123);

    // Float
    let col_float = row.get("col_float").expect("Missing col_float");
    let col_float = extract_arrow_value(col_float).expect("Failed to extract col_float");
    assert!(col_float.is_number(), "col_float should be a number");
    // FLOAT is typically stored as 32-bit and may not round-trip exactly through JSON.
    let actual_float = col_float.as_f64().unwrap();
    let expected_float = 45.67_f64;
    assert!(
        (actual_float - expected_float).abs() < 1e-4,
        "col_float should be approximately {} but was {}",
        expected_float,
        actual_float
    );

    // Boolean
    let col_bool = row.get("col_bool").expect("Missing col_bool");
    let col_bool = extract_arrow_value(col_bool).expect("Failed to extract col_bool");
    assert!(col_bool.is_boolean(), "col_bool should be a boolean");
    assert!(col_bool.as_bool().unwrap());

    // Timestamp
    // Timestamp is returned as Arrow JSON with TimestampMicrosecond type
    let col_timestamp = row.get("col_timestamp").expect("Missing col_timestamp");
    let col_timestamp =
        extract_arrow_value(col_timestamp).expect("Failed to extract col_timestamp");

    // Timestamp is returned as an object with timezone and value fields
    if let Some(obj) = col_timestamp.as_object() {
        let value = obj.get("value").expect("Timestamp should have value field");
        let ts_micros = value.as_i64().expect("Timestamp value should be i64");
        // Expected: 2023-01-01 12:00:00 UTC = 1672574400 seconds = 1672574400000000 microseconds
        assert!(
            ts_micros > 1672574000000000 && ts_micros < 1672575000000000,
            "Timestamp should be around 2023-01-01 12:00:00 UTC, got: {}",
            ts_micros
        );
    } else if col_timestamp.is_string() {
        // Fallback: string representation
        let ts_val = col_timestamp.as_str().unwrap();
        assert!(ts_val.contains("2023-01-01"), "Timestamp should contain date part");
    } else if col_timestamp.is_number() {
        // Fallback: numeric epoch
        let ts_micros = col_timestamp.as_i64().expect("Timestamp should be i64");
        assert!(
            ts_micros > 1672574000000000 && ts_micros < 1672575000000000,
            "Timestamp should be around 2023-01-01 12:00:00 UTC"
        );
    } else {
        panic!("Unexpected type for col_timestamp: {:?}", col_timestamp);
    }

    // Cleanup
    let drop_sql = format!("DROP TABLE IF EXISTS {}.{}", namespace, table_name);
    let _ = common::execute_sql_as_root_via_cli(&drop_sql);
    let _ = common::execute_sql_as_root_via_cli(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace
    ));
}
