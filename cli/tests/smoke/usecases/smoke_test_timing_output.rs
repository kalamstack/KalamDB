//! Smoke tests for CLI timing output validation
//!
//! Verifies that the CLI displays "Took: X.XXX ms" for query execution
//! and validates timing output at different table sizes.
//!
//! Reference: docs/CLI.md lines 146-152

use std::time::Duration;

use regex::Regex;

use crate::common::*;

/// Parse timing from CLI output like "Took: 1.234 ms"
/// Returns timing in milliseconds or None if not found
fn parse_timing_ms(output: &str) -> Option<f64> {
    let re = Regex::new(r"Took:\s+(\d+\.\d+)\s+ms").ok()?;
    re.captures(output)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<f64>().ok())
}

#[ntest::timeout(180_000)]
#[test]
fn smoke_test_timing_output_format() {
    if !is_server_running() {
        println!("Skipping smoke_test_timing_output_format: server not running");
        return;
    }

    let namespace = generate_unique_namespace("timing");
    let table = generate_unique_table("perf");
    let full = format!("{}.{}", namespace, table);

    // Create namespace
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace");

    // Create simple table
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, value TEXT) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:1000')",
        full
    ))
    .expect("create table");

    // Insert one row
    execute_sql_as_root_via_cli(&format!("INSERT INTO {} (id, value) VALUES (1, 'test')", full))
        .expect("insert row");

    // Execute SELECT and verify timing output
    let output =
        execute_sql_as_root_via_cli(&format!("SELECT * FROM {}", full)).expect("select query");

    // Verify timing format exists
    let timing = parse_timing_ms(&output)
        .expect("CLI output should contain 'Took: X.XXX ms' timing information");

    println!("Parsed timing: {} ms", timing);

    // Sanity check: timing should be positive and reasonable (< 5 seconds)
    assert!(timing > 0.0, "Timing should be positive");
    assert!(timing < 5000.0, "Single row SELECT should complete in < 5000ms");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(180_000)]
#[test]
fn smoke_test_timing_scaling_small_table() {
    if !is_server_running() {
        println!("Skipping smoke_test_timing_scaling_small_table: server not running");
        return;
    }

    let namespace = generate_unique_namespace("timing");
    let table = generate_unique_table("small");
    let full = format!("{}.{}", namespace, table);

    // Create namespace
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace");

    // Create table
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, data TEXT) WITH (TYPE='USER')",
        full
    ))
    .expect("create table");

    // Insert 10 rows
    for i in 1..=10 {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, data) VALUES ({}, 'row_{}')",
            full, i, i
        ))
        .expect("insert row");
    }

    // Query all rows and parse timing
    let output = execute_sql_as_root_via_cli(&format!("SELECT * FROM {} ORDER BY id", full))
        .expect("select query");

    let timing = parse_timing_ms(&output).expect("Should have timing output");
    println!("Small table (10 rows) timing: {} ms", timing);

    // Log timing for reference (no strict threshold to avoid flakiness)
    assert!(timing > 0.0, "Timing should be positive");
    assert!(timing < 10000.0, "10 row query should complete in < 10s");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(180_000)]
#[test]
fn smoke_test_timing_scaling_medium_table() {
    if !is_server_running() {
        println!("Skipping smoke_test_timing_scaling_medium_table: server not running");
        return;
    }

    let namespace = generate_unique_namespace("timing");
    let table = generate_unique_table("medium");
    let full = format!("{}.{}", namespace, table);

    // Cleanup first
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Create namespace
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace");

    // Create table
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, data TEXT) WITH (TYPE='USER')",
        full
    ))
    .expect("create table");

    // Insert 1000 rows in batches (faster than individual inserts)
    let batch_size = 100;
    for batch in 0..10 {
        let start = batch * batch_size + 1;
        let end = (batch + 1) * batch_size;

        let mut values = Vec::new();
        for i in start..=end {
            values.push(format!("({}, 'row_{}')", i, i));
        }

        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, data) VALUES {}",
            full,
            values.join(", ")
        ))
        .expect("insert batch");
    }

    // Query all rows and parse timing
    let output =
        execute_sql_as_root_via_cli(&format!("SELECT * FROM {} ORDER BY id LIMIT 1000", full))
            .expect("select query");

    let timing = parse_timing_ms(&output).expect("Should have timing output");
    println!("Medium table (1000 rows) timing: {} ms", timing);

    // Log timing for reference
    assert!(timing > 0.0, "Timing should be positive");
    assert!(timing < 30000.0, "1000 row query should complete in < 30s");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(180_000)]
#[test]
fn smoke_test_timing_aggregation_query() {
    if !is_server_running() {
        println!("Skipping smoke_test_timing_aggregation_query: server not running");
        return;
    }

    let namespace = generate_unique_namespace("timing");
    let table = generate_unique_table("agg");
    let full = format!("{}.{}", namespace, table);

    // Create namespace
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace");

    // Create table with groupable column
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, category TEXT, amount DOUBLE) WITH (TYPE='USER')",
        full
    ))
    .expect("create table");

    // Insert test data with multiple categories
    let categories = vec!["A", "B", "C", "A", "B", "C", "A", "B", "C", "A"];
    for (i, cat) in categories.iter().enumerate() {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, category, amount) VALUES ({}, '{}', {})",
            full,
            i + 1,
            cat,
            (i + 1) as f64 * 10.0
        ))
        .expect("insert row");
    }

    // Execute aggregation query
    let output = execute_sql_as_root_via_cli(&format!(
        "SELECT category, COUNT(*) as count, SUM(amount) as total FROM {} GROUP BY category ORDER \
         BY category",
        full
    ))
    .expect("aggregation query");

    let timing = parse_timing_ms(&output).expect("Should have timing output");
    println!("Aggregation query timing: {} ms", timing);

    // Verify timing is captured
    assert!(timing > 0.0, "Timing should be positive");
    assert!(timing < 15000.0, "Aggregation should complete in < 15s");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(180_000)]
#[test]
fn smoke_test_timing_join_query() {
    if !is_server_running() {
        println!("Skipping smoke_test_timing_join_query: server not running");
        return;
    }

    let namespace = generate_unique_namespace("timing");
    let table1 = generate_unique_table("users");
    let table2 = generate_unique_table("orders");
    let full1 = format!("{}.{}", namespace, table1);
    let full2 = format!("{}.{}", namespace, table2);

    // Create namespace
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace");

    // Create users table
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED', \
         ACCESS_LEVEL='PUBLIC')",
        full1
    ))
    .expect("create users table");
    let users_table_check = format!(
        "SELECT table_name FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table1
    );
    wait_for_query_contains_with(
        &users_table_check,
        &table1,
        Duration::from_secs(10),
        execute_sql_as_root_via_cli_json,
    )
    .expect("users table should be visible");

    // Create orders table
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, user_id BIGINT, total DOUBLE) WITH \
         (TYPE='SHARED', ACCESS_LEVEL='PUBLIC')",
        full2
    ))
    .expect("create orders table");
    let orders_table_check = format!(
        "SELECT table_name FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table2
    );
    wait_for_query_contains_with(
        &orders_table_check,
        &table2,
        Duration::from_secs(10),
        execute_sql_as_root_via_cli_json,
    )
    .expect("orders table should be visible");

    // Insert test data
    for i in 1..=5 {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, name) VALUES ({}, 'User{}')",
            full1, i, i
        ))
        .expect("insert user");

        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, user_id, total) VALUES ({}, {}, {})",
            full2,
            i,
            i,
            i as f64 * 100.0
        ))
        .expect("insert order");
    }

    // Execute JOIN query
    let output = execute_sql_as_root_via_cli(&format!(
        "SELECT u.name, o.total FROM {} u JOIN {} o ON u.id = o.user_id ORDER BY u.id",
        full1, full2
    ))
    .expect("join query");

    let timing = parse_timing_ms(&output).expect("Should have timing output");
    println!("JOIN query timing: {} ms", timing);

    assert!(timing > 0.0, "Timing should be positive");
    assert!(timing < 20000.0, "JOIN should complete in < 20s");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full1));
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full2));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(180_000)]
#[test]
fn smoke_test_timing_ddl_operations() {
    if !is_server_running() {
        println!("Skipping smoke_test_timing_ddl_operations: server not running");
        return;
    }

    let namespace = generate_unique_namespace("timing");
    let table = generate_unique_table("ddl");
    let full = format!("{}.{}", namespace, table);

    // Create namespace - verify timing output
    let output =
        execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .expect("create namespace");

    let timing = parse_timing_ms(&output);
    println!("CREATE NAMESPACE timing: {:?}", timing);
    // Note: Some DDL operations may not include timing - this is informational

    // Create table - verify timing output
    let output = execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, data TEXT) WITH (TYPE='USER')",
        full
    ))
    .expect("create table");

    let timing = parse_timing_ms(&output);
    println!("CREATE TABLE timing: {:?}", timing);

    // Drop table - verify timing output
    let output =
        execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full)).expect("drop table");

    let timing = parse_timing_ms(&output);
    println!("DROP TABLE timing: {:?}", timing);

    // Note: This test is primarily informational - DDL timing may vary
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(180_000)]
#[test]
fn smoke_test_timing_flush_operation() {
    if !is_server_running() {
        println!("Skipping smoke_test_timing_flush_operation: server not running");
        return;
    }

    let namespace = generate_unique_namespace("timing");
    let table = generate_unique_table("flush");
    let full = format!("{}.{}", namespace, table);

    // Create namespace
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace");

    // Create table with low flush threshold
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, data TEXT) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:10')",
        full
    ))
    .expect("create table");

    // Insert enough rows to trigger flush
    for i in 1..=20 {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, data) VALUES ({}, 'row_{}')",
            full, i, i
        ))
        .expect("insert row");
    }

    // Execute FLUSH and verify timing
    let output =
        execute_sql_as_root_via_cli(&format!("STORAGE FLUSH TABLE {}", full)).expect("flush table");

    let timing = parse_timing_ms(&output);
    println!("STORAGE FLUSH TABLE timing: {:?}", timing);

    // Note: FLUSH may return job ID instead of timing - this is informational

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
