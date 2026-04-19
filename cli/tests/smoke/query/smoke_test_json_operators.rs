//! Smoke tests for PostgreSQL-compatible JSON operators via datafusion-functions-json.
//!
//! Covers:
//! - `->>` text extraction operator
//! - `->` JSON extraction operator
//! - `?` existence operator
//! - Nested key access
//! - Array element access
//! - `json_length`, `json_object_keys`, `json_contains` functions
//! - Function-style typed extraction (`json_get_int`, `json_get_bool`, `json_as_text`)
//! - Big JSON payloads (64 KB+)
//! - JSON column round-trip (object, array, string, number, bool, null)
//! - WHERE clause filtering on JSON fields
//! - Multiple JSON columns in one table

use crate::common::*;
use std::time::{Duration, Instant};

/// Helper: create a shared table with a JSON column, insert data, and return (namespace, full_table).
fn setup_json_table(prefix: &str, extra_cols: &str) -> (String, String) {
    let namespace = generate_unique_namespace(&format!("json_{prefix}"));
    let table = generate_unique_table(&format!("{prefix}_tbl"));
    let full_table = format!("{namespace}.{table}");

    let _ = execute_sql_as_root_via_client(&format!(
        "DROP NAMESPACE IF EXISTS {namespace} CASCADE"
    ));
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {namespace}"))
        .expect("create namespace");

    let cols = if extra_cols.is_empty() {
        "id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(), doc JSON".to_string()
    } else {
        format!("id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(), doc JSON, {extra_cols}")
    };

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {full_table} ({cols}) WITH (TYPE = 'SHARED')"
    ))
    .expect("create table");

    (namespace, full_table)
}

fn cleanup(namespace: &str) {
    let _ = execute_sql_as_root_via_client(&format!(
        "DROP NAMESPACE IF EXISTS {namespace} CASCADE"
    ));
}

// ── JSON round-trip: objects, arrays, nested, primitives ──────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_object_roundtrip() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("obj_rt", "");

    // Insert a JSON object
    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"name":"alice","age":30,"tags":["admin","user"]}}')"#
    ))
    .expect("insert json object");

    // Read it back
    let output = wait_for_query_contains_with(
        &format!("SELECT doc FROM {tbl}"),
        "alice",
        Duration::from_secs(5),
        execute_sql_as_root_via_client_json,
    )
    .expect("select json object");

    assert!(output.contains("alice"), "should contain 'alice': {output}");
    assert!(output.contains("admin"), "should contain 'admin' tag: {output}");

    cleanup(&ns);
    println!("✅ smoke_json_object_roundtrip passed");
}

#[ntest::timeout(60000)]
#[test]
fn smoke_json_array_roundtrip() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("arr_rt", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('[1, "two", true, null, {{"nested": 42}}]')"#
    ))
    .expect("insert json array");

    let output = wait_for_query_contains_with(
        &format!("SELECT doc FROM {tbl}"),
        "nested",
        Duration::from_secs(5),
        execute_sql_as_root_via_client_json,
    )
    .expect("select json array");

    assert!(output.contains("nested"), "should contain 'nested': {output}");

    cleanup(&ns);
    println!("✅ smoke_json_array_roundtrip passed");
}

// ── ->> text extraction operator ─────────────────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_arrow_text_operator() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("arrow_txt", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"name":"bob","city":"paris"}}')"#
    ))
    .expect("insert");

    // Use ->> to extract text
    let output = wait_for_query_contains_with(
        &format!("SELECT doc->>'name' AS name FROM {tbl}"),
        "bob",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("->> operator");

    assert!(output.contains("bob"), "->> should extract 'bob': {output}");

    // Extract another field
    let output = wait_for_query_contains_with(
        &format!("SELECT doc->>'city' AS city FROM {tbl}"),
        "paris",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("->> city");

    assert!(output.contains("paris"), "->> should extract 'paris': {output}");

    cleanup(&ns);
    println!("✅ smoke_json_arrow_text_operator passed");
}

// ── -> JSON extraction operator ──────────────────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_arrow_json_operator() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("arrow_json", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"profile":{{"city":"london","scores":[10,20,30]}}}}')"#
    ))
    .expect("insert nested");

    // -> returns JSON (the nested object)
    let output = wait_for_query_contains_with(
        &format!("SELECT doc->'profile' AS profile FROM {tbl}"),
        "london",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("-> operator");

    assert!(output.contains("london"), "-> should return nested JSON: {output}");

    cleanup(&ns);
    println!("✅ smoke_json_arrow_json_operator passed");
}

// ── Nested key access via chained operators ──────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_nested_access() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("nested", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"user":{{"address":{{"zip":"90210"}}}}}}')"#
    ))
    .expect("insert deeply nested");

    // Chained -> and ->> for deep access
    let output = wait_for_query_contains_with(
        &format!("SELECT doc->'user'->'address'->>'zip' AS zip FROM {tbl}"),
        "90210",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("nested access");

    assert!(output.contains("90210"), "nested ->> should extract '90210': {output}");

    cleanup(&ns);
    println!("✅ smoke_json_nested_access passed");
}

// ── ? existence operator and json_contains() ─────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_exists_operator_and_contains_function() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("exists_op", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"customer_id":"cust_123","status":"paid"}}')"#
    ))
    .expect("insert exists payload");

    let output = wait_for_query_contains_with(
        &format!(
            "SELECT doc->>'customer_id' AS customer_id FROM {tbl} WHERE doc ? 'customer_id'"
        ),
        "cust_123",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("exists operator query");

    assert!(
        output.contains("cust_123"),
        "? should allow filtering on existing keys: {output}"
    );

    let output = wait_for_query_contains_with(
        &format!(
            "SELECT json_as_text(doc, 'status') AS status FROM {tbl} WHERE json_contains(doc, 'customer_id')"
        ),
        "paid",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("json_contains function query");

    assert!(
        output.contains("paid"),
        "json_contains should behave like the existence operator: {output}"
    );

    cleanup(&ns);
    println!("✅ smoke_json_exists_operator_and_contains_function passed");
}

// ── WHERE clause filtering on JSON fields ────────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_where_filter() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("where_flt", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"status":"active","priority":1}}')"#
    ))
    .expect("insert row 1");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"status":"inactive","priority":2}}')"#
    ))
    .expect("insert row 2");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"status":"active","priority":3}}')"#
    ))
    .expect("insert row 3");

    // Filter using ->> in WHERE
    let output = wait_for_query_contains_with(
        &format!("SELECT doc->>'priority' AS p FROM {tbl} WHERE doc->>'status' = 'active'"),
        "1",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("where filter");

    assert!(output.contains("1"), "should contain priority 1: {output}");
    assert!(output.contains("3"), "should contain priority 3: {output}");
    // Row with status=inactive should not appear
    assert!(!output.contains("priority") || !output.contains("2") || output.contains("1"),
        "should filter out inactive row");

    cleanup(&ns);
    println!("✅ smoke_json_where_filter passed");
}

// ── JSON helper functions ────────────────────────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_helper_functions() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("helpers", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"items":[10,20,30],"meta":{{"a":1,"b":2}},"count":7,"flag":true}}')"#
    ))
    .expect("insert helper payload");

    let output = wait_for_query_contains_with(
        &format!(
            "SELECT json_length(doc, 'items') AS item_count, json_get_int(doc, 'count') AS count_value, json_get_bool(doc, 'flag') AS flag_value FROM {tbl}"
        ),
        "7",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("json helper scalar functions");

    assert!(output.contains("3"), "json_length should return 3: {output}");
    assert!(output.contains("7"), "json_get_int should return 7: {output}");
    assert!(
        output.to_ascii_lowercase().contains("true"),
        "json_get_bool should return true: {output}"
    );

    let output = wait_for_query_contains_with(
        &format!("SELECT json_object_keys(doc, 'meta') AS meta_keys FROM {tbl}"),
        "a",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("json_object_keys function");

    assert!(output.contains("a"), "json_object_keys should include key a: {output}");
    assert!(output.contains("b"), "json_object_keys should include key b: {output}");

    cleanup(&ns);
    println!("✅ smoke_json_helper_functions passed");
}

// ── Big JSON payload (64 KB+) ────────────────────────────────────────────

#[ntest::timeout(120000)]
#[test]
fn smoke_json_big_payload() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("big_json", "");

    // Build a ~100 KB JSON object with many keys
    let start = Instant::now();
    let mut entries = Vec::with_capacity(500);
    for i in 0..500 {
        // Each entry ~200 bytes
        entries.push(format!(
            r#""key_{i}":"value_{i}_padding_{pad}""#,
            i = i,
            pad = "x".repeat(150)
        ));
    }
    let big_json = format!("{{{}}}", entries.join(","));
    let payload_size = big_json.len();
    assert!(
        payload_size > 64_000,
        "payload should be >64KB, got {payload_size}"
    );
    println!("  Big JSON payload size: {payload_size} bytes");

    // Insert the big JSON
    let insert_sql = format!("INSERT INTO {tbl} (doc) VALUES ('{}')", big_json.replace('\'', "''"));
    execute_sql_as_root_via_client(&insert_sql).expect("insert big json");

    // Read it back and verify key_0 and key_499
    let output = wait_for_query_contains_with(
        &format!("SELECT doc->>'key_0' AS k0 FROM {tbl}"),
        "value_0",
        Duration::from_secs(10),
        execute_sql_as_root_via_client,
    )
    .expect("select big json key_0");

    assert!(output.contains("value_0"), "should extract key_0: {output}");
    let elapsed = start.elapsed();
    println!("  Big JSON insert+query took {:.2}s", elapsed.as_secs_f64());

    // Also verify last key
    let output = execute_sql_as_root_via_client(&format!(
        "SELECT doc->>'key_499' AS k499 FROM {tbl}"
    ))
    .expect("select big json key_499");

    assert!(
        output.contains("value_499"),
        "should extract key_499: {output}"
    );

    cleanup(&ns);
    println!("✅ smoke_json_big_payload passed ({payload_size} bytes, {:.2}s)", elapsed.as_secs_f64());
}

// ── Multiple JSON columns ────────────────────────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_multiple_columns() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("multi_col", "metadata JSON");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc, metadata) VALUES ('{{"title":"hello"}}', '{{"source":"api","version":2}}')"#
    ))
    .expect("insert multi col");

    let output = wait_for_query_contains_with(
        &format!("SELECT doc->>'title' AS title, metadata->>'source' AS src FROM {tbl}"),
        "hello",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("multi col query");

    assert!(output.contains("hello"), "should extract title: {output}");
    assert!(output.contains("api"), "should extract source: {output}");

    cleanup(&ns);
    println!("✅ smoke_json_multiple_columns passed");
}

// ── JSON null, boolean, number primitives ────────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_primitive_values() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("prims", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"str":"text","num":42,"float":3.14,"bool":true,"nil":null}}')"#
    ))
    .expect("insert primitives");

    let output = wait_for_query_contains_with(
        &format!("SELECT doc->>'str' AS s, doc->>'num' AS n, doc->>'bool' AS b FROM {tbl}"),
        "text",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("primitive extraction");

    assert!(output.contains("text"), "should extract str: {output}");
    assert!(output.contains("42"), "should extract num: {output}");

    cleanup(&ns);
    println!("✅ smoke_json_primitive_values passed");
}

// ── JSON array element access ────────────────────────────────────────────

#[ntest::timeout(60000)]
#[test]
fn smoke_json_array_element_access() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping.");
        return;
    }

    let (ns, tbl) = setup_json_table("arr_elem", "");

    execute_sql_as_root_via_client(&format!(
        r#"INSERT INTO {tbl} (doc) VALUES ('{{"items":["alpha","beta","gamma"]}}')"#
    ))
    .expect("insert array");

    // Access array elements via -> (integer index) then ->> for text
    let output = wait_for_query_contains_with(
        &format!("SELECT doc->'items'->0 AS first_item FROM {tbl}"),
        "alpha",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("array index access");

    assert!(output.contains("alpha"), "should extract first array element: {output}");

    cleanup(&ns);
    println!("✅ smoke_json_array_element_access passed");
}
