//! Smoke tests for ALTER TABLE SET TBLPROPERTIES across table types.
//!
//! These tests verify that changing persisted table options:
//! - creates a new schema version in `system.schemas`
//! - preserves query and DML behavior
//! - preserves flush behavior for USER and SHARED tables
//! - keeps STREAM tables readable/writable while flush remains unsupported

use std::{
    collections::HashMap,
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

use crate::common::*;

const SCHEMA_WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const FLUSH_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_user_table_options_preserves_query_dml_and_flush() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("ddl_user_opts");
    let table = generate_unique_table("profiles");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing ALTER TABLE SET TBLPROPERTIES for USER tables");

    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            visits INT NOT NULL
        ) WITH (
            TYPE = 'USER',
            USE_USER_STORAGE = true,
            FLUSH_POLICY = 'rows:500',
            COMPRESSION = 'snappy'
        )"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create user table");
    wait_for_schema_history(&namespace, &table, 1, 1, "user create");

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, name, visits) VALUES (1, 'Alice', 10)",
        full_table
    ))
    .expect("Failed to insert initial user-table row");

    let alter_sql = format!(
        "ALTER TABLE {} SET TBLPROPERTIES (USE_USER_STORAGE = false, FLUSH_POLICY = \
         'rows:50,interval:120', COMPRESSION = 'none')",
        full_table
    );
    execute_sql_as_root_via_client(&alter_sql).expect("Failed to alter user table options");

    let history = wait_for_schema_history(&namespace, &table, 2, 2, "user alter");
    assert_eq!(row_i64(&history[0], "schema_version"), Some(1));
    assert_eq!(row_bool(&history[0], "is_latest"), Some(false));
    assert_eq!(row_i64(&history[1], "schema_version"), Some(2));
    assert_eq!(row_bool(&history[1], "is_latest"), Some(true));

    let latest_output = execute_sql_as_root_via_client_json(&format!(
        "SELECT use_user_storage, options FROM system.schemas WHERE namespace_id = '{}' AND \
         table_name = '{}' AND is_latest = true",
        namespace, table
    ))
    .expect("Failed to query latest user table schema row");
    assert_contains_all_case_insensitive(
        &latest_output,
        &[
            "false",
            "none",
            "row_limit",
            "50",
            "interval_seconds",
            "120",
        ],
        "latest user table schema options",
    );

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, name, visits) VALUES (2, 'Bob', 20)",
        full_table
    ))
    .expect("Failed to insert second user-table row after ALTER");
    execute_sql_as_root_via_client(&format!("UPDATE {} SET visits = 15 WHERE id = 1", full_table))
        .expect("Failed to update user-table row after ALTER");

    let pre_flush = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY id", full_table),
        "Alice",
        Duration::from_secs(20),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query USER table after ALTER DML");
    assert_contains_all_case_insensitive(
        &pre_flush,
        &["Alice", "15", "Bob", "20"],
        "user table rows after ALTER DML",
    );

    flush_table_and_assert(&full_table, &namespace, &table, true, "user table option alter");

    let post_flush = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY id", full_table),
        "Bob",
        Duration::from_secs(20),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query USER table after flush");
    assert_contains_all_case_insensitive(
        &post_flush,
        &["Alice", "15", "Bob", "20"],
        "user table rows after flush",
    );

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, name, visits) VALUES (3, 'Carol', 30)",
        full_table
    ))
    .expect("Failed to insert USER-table row after flush");

    let final_output = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY id", full_table),
        "Carol",
        Duration::from_secs(20),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query USER table after post-flush insert");
    assert_contains_all_case_insensitive(
        &final_output,
        &["Alice", "Bob", "Carol", "30"],
        "user table rows after post-flush insert",
    );

    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_shared_table_options_preserves_query_dml_and_flush() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("ddl_shared_opts");
    let table = generate_unique_table("settings");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing ALTER TABLE SET TBLPROPERTIES for SHARED tables");

    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            key TEXT NOT NULL,
            enabled BOOLEAN NOT NULL
        ) WITH (
            TYPE = 'SHARED',
            FLUSH_POLICY = 'rows:500',
            COMPRESSION = 'snappy'
        )"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create shared table");
    grant_public_shared_table_access(&full_table);
    wait_for_schema_history(&namespace, &table, 1, 1, "shared create");

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, key, enabled) VALUES (1, 'dark_mode', true)",
        full_table
    ))
    .expect("Failed to insert initial shared-table row");

    let alter_sql = format!(
        "ALTER TABLE {} SET TBLPROPERTIES (FLUSH_POLICY = 'rows:25,interval:180', COMPRESSION = \
         'zstd')",
        full_table
    );
    execute_sql_as_root_via_client(&alter_sql).expect("Failed to alter shared table options");

    let history = wait_for_schema_history(&namespace, &table, 2, 2, "shared alter");
    assert_eq!(row_i64(&history[0], "schema_version"), Some(1));
    assert_eq!(row_bool(&history[0], "is_latest"), Some(false));
    assert_eq!(row_i64(&history[1], "schema_version"), Some(2));
    assert_eq!(row_bool(&history[1], "is_latest"), Some(true));

    let latest_output = execute_sql_as_root_via_client_json(&format!(
        "SELECT options FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}' AND \
         is_latest = true",
        namespace, table
    ))
    .expect("Failed to query latest shared table schema row");
    assert_contains_all_case_insensitive(
        &latest_output,
        &["zstd", "row_limit", "25", "interval_seconds", "180"],
        "latest shared table schema options",
    );

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, key, enabled) VALUES (2, 'beta_access', false)",
        full_table
    ))
    .expect("Failed to insert second shared-table row after ALTER");
    execute_sql_as_root_via_client(&format!(
        "UPDATE {} SET enabled = false WHERE id = 1",
        full_table
    ))
    .expect("Failed to update shared-table row after ALTER");

    let pre_flush = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY id", full_table),
        "dark_mode",
        Duration::from_secs(20),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query SHARED table after ALTER DML");
    assert_contains_all_case_insensitive(
        &pre_flush,
        &["dark_mode", "beta_access"],
        "shared table rows after ALTER DML",
    );

    flush_table_and_assert(&full_table, &namespace, &table, false, "shared table option alter");

    let post_flush = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY id", full_table),
        "beta_access",
        Duration::from_secs(20),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query SHARED table after flush");
    assert_contains_all_case_insensitive(
        &post_flush,
        &["dark_mode", "beta_access"],
        "shared table rows after flush",
    );

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, key, enabled) VALUES (3, 'feature_x', true)",
        full_table
    ))
    .expect("Failed to insert SHARED-table row after flush");

    let final_output = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY id", full_table),
        "feature_x",
        Duration::from_secs(20),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query SHARED table after post-flush insert");
    assert_contains_all_case_insensitive(
        &final_output,
        &["dark_mode", "beta_access", "feature_x"],
        "shared table rows after post-flush insert",
    );

    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_stream_table_options_preserves_query_and_dml() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("ddl_stream_opts");
    let table = generate_unique_table("events");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing ALTER TABLE SET TBLPROPERTIES for STREAM tables");

    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            event_id BIGINT PRIMARY KEY,
            event_type TEXT NOT NULL,
            payload TEXT
        ) WITH (
            TYPE = 'STREAM',
            TTL_SECONDS = 60,
            EVICTION_STRATEGY = 'time_based',
            MAX_STREAM_SIZE_BYTES = 1024
        )"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create stream table");
    wait_for_schema_history(&namespace, &table, 1, 1, "stream create");

    let invalid_stream_create = format!(
        r#"CREATE TABLE {}.invalid_stream_compression (
            event_id BIGINT PRIMARY KEY,
            event_type TEXT NOT NULL
        ) WITH (
            TYPE = 'STREAM',
            TTL_SECONDS = 60,
            COMPRESSION = 'snappy'
        )"#,
        namespace
    );
    assert_sql_rejected_with(
        &invalid_stream_create,
        "COMPRESSION is only supported for USER and SHARED tables",
        "stream CREATE with compression",
    );

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (event_id, event_type, payload) VALUES (1, 'created', 'before alter')",
        full_table
    ))
    .expect("Failed to insert initial stream-table row");

    let alter_sql = format!(
        "ALTER TABLE {} SET TBLPROPERTIES (TTL_SECONDS = 120, EVICTION_STRATEGY = 'hybrid', \
         MAX_STREAM_SIZE_BYTES = 4096)",
        full_table
    );
    execute_sql_as_root_via_client(&alter_sql).expect("Failed to alter stream table options");

    assert_sql_rejected_with(
        &format!("ALTER TABLE {} SET TBLPROPERTIES (COMPRESSION = 'zstd')", full_table),
        "COMPRESSION is not supported for STREAM tables",
        "stream ALTER with compression",
    );

    let history = wait_for_schema_history(&namespace, &table, 2, 2, "stream alter");
    assert_eq!(row_i64(&history[0], "schema_version"), Some(1));
    assert_eq!(row_bool(&history[0], "is_latest"), Some(false));
    assert_eq!(row_i64(&history[1], "schema_version"), Some(2));
    assert_eq!(row_bool(&history[1], "is_latest"), Some(true));

    let latest_output = execute_sql_as_root_via_client_json(&format!(
        "SELECT options FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}' AND \
         is_latest = true",
        namespace, table
    ))
    .expect("Failed to query latest stream table schema row");
    assert_contains_all_case_insensitive(
        &latest_output,
        &[
            "ttl_seconds",
            "120",
            "hybrid",
            "max_stream_size_bytes",
            "4096",
        ],
        "latest stream table schema options",
    );

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (event_id, event_type, payload) VALUES (2, 'updated', 'after alter')",
        full_table
    ))
    .expect("Failed to insert second stream-table row after ALTER");

    let stream_output = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY event_id", full_table),
        "after alter",
        Duration::from_secs(20),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query STREAM table after ALTER insert");
    assert_contains_all_case_insensitive(
        &stream_output,
        &["before alter", "after alter", "created", "updated"],
        "stream table rows after ALTER insert",
    );

    assert_stream_flush_is_rejected(&full_table);

    let after_rejection = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY event_id", full_table),
        "after alter",
        Duration::from_secs(20),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query STREAM table after rejected flush");
    assert_contains_all_case_insensitive(
        &after_rejection,
        &["before alter", "after alter"],
        "stream table rows after rejected flush",
    );

    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

fn wait_for_schema_history(
    namespace: &str,
    table: &str,
    expected_rows: usize,
    expected_latest_version: i64,
    context: &str,
) -> Vec<HashMap<String, Value>> {
    let query = format!(
        "SELECT schema_version, is_latest FROM system.schemas WHERE namespace_id = '{}' AND \
         table_name = '{}' ORDER BY schema_version",
        namespace, table
    );
    let deadline = Instant::now() + SCHEMA_WAIT_TIMEOUT;

    loop {
        let output = execute_sql_as_root_via_client_json(&query)
            .unwrap_or_else(|err| panic!("{}: failed to query system.schemas: {}", context, err));
        let json: Value = serde_json::from_str(&output).unwrap_or_else(|err| {
            panic!("{}: failed to parse JSON output: {} ({})", context, err, output)
        });
        let rows = get_rows_as_hashmaps(&json).unwrap_or_default();

        let matches = rows.len() == expected_rows
            && rows.last().and_then(|row| row_i64(row, "schema_version"))
                == Some(expected_latest_version)
            && rows.last().and_then(|row| row_bool(row, "is_latest")) == Some(true);

        if matches {
            return rows;
        }

        if Instant::now() >= deadline {
            panic!(
                "{}: timed out waiting for {} schema rows and latest version {}. Last rows: {:?}",
                context, expected_rows, expected_latest_version, rows
            );
        }

        thread::sleep(Duration::from_millis(200));
    }
}

fn row_i64(row: &HashMap<String, Value>, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(extract_arrow_value)
        .or_else(|| row.get(key).cloned())
        .and_then(|value| match value {
            Value::Number(number) => number.as_i64(),
            Value::String(text) => text.parse::<i64>().ok(),
            _ => None,
        })
}

fn row_bool(row: &HashMap<String, Value>, key: &str) -> Option<bool> {
    row.get(key)
        .and_then(extract_arrow_value)
        .or_else(|| row.get(key).cloned())
        .and_then(|value| match value {
            Value::Bool(flag) => Some(flag),
            Value::String(text) => match text.to_ascii_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
            _ => None,
        })
}

fn assert_contains_all_case_insensitive(output: &str, needles: &[&str], context: &str) {
    let haystack = output.to_ascii_lowercase();
    for needle in needles {
        assert!(
            haystack.contains(&needle.to_ascii_lowercase()),
            "{}: expected '{}' in output: {}",
            context,
            needle,
            output
        );
    }
}

fn assert_sql_rejected_with(sql: &str, expected: &str, context: &str) {
    match execute_sql_as_root_via_client(sql) {
        Err(err) => {
            let message = err.to_string();
            assert!(
                message.to_ascii_lowercase().contains(&expected.to_ascii_lowercase()),
                "{}: expected rejection containing '{}', got error: {}",
                context,
                expected,
                err
            );
        },
        Ok(output) => {
            let message = output.to_ascii_lowercase();
            assert!(
                message.contains("error") && message.contains(&expected.to_ascii_lowercase()),
                "{}: expected rejection containing '{}', got success output: {}",
                context,
                expected,
                output
            );
        },
    }
}

fn flush_table_and_assert(
    full_table: &str,
    namespace: &str,
    table: &str,
    is_user_table: bool,
    context: &str,
) {
    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table))
            .unwrap_or_else(|err| panic!("{}: flush command failed: {}", context, err));
    let job_id = parse_job_id_from_flush_output(&flush_output).unwrap_or_else(|err| {
        panic!("{}: failed to parse flush job id from '{}': {}", context, flush_output, err)
    });
    let timeout = if is_cluster_mode() {
        FLUSH_WAIT_TIMEOUT + Duration::from_secs(15)
    } else {
        FLUSH_WAIT_TIMEOUT
    };
    verify_job_completed(&job_id, timeout)
        .unwrap_or_else(|err| panic!("{}: flush job {} failed: {}", context, job_id, err));
    assert_flush_storage_files_exist(namespace, table, is_user_table, context);
}

fn assert_stream_flush_is_rejected(full_table: &str) {
    match execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table)) {
        Err(err) => {
            let message = err.to_string().to_ascii_lowercase();
            assert!(
                message.contains("stream") || message.contains("not supported"),
                "expected stream flush rejection message, got: {}",
                err
            );
        },
        Ok(output) => {
            let message = output.to_ascii_lowercase();
            assert!(
                message.contains("error")
                    || message.contains("stream")
                    || message.contains("not supported"),
                "expected stream flush rejection output, got success output: {}",
                output
            );
        },
    }
}
