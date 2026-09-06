//! Shared helpers for 0.7 ordinal-row / scalar-index e2e tests.

use std::{collections::HashMap, time::Duration};

use serde_json::Value;

use crate::common::*;

pub fn skip_if_no_server() -> bool {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping 0.7 storage e2e.");
        return true;
    }
    false
}

pub fn setup_namespace(prefix: &str) -> String {
    let namespace = generate_unique_namespace(prefix);
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {namespace} CASCADE"));
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {namespace}"))
        .unwrap_or_else(|err| panic!("CREATE NAMESPACE {namespace}: {err}"));
    namespace
}

pub fn ready(full_table: &str) {
    wait_for_table_ready(full_table, Duration::from_secs(15))
        .unwrap_or_else(|err| panic!("{full_table} not ready: {err}"));
}

pub fn exec(sql: &str) -> String {
    execute_sql_as_root_via_client(sql).unwrap_or_else(|err| panic!("SQL failed: {err}\n{sql}"))
}

pub fn exec_err(sql: &str) -> String {
    execute_sql_as_root_via_client(sql)
        .expect_err(&format!("expected error for: {sql}"))
        .to_string()
}

pub fn query_rows(sql: &str) -> Vec<HashMap<String, Value>> {
    let raw = execute_sql_as_root_via_client_json(sql)
        .unwrap_or_else(|err| panic!("query failed: {err}\n{sql}"));
    let json: Value =
        serde_json::from_str(&raw).unwrap_or_else(|err| panic!("json parse: {err}\n{raw}"));
    get_rows_as_hashmaps(&json).unwrap_or_default()
}

pub fn query_rows_as(user: &str, password: &str, sql: &str) -> Vec<HashMap<String, Value>> {
    let raw = execute_sql_via_client_as_json(user, password, sql)
        .unwrap_or_else(|err| panic!("query as {user} failed: {err}\n{sql}"));
    let json: Value =
        serde_json::from_str(&raw).unwrap_or_else(|err| panic!("json parse: {err}\n{raw}"));
    get_rows_as_hashmaps(&json).unwrap_or_default()
}

pub fn cell(row: &HashMap<String, Value>, column: &str) -> Value {
    row.get(column).map(extract_typed_value).unwrap_or(Value::Null)
}

pub fn cell_str(row: &HashMap<String, Value>, column: &str) -> Option<String> {
    match cell(row, column) {
        Value::Null => None,
        Value::String(s) => Some(s),
        other => Some(other.to_string().trim_matches('"').to_string()),
    }
}

pub fn cell_i64(row: &HashMap<String, Value>, column: &str) -> Option<i64> {
    match cell(row, column) {
        Value::Null => None,
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub fn cell_bool(row: &HashMap<String, Value>, column: &str) -> Option<bool> {
    match cell(row, column) {
        Value::Null => None,
        Value::Bool(b) => Some(b),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

pub fn is_null(row: &HashMap<String, Value>, column: &str) -> bool {
    matches!(cell(row, column), Value::Null)
}

fn parse_count_cell(value: &Value) -> Option<i64> {
    let extracted = extract_typed_value(value);
    extracted
        .as_i64()
        .or_else(|| extracted.as_u64().map(|n| n as i64))
        .or_else(|| extracted.as_f64().map(|n| n as i64))
        .or_else(|| extracted.as_str()?.parse().ok())
}

pub fn count_sql(sql: &str) -> i64 {
    let rows = query_rows(sql);
    let row = rows.first().expect("count query returned no rows");
    for key in ["n", "count", "COUNT(*)", "count(*)"] {
        if let Some(value) = row.get(key) {
            if let Some(n) = parse_count_cell(value) {
                return n;
            }
        }
    }
    row.values()
        .find_map(parse_count_cell)
        .unwrap_or_else(|| panic!("could not parse count from {row:?}"))
}

pub fn create_login_user(prefix: &str) -> (String, String) {
    let suffix = random_string(6).to_lowercase();
    let username = format!("{prefix}_{suffix}");
    let password = "test_pass_123".to_string();
    let sql = format!("CREATE USER {username} WITH PASSWORD '{password}' ROLE 'user'");
    match execute_sql_as_root_via_client(&sql) {
        Ok(_) => {},
        Err(err) => {
            let msg = err.to_string();
            if msg.to_ascii_lowercase().contains("already exists") {
                let _ = execute_sql_as_root_via_client(&format!(
                    "ALTER USER {username} SET PASSWORD '{password}'"
                ));
            } else {
                panic!("CREATE USER {username}: {msg}");
            }
        },
    }
    (username, password)
}

pub fn flush_table(full_table: &str) {
    let output = exec(&format!("STORAGE FLUSH TABLE {full_table}"));
    let job_id = parse_job_id_from_flush_output(&output)
        .unwrap_or_else(|err| panic!("flush job id for {full_table}: {err}\n{output}"));
    verify_job_completed(&job_id, Duration::from_secs(45))
        .unwrap_or_else(|err| panic!("flush {full_table} did not complete: {err}"));
}

pub fn latest_indexes_json(namespace: &str, table: &str) -> String {
    let rows = query_rows(&format!(
        "SELECT indexes FROM system.schemas WHERE namespace_id = '{namespace}' AND table_name = \
         '{table}' AND is_latest = true"
    ));
    assert_eq!(rows.len(), 1, "expected one latest schema for {namespace}.{table}");
    match cell(&rows[0], "indexes") {
        Value::Null => "[]".to_string(),
        Value::String(s) => s,
        other => other.to_string(),
    }
}

pub fn assert_index_named(indexes_json: &str, name: &str) {
    assert!(
        indexes_json.contains(name),
        "catalog indexes should contain {name}: {indexes_json}"
    );
}

pub fn assert_index_absent(indexes_json: &str, name: &str) {
    assert!(
        !indexes_json.contains(&format!("\"{name}\""))
            && !indexes_json.contains(&format!("'{name}'")),
        "catalog indexes should not contain {name}: {indexes_json}"
    );
}
