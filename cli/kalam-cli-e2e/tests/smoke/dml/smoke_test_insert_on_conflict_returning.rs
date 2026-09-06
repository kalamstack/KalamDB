//! Smoke tests for PostgreSQL-style INSERT ... RETURNING and
//! INSERT ... ON CONFLICT ... RETURNING against a running server.
//!
//! Covers hot-path upsert semantics, RETURNING row shapes, conflict handling,
//! partial failure atomicity, and per-user conflict isolation.

use std::collections::HashMap;

use serde_json::Value;

use crate::common::*;

struct UsersFixture {
    namespace:  String,
    full_table: String,
}

impl UsersFixture {
    fn new_shared() -> Self {
        let namespace = generate_unique_namespace("conflict_ret");
        let table = generate_unique_table("users");
        let full_table = format!("{}.{}", namespace, table);

        execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .expect("create namespace should succeed");

        let create_sql = format!(
            "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT, age INT) WITH (TYPE='SHARED')",
            full_table
        );
        execute_sql_as_root_via_client(&create_sql)
            .expect("create shared users table should succeed");
        grant_public_shared_table_access(&full_table);

        Self {
            namespace,
            full_table,
        }
    }

    fn new_user_table() -> Self {
        let namespace = generate_unique_namespace("conflict_ret_user");
        let table = generate_unique_table("notes");
        let full_table = format!("{}.{}", namespace, table);

        execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
            .expect("create namespace should succeed");

        let create_sql = format!(
            "CREATE TABLE {} (id BIGINT PRIMARY KEY, text TEXT) WITH (TYPE='USER')",
            full_table
        );
        execute_sql_as_root_via_client(&create_sql).expect("create user table should succeed");

        Self {
            namespace,
            full_table,
        }
    }

    fn drop(&self) {
        let _ = execute_sql_as_root_via_client(&format!(
            "DROP NAMESPACE IF EXISTS {} CASCADE",
            self.namespace
        ));
    }
}

fn parse_client_json(output: &str) -> Value {
    serde_json::from_str(output).expect("failed to parse SQL JSON response")
}

fn assert_status_success(json: &Value, context: &str) {
    let status = json.get("status").and_then(|value| value.as_str()).unwrap_or("error");
    assert!(status.eq_ignore_ascii_case("success"), "{context} should succeed, got: {json}");
}

fn first_result(json: &Value) -> &Value {
    json.get("results")
        .and_then(|results| results.as_array())
        .and_then(|results| results.first())
        .expect("expected at least one result set")
}

fn result_columns(json: &Value) -> Vec<String> {
    first_result(json)
        .get("schema")
        .and_then(|schema| schema.as_array())
        .map(|schema| {
            schema
                .iter()
                .filter_map(|column| column.get("name").and_then(|name| name.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn result_row_count(json: &Value) -> usize {
    first_result(json)
        .get("row_count")
        .and_then(|count| count.as_u64())
        .unwrap_or(0) as usize
}

fn result_rows(json: &Value) -> Vec<HashMap<String, Value>> {
    get_rows_as_hashmaps(json).unwrap_or_default()
}

fn cell_string(row: &HashMap<String, Value>, column: &str) -> String {
    let value = row.get(column).expect("missing column in row");
    let extracted = extract_typed_value(value);
    match extracted {
        Value::String(text) => text,
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn cell_i64(row: &HashMap<String, Value>, column: &str) -> i64 {
    cell_string(row, column)
        .parse::<i64>()
        .unwrap_or_else(|_| panic!("expected integer column '{column}', got {:?}", row.get(column)))
}

fn exec_sql_json(sql: &str) -> Value {
    let output = execute_sql_as_root_via_client_json(sql)
        .unwrap_or_else(|error| panic!("SQL failed: {error}\nSQL: {sql}"));
    parse_client_json(&output)
}

fn exec_sql_json_expect_error(sql: &str) -> String {
    match execute_sql_as_root_via_client_json(sql) {
        Ok(output) => {
            let json = parse_client_json(&output);
            if json.get("status").and_then(|status| status.as_str()) == Some("success") {
                panic!("expected SQL error, but succeeded: {sql}\n{output}");
            }
            json.get("error")
                .and_then(|error| error.get("message").or(Some(error)))
                .and_then(|message| message.as_str())
                .unwrap_or(&output)
                .to_string()
        },
        Err(error) => error.to_string(),
    }
}

fn assert_row_values(row: &HashMap<String, Value>, id: i64, name: &str, age: i64) {
    assert_eq!(cell_i64(row, "id"), id, "unexpected id");
    assert_eq!(cell_string(row, "name"), name, "unexpected name");
    assert_eq!(cell_i64(row, "age"), age, "unexpected age");
}

// ── Priority 1: INSERT ... RETURNING * ───────────────────────────────────────

#[ntest::timeout(180000)]
#[test]
fn smoke_insert_returning_star_basic() {
    if !is_server_running() {
        eprintln!("Skipping smoke_insert_returning_star_basic: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    let sql = format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader', 3) RETURNING *",
        fixture.full_table
    );

    let json = exec_sql_json(&sql);
    assert_status_success(&json, "insert returning star");

    let rows = result_rows(&json);
    assert_eq!(rows.len(), 1, "expected one returned row");
    assert_row_values(&rows[0], 1, "Nader", 3);

    let columns = result_columns(&json);
    assert!(
        columns.windows(3).any(|window| window == ["id", "name", "age"]),
        "RETURNING * columns should include id, name, age in table order; got: {columns:?}"
    );

    let select_json =
        exec_sql_json(&format!("SELECT id, name, age FROM {} WHERE id = 1", fixture.full_table));
    let stored = result_rows(&select_json);
    assert_eq!(stored.len(), 1);
    assert_row_values(&stored[0], 1, "Nader", 3);

    fixture.drop();
}

// ── Priority 2: INSERT ... RETURNING selected columns ────────────────────────

#[ntest::timeout(180000)]
#[test]
fn smoke_insert_returning_selected_columns() {
    if !is_server_running() {
        eprintln!("Skipping smoke_insert_returning_selected_columns: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    let sql = format!(
        "INSERT INTO {} (id, name, age) VALUES (3, 'Alice', 20) RETURNING id",
        fixture.full_table
    );

    let json = exec_sql_json(&sql);
    assert_status_success(&json, "insert returning id");

    assert_eq!(result_columns(&json), vec!["id"]);
    assert_eq!(result_row_count(&json), 1);

    let rows = result_rows(&json);
    assert_eq!(rows.len(), 1);
    assert_eq!(cell_i64(&rows[0], "id"), 3);

    fixture.drop();
}

// ── Priority 3: ON CONFLICT DO UPDATE RETURNING * ────────────────────────────

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_do_update_returning_star() {
    if !is_server_running() {
        eprintln!("Skipping smoke_on_conflict_do_update_returning_star: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    exec_sql_json(&format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader', 3)",
        fixture.full_table
    ));

    let sql = format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader Updated', 5) ON CONFLICT (id) DO UPDATE \
         SET name = EXCLUDED.name, age = EXCLUDED.age RETURNING *",
        fixture.full_table
    );
    let json = exec_sql_json(&sql);
    assert_status_success(&json, "upsert returning star");

    let rows = result_rows(&json);
    assert_eq!(rows.len(), 1);
    assert_row_values(&rows[0], 1, "Nader Updated", 5);

    let stored = result_rows(&exec_sql_json(&format!(
        "SELECT id, name, age FROM {} WHERE id = 1",
        fixture.full_table
    )));
    assert_row_values(&stored[0], 1, "Nader Updated", 5);

    fixture.drop();
}

// ── Priority 4: ON CONFLICT DO NOTHING RETURNING returns zero rows ───────────

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_do_nothing_returning_zero_rows() {
    if !is_server_running() {
        eprintln!("Skipping smoke_on_conflict_do_nothing_returning_zero_rows: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    exec_sql_json(&format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader', 3)",
        fixture.full_table
    ));

    let sql = format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Ignored', 99) ON CONFLICT (id) DO NOTHING \
         RETURNING *",
        fixture.full_table
    );
    let json = exec_sql_json(&sql);
    assert_status_success(&json, "do nothing returning");

    assert_eq!(result_row_count(&json), 0, "DO NOTHING RETURNING should return zero rows");
    assert!(result_rows(&json).is_empty());

    let stored = result_rows(&exec_sql_json(&format!(
        "SELECT id, name, age FROM {} WHERE id = 1",
        fixture.full_table
    )));
    assert_row_values(&stored[0], 1, "Nader", 3);

    fixture.drop();
}

// ── Priority 5: ON CONFLICT DO UPDATE WHERE false RETURNING zero rows ────────

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_do_update_where_false_returning_zero_rows() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_on_conflict_do_update_where_false_returning_zero_rows: server not \
             running"
        );
        return;
    }

    let fixture = UsersFixture::new_shared();
    exec_sql_json(&format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader', 3)",
        fixture.full_table
    ));

    let sql = format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Ignored', 99) ON CONFLICT (id) DO UPDATE SET \
         age = EXCLUDED.age WHERE false RETURNING *",
        fixture.full_table
    );
    let json = exec_sql_json(&sql);
    assert_status_success(&json, "do update where false returning");

    assert_eq!(
        result_row_count(&json),
        0,
        "conflict row skipped by WHERE should not be returned"
    );
    assert!(result_rows(&json).is_empty());

    let stored = result_rows(&exec_sql_json(&format!(
        "SELECT id, name, age FROM {} WHERE id = 1",
        fixture.full_table
    )));
    assert_row_values(&stored[0], 1, "Nader", 3);

    fixture.drop();
}

// ── Priority 6: multi-row mixed insert/update RETURNING ──────────────────────

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_multi_row_mixed_returning() {
    if !is_server_running() {
        eprintln!("Skipping smoke_on_conflict_multi_row_mixed_returning: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    exec_sql_json(&format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader', 3)",
        fixture.full_table
    ));

    let sql = format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Updated', 30), (4, 'New User', 18) ON \
         CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age RETURNING id, name, \
         age",
        fixture.full_table
    );
    let json = exec_sql_json(&sql);
    assert_status_success(&json, "multi-row upsert returning");

    let rows = result_rows(&json);
    assert_eq!(rows.len(), 2, "expected one returned row per input row");
    assert_row_values(&rows[0], 1, "Updated", 30);
    assert_row_values(&rows[1], 4, "New User", 18);

    fixture.drop();
}

// ── Priority 7: type/constraint error does not partially write ───────────────

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_type_error_does_not_partially_write() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_on_conflict_type_error_does_not_partially_write: server not running"
        );
        return;
    }

    let fixture = UsersFixture::new_shared();
    exec_sql_json(&format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader', 3)",
        fixture.full_table
    ));

    let sql = format!(
        "INSERT INTO {} (id, age) VALUES (1, 'not-number') ON CONFLICT (id) DO UPDATE SET age = \
         EXCLUDED.age RETURNING *",
        fixture.full_table
    );
    let error = exec_sql_json_expect_error(&sql);
    assert!(
        error.to_lowercase().contains("integer")
            || error.to_lowercase().contains("type")
            || error.to_lowercase().contains("invalid"),
        "expected type validation error, got: {error}"
    );

    let stored = result_rows(&exec_sql_json(&format!(
        "SELECT id, name, age FROM {} WHERE id = 1",
        fixture.full_table
    )));
    assert_row_values(&stored[0], 1, "Nader", 3);

    fixture.drop();
}

// ── Priority 8: user-table conflict scoped per user ──────────────────────────

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_user_table_isolation() {
    if !is_server_running() {
        eprintln!("Skipping smoke_on_conflict_user_table_isolation: server not running");
        return;
    }

    let fixture = UsersFixture::new_user_table();
    let user_a = generate_unique_namespace("conflict_user_a");
    let user_b = generate_unique_namespace("conflict_user_b");
    let password = "test_pass_123";

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user_a, password
    ))
    .expect("create user_a");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user_b, password
    ))
    .expect("create user_b");

    let upsert_a = format!(
        "INSERT INTO {} (id, text) VALUES (1, 'A note') ON CONFLICT (id) DO UPDATE SET text = \
         EXCLUDED.text RETURNING id, text",
        fixture.full_table
    );
    let upsert_b = format!(
        "INSERT INTO {} (id, text) VALUES (1, 'B note') ON CONFLICT (id) DO UPDATE SET text = \
         EXCLUDED.text RETURNING id, text",
        fixture.full_table
    );

    let json_a = parse_client_json(
        &execute_sql_via_client_as_json(&user_a, password, &upsert_a).expect("user A upsert"),
    );
    let json_b = parse_client_json(
        &execute_sql_via_client_as_json(&user_b, password, &upsert_b).expect("user B upsert"),
    );

    assert_status_success(&json_a, "user A upsert");
    assert_status_success(&json_b, "user B upsert");

    let rows_a = result_rows(&json_a);
    let rows_b = result_rows(&json_b);
    assert_eq!(rows_a.len(), 1);
    assert_eq!(rows_b.len(), 1);
    assert_eq!(cell_string(&rows_a[0], "text"), "A note");
    assert_eq!(cell_string(&rows_b[0], "text"), "B note");

    let view_a = execute_sql_via_client_as(
        &user_a,
        password,
        &format!("SELECT text FROM {} WHERE id = 1", fixture.full_table),
    )
    .expect("user A select");
    let view_b = execute_sql_via_client_as(
        &user_b,
        password,
        &format!("SELECT text FROM {} WHERE id = 1", fixture.full_table),
    )
    .expect("user B select");

    assert!(view_a.contains("A note"), "user A should see own row: {view_a}");
    assert!(!view_a.contains("B note"), "user A must not see user B row: {view_a}");
    assert!(view_b.contains("B note"), "user B should see own row: {view_b}");
    assert!(!view_b.contains("A note"), "user B must not see user A row: {view_b}");

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_a));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_b));
    fixture.drop();
}

// ── Additional coverage ─────────────────────────────────────────────────────

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_partial_update_preserves_unassigned_columns() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_on_conflict_partial_update_preserves_unassigned_columns: server not \
             running"
        );
        return;
    }

    let fixture = UsersFixture::new_shared();
    exec_sql_json(&format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader', 3)",
        fixture.full_table
    ));

    let sql = format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Nader', 4) ON CONFLICT (id) DO UPDATE SET age \
         = EXCLUDED.age RETURNING id, name, age",
        fixture.full_table
    );
    let rows = result_rows(&exec_sql_json(&sql));
    assert_eq!(rows.len(), 1);
    assert_row_values(&rows[0], 1, "Nader", 4);

    fixture.drop();
}

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_returning_api_response_shape() {
    if !is_server_running() {
        eprintln!("Skipping smoke_on_conflict_returning_api_response_shape: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    let insert_sql = format!(
        "INSERT INTO {} (id, name) VALUES (1, 'Nader') RETURNING id, name",
        fixture.full_table
    );
    let insert_json = exec_sql_json(&insert_sql);

    assert_status_success(&insert_json, "insert returning api shape");
    assert_eq!(result_columns(&insert_json), vec!["id", "name"]);
    assert_eq!(result_row_count(&insert_json), 1);

    let rows = result_rows(&insert_json);
    assert_eq!(cell_i64(&rows[0], "id"), 1);
    assert_eq!(cell_string(&rows[0], "name"), "Nader");

    let nothing_sql = format!(
        "INSERT INTO {} (id, name) VALUES (1, 'Ignored') ON CONFLICT (id) DO NOTHING RETURNING \
         id, name",
        fixture.full_table
    );
    let nothing_json = exec_sql_json(&nothing_sql);
    assert_status_success(&nothing_json, "do nothing returning api shape");
    assert_eq!(result_columns(&nothing_json), vec!["id", "name"]);
    assert_eq!(result_row_count(&nothing_json), 0);
    assert!(result_rows(&nothing_json).is_empty());

    fixture.drop();
}

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_missing_conflict_target_errors() {
    if !is_server_running() {
        eprintln!("Skipping smoke_on_conflict_missing_conflict_target_errors: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    let sql = format!(
        "INSERT INTO {} (id, name) VALUES (1, 'Nader') ON CONFLICT DO UPDATE SET name = \
         EXCLUDED.name RETURNING *",
        fixture.full_table
    );
    let error = exec_sql_json_expect_error(&sql);
    assert!(
        error.to_lowercase().contains("conflict target")
            || error.to_lowercase().contains("primary key"),
        "expected missing conflict target error, got: {error}"
    );

    fixture.drop();
}

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_non_primary_key_target_errors() {
    if !is_server_running() {
        eprintln!("Skipping smoke_on_conflict_non_primary_key_target_errors: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    let sql = format!(
        "INSERT INTO {} (id, name) VALUES (1, 'Nader') ON CONFLICT (name) DO UPDATE SET name = \
         EXCLUDED.name RETURNING *",
        fixture.full_table
    );
    let error = exec_sql_json_expect_error(&sql);
    assert!(
        error.to_lowercase().contains("primary key")
            || error.to_lowercase().contains("conflict target")
            || error.to_lowercase().contains("unique"),
        "expected non-primary-key conflict target error, got: {error}"
    );

    fixture.drop();
}

#[ntest::timeout(180000)]
#[test]
fn smoke_on_conflict_shared_table_global_conflict() {
    if !is_server_running() {
        eprintln!("Skipping smoke_on_conflict_shared_table_global_conflict: server not running");
        return;
    }

    let fixture = UsersFixture::new_shared();
    exec_sql_json(&format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'First', 1)",
        fixture.full_table
    ));

    let second = format!(
        "INSERT INTO {} (id, name, age) VALUES (1, 'Second', 2) ON CONFLICT (id) DO UPDATE SET \
         name = EXCLUDED.name, age = EXCLUDED.age RETURNING id, name, age",
        fixture.full_table
    );
    let rows = result_rows(&exec_sql_json(&second));
    assert_eq!(rows.len(), 1);
    assert_row_values(&rows[0], 1, "Second", 2);

    let stored = result_rows(&exec_sql_json(&format!(
        "SELECT id, name, age FROM {} WHERE id = 1",
        fixture.full_table
    )));
    assert_row_values(&stored[0], 1, "Second", 2);

    fixture.drop();
}
