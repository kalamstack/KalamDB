use std::collections::HashMap;

use serde_json::Value;

use crate::common::*;

struct CleanupGuard {
    namespace: String,
    user: String,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", self.user));
        let _ = execute_sql_as_root_via_client(&format!(
            "DROP NAMESPACE IF EXISTS {} CASCADE",
            self.namespace
        ));
    }
}

fn query_rows(sql: &str) -> Vec<HashMap<String, Value>> {
    let json_output = execute_sql_as_root_via_client_json(sql)
        .unwrap_or_else(|error| panic!("query failed for '{}': {}", sql, error));
    let parsed = parse_cli_json_output(&json_output)
        .unwrap_or_else(|error| panic!("parse failed for '{}': {}", sql, error));
    get_rows_as_hashmaps(&parsed).unwrap_or_default()
}

fn scalar_string(row: &HashMap<String, Value>, column: &str) -> String {
    let value = extract_typed_value(
        row.get(column)
            .unwrap_or_else(|| panic!("expected column '{}' in row {:?}", column, row)),
    );
    match value {
        Value::String(text) => text,
        other => panic!("expected '{}' to be string-like, got {:?}", column, other),
    }
}

#[ntest::timeout(180000)]
#[test]
fn smoke_test_filter_pushdown() {
    if !is_server_running() {
        println!("Skipping smoke_test_filter_pushdown: server not running at {}", server_url());
        return;
    }

    let namespace = generate_unique_namespace("smoke_pushdown");
    let shared_table = generate_unique_table("items");
    let stream_table = generate_unique_table("events");
    let user = generate_unique_table("pushdown_user");
    let full_shared_table = format!("{}.{}", namespace, shared_table);
    let full_stream_table = format!("{}.{}", namespace, stream_table);
    let _cleanup = CleanupGuard {
        namespace: namespace.clone(),
        user: user.clone(),
    };

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD 'pushdown_pass_123' ROLE 'user'",
        user
    ))
    .expect("CREATE USER should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE = 'SHARED', ACCESS_LEVEL = \
         'PUBLIC')",
        full_shared_table
    ))
    .expect("CREATE SHARED TABLE should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (event_id TEXT PRIMARY KEY, payload TEXT) WITH (TYPE = 'STREAM', \
         TTL_SECONDS = 60)",
        full_stream_table
    ))
    .expect("CREATE STREAM TABLE should succeed");

    for sql in [
        format!("INSERT INTO {} (id, name) VALUES (1, 'alpha')", full_shared_table),
        format!("INSERT INTO {} (id, name) VALUES (2, 'beta')", full_shared_table),
        format!("INSERT INTO {} (id, name) VALUES (3, 'beta')", full_shared_table),
        format!(
            "INSERT INTO {} (event_id, payload) VALUES ('evt-1', 'alpha')",
            full_stream_table
        ),
        format!("INSERT INTO {} (event_id, payload) VALUES ('evt-2', 'beta')", full_stream_table),
        format!("INSERT INTO {} (event_id, payload) VALUES ('evt-3', 'beta')", full_stream_table),
    ] {
        execute_sql_as_root_via_client(&sql)
            .unwrap_or_else(|error| panic!("seed statement failed for '{}': {}", sql, error));
    }

    let system_rows =
        query_rows(&format!("SELECT user_id FROM system.users WHERE user_id = '{}'", user));
    assert_eq!(system_rows.len(), 1, "system exact filter should find one user");
    assert_eq!(scalar_string(&system_rows[0], "user_id"), user);

    let shared_pk_rows = query_rows(&format!("SELECT id FROM {} WHERE id = 2", full_shared_table));
    assert_eq!(shared_pk_rows.len(), 1, "shared PK filter should find one row");
    assert_eq!(scalar_string(&shared_pk_rows[0], "id"), "2");

    let shared_name_rows = query_rows(&format!(
        "SELECT id FROM {} WHERE name = 'beta' ORDER BY id",
        full_shared_table
    ));
    let shared_name_ids: Vec<String> =
        shared_name_rows.iter().map(|row| scalar_string(row, "id")).collect();
    assert_eq!(shared_name_ids, vec!["2".to_string(), "3".to_string()]);

    let stream_rows =
        query_rows(&format!("SELECT event_id FROM {} WHERE event_id = 'evt-2'", full_stream_table));
    assert_eq!(stream_rows.len(), 1, "stream exact filter should find one row");
    assert_eq!(scalar_string(&stream_rows[0], "event_id"), "evt-2");
}
