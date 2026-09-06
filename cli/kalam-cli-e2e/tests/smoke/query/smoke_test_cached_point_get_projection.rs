use std::collections::HashMap;

use serde_json::Value;

use crate::common::*;

struct CleanupGuard {
    namespace: String,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let _ = execute_sql_as_root_via_client(&format!(
            "DROP NAMESPACE IF EXISTS {} CASCADE",
            self.namespace
        ));
    }
}

fn query_json(sql: &str, params: Vec<Value>) -> Value {
    let output = execute_sql_as_root_via_client_json_with_params(sql, params)
        .unwrap_or_else(|error| panic!("query failed for '{sql}': {error}"));
    parse_cli_json_output(&output)
        .unwrap_or_else(|error| panic!("parse failed for '{sql}': {error}\n{output}"))
}

fn result_schema_names(json: &Value) -> Vec<String> {
    json.get("results")
        .and_then(|results| results.as_array())
        .and_then(|results| results.first())
        .and_then(|result| result.get("schema"))
        .and_then(|schema| schema.as_array())
        .map(|schema| {
            schema
                .iter()
                .filter_map(|column| column.get("name")?.as_str().map(ToOwned::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn query_rows(sql: &str, params: Vec<Value>) -> (Vec<String>, Vec<HashMap<String, Value>>) {
    let json = query_json(sql, params);
    let names = result_schema_names(&json);
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
    (names, rows)
}

fn typed_value(row: &HashMap<String, Value>, column: &str) -> Value {
    let value = row
        .get(column)
        .unwrap_or_else(|| panic!("expected column '{column}' in row {row:?}"));
    extract_typed_value(value)
}

fn string_value(row: &HashMap<String, Value>, column: &str) -> String {
    match typed_value(row, column) {
        Value::String(text) => text,
        other => panic!("expected '{column}' to be a string, got {other:?}"),
    }
}

fn file_ref_sha256(row: &HashMap<String, Value>) -> String {
    match typed_value(row, "file_ref") {
        Value::String(text) => serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|parsed| parsed.get("sha256")?.as_str().map(ToOwned::to_owned))
            .unwrap_or(text),
        Value::Object(map) => map
            .get("sha256")
            .and_then(|sha| sha.as_str())
            .unwrap_or_else(|| panic!("file_ref object missing sha256: {map:?}"))
            .to_string(),
        other => panic!("file_ref was not FileRef JSON, got {other:?}"),
    }
}

fn file_ref_json(id: &str, sha256: &str) -> String {
    format!(
        r#"{{"id":"{id}","sub":"f0001","name":"index.md","size":12,"mime":"text/markdown","sha256":"{sha256}"}}"#
    )
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn assert_columns(names: &[String], expected: &[&str]) {
    for column in expected {
        assert!(
            names.iter().any(|name| name == column),
            "expected column '{column}' in schema {names:?}"
        );
    }
}

#[ntest::timeout(180000)]
#[test]
fn smoke_test_cached_point_get_projection() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_cached_point_get_projection: server not running at {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_point_get");
    let files = generate_unique_table("context_files");
    let blogs = generate_unique_table("blogs");
    let shared = generate_unique_table("items");
    let stream = generate_unique_table("events");
    let _cleanup = CleanupGuard {
        namespace: namespace.clone(),
    };

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {namespace}"))
        .expect("CREATE NAMESPACE should succeed");

    let files_table = format!("{namespace}.{files}");
    let blogs_table = format!("{namespace}.{blogs}");
    let shared_table = format!("{namespace}.{shared}");
    let stream_table = format!("{namespace}.{stream}");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {files_table} (path TEXT PRIMARY KEY, file_ref FILE NOT NULL, body TEXT) \
         WITH (TYPE = 'USER')"
    ))
    .expect("CREATE USER FILE table should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {blogs_table} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE = 'USER')"
    ))
    .expect("CREATE USER blogs table should succeed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {shared_table} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='SHARED')"
    ))
    .expect("CREATE SHARED table should succeed");
    grant_public_select_shared_table(&shared_table);
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {stream_table} (event_id TEXT PRIMARY KEY, payload TEXT) WITH (TYPE = \
         'STREAM', TTL_SECONDS = 60)"
    ))
    .expect("CREATE STREAM table should succeed");

    let first_sha = "dee5ff32e586f4517a0bad1155f89f1345f8dbda6c5c29aa80999650f2810804";
    let second_sha = "a8f1c5ed021d54b8890b36883766608261b5acf684b6decf9dfdbabeddef619c";
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {files_table} (path, file_ref, body) VALUES ('index.md', {}, 'hello')",
        sql_string(&file_ref_json("328909262921138176", first_sha))
    ))
    .expect("insert context file should succeed");
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {blogs_table} (id, name) VALUES (1, 'alpha'), (2, 'beta')"
    ))
    .expect("insert blogs should succeed");
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {shared_table} (id, name) VALUES (1, 'shared-alpha')"
    ))
    .expect("insert shared row should succeed");
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {stream_table} (event_id, payload) VALUES ('evt-1', 'stream-alpha')"
    ))
    .expect("insert stream row should succeed");

    let select_file_ref = format!("SELECT file_ref FROM {files_table} WHERE path = $1");
    let path_param = vec![Value::String("index.md".to_string())];
    for i in 0..5 {
        let (names, rows) = query_rows(&select_file_ref, path_param.clone());
        assert_eq!(names, vec!["file_ref"], "file_ref lookup {i} schema");
        assert_eq!(rows.len(), 1, "file_ref lookup {i} row count");
        assert_eq!(
            file_ref_sha256(&rows[0]),
            first_sha,
            "file_ref lookup {i} must not return the path PK as file_ref"
        );
    }

    execute_sql_as_root_via_client(&format!(
        "UPDATE {files_table} SET file_ref = {}, body = 'updated' WHERE path = 'index.md'",
        sql_string(&file_ref_json("328909262921138177", second_sha))
    ))
    .expect("update context file should succeed");

    let (names, rows) = query_rows(&select_file_ref, path_param.clone());
    assert_eq!(names, vec!["file_ref"]);
    assert_eq!(
        file_ref_sha256(&rows[0]),
        second_sha,
        "cached SELECT after UPDATE must see new hash"
    );

    let (names, rows) = query_rows(
        &format!("SELECT file_ref FROM {files_table} WHERE path = $1"),
        vec![Value::String("missing.md".to_string())],
    );
    assert_eq!(names, vec!["file_ref"]);
    assert!(rows.is_empty(), "missing PK must return no file_ref row");

    let (names, rows) =
        query_rows(&format!("SELECT * FROM {files_table} WHERE path = $1"), path_param.clone());
    assert_columns(&names, &["path", "file_ref", "body"]);
    assert_eq!(string_value(&rows[0], "path"), "index.md");
    assert_eq!(file_ref_sha256(&rows[0]), second_sha);
    assert_eq!(string_value(&rows[0], "body"), "updated");

    let (names, rows) = query_rows(
        &format!("SELECT file_ref, path, body FROM {files_table} WHERE path = $1"),
        path_param.clone(),
    );
    assert_eq!(names, vec!["file_ref", "path", "body"]);
    assert_eq!(file_ref_sha256(&rows[0]), second_sha);
    assert_eq!(string_value(&rows[0], "path"), "index.md");

    let (names, rows) =
        query_rows(&format!("SELECT path FROM {files_table} WHERE path = $1"), path_param.clone());
    assert_eq!(names, vec!["path"]);
    assert_eq!(string_value(&rows[0], "path"), "index.md");

    let literal_sql = format!("SELECT file_ref FROM {files_table} WHERE path = 'index.md'");
    let first_literal = query_rows(&literal_sql, Vec::new());
    let cached_literal = query_rows(&literal_sql, Vec::new());
    assert_eq!(first_literal.0, vec!["file_ref"]);
    assert_eq!(file_ref_sha256(&first_literal.1[0]), second_sha);
    assert_eq!(file_ref_sha256(&cached_literal.1[0]), second_sha);

    let select_name = format!("SELECT name FROM {blogs_table} WHERE id = $1");
    for i in 0..4 {
        let (names, rows) = query_rows(&select_name, vec![Value::from(1)]);
        assert_eq!(names, vec!["name"], "blogs lookup {i} schema");
        assert_eq!(string_value(&rows[0], "name"), "alpha");
    }
    let (names, rows) = query_rows(&select_name, vec![Value::from(2)]);
    assert_eq!(names, vec!["name"]);
    assert_eq!(string_value(&rows[0], "name"), "beta");

    let alias_sql = format!("SELECT name AS title FROM {blogs_table} WHERE id = $1");
    let (names, rows) = query_rows(&alias_sql, vec![Value::from(1)]);
    let (cached_names, cached_rows) = query_rows(&alias_sql, vec![Value::from(1)]);
    assert_eq!(names, vec!["title"]);
    assert_eq!(string_value(&rows[0], "title"), "alpha");
    assert_eq!(cached_names, vec!["title"]);
    assert_eq!(string_value(&cached_rows[0], "title"), "alpha");

    let shared_sql = format!("SELECT name FROM {shared_table} WHERE id = $1");
    for i in 0..3 {
        let (names, rows) = query_rows(&shared_sql, vec![Value::from(1)]);
        assert_eq!(names, vec!["name"], "shared lookup {i}");
        assert_eq!(string_value(&rows[0], "name"), "shared-alpha");
    }

    let stream_sql = format!("SELECT payload FROM {stream_table} WHERE event_id = $1");
    for i in 0..3 {
        let (names, rows) = query_rows(&stream_sql, vec![Value::String("evt-1".to_string())]);
        assert_eq!(names, vec!["payload"], "stream lookup {i}");
        assert_eq!(string_value(&rows[0], "payload"), "stream-alpha");
    }

    let limited_sql = format!("SELECT file_ref FROM {files_table} WHERE path = $1 LIMIT 1");
    for i in 0..3 {
        let (names, rows) = query_rows(&limited_sql, path_param.clone());
        assert_eq!(names, vec!["file_ref"], "LIMIT 1 lookup {i}");
        assert_eq!(file_ref_sha256(&rows[0]), second_sha);
    }
}
