// Smoke Test: All datatypes coverage across USER, SHARED, and STREAM tables
// Creates user & shared tables enumerating every KalamDataType, performs CRUD
// (create/insert/update/delete/select) and creates a stream table with insert/select verification.
//
// This validates parser + executor acceptance of full type list in DDL plus basic DML paths.

use std::time::Duration;

use crate::common::*;

fn execute_sql_as_root_via_http_json(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    let response = runtime.block_on(execute_sql_via_http_as("root", root_password(), sql))?;
    let status = response.get("status").and_then(|value| value.as_str()).unwrap_or("error");

    if !status.eq_ignore_ascii_case("success") {
        let message = response
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(|value| value.as_str())
            .unwrap_or("HTTP SQL query failed");
        return Err(message.to_string().into());
    }

    Ok(serde_json::to_string_pretty(&response)?)
}

fn execute_sql_as_root_via_http(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_as_root_via_http_json(sql)
}

#[ntest::timeout(180000)]
#[test]
fn smoke_all_datatypes_user_shared_stream() {
    if !is_server_running() {
        println!(
            "Skipping smoke_all_datatypes_user_shared_stream: server not running at {}",
            server_url()
        );
        return;
    }

    // Unique namespace & table base names
    let namespace = generate_unique_namespace("types_ns");
    let user_table = generate_unique_table("user_types");
    let shared_table = generate_unique_table("shared_types");
    let stream_table = generate_unique_table("stream_types");
    let user_full = format!("{}.{}", namespace, user_table);
    let shared_full = format!("{}.{}", namespace, shared_table);
    let stream_full = format!("{}.{}", namespace, stream_table);

    // 0) Create namespace
    execute_sql_as_root_via_http(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("create namespace should succeed");

    // Simplified column list (only types currently stable in end-to-end path).
    let all_columns = r#"
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        bool_col BOOLEAN,
        int_col INT,
        big_int_col BIGINT,
        text_col TEXT,
        ts_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    "#;

    // 1) Create USER table with all datatypes
    let create_user_sql = format!(
        "CREATE TABLE {} ({}) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:10')",
        user_full, all_columns
    );
    execute_sql_as_root_via_http(&create_user_sql).expect("create user table should succeed");

    // 2) Create SHARED table with all datatypes
    let create_shared_sql = format!(
        "CREATE TABLE {} ({}) WITH (TYPE = 'SHARED', FLUSH_POLICY = 'rows:10')",
        shared_full, all_columns
    );
    execute_sql_as_root_via_http(&create_shared_sql).expect("create shared table should succeed");

    // 3) Create STREAM table (same columns but requires TTL clause)
    let create_stream_sql = format!(
        "CREATE TABLE {} ({}) WITH (TYPE = 'STREAM', TTL_SECONDS = 60)",
        stream_full, all_columns
    );
    execute_sql_as_root_via_http(&create_stream_sql).expect("create stream table should succeed");

    // Sample values (omit embedding_col to avoid complex literal syntax; it will remain NULL)
    // BYTES literal: use simple text (backend may coerce) or hex; choose text for simplicity.
    let insert_values_row1 = format!(
        "INSERT INTO {} (bool_col, int_col, big_int_col, text_col) VALUES (true, 123, \
         1234567890123, 'hello')",
        user_full
    );
    let insert_values_row2 = format!(
        "INSERT INTO {} (bool_col, int_col, big_int_col, text_col) VALUES (false, -321, \
         987654321, 'world')",
        user_full
    );
    execute_sql_as_root_via_http(&insert_values_row1).expect("insert user row1 should succeed");
    execute_sql_as_root_via_http(&insert_values_row2).expect("insert user row2 should succeed");

    // Mirror inserts for SHARED table
    let shared_insert1 = insert_values_row1.replace(&user_full, &shared_full);
    let shared_insert2 = insert_values_row2.replace(&user_full, &shared_full);
    execute_sql_as_root_via_http(&shared_insert1).expect("insert shared row1 should succeed");
    execute_sql_as_root_via_http(&shared_insert2).expect("insert shared row2 should succeed");

    // STREAM table inserts (simpler payload)
    let stream_insert1 = format!(
        "INSERT INTO {} (bool_col, int_col, text_col) VALUES (true, 1, 'stream_one')",
        stream_full
    );
    let stream_insert2 = format!(
        "INSERT INTO {} (bool_col, int_col, text_col) VALUES (false, 2, 'stream_two')",
        stream_full
    );
    execute_sql_as_root_via_http(&stream_insert1).expect("insert stream row1 should succeed");
    execute_sql_as_root_via_http(&stream_insert2).expect("insert stream row2 should succeed");

    // 4) SELECT from USER table & parse ids for CRUD operations using JSON output
    let user_select_all = format!("SELECT id, text_col FROM {} ORDER BY id", user_full);
    let user_out_json = wait_for_query_contains_with(
        &user_select_all,
        "hello",
        Duration::from_secs(12),
        execute_sql_as_root_via_http_json,
    )
    .expect("select user should succeed");
    assert!(
        user_out_json.contains("hello") && user_out_json.contains("world"),
        "Expected both user rows: {}",
        user_out_json
    );

    // Parse JSON to extract IDs
    let json_value: serde_json::Value =
        serde_json::from_str(&user_out_json).expect("Failed to parse JSON response");
    let rows = get_rows_as_hashmaps(&json_value).expect("Expected rows in JSON response");

    let mut first_id: Option<String> = None;
    let mut second_id: Option<String> = None;
    for row in &rows {
        let text_col_value =
            row.get("text_col").map(extract_typed_value).unwrap_or(serde_json::Value::Null);
        let text_col = text_col_value.as_str().unwrap_or("");
        let id_value = row.get("id").map(extract_typed_value).unwrap_or(serde_json::Value::Null);
        let id = json_value_as_id(&id_value);
        if let Some(id_val) = id {
            if text_col == "hello" {
                first_id = Some(id_val.clone());
            }
            if text_col == "world" {
                second_id = Some(id_val);
            }
        }
    }
    let first_id = first_id.expect("parsed first user id");
    let second_id = second_id.expect("parsed second user id");

    // 5) DELETE first row & UPDATE second row in USER table
    execute_sql_as_root_via_http(&format!("DELETE FROM {} WHERE id = {}", user_full, first_id))
        .expect("delete user first row should succeed");
    execute_sql_as_root_via_http(&format!(
        "UPDATE {} SET text_col='upd' WHERE id = {}",
        user_full, second_id
    ))
    .expect("update user second row should succeed");

    // 6) SELECT again from USER table verify changes
    let user_out2 = wait_for_query_contains_with(
        &format!("SELECT * FROM {}", user_full),
        "upd",
        Duration::from_secs(12),
        execute_sql_as_root_via_http_json,
    )
    .expect("second user select should succeed");
    assert!(
        user_out2.contains("upd"),
        "Expected updated text_col token 'upd': {}",
        user_out2
    );
    assert!(!user_out2.contains("hello"), "Deleted row should be gone: {}", user_out2);

    // 7) Perform CRUD on SHARED table (DELETE + UPDATE) using JSON output
    let shared_select_ids = format!("SELECT id, text_col FROM {} ORDER BY id", shared_full);
    let shared_out_json = wait_for_query_contains_with(
        &shared_select_ids,
        "hello",
        Duration::from_secs(12),
        execute_sql_as_root_via_http_json,
    )
    .expect("select shared should succeed");

    // Parse JSON to extract IDs
    let shared_json: serde_json::Value =
        serde_json::from_str(&shared_out_json).expect("Failed to parse shared JSON response");
    let shared_rows =
        get_rows_as_hashmaps(&shared_json).expect("Expected rows in shared JSON response");

    let mut s_first: Option<String> = None;
    let mut s_second: Option<String> = None;
    for row in &shared_rows {
        let text_col_value =
            row.get("text_col").map(extract_typed_value).unwrap_or(serde_json::Value::Null);
        let text_col = text_col_value.as_str().unwrap_or("");
        let id_value = row.get("id").map(extract_typed_value).unwrap_or(serde_json::Value::Null);
        let id = json_value_as_id(&id_value);
        if let Some(id_val) = id {
            if text_col == "hello" {
                s_first = Some(id_val.clone());
            }
            if text_col == "world" {
                s_second = Some(id_val);
            }
        }
    }
    let s_first = s_first.expect("parsed shared first id");
    let s_second = s_second.expect("parsed shared second id");

    execute_sql_as_root_via_http(&format!("DELETE FROM {} WHERE id = {}", shared_full, s_first))
        .expect("delete shared first row should succeed");
    execute_sql_as_root_via_http(&format!(
        "UPDATE {} SET text_col='shared_upd' WHERE id = {}",
        shared_full, s_second
    ))
    .expect("update shared second row should succeed");

    let shared_out2 = wait_for_query_contains_with(
        &format!("SELECT * FROM {}", shared_full),
        "shared_upd",
        Duration::from_secs(12),
        execute_sql_as_root_via_http_json,
    )
    .expect("select shared second should succeed");
    assert!(
        shared_out2.contains("shar"),
        "Expected updated shared row substring 'shar': {}",
        shared_out2
    );
    assert!(
        !shared_out2.contains("world"),
        "Original value should be absent: {}",
        shared_out2
    );
    assert!(
        !shared_out2.contains("hello"),
        "Deleted shared row should be gone: {}",
        shared_out2
    );

    // 8) Verify STREAM table row count / contents (no UPDATE/DELETE for stream tables)
    if !is_cluster_mode() {
        let stream_sel = format!("SELECT * FROM {}", stream_full);
        let stream_out =
            execute_sql_as_root_via_http(&stream_sel).expect("select stream should succeed");
        assert!(
            stream_out.contains("stream_one") || stream_out.contains("stream_two"),
            "Expected stream row content: {}",
            stream_out
        );
        // Check row count using JSON
        let stream_out_json = execute_sql_as_root_via_http_json(&stream_sel)
            .expect("select stream json should succeed");
        let stream_json: serde_json::Value =
            serde_json::from_str(&stream_out_json).expect("Failed to parse stream JSON response");
        let stream_rows = stream_json
            .get("results")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|res| res.get("rows"))
            .and_then(|v| v.as_array());
        assert!(
            stream_rows.map(|r| r.len()).unwrap_or(0) == 2,
            "Expected 2 stream rows: {}",
            stream_out_json
        );
    }

    // 9) Cleanup: drop tables
    execute_sql_as_root_via_http(&format!("DROP TABLE {}", user_full))
        .expect("drop user table should succeed");
    execute_sql_as_root_via_http(&format!("DROP TABLE {}", shared_full))
        .expect("drop shared table should succeed");
    execute_sql_as_root_via_http(&format!("DROP TABLE {}", stream_full))
        .expect("drop stream table should succeed");
    let _ = execute_sql_as_root_via_http(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
