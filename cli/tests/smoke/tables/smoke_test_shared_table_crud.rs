// Smoke Test 2: Shared table CRUD
// Covers: namespace creation, shared table creation, insert/select, delete/update, final select,
// drop table

use std::time::Duration;

use crate::common::*;

#[ntest::timeout(180000)]
#[test]
fn smoke_shared_table_crud() {
    if !is_server_running() {
        println!("Skipping smoke_shared_table_crud: server not running at {}", server_url());
        return;
    }

    // Unique names per run
    let namespace = generate_unique_namespace("smoke_ns");
    let table = generate_unique_table("shared_crud");
    let full = format!("{}.{}", namespace, table);

    // 0) Create namespace
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("create namespace should succeed");

    // 1) Create shared table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR NOT NULL,
            status VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (
            TYPE = 'SHARED',
            FLUSH_POLICY = 'rows:10'
        )"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create shared table should succeed");
    wait_for_table_ready(&full, Duration::from_secs(3)).expect("table should be ready");

    // 2) Insert rows
    let ins1 = format!("INSERT INTO {} (name, status) VALUES ('alpha', 'new')", full);
    let ins2 = format!("INSERT INTO {} (name, status) VALUES ('beta', 'new')", full);
    execute_sql_as_root_via_client(&ins1).expect("insert alpha should succeed");
    execute_sql_as_root_via_client(&ins2).expect("insert beta should succeed");

    // 3) SELECT and verify both rows present
    let sel_all = format!("SELECT * FROM {}", full);
    let out = execute_sql_as_root_via_client(&sel_all).expect("select should succeed");
    assert!(out.contains("alpha"), "expected 'alpha' in results: {}", out);
    assert!(out.contains("beta"), "expected 'beta' in results: {}", out);

    // 4) Retrieve ids for rows we will mutate (backend requires primary key equality for
    //    UPDATE/DELETE)
    // Use JSON output for reliable parsing
    let id_sel =
        format!("SELECT id, name FROM {} WHERE name IN ('alpha','beta') ORDER BY name", full);
    let id_out_json =
        execute_sql_as_root_via_client_json(&id_sel).expect("id select should succeed");

    // Parse JSON response to extract IDs
    let json_value: serde_json::Value =
        serde_json::from_str(&id_out_json).expect("Failed to parse JSON response");
    let rows = json_value
        .get("results")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|res| res.get("rows"))
        .and_then(|v| v.as_array())
        .expect("Expected rows in JSON response");

    let mut alpha_id: Option<String> = None;
    let mut beta_id: Option<String> = None;
    for row in rows {
        // Rows are arrays: [id, name] based on "SELECT id, name FROM ..."
        let row_arr = row.as_array().expect("Expected row to be an array");
        let id_value = row_arr.first().cloned().unwrap_or(serde_json::Value::Null);
        let name_value = row_arr.get(1).cloned().unwrap_or(serde_json::Value::Null);
        let name = name_value.as_str().unwrap_or("");
        let id = json_value_as_id(&id_value);
        if let Some(id_val) = id {
            if name == "alpha" {
                alpha_id = Some(id_val.clone());
            }
            if name == "beta" {
                beta_id = Some(id_val);
            }
        }
    }
    let alpha_id = alpha_id.expect("alpha id parsed");
    let beta_id = beta_id.expect("beta id parsed");

    // 5) DELETE one row via id
    let del = format!("DELETE FROM {} WHERE id = {}", full, alpha_id);
    execute_sql_as_root_via_client(&del).expect("delete should succeed");

    // 6) UPDATE one row via id
    let upd = format!("UPDATE {} SET status='done' WHERE id = {}", full, beta_id);
    execute_sql_as_root_via_client(&upd).expect("update should succeed");

    // 7) SELECT non-deleted rows and verify contents reflect changes
    let out2 = execute_sql_as_root_via_client(&sel_all).expect("second select should succeed");
    assert!(out2.contains("beta"), "expected 'beta' to remain: {}", out2);
    assert!(out2.contains("done"), "expected updated status 'done': {}", out2);

    // 8) DROP TABLE and verify selecting fails
    let drop_sql = format!("DROP TABLE {}", full);
    execute_sql_as_root_via_client(&drop_sql).expect("drop table should succeed");

    let select_after_drop = execute_sql_as_root_via_client(&sel_all);
    match select_after_drop {
        Ok(s) => panic!("expected failure selecting dropped table, got output: {}", s),
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("not found")
                    || msg.contains("does not exist")
                    || msg.contains("unknown table"),
                "unexpected error selecting dropped table: {}",
                msg
            );
        },
    }

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
