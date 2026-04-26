use crate::common::*;

// Smoke Test 5: User table per-user isolation (RLS)
// Steps:
// 0) As root: create namespace
// 1) As root: create a user table
// 2) As root: insert several rows
// 3) Create a new regular user
// 4) Login via CLI as the regular user
// 5) As regular user: insert multiple rows, update one, delete one, then SELECT all
// 6) Verify: (a) regular user can insert, (b) login succeeds, (c) SELECT shows only this user's
//    rows (no root rows)
#[ntest::timeout(180000)]
#[test]
fn smoke_user_table_rls_isolation() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_user_table_rls_isolation: server not running at {}",
            server_url()
        );
        return;
    }

    // Unique namespace/table and user credentials per run
    let namespace = generate_unique_namespace("smoke_rls_ns");
    let table = generate_unique_table("smoke_rls_tbl");
    let full_table = format!("{}.{}", namespace, table);

    let user_name = generate_unique_namespace("smoke_user");
    let user_pass = "smoke_pass_123";

    // 0) As root: create namespace
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // 1) As root: create a user table
    let create_table_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            content TEXT NOT NULL,
            updated INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("Failed to create table");

    // 2) As root: insert several rows
    let root_rows = vec!["root_row_1", "root_row_2", "root_row_3"];
    for val in &root_rows {
        let ins = format!("INSERT INTO {} (content) VALUES ('{}')", full_table, val);
        execute_sql_as_root_via_client(&ins).expect("Failed to insert root row");
    }

    // 3) Create a new regular user
    let create_user_sql =
        format!("CREATE USER {} WITH PASSWORD '{}' ROLE 'user'", user_name, user_pass);
    execute_sql_as_root_via_client(&create_user_sql).expect("Failed to create user");

    // 4) Login via CLI as the regular user (implicit via next commands)
    // Validate auth by running a trivial command
    let _ = execute_sql_via_client_as(&user_name, user_pass, "SELECT 1")
        .expect("Failed to login as user");

    // 5) As regular user: insert multiple rows
    let user_rows = vec!["user_row_a", "user_row_b", "user_row_c"];
    for val in &user_rows {
        let ins = format!("INSERT INTO {} (content) VALUES ('{}')", full_table, val);
        // Retry on network errors (server may be under load from concurrent tests)
        let mut attempts = 0;
        let max_attempts = 2;
        loop {
            match execute_sql_via_client_as(&user_name, user_pass, &ins) {
                Ok(_) => break,
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        panic!(
                            "Failed to insert user row after {} attempts: {:?}",
                            max_attempts, e
                        );
                    }
                    eprintln!("Insert attempt {} failed, retrying: {}", attempts, e);
                    std::thread::sleep(std::time::Duration::from_millis(100 * attempts as u64));
                },
            }
        }
    }

    // Fetch ids for the specific user rows so we can perform id-based UPDATE/DELETE
    // (backend currently restricts USER table UPDATE/DELETE to primary key equality predicates)
    // Use JSON output for reliable parsing
    let id_query = format!(
        "SELECT id, content FROM {} WHERE content IN ('user_row_b','user_row_c') ORDER BY content",
        full_table
    );
    let id_out_json = execute_sql_via_client_as_json(&user_name, user_pass, &id_query)
        .expect("Failed to query user rows");

    // Parse JSON response to extract IDs
    let json_value: serde_json::Value =
        serde_json::from_str(&id_out_json).expect("Failed to parse JSON response");
    let rows = get_rows_as_hashmaps(&json_value).expect("Expected rows in JSON response");

    let mut row_b_id: Option<String> = None;
    let mut row_c_id: Option<String> = None;
    for row in &rows {
        let content_value =
            row.get("content").map(extract_typed_value).unwrap_or(serde_json::Value::Null);
        let content = content_value.as_str().unwrap_or("");
        let id_value = row.get("id").map(extract_typed_value).unwrap_or(serde_json::Value::Null);
        let id = json_value_as_id(&id_value);
        if let Some(id_val) = id {
            if content == "user_row_b" {
                row_b_id = Some(id_val.clone());
            }
            if content == "user_row_c" {
                row_c_id = Some(id_val);
            }
        }
    }
    let row_b_id = row_b_id.unwrap_or_else(|| {
        panic!("Failed to parse id for user_row_b from output: {}", id_out_json)
    });
    let row_c_id = row_c_id.unwrap_or_else(|| {
        panic!("Failed to parse id for user_row_c from output: {}", id_out_json)
    });

    // Update one of the user's rows (set updated=1) using id predicate
    let upd = format!("UPDATE {} SET updated = 1 WHERE id = {}", full_table, row_b_id);
    execute_sql_via_client_as(&user_name, user_pass, &upd).expect("Failed to update user row");

    // Delete one of the user's rows using id predicate
    let del = format!("DELETE FROM {} WHERE id = {}", full_table, row_c_id);
    execute_sql_via_client_as(&user_name, user_pass, &del).expect("Failed to delete user row");

    // 6) SELECT as the regular user and verify visibility
    let select_out = execute_sql_via_client_as(
        &user_name,
        user_pass,
        &format!("SELECT id, content, updated FROM {} ORDER BY id", full_table),
    )
    .expect("Failed to select user rows");

    // (a) user could insert (at least one of user's values appears)
    assert!(
        select_out.contains("user_row_a") || select_out.contains("user_row_b"),
        "Expected at least one user row to be present in selection, got: {}",
        select_out
    );

    // (b) CLI login succeeded implicitly through previous commands; also ensured via SELECT 1

    // (c) ensure no root rows are visible
    for r in &root_rows {
        assert!(
            !select_out.contains(r),
            "User selection should not contain root row '{}': {}",
            r,
            select_out
        );
    }

    // Ensure update took effect and delete removed the row
    assert!(select_out.contains("user_row_b"), "Expected updated row to be present");
    assert!(!select_out.contains("user_row_c"), "Expected deleted row to be absent");

    // Cleanup (best-effort)
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user_name));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}
