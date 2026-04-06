//! Smoke tests for AS USER impersonation functionality
//!
//! Tests cover against a running server:
//! - Authorization: Regular users cannot use AS USER
//! - Service/DBA roles can use AS USER
//! - INSERT AS USER creates records owned by impersonated user
//! - UPDATE AS USER modifies records as impersonated user
//! - DELETE AS USER removes records as impersonated user
//! - AS USER rejected on SHARED tables

use crate::common::*;
use std::time::Duration;

/// Helper to create a unique namespace for this test
fn create_test_namespace(suffix: &str) -> String {
    generate_unique_namespace(&format!("smoke_as_user_{}", suffix))
}

fn create_user_with_retry(username: &str, password: &str, role: &str) {
    let sql = format!("CREATE USER {} WITH PASSWORD '{}' ROLE '{}'", username, password, role);
    let mut last_err = None;
    for attempt in 0..3 {
        match execute_sql_as_root_via_client(&sql) {
            Ok(_) => return,
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("Already exists") {
                    let alter_sql = format!("ALTER USER {} SET PASSWORD '{}'", username, password);
                    let _ = execute_sql_as_root_via_client(&alter_sql);
                    return;
                }
                if msg.contains("Serialization error") || msg.contains("UnexpectedEnd") {
                    last_err = Some(msg);
                    std::thread::sleep(Duration::from_millis(200 * (attempt + 1) as u64));
                    continue;
                }
                panic!("Failed to create user {}: {}", username, msg);
            },
        }
    }
    panic!(
        "Failed to create user {} after retries: {}",
        username,
        last_err.unwrap_or_else(|| "unknown error".to_string())
    );
}

/// Helper to get user_id from username by querying system.users
/// AS USER requires the user_id (UUID), not the username
fn get_user_id_for_username(username: &str) -> Option<String> {
    let query = format!("SELECT user_id FROM system.users WHERE username = '{}'", username);
    let result = execute_sql_as_root_via_client_json(&query).ok()?;

    // Parse JSON response
    let json: serde_json::Value = serde_json::from_str(&result).ok()?;
    let rows = get_rows_as_hashmaps(&json)?;

    if let Some(row) = rows.first() {
        let user_id_value = row.get("user_id").map(extract_typed_value)?;
        return user_id_value.as_str().map(|s| s.to_string());
    }
    None
}

/// Smoke Test: Regular user CANNOT use AS USER (authorization check)
#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_blocked_for_regular_user() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_blocked_for_regular_user: server not running");
        return;
    }

    let namespace = create_test_namespace("blocked");
    let table = generate_unique_table("items");
    let full_table = format!("{}.{}", namespace, table);

    // Create a regular user and a target user
    let regular_user = generate_unique_namespace("regular");
    let target_user = generate_unique_namespace("target");
    let password = "test_pass_123";

    // Setup: create namespace, table, and users as root
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name VARCHAR) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create table");

    create_user_with_retry(&regular_user, password, "user");
    create_user_with_retry(&target_user, password, "user");

    // Get the target user's user_id (UUID)
    let target_user_id =
        get_user_id_for_username(&target_user).expect("Failed to get target user_id");

    // Attempt INSERT AS USER as regular user - should FAIL
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {} (id, name) VALUES (1, 'Test'))",
        target_user_id, full_table
    );
    let result = execute_sql_via_client_as(&regular_user, password, &insert_sql);

    assert!(
        result.is_err(),
        "Regular user should NOT be able to use AS USER, but got: {:?}",
        result
    );

    let error_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        error_msg.contains("not authorized")
            || error_msg.contains("unauthorized")
            || error_msg.contains("permission"),
        "Error should mention authorization: {}",
        error_msg
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", regular_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_as_user_blocked_for_regular_user passed!");
}

/// Smoke Test: Service role CAN use AS USER for INSERT
#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_insert_with_service_role() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_insert_with_service_role: server not running");
        return;
    }

    let namespace = create_test_namespace("svc_insert");
    let table = generate_unique_table("orders");
    let full_table = format!("{}.{}", namespace, table);

    // Create a service user and a target user
    let service_user = generate_unique_namespace("service");
    let target_user = generate_unique_namespace("target");
    let password = "test_pass_123";

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, amount VARCHAR) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create table");

    create_user_with_retry(&service_user, password, "service");
    create_user_with_retry(&target_user, password, "user");

    // Get user_ids
    let target_user_id =
        get_user_id_for_username(&target_user).expect("Failed to get target user_id");

    // INSERT AS USER target_user (executed by service user)
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {} (id, amount) VALUES (100, '99.99'))",
        target_user_id, full_table
    );
    let result = execute_sql_via_client_as(&service_user, password, &insert_sql);
    assert!(result.is_ok(), "Service role should be able to use AS USER: {:?}", result);

    // Verify: target_user can see the record (RLS)
    let select_result =
        execute_sql_via_client_as(&target_user, password, &format!("SELECT * FROM {}", full_table))
            .expect("Failed to select as target user");

    assert!(
        select_result.contains("99.99"),
        "Target user should see the inserted record: {}",
        select_result
    );

    // Verify: service user can see the record (service role can access all users)
    let service_select = execute_sql_via_client_as(
        &service_user,
        password,
        &format!("SELECT * FROM {}", full_table),
    )
    .expect("Failed to select as service user");

    assert!(
        service_select.contains("99.99"),
        "Service user should see target user's data when querying normally"
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", service_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_as_user_insert_with_service_role passed!");
}

/// Smoke Test: DBA can use AS USER for UPDATE
#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_update_with_dba_role() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_update_with_dba_role: server not running");
        return;
    }

    let namespace = create_test_namespace("dba_update");
    let table = generate_unique_table("profiles");
    let full_table = format!("{}.{}", namespace, table);

    let dba_user = generate_unique_namespace("dba");
    let target_user = generate_unique_namespace("target");
    let password = "test_pass_123";

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, status VARCHAR) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create table");

    create_user_with_retry(&dba_user, password, "dba");
    create_user_with_retry(&target_user, password, "user");

    // Get user_id
    let target_user_id =
        get_user_id_for_username(&target_user).expect("Failed to get target user_id");

    // INSERT AS USER first
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {} (id, status) VALUES (1, 'active'))",
        target_user_id, full_table
    );
    execute_sql_via_client_as(&dba_user, password, &insert_sql).expect("Failed to INSERT AS USER");

    // UPDATE AS USER
    let update_sql = format!(
        "EXECUTE AS USER '{}' (UPDATE {} SET status = 'inactive' WHERE id = 1)",
        target_user_id, full_table
    );
    let result = execute_sql_via_client_as(&dba_user, password, &update_sql);
    assert!(result.is_ok(), "DBA should be able to UPDATE AS USER: {:?}", result);

    // Verify: target_user sees the updated record
    let select_result = execute_sql_via_client_as(
        &target_user,
        password,
        &format!("SELECT * FROM {} WHERE id = 1", full_table),
    )
    .expect("Failed to select as target user");

    assert!(
        select_result.contains("inactive"),
        "Status should be updated to 'inactive': {}",
        select_result
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", dba_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_as_user_update_with_dba_role passed!");
}

/// Smoke Test: DBA can use AS USER for DELETE
#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_delete_with_dba_role() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_delete_with_dba_role: server not running");
        return;
    }

    let namespace = create_test_namespace("dba_delete");
    let table = generate_unique_table("sessions");
    let full_table = format!("{}.{}", namespace, table);

    let dba_user = generate_unique_namespace("dba");
    let target_user = generate_unique_namespace("target");
    let password = "test_pass_123";

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, active BOOLEAN) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create table");

    create_user_with_retry(&dba_user, password, "dba");
    create_user_with_retry(&target_user, password, "user");

    // INSERT AS USER first
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {} (id, active) VALUES (1, true))",
        target_user, full_table
    );
    execute_sql_via_client_as(&dba_user, password, &insert_sql).expect("Failed to INSERT AS USER");

    // Verify record exists
    let before_delete =
        execute_sql_via_client_as(&target_user, password, &format!("SELECT * FROM {}", full_table))
            .expect("Failed to select before delete");
    assert!(
        before_delete.contains("true") || before_delete.contains("1"),
        "Record should exist before delete: {}",
        before_delete
    );

    // DELETE AS USER
    let delete_sql =
        format!("EXECUTE AS USER '{}' (DELETE FROM {} WHERE id = 1)", target_user, full_table);
    let result = execute_sql_via_client_as(&dba_user, password, &delete_sql);
    assert!(result.is_ok(), "DBA should be able to DELETE AS USER: {:?}", result);

    // Verify: record is deleted
    let after_delete =
        execute_sql_via_client_as(&target_user, password, &format!("SELECT * FROM {}", full_table))
            .expect("Failed to select after delete");

    assert!(
        after_delete.contains("0 rows") || !after_delete.contains("true"),
        "Record should be deleted: {}",
        after_delete
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", dba_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_as_user_delete_with_dba_role passed!");
}

/// Smoke Test: AS USER rejected on SHARED tables
#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_rejected_on_shared_table() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_rejected_on_shared_table: server not running");
        return;
    }

    let namespace = create_test_namespace("shared_reject");
    let table = generate_unique_table("config");
    let full_table = format!("{}.{}", namespace, table);

    let dba_user = generate_unique_namespace("dba");
    let target_user = generate_unique_namespace("target");
    let password = "test_pass_123";

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    // Create SHARED table (not USER table)
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (config_key VARCHAR PRIMARY KEY, config_value VARCHAR) WITH (TYPE='SHARED')",
        full_table
    ))
    .expect("Failed to create SHARED table");

    create_user_with_retry(&dba_user, password, "dba");
    create_user_with_retry(&target_user, password, "user");

    // Get user_id
    let target_user_id =
        get_user_id_for_username(&target_user).expect("Failed to get target user_id");

    // Attempt INSERT AS USER on SHARED table - should FAIL
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {} (config_key, config_value) VALUES ('k1', 'v1'))",
        target_user_id, full_table
    );
    let result = execute_sql_via_client_as(&dba_user, password, &insert_sql);

    assert!(
        result.is_err(),
        "AS USER should be rejected on SHARED tables, but got: {:?}",
        result
    );

    let error_msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        error_msg.contains("shared") || error_msg.contains("as user"),
        "Error should mention SHARED tables or AS USER restriction: {}",
        error_msg
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", dba_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_as_user_rejected_on_shared_table passed!");
}

/// Smoke Test: Full AS USER workflow - INSERT, UPDATE, DELETE with ownership verification
#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_full_workflow() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_full_workflow: server not running");
        return;
    }

    let namespace = create_test_namespace("full_workflow");
    let table = generate_unique_table("tasks");
    let full_table = format!("{}.{}", namespace, table);

    let service_user = generate_unique_namespace("service");
    let user_alice = generate_unique_namespace("alice");
    let user_bob = generate_unique_namespace("bob");
    let password = "test_pass_123";

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, title VARCHAR, done BOOLEAN) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create table");

    create_user_with_retry(&service_user, password, "service");
    create_user_with_retry(&user_alice, password, "user");
    create_user_with_retry(&user_bob, password, "user");

    // Get user_ids
    let alice_user_id = get_user_id_for_username(&user_alice).expect("Failed to get alice user_id");
    let bob_user_id = get_user_id_for_username(&user_bob).expect("Failed to get bob user_id");

    // 1. INSERT AS USER alice (service user acting on behalf of alice)
    execute_sql_via_client_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, title, done) VALUES (1, 'Alice Task 1', false))",
            alice_user_id, full_table
        ),
    )
    .expect("Failed to INSERT AS USER alice");

    execute_sql_via_client_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, title, done) VALUES (2, 'Alice Task 2', false))",
            alice_user_id, full_table
        ),
    )
    .expect("Failed to INSERT second task AS USER alice");

    // 2. INSERT AS USER bob
    execute_sql_via_client_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, title, done) VALUES (10, 'Bob Task 1', false))",
            bob_user_id, full_table
        ),
    )
    .expect("Failed to INSERT AS USER bob");

    // 3. Verify RLS: Alice sees only her tasks
    let alice_select = execute_sql_via_client_as(
        &user_alice,
        password,
        &format!("SELECT * FROM {} ORDER BY id", full_table),
    )
    .expect("Failed to select as alice");

    assert!(
        alice_select.contains("Alice Task 1") && alice_select.contains("Alice Task 2"),
        "Alice should see her tasks: {}",
        alice_select
    );
    assert!(
        !alice_select.contains("Bob Task"),
        "Alice should NOT see Bob's tasks: {}",
        alice_select
    );

    // 4. Verify RLS: Bob sees only his tasks
    let bob_select = execute_sql_via_client_as(
        &user_bob,
        password,
        &format!("SELECT * FROM {} ORDER BY id", full_table),
    )
    .expect("Failed to select as bob");

    assert!(bob_select.contains("Bob Task 1"), "Bob should see his task: {}", bob_select);
    assert!(
        !bob_select.contains("Alice Task"),
        "Bob should NOT see Alice's tasks: {}",
        bob_select
    );

    // 5. UPDATE AS USER alice (mark task 1 as done)
    execute_sql_via_client_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (UPDATE {} SET done = true WHERE id = 1)",
            alice_user_id, full_table
        ),
    )
    .expect("Failed to UPDATE AS USER alice");

    // 6. Verify update
    let alice_after_update = execute_sql_via_client_as(
        &user_alice,
        password,
        &format!("SELECT * FROM {} WHERE id = 1", full_table),
    )
    .expect("Failed to select after update");

    assert!(
        alice_after_update.contains("true") || alice_after_update.contains("1"),
        "Task 1 should be marked as done: {}",
        alice_after_update
    );

    // 7. DELETE AS USER alice (remove task 2)
    execute_sql_via_client_as(
        &service_user,
        password,
        &format!("EXECUTE AS USER '{}' (DELETE FROM {} WHERE id = 2)", alice_user_id, full_table),
    )
    .expect("Failed to DELETE AS USER alice");

    // 8. Verify delete
    let alice_after_delete =
        execute_sql_via_client_as(&user_alice, password, &format!("SELECT * FROM {}", full_table))
            .expect("Failed to select after delete");

    assert!(
        !alice_after_delete.contains("Alice Task 2"),
        "Task 2 should be deleted: {}",
        alice_after_delete
    );
    assert!(
        alice_after_delete.contains("Alice Task 1"),
        "Task 1 should still exist: {}",
        alice_after_delete
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", service_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_alice));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_bob));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_as_user_full_workflow passed!");
}

/// Smoke Test: SELECT AS USER scopes reads to the impersonated subject on USER tables
#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_select_scopes_reads_for_user_tables() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_select_scopes_reads_for_user_tables: server not running");
        return;
    }

    let namespace = create_test_namespace("select_scope_user");
    let table = generate_unique_table("messages");
    let full_table = format!("{}.{}", namespace, table);

    let service_user = generate_unique_namespace("service");
    let user1 = generate_unique_namespace("user1");
    let user2 = generate_unique_namespace("user2");
    let password = "test_pass_123";

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, body VARCHAR) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create table");

    create_user_with_retry(&service_user, password, "service");
    create_user_with_retry(&user1, password, "user");
    create_user_with_retry(&user2, password, "user");

    let user1_id = get_user_id_for_username(&user1).expect("Failed to get user1 user_id");
    let user2_id = get_user_id_for_username(&user2).expect("Failed to get user2 user_id");

    execute_sql_via_client_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, body) VALUES (1, 'u1-only'))",
            user1_id, full_table
        ),
    )
    .expect("Failed to insert row for user1");

    execute_sql_via_client_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, body) VALUES (2, 'u2-only'))",
            user2_id, full_table
        ),
    )
    .expect("Failed to insert row for user2");

    let service_all = execute_sql_via_client_as(
        &service_user,
        password,
        &format!("SELECT * FROM {} ORDER BY id", full_table),
    )
    .expect("Service select failed");
    assert!(service_all.contains("u1-only") && service_all.contains("u2-only"));

    let service_as_user1 = execute_sql_via_client_as(
        &service_user,
        password,
        &format!("EXECUTE AS USER '{}' (SELECT * FROM {} ORDER BY id)", user1_id, full_table),
    )
    .expect("Service SELECT AS USER user1 failed");
    assert!(service_as_user1.contains("u1-only"));
    assert!(!service_as_user1.contains("u2-only"));

    let user2_view = execute_sql_via_client_as(
        &user2,
        password,
        &format!("SELECT * FROM {} ORDER BY id", full_table),
    )
    .expect("User2 select failed");
    assert!(user2_view.contains("u2-only"));
    assert!(!user2_view.contains("u1-only"));

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", service_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user1));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user2));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_as_user_select_scopes_reads_for_user_tables passed!");
}

/// Smoke Test: USER isolation also holds for STREAM tables (direct and AS USER reads)
#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_stream_table_isolation() {
    if !is_server_running() {
        eprintln!("Skipping smoke_as_user_stream_table_isolation: server not running");
        return;
    }

    let namespace = create_test_namespace("select_scope_stream");
    let table = generate_unique_table("events");
    let full_table = format!("{}.{}", namespace, table);

    let service_user = generate_unique_namespace("service");
    let user1 = generate_unique_namespace("user1");
    let user2 = generate_unique_namespace("user2");
    let password = "test_pass_123";

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, payload VARCHAR) WITH (TYPE='STREAM', TTL_SECONDS=3600)",
        full_table
    ))
    .expect("Failed to create stream table");

    create_user_with_retry(&service_user, password, "service");
    create_user_with_retry(&user1, password, "user");
    create_user_with_retry(&user2, password, "user");

    let user1_id = get_user_id_for_username(&user1).expect("Failed to get user1 user_id");
    let user2_id = get_user_id_for_username(&user2).expect("Failed to get user2 user_id");

    execute_sql_via_client_as(
        &service_user,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, payload) VALUES (1, 'stream-u1'))",
            user1_id, full_table
        ),
    )
    .expect("Failed to insert stream row for user1");

    let user2_direct =
        execute_sql_via_client_as(&user2, password, &format!("SELECT * FROM {}", full_table))
            .expect("User2 stream select failed");
    assert!(!user2_direct.contains("stream-u1"));

    let user1_select_sql = format!("EXECUTE AS USER '{}' (SELECT * FROM {})", user1_id, full_table);
    let service_as_user1 = wait_for_query_contains_with(
        &user1_select_sql,
        "stream-u1",
        Duration::from_secs(30),
        |sql| execute_sql_via_client_as(&service_user, password, sql),
    )
    .expect("Service SELECT AS USER user1 on stream failed");
    assert!(service_as_user1.contains("stream-u1"));

    let service_as_user2 = execute_sql_via_client_as(
        &service_user,
        password,
        &format!("EXECUTE AS USER '{}' (SELECT * FROM {})", user2_id, full_table),
    )
    .expect("Service SELECT AS USER user2 on stream failed");
    assert!(!service_as_user2.contains("stream-u1"));

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", service_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user1));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user2));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_as_user_stream_table_isolation passed!");
}
