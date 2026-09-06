//! Smoke tests for EXECUTE AS USER subject isolation and role hierarchy.
//!
//! Ordinary USER-table reads remain subject-scoped. Explicit EXECUTE AS USER can
//! switch subject only when the actor role is allowed to target the resolved
//! user's role.

use crate::common::*;

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
                    std::thread::sleep(std::time::Duration::from_millis(
                        200 * (attempt + 1) as u64,
                    ));
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

fn expect_execute_as_denied(result: Result<String, Box<dyn std::error::Error>>, context: &str) {
    assert!(result.is_err(), "{} should fail", context);
    let msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        msg.contains("not authorized")
            || msg.contains("unauthorized")
            || msg.contains("permission")
            || msg.contains("not allowed"),
        "{} should mention EXECUTE AS USER denial: {}",
        context,
        msg
    );
}

#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_role_matrix_allows_privileged_roles_and_denies_user() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_as_user_role_matrix_allows_privileged_roles_and_denies_user: server \
             not running"
        );
        return;
    }

    let namespace = create_test_namespace("role_matrix");
    let table = generate_unique_table("items");
    let full_table = format!("{}.{}", namespace, table);
    let password = "test_pass_123";

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id VARCHAR PRIMARY KEY, value VARCHAR) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create user table");

    let target_user = generate_unique_namespace("target");
    create_user_with_retry(&target_user, password, "user");

    let allowed_actors = [
        (generate_unique_namespace("system_actor"), "system"),
        (generate_unique_namespace("dba_actor"), "dba"),
        (generate_unique_namespace("service_actor"), "service"),
    ];

    for (actor, role) in &allowed_actors {
        create_user_with_retry(actor, password, role);
        let sql = format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, value) VALUES ('{}', 'allowed'))",
            target_user, full_table, actor
        );
        execute_sql_via_client_as(actor, password, &sql)
            .unwrap_or_else(|err| panic!("{} cross-user insert should succeed: {}", role, err));
    }

    let denied_actor = generate_unique_namespace("user_actor");
    create_user_with_retry(&denied_actor, password, "user");
    expect_execute_as_denied(
        execute_sql_via_client_as(
            &denied_actor,
            password,
            &format!(
                "EXECUTE AS USER '{}' (INSERT INTO {} (id, value) VALUES ('denied-user', \
                 'blocked'))",
                target_user, full_table
            ),
        ),
        "regular user cross-user insert",
    );

    let target_rows =
        execute_sql_via_client_as(&target_user, password, &format!("SELECT * FROM {}", full_table))
            .expect("Failed to select as target");
    assert!(
        target_rows.contains("allowed") && !target_rows.contains("blocked"),
        "Role matrix inserts did not isolate target rows correctly: {}",
        target_rows
    );

    let direct_dba = execute_sql_via_client_as(
        &allowed_actors[1].0,
        password,
        &format!("SELECT * FROM {}", full_table),
    )
    .expect("DBA direct select should succeed");
    assert!(
        !direct_dba.contains("allowed"),
        "DBA direct select leaked target rows: {}",
        direct_dba
    );

    for (actor, _) in allowed_actors {
        let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", actor));
    }
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", denied_actor));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_self_target_dml_stays_under_actor_user_id() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_as_user_self_target_dml_stays_under_actor_user_id: server not running"
        );
        return;
    }

    let namespace = create_test_namespace("self_dml");
    let table = generate_unique_table("notes");
    let full_table = format!("{}.{}", namespace, table);
    let password = "test_pass_123";
    let actor = generate_unique_namespace("service_self");
    let other = generate_unique_namespace("other_user");

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id VARCHAR PRIMARY KEY, value VARCHAR) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create user table");
    create_user_with_retry(&actor, password, "service");
    create_user_with_retry(&other, password, "user");

    execute_sql_via_client_as(
        &actor,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, value) VALUES ('n1', 'actor-row'))",
            actor, full_table
        ),
    )
    .expect("Self-targeted insert should succeed");

    let actor_view = execute_sql_via_client_as(
        &actor,
        password,
        &format!("SELECT value FROM {} WHERE id = 'n1'", full_table),
    )
    .expect("Actor select failed");
    assert!(actor_view.contains("actor-row"), "Actor should see own row: {}", actor_view);

    let other_view =
        execute_sql_via_client_as(&other, password, &format!("SELECT * FROM {}", full_table))
            .expect("Other select failed");
    assert!(!other_view.contains("actor-row"), "Other user saw actor row: {}", other_view);

    execute_sql_via_client_as(
        &actor,
        password,
        &format!(
            "EXECUTE AS USER '{}' (UPDATE {} SET value = 'actor-updated' WHERE id = 'n1')",
            actor, full_table
        ),
    )
    .expect("Self-targeted update should succeed");

    let updated = execute_sql_via_client_as(
        &actor,
        password,
        &format!("SELECT value FROM {} WHERE id = 'n1'", full_table),
    )
    .expect("Actor select after update failed");
    assert!(updated.contains("actor-updated"), "Self update did not apply: {}", updated);

    execute_sql_via_client_as(
        &actor,
        password,
        &format!("EXECUTE AS USER '{}' (DELETE FROM {} WHERE id = 'n1')", actor, full_table),
    )
    .expect("Self-targeted delete should succeed");

    let after_delete =
        execute_sql_via_client_as(&actor, password, &format!("SELECT * FROM {}", full_table))
            .expect("Actor select after delete failed");
    assert!(
        !after_delete.contains("actor-updated"),
        "Self delete did not remove row: {}",
        after_delete
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", actor));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", other));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_allowed_select_update_delete_runs_in_target_scope() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_as_user_allowed_select_update_delete_runs_in_target_scope: server not \
             running"
        );
        return;
    }

    let namespace = create_test_namespace("dml_allowed");
    let table = generate_unique_table("profiles");
    let full_table = format!("{}.{}", namespace, table);
    let password = "test_pass_123";
    let actor = generate_unique_namespace("dba_actor");
    let target = generate_unique_namespace("target_user");

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id VARCHAR PRIMARY KEY, value VARCHAR) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create user table");
    create_user_with_retry(&actor, password, "dba");
    create_user_with_retry(&target, password, "user");

    execute_sql_via_client_as(
        &target,
        password,
        &format!("INSERT INTO {} (id, value) VALUES ('p1', 'original')", full_table),
    )
    .expect("Target insert failed");

    let selected = execute_sql_via_client_as(
        &actor,
        password,
        &format!("EXECUTE AS USER '{}' (SELECT * FROM {} WHERE id = 'p1')", target, full_table),
    )
    .expect("DBA should select target row through EXECUTE AS USER");
    assert!(selected.contains("original"), "Expected target row, got: {}", selected);

    execute_sql_via_client_as(
        &actor,
        password,
        &format!(
            "EXECUTE AS USER '{}' (UPDATE {} SET value = 'changed' WHERE id = 'p1')",
            target, full_table
        ),
    )
    .expect("DBA should update target row through EXECUTE AS USER");

    let after_update = execute_sql_via_client_as(
        &target,
        password,
        &format!("SELECT value FROM {} WHERE id = 'p1'", full_table),
    )
    .expect("Target select after update failed");
    assert!(after_update.contains("changed"), "Target row was not updated: {}", after_update);

    let actor_direct =
        execute_sql_via_client_as(&actor, password, &format!("SELECT * FROM {}", full_table))
            .expect("Actor direct select failed");
    assert!(
        !actor_direct.contains("changed"),
        "Actor saw target row directly: {}",
        actor_direct
    );

    execute_sql_via_client_as(
        &actor,
        password,
        &format!("EXECUTE AS USER '{}' (DELETE FROM {} WHERE id = 'p1')", target, full_table),
    )
    .expect("DBA should delete target row through EXECUTE AS USER");

    let target_after = execute_sql_via_client_as(
        &target,
        password,
        &format!("SELECT value FROM {} WHERE id = 'p1'", full_table),
    )
    .expect("Target select after delete failed");
    assert!(
        !target_after.contains("changed") && !target_after.contains("original"),
        "Target row should be deleted: {}",
        target_after
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", actor));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

#[ntest::timeout(120000)]
#[test]
fn smoke_as_user_shared_table_denied_without_mutation() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_as_user_shared_table_denied_without_mutation: server not running"
        );
        return;
    }

    let namespace = create_test_namespace("shared_denied");
    let table = generate_unique_table("config");
    let full_table = format!("{}.{}", namespace, table);
    let password = "test_pass_123";
    let actor = generate_unique_namespace("dba_actor");
    let target = generate_unique_namespace("target_user");

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id VARCHAR PRIMARY KEY, value VARCHAR) WITH (TYPE='SHARED')",
        full_table
    ))
    .expect("Failed to create shared table");
    grant_public_select_shared_table(&full_table);
    create_user_with_retry(&actor, password, "dba");
    create_user_with_retry(&target, password, "user");

    let result = execute_sql_via_client_as(
        &actor,
        password,
        &format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, value) VALUES ('s1', 'blocked'))",
            target, full_table
        ),
    );
    assert!(result.is_err(), "AS USER on shared table should fail");
    let msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        msg.contains("shared") || msg.contains("user tables") || msg.contains("table type"),
        "Shared table AS USER should mention table-type denial: {}",
        msg
    );

    let shared_rows =
        execute_sql_via_client_as(&actor, password, &format!("SELECT * FROM {}", full_table))
            .expect("Shared table select failed");
    assert!(
        !shared_rows.contains("blocked"),
        "Denied shared insert created a row: {}",
        shared_rows
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", actor));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", target));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}
