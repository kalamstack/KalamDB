use crate::common::*;

fn get_user_id(user_id: &str) -> Option<String> {
    let query = format!("SELECT user_id FROM system.users WHERE user_id = '{}'", user_id);
    let result = execute_sql_as_root_via_client_json(&query).ok()?;

    let json: serde_json::Value = serde_json::from_str(&result).ok()?;
    let rows = get_rows_as_hashmaps(&json)?;

    if let Some(row) = rows.first() {
        let user_id_value = row.get("user_id").map(extract_typed_value)?;
        return user_id_value.as_str().map(|s| s.to_string());
    }
    None
}

fn expect_unauthorized(result: Result<String, Box<dyn std::error::Error>>, context: &str) {
    assert!(result.is_err(), "Expected authorization failure: {}", context);
    if let Err(err) = result {
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("unauthorized")
                || msg.contains("not authorized")
                || msg.contains("permission")
                || msg.contains("not allowed")
                || msg.contains("privilege")
                || msg.contains("access denied"),
            "Expected authorization error for {}: {}",
            context,
            err
        );
    }
}

// Disallowed role-matrix edges are rejected before executing the wrapped batch.
#[ntest::timeout(180000)]
#[test]
fn smoke_security_disallowed_roles_cannot_impersonate_in_batch() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_disallowed_roles_cannot_impersonate_in_batch: server not \
             running at {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_imp_ns");
    let table = generate_unique_table("smoke_imp_tbl");
    let full_table = format!("{}.{}", namespace, table);

    let regular_user = generate_unique_namespace("smoke_regular");
    let target_user = generate_unique_namespace("smoke_target_user");
    let service_user = generate_unique_namespace("smoke_service");
    let dba_user = generate_unique_namespace("smoke_dba");
    let password = "smoke_pass_123";

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    let create_table_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT) WITH (TYPE='USER')",
        full_table
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("Failed to create table");

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        regular_user, password
    ))
    .expect("Failed to create regular user");

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        target_user, password
    ))
    .expect("Failed to create target user");

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'service'",
        service_user, password
    ))
    .expect("Failed to create service user");

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        dba_user, password
    ))
    .expect("Failed to create dba user");

    let target_user_id = get_user_id(&target_user).expect("Failed to get target user_id");
    let service_user_id = get_user_id(&service_user).expect("Failed to get service user_id");
    let dba_user_id = get_user_id(&dba_user).expect("Failed to get dba user_id");
    let system_user_id = get_user_id("root")
        .or_else(|| get_user_id("system"))
        .expect("Failed to get system user_id");

    let attempts = [
        (&regular_user, target_user_id.as_str(), "regular user -> user"),
        (&regular_user, service_user_id.as_str(), "regular user -> service"),
        (&regular_user, dba_user_id.as_str(), "regular user -> dba"),
        (&regular_user, system_user_id.as_str(), "regular user -> system"),
        (&service_user, dba_user_id.as_str(), "service -> dba"),
        (&service_user, system_user_id.as_str(), "service -> system"),
        (&dba_user, system_user_id.as_str(), "dba -> system"),
    ];

    for (idx, (actor, target_user_id, label)) in attempts.iter().enumerate() {
        let batch_sql = format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, name) VALUES ({}, 'x')); SELECT 1;",
            target_user_id,
            full_table,
            idx + 1
        );
        let result = execute_sql_via_client_as(actor.as_str(), password, &batch_sql);
        expect_unauthorized(result, &format!("AS USER batch for {}", label));
    }

    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", regular_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", target_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", service_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", dba_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}
