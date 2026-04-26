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
                || msg.contains("privilege")
                || msg.contains("access denied"),
            "Expected authorization error for {}: {}",
            context,
            err
        );
    }
}

// Regular users cannot impersonate service/DBA/system users via AS USER (including batch)
#[ntest::timeout(180000)]
#[test]
fn smoke_security_regular_user_cannot_impersonate_privileged_users_in_batch() {
    if !is_server_running() {
        eprintln!(
            "Skipping smoke_security_regular_user_cannot_impersonate_privileged_users_in_batch: \
             server not running at {}",
            server_url()
        );
        return;
    }

    let namespace = generate_unique_namespace("smoke_imp_ns");
    let table = generate_unique_table("smoke_imp_tbl");
    let full_table = format!("{}.{}", namespace, table);

    let regular_user = generate_unique_namespace("smoke_regular");
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
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'service'",
        service_user, password
    ))
    .expect("Failed to create service user");

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        dba_user, password
    ))
    .expect("Failed to create dba user");

    let service_user_id = get_user_id(&service_user).expect("Failed to get service user_id");
    let dba_user_id = get_user_id(&dba_user).expect("Failed to get dba user_id");
    let system_user_id = get_user_id("root")
        .or_else(|| get_user_id("system"))
        .expect("Failed to get system user_id");

    let attempts = vec![
        (service_user_id, "service"),
        (dba_user_id, "dba"),
        (system_user_id, "system"),
    ];

    for (target_user_id, label) in attempts {
        let batch_sql = format!(
            "EXECUTE AS USER '{}' (INSERT INTO {} (id, name) VALUES (1, 'x')); SELECT 1;",
            target_user_id, full_table
        );
        let result = execute_sql_via_client_as(&regular_user, password, &batch_sql);
        expect_unauthorized(result, &format!("AS USER batch for {}", label));
    }

    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", regular_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", service_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", dba_user));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}
