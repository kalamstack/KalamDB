//! Shared-table FORCE RLS: catalog reads vs default-deny vs collaborative writes.

use crate::common;

/// Catalog-style shared table: User can SELECT after a PUBLIC SELECT policy,
/// but cannot INSERT without WITH CHECK.
#[ntest::timeout(120000)]
#[test]
fn test_shared_catalog_select_without_write() {
    if !common::is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = common::generate_unique_namespace("shared_catalog");
    let table = common::generate_unique_table("plans");
    let full = format!("{namespace}.{table}");
    let user = common::generate_unique_namespace("catalog_reader");
    let password = "smoke_pass_123";

    common::execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {namespace}"))
        .expect("create namespace");
    common::execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {full} (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL
        ) WITH (TYPE='SHARED')"
    ))
    .expect("create shared catalog table");
    common::grant_public_select_shared_table(&full);
    common::execute_sql_as_root_via_client(&format!(
        "INSERT INTO {full} (id, name) VALUES (1, 'free')"
    ))
    .expect("dba seed catalog");
    common::execute_sql_as_root_via_client(&format!(
        "CREATE USER {user} WITH PASSWORD '{password}' ROLE 'user'"
    ))
    .expect("create user");

    let visible =
        common::execute_sql_via_client_as(&user, password, &format!("SELECT name FROM {full}"))
            .expect("user SELECT catalog");
    assert!(
        visible.to_lowercase().contains("free"),
        "PUBLIC SELECT policy must expose catalog rows: {visible}"
    );

    let insert = common::execute_sql_via_client_as(
        &user,
        password,
        &format!("INSERT INTO {full} (id, name) VALUES (2, 'pwned')"),
    );
    assert!(insert.is_err(), "SELECT-only catalog must reject subject INSERT: {insert:?}");

    let _ = common::execute_sql_as_root_via_client(&format!("DROP USER {user}"));
    let _ = common::execute_sql_as_root_via_client(&format!(
        "DROP NAMESPACE IF EXISTS {namespace} CASCADE"
    ));
}

/// No policy means User sees zero rows (FORCE RLS default deny).
#[ntest::timeout(120000)]
#[test]
fn test_shared_default_deny_without_policy() {
    if !common::is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = common::generate_unique_namespace("shared_deny");
    let table = common::generate_unique_table("secrets");
    let full = format!("{namespace}.{table}");
    let user = common::generate_unique_namespace("denied_reader");
    let password = "smoke_pass_123";

    common::execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {namespace}"))
        .expect("create namespace");
    common::execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {full} (id BIGINT PRIMARY KEY, content TEXT NOT NULL) WITH (TYPE='SHARED')"
    ))
    .expect("create private shared table");
    common::execute_sql_as_root_via_client(&format!(
        "INSERT INTO {full} (id, content) VALUES (1, 'classified')"
    ))
    .expect("dba seed");
    common::execute_sql_as_root_via_client(&format!(
        "CREATE USER {user} WITH PASSWORD '{password}' ROLE 'user'"
    ))
    .expect("create user");

    let output =
        common::execute_sql_via_client_as(&user, password, &format!("SELECT content FROM {full}"))
            .expect("default-deny SELECT returns zero rows");
    assert!(
        !output.to_lowercase().contains("classified"),
        "User must not see rows without a policy: {output}"
    );

    let _ = common::execute_sql_as_root_via_client(&format!("DROP USER {user}"));
    let _ = common::execute_sql_as_root_via_client(&format!(
        "DROP NAMESPACE IF EXISTS {namespace} CASCADE"
    ));
}
