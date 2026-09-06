use tokio::runtime::Runtime;

use super::common::{should_run_minio_storage_tests, *};
use crate::common::{
    admin_username, execute_sql_as_root_via_cli, execute_sql_as_root_via_client_json,
    generate_unique_namespace, generate_unique_table, get_rows_as_hashmaps, parse_cli_json_output,
};

#[test]
fn test_minio_storage_end_to_end() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let runtime = Runtime::new().expect("minio runtime");
    let storage_id = generate_unique_namespace("minio_storage");
    let namespace = generate_unique_namespace("minio_ns");
    let user_table = generate_unique_table("minio_user");
    let shared_table = generate_unique_table("minio_shared");

    setup_minio_storage(&storage_id, "MinIO Test Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");

    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL) WITH (TYPE='USER', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:2')",
        namespace, user_table, storage_id
    ))
    .expect("user table creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL) WITH (TYPE='SHARED', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:2')",
        namespace, shared_table, storage_id
    ))
    .expect("shared table creation");

    for (table, rows) in [
        (user_table.as_str(), [(1, "Alice"), (2, "Bob")].as_slice()),
        (shared_table.as_str(), [(1, "Carol"), (2, "Dave")].as_slice()),
    ] {
        for (id, name) in rows {
            execute_sql_as_root_via_cli(&format!(
                "INSERT INTO {}.{} (id, name) VALUES ({}, '{}')",
                namespace, table, id, name
            ))
            .expect("insert row");
        }
        flush_table_and_wait(&format!("{}.{}", namespace, table));
    }

    let storage_meta = fetch_storage_metadata(&storage_id);
    let store = build_minio_store(&storage_meta.base_directory);
    let admin_user_id = admin_username().to_string();

    let user_dir = resolve_template(
        &storage_meta.user_template,
        &namespace,
        &user_table,
        Some(&admin_user_id),
    );
    let shared_dir =
        resolve_template(&storage_meta.shared_template, &namespace, &shared_table, None);

    assert_minio_files(&runtime, &store, &user_dir, "user end-to-end table");
    assert_minio_files(&runtime, &store, &shared_dir, "shared end-to-end table");

    assert_eq!(
        query_count(&format!("SELECT COUNT(*) AS c FROM {}.{}", namespace, user_table)),
        2
    );
    assert_eq!(
        query_count(&format!("SELECT COUNT(*) AS c FROM {}.{}", namespace, shared_table)),
        2
    );

    cleanup_minio_resources(&namespace, &user_table, &shared_table, &storage_id);
}

#[test]
fn test_minio_storage_check() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_check");
    setup_minio_storage(&storage_id, "MinIO Check Storage");

    let basic_output =
        execute_sql_as_root_via_client_json(&format!("STORAGE CHECK {}", storage_id))
            .expect("storage check basic");
    let basic_json = parse_cli_json_output(&basic_output).expect("basic check json");
    let basic_rows = get_rows_as_hashmaps(&basic_json).unwrap_or_default();
    assert_eq!(basic_rows.len(), 1, "expected one row from STORAGE CHECK");

    let status_value = basic_rows[0]
        .get("status")
        .map(crate::common::extract_typed_value)
        .and_then(|value| value.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown".to_string());
    assert_eq!(status_value, "healthy", "STORAGE CHECK should be healthy");

    let extended_output =
        execute_sql_as_root_via_client_json(&format!("STORAGE CHECK {} EXTENDED", storage_id))
            .expect("storage check extended");
    let extended_json = parse_cli_json_output(&extended_output).expect("extended check json");
    let extended_rows = get_rows_as_hashmaps(&extended_json).unwrap_or_default();
    assert_eq!(extended_rows.len(), 1, "expected one row from STORAGE CHECK EXTENDED");

    let _ = execute_sql_as_root_via_cli(&format!("DROP STORAGE {}", storage_id));
}
