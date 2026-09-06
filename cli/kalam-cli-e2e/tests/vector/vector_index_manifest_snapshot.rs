use super::helpers::{flush_user_table_and_wait, vector_query_ids};
use crate::{
    common::{
        assert_flush_storage_files_exist, execute_sql_as_root_via_cli,
        execute_sql_as_root_via_client_json, generate_unique_namespace, generate_unique_table,
    },
    minio_common::{
        cleanup_minio_resources, query_count, setup_minio_storage, should_run_minio_storage_tests,
    },
};

#[test]
fn test_minio_vector_index_manifest_snapshot_exists() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_vector");
    let namespace = generate_unique_namespace("minio_vector_ns");
    let table = generate_unique_table("minio_vector_table");
    let full_table = format!("{}.{}", namespace, table);

    setup_minio_storage(&storage_id, "MinIO Vector Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, embedding EMBEDDING(3), body TEXT NOT NULL) WITH \
         (TYPE='USER', STORAGE_ID='{}', FLUSH_POLICY='rows:100')",
        full_table, storage_id
    ))
    .expect("vector table creation");

    for (id, embedding, body) in [
        (1, "[1.0,0.0,0.0]", "alpha"),
        (2, "[0.0,1.0,0.0]", "beta"),
        (3, "[0.0,0.0,1.0]", "gamma"),
    ] {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, embedding, body) VALUES ({}, '{}', '{}')",
            full_table, id, embedding, body
        ))
        .expect("insert vector row");
    }

    let alter_output = execute_sql_as_root_via_client_json(&format!(
        "ALTER TABLE {} CREATE INDEX embedding USING COSINE",
        full_table
    ))
    .expect("create vector index");
    let alter_json: serde_json::Value =
        serde_json::from_str(&alter_output).expect("vector alter json");
    let alter_status =
        alter_json.get("status").and_then(|value| value.as_str()).unwrap_or_default();
    assert_eq!(alter_status.to_lowercase(), "success", "vector index enable should succeed");

    flush_user_table_and_wait(&namespace, &table, &full_table, "vector table flush storage");

    assert_flush_storage_files_exist(&namespace, &table, true, "vector table flush storage");

    let ids = vector_query_ids(&full_table, "[1.0,0.0,0.0]");
    assert!(ids.contains(&1), "vector query should return the closest row; ids={:?}", ids);

    assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}", full_table)), 3);

    cleanup_minio_resources(&namespace, &table, &table, &storage_id);
}
