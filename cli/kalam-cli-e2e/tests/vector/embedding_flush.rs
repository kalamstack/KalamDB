use super::helpers::{embedding_literal, flush_user_table_and_wait, vector_query_ids};
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
fn test_minio_embedding_flush_multiple_common_dimensions() {
    if !should_run_minio_storage_tests() {
        return;
    }

    for dimension in [384usize, 768usize] {
        let storage_id = generate_unique_namespace(&format!("minio_embedding_{}", dimension));
        let namespace = generate_unique_namespace(&format!("minio_embedding_ns_{}", dimension));
        let table = generate_unique_table(&format!("minio_embedding_table_{}", dimension));
        let full_table = format!("{}.{}", namespace, table);

        setup_minio_storage(&storage_id, &format!("MinIO Embedding {} Storage", dimension));
        execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
            .expect("namespace creation");
        execute_sql_as_root_via_cli(&format!(
            "CREATE TABLE {} (id BIGINT PRIMARY KEY, embedding EMBEDDING({}), body TEXT NOT NULL) \
             WITH (TYPE='USER', STORAGE_ID='{}', FLUSH_POLICY='rows:100')",
            full_table, dimension, storage_id
        ))
        .expect("embedding table creation");

        for (id, active_index, body) in [
            (1, 0usize, "alpha"),
            (2, 1usize, "beta"),
            (3, 2usize, "gamma"),
        ] {
            let embedding = embedding_literal(dimension, active_index);
            execute_sql_as_root_via_cli(&format!(
                "INSERT INTO {} (id, embedding, body) VALUES ({}, '{}', '{}')",
                full_table, id, embedding, body
            ))
            .expect("insert embedding row");
        }

        let alter_output = execute_sql_as_root_via_client_json(&format!(
            "ALTER TABLE {} CREATE INDEX embedding USING COSINE",
            full_table
        ))
        .expect("create vector index");
        let alter_json: serde_json::Value =
            serde_json::from_str(&alter_output).expect("embedding alter json");
        let alter_status =
            alter_json.get("status").and_then(|value| value.as_str()).unwrap_or_default();
        assert_eq!(alter_status.to_lowercase(), "success", "embedding index enable should succeed");

        flush_user_table_and_wait(
            &namespace,
            &table,
            &full_table,
            &format!("embedding table {} flush storage", dimension),
        );

        let context = format!("embedding table {} flush storage", dimension);
        assert_flush_storage_files_exist(&namespace, &table, true, &context);

        let ids = vector_query_ids(&full_table, &embedding_literal(dimension, 0));
        assert!(
            ids.contains(&1),
            "embedding query should return the closest row for dimension {}; ids={:?}",
            dimension,
            ids
        );

        assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}", full_table)), 3);

        cleanup_minio_resources(&namespace, &table, &table, &storage_id);
    }
}
