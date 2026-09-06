use tokio::runtime::Runtime;

use super::common::{should_run_minio_storage_tests, *};
use crate::common::{
    execute_sql_as_root_via_cli, execute_sql_as_root_via_client_json, generate_unique_namespace,
    generate_unique_table, parse_job_id_from_json_message,
};

#[test]
fn test_minio_storage_compact_table_command_runs() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_compact");
    let namespace = generate_unique_namespace("minio_compact_ns");
    let table = generate_unique_table("minio_compact_table");
    let full_table = format!("{}.{}", namespace, table);

    setup_minio_storage(&storage_id, "MinIO Compaction Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, body TEXT NOT NULL) WITH (TYPE='SHARED', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:1')",
        full_table, storage_id
    ))
    .expect("shared table creation");

    for (id, body) in [(1, "A"), (2, "B"), (3, "C")] {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, body) VALUES ({}, '{}')",
            full_table, id, body
        ))
        .expect("insert shared row");
        flush_table_and_wait(&full_table);
    }

    let before = manifest_segment_count(&namespace, &table);
    assert!(before >= 3, "expected multiple segments before compaction, got {}", before);

    let compact_output =
        execute_sql_as_root_via_client_json(&format!("STORAGE COMPACT TABLE {}", full_table))
            .expect("compact table");
    let compact_job_id = parse_job_id_from_json_message(&compact_output)
        .expect("compaction job id from JSON output");
    let compact_status =
        wait_for_terminal_job_status(&compact_job_id, std::time::Duration::from_secs(30))
            .expect("wait for compaction job terminal status");

    assert!(
        compact_status == "completed" || compact_status == "skipped",
        "unexpected compaction status: {}",
        compact_status
    );

    let after = manifest_segment_count(&namespace, &table);
    assert!(after <= before, "compaction should not increase segment count");
    assert!(after >= 1, "manifest should remain readable after compaction");
    assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}", full_table)), 3);

    // Verify manifest.json and at least one parquet file remain in MinIO after compaction.
    let storage_meta = fetch_storage_metadata(&storage_id);
    let store = build_minio_store(&storage_meta.base_directory);
    let table_dir = resolve_template(&storage_meta.shared_template, &namespace, &table, None);
    let runtime = Runtime::new().expect("compaction test runtime");
    assert_minio_files(&runtime, &store, &table_dir, "compacted table MinIO files");

    cleanup_minio_resources(&namespace, &table, &table, &storage_id);
}
