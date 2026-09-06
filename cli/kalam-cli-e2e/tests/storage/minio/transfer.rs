use tokio::runtime::Runtime;

use super::common::{should_run_minio_storage_tests, *};
use crate::common::{
    execute_sql_as_root_via_cli, generate_unique_namespace, generate_unique_table,
    leader_or_server_url, wait_for_job_finished,
};

#[test]
fn test_minio_shared_table_export_import_roundtrip() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_transfer");
    let namespace = generate_unique_namespace("minio_transfer_ns");
    let source_table = generate_unique_table("minio_transfer_src");
    let target_table = generate_unique_table("minio_transfer_dst");
    let source_full = format!("{}.{}", namespace, source_table);
    let target_full = format!("{}.{}", namespace, target_table);

    setup_minio_storage(&storage_id, "MinIO Transfer Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");

    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, body TEXT NOT NULL) WITH (TYPE='SHARED', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:10')",
        source_full, storage_id
    ))
    .expect("source table creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, body TEXT NOT NULL) WITH (TYPE='SHARED', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:10')",
        target_full, storage_id
    ))
    .expect("target table creation");

    for (id, body) in [(1, "alpha"), (2, "beta"), (3, "gamma"), (4, "delta")] {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, body) VALUES ({}, '{}')",
            source_full, id, body
        ))
        .expect("insert source row");
    }
    flush_table_and_wait(&source_full);

    for (id, body) in [(5, "hot-epsilon"), (6, "hot-zeta")] {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (id, body) VALUES ({}, '{}')",
            source_full, id, body
        ))
        .expect("insert hot source row");
    }

    let token = admin_access_token();
    let export_response =
        start_table_export_sync(&token, &namespace, &source_table, "shared", None)
            .expect("start shared table export");
    let export_job_id = export_response
        .get("job_id")
        .and_then(|value| value.as_str())
        .expect("export job_id")
        .to_string();
    wait_for_job_finished(&export_job_id, std::time::Duration::from_secs(180))
        .expect("export job should finish");

    let download_url = export_response
        .get("download_url")
        .and_then(|value| value.as_str())
        .expect("export download_url");
    let download_url =
        if download_url.starts_with("http://") || download_url.starts_with("https://") {
            download_url.to_string()
        } else {
            format!("{}{}", leader_or_server_url().trim_end_matches('/'), download_url)
        };

    let (status_code, content_type, zip_bytes) =
        http_get_with_token(&download_url, &token).expect("download table export zip");
    assert_eq!(status_code, 200, "expected successful export download");
    assert!(
        content_type.contains("application/zip")
            || content_type.contains("application/octet-stream"),
        "unexpected content-type: {}",
        content_type
    );

    let import_response = start_table_import_sync(
        &token,
        &namespace,
        &target_table,
        "shared",
        None,
        "table-export.zip",
        zip_bytes,
    )
    .expect("start table import");
    let import_job_id = import_response
        .get("job_id")
        .and_then(|value| value.as_str())
        .expect("import job_id")
        .to_string();
    wait_for_job_finished(&import_job_id, std::time::Duration::from_secs(180))
        .expect("import job should finish");

    let source_count = query_count(&format!("SELECT COUNT(*) AS c FROM {}", source_full));
    let target_count = query_count(&format!("SELECT COUNT(*) AS c FROM {}", target_full));
    assert_eq!(target_count, source_count, "imported row count should match source");

    let hot_row_count =
        query_count(&format!("SELECT COUNT(*) AS c FROM {} WHERE body = 'hot-zeta'", target_full));
    assert_eq!(hot_row_count, 1, "imported data should include hot rows");

    // Verify manifest.json and parquet files exist in MinIO for both source (flushed before
    // export) and target (written by the import job).
    let storage_meta = fetch_storage_metadata(&storage_id);
    let store = build_minio_store(&storage_meta.base_directory);
    let runtime = Runtime::new().expect("transfer test runtime");
    let source_dir =
        resolve_template(&storage_meta.shared_template, &namespace, &source_table, None);
    let target_dir =
        resolve_template(&storage_meta.shared_template, &namespace, &target_table, None);
    assert_minio_files(&runtime, &store, &source_dir, "source table MinIO files after flush");
    assert_minio_files(&runtime, &store, &target_dir, "target table MinIO files after import");

    cleanup_minio_resources(&namespace, &source_table, &target_table, &storage_id);
}
