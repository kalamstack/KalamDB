use std::{
    sync::{Arc, Barrier},
    thread,
};

use tokio::runtime::Runtime;

use super::common::{should_run_minio_storage_tests, *};
use crate::common::{
    admin_username, execute_sql_as_root_via_cli, generate_unique_namespace, generate_unique_table,
};

#[test]
fn test_minio_user_flush_manifest_and_query() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_flush_user");
    let namespace = generate_unique_namespace("minio_flush_ns");
    let table = generate_unique_table("minio_flush_user_table");

    setup_minio_storage(&storage_id, "MinIO User Flush Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT NOT NULL) WITH (TYPE='USER', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:2')",
        namespace, table, storage_id
    ))
    .expect("user table creation");

    execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {}.{} (id, name) VALUES (1, 'Alice')",
        namespace, table
    ))
    .expect("insert row 1");
    execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {}.{} (id, name) VALUES (2, 'Bob')",
        namespace, table
    ))
    .expect("insert row 2");

    flush_table_and_wait(&format!("{}.{}", namespace, table));

    let storage_meta = fetch_storage_metadata(&storage_id);
    let store = build_minio_store(&storage_meta.base_directory);
    let admin_user_id = admin_username().to_string();
    let table_dir =
        resolve_template(&storage_meta.user_template, &namespace, &table, Some(&admin_user_id));
    let runtime = Runtime::new().expect("runtime");
    assert_minio_files(&runtime, &store, &table_dir, "user flush manifest");
    assert_manifest_segment_count(&namespace, &table, 1);
    assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}.{}", namespace, table)), 2);

    cleanup_minio_resources(&namespace, &table, "", &storage_id);
}

#[test]
fn test_minio_shared_flush_manifest_and_query() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_flush_shared");
    let namespace = generate_unique_namespace("minio_flush_shared_ns");
    let table = generate_unique_table("minio_flush_shared_table");

    setup_minio_storage(&storage_id, "MinIO Shared Flush Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT NOT NULL) WITH (TYPE='SHARED', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:2')",
        namespace, table, storage_id
    ))
    .expect("shared table creation");

    execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {}.{} (id, name) VALUES (1, 'Alice')",
        namespace, table
    ))
    .expect("insert row 1");
    execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {}.{} (id, name) VALUES (2, 'Bob')",
        namespace, table
    ))
    .expect("insert row 2");

    flush_table_and_wait(&format!("{}.{}", namespace, table));

    let storage_meta = fetch_storage_metadata(&storage_id);
    let store = build_minio_store(&storage_meta.base_directory);
    let table_dir = resolve_template(&storage_meta.shared_template, &namespace, &table, None);
    let runtime = Runtime::new().expect("runtime");
    assert_minio_files(&runtime, &store, &table_dir, "shared flush manifest");
    assert_manifest_segment_count(&namespace, &table, 1);
    assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}.{}", namespace, table)), 2);

    cleanup_minio_resources(&namespace, "", &table, &storage_id);
}

#[test]
fn test_minio_user_multiple_flushes_and_segments() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_multi_user");
    let namespace = generate_unique_namespace("minio_multi_ns");
    let table = generate_unique_table("minio_multi_user_table");

    setup_minio_storage(&storage_id, "MinIO Multi Flush Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT NOT NULL) WITH (TYPE='USER', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:1')",
        namespace, table, storage_id
    ))
    .expect("user table creation");

    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol")] {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {}.{} (id, name) VALUES ({}, '{}')",
            namespace, table, id, name
        ))
        .expect("insert row");
        flush_table_and_wait(&format!("{}.{}", namespace, table));
    }

    assert!(manifest_segment_count(&namespace, &table) >= 3);
    assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}.{}", namespace, table)), 3);

    cleanup_minio_resources(&namespace, &table, "", &storage_id);
}

#[test]
fn test_minio_shared_multiple_flushes_and_segments() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_multi_shared");
    let namespace = generate_unique_namespace("minio_multi_shared_ns");
    let table = generate_unique_table("minio_multi_shared_table");

    setup_minio_storage(&storage_id, "MinIO Shared Multi Flush Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT NOT NULL) WITH (TYPE='SHARED', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:1')",
        namespace, table, storage_id
    ))
    .expect("shared table creation");

    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol")] {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {}.{} (id, name) VALUES ({}, '{}')",
            namespace, table, id, name
        ))
        .expect("insert row");
        flush_table_and_wait(&format!("{}.{}", namespace, table));
    }

    assert!(manifest_segment_count(&namespace, &table) >= 3);
    assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}.{}", namespace, table)), 3);

    cleanup_minio_resources(&namespace, "", &table, &storage_id);
}

#[test]
fn test_minio_parallel_flushes_across_tables() {
    if !should_run_minio_storage_tests() {
        return;
    }

    let storage_id = generate_unique_namespace("minio_parallel");
    let namespace = generate_unique_namespace("minio_parallel_ns");
    let table_a = generate_unique_table("minio_parallel_a");
    let table_b = generate_unique_table("minio_parallel_b");

    setup_minio_storage(&storage_id, "MinIO Parallel Flush Storage");
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    for table in [&table_a, &table_b] {
        execute_sql_as_root_via_cli(&format!(
            "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT NOT NULL) WITH (TYPE='SHARED', \
             STORAGE_ID='{}', FLUSH_POLICY='rows:1')",
            namespace, table, storage_id
        ))
        .expect("table creation");
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {}.{} (id, name) VALUES (1, 'one')",
            namespace, table
        ))
        .expect("insert row 1");
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {}.{} (id, name) VALUES (2, 'two')",
            namespace, table
        ))
        .expect("insert row 2");
    }

    let barrier = Arc::new(Barrier::new(3));
    let namespace_a = namespace.clone();
    let namespace_b = namespace.clone();
    let table_a_clone = table_a.clone();
    let table_b_clone = table_b.clone();
    let barrier_a = barrier.clone();
    let barrier_b = barrier.clone();

    let flush_a = thread::spawn(move || {
        barrier_a.wait();
        flush_table_and_wait(&format!("{}.{}", namespace_a, table_a_clone));
    });
    let flush_b = thread::spawn(move || {
        barrier_b.wait();
        flush_table_and_wait(&format!("{}.{}", namespace_b, table_b_clone));
    });

    barrier.wait();
    flush_a.join().expect("flush thread a");
    flush_b.join().expect("flush thread b");

    assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}.{}", namespace, table_a)), 2);
    assert_eq!(query_count(&format!("SELECT COUNT(*) AS c FROM {}.{}", namespace, table_b)), 2);
    assert!(manifest_segment_count(&namespace, &table_a) >= 1);
    assert!(manifest_segment_count(&namespace, &table_b) >= 1);

    cleanup_minio_resources(&namespace, &table_a, &table_b, &storage_id);
}
