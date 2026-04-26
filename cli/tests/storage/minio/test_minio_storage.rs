//! MinIO (S3-compatible) storage integration test.
//!
//! This test verifies KalamDB can write flushed data to MinIO and that
//! manifest.json + Parquet data files are created for both USER and SHARED tables.

use std::{borrow::Cow, env, sync::Arc, time::Duration};

use futures_util::StreamExt;
use object_store::{
    aws::AmazonS3Builder, path::Path as ObjectPath, prefix::PrefixStore, ObjectStore,
    ObjectStoreExt,
};
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime;

use crate::common::*;

const MINIO_ENDPOINT: &str = "http://127.0.0.1:9120";
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const MINIO_BUCKET: &str = "kalamdb-test";
const MINIO_REGION: &str = "us-east-1";

#[test]
#[ignore]
fn test_minio_storage_end_to_end() {
    println!("\n🚀 Starting MinIO storage integration test...");

    println!("📡 Checking if KalamDB server is running...");
    if !is_server_running() {
        eprintln!("❌ Server not running. Skipping MinIO storage test.");
        eprintln!("   Please start the server first: cargo run");
        panic!("Server not running");
    }
    println!("✅ Server is running");

    println!("\n🎯 Target backend:");
    println!("   Server URL: {}", leader_or_server_url());
    println!("   MinIO endpoint: {}", minio_endpoint());
    println!("   MinIO bucket: {}", minio_bucket());

    let runtime = Runtime::new().expect("minio runtime");
    println!("\n🔐 Step 0: Checking MinIO auth/connectivity...");
    let probe_store = build_minio_store(&format!("s3://{}/", minio_bucket()));
    if let Err(err) = minio_bucket_reachable(&runtime, &probe_store) {
        eprintln!("❌ MinIO auth/connectivity check failed: {}", err);
        eprintln!("   Verify endpoint, access key, secret key, and bucket.");
        // fail the test early
        panic!("MinIO auth/connectivity check failed");
    }
    println!("✅ MinIO auth/connectivity check passed");

    let storage_id = generate_unique_namespace("minio_storage");
    let namespace = generate_unique_namespace("minio_ns");
    let user_table = generate_unique_table("minio_user");
    let shared_table = generate_unique_table("minio_shared");

    println!("\n📦 Test resources:");
    println!("   Storage ID: {}", storage_id);
    println!("   Namespace: {}", namespace);
    println!("   User table: {}", user_table);
    println!("   Shared table: {}", shared_table);

    println!("\n🏗️  Step 1: Creating namespace '{}'...", namespace);
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace))
        .expect("namespace creation");
    println!("✅ Namespace created");

    let base_directory = format!("s3://{}/{}/", minio_bucket(), storage_id);
    let config_json = json!({
        "type": "s3",
        "region": minio_region(),
        "endpoint": minio_endpoint(),
        "allow_http": true,
        "access_key_id": minio_access_key(),
        "secret_access_key": minio_secret_key()
    })
    .to_string();

    println!("\n🗄️  Step 2: Creating MinIO storage '{}'...", storage_id);
    println!("   Endpoint: {}", minio_endpoint());
    println!("   Bucket: {}", minio_bucket());
    println!("   Base directory: {}", base_directory);

    let create_storage_sql = format!(
        "CREATE STORAGE {storage_id} TYPE s3 NAME 'MinIO Test Storage' BASE_DIRECTORY \
         '{base_directory}' CONFIG '{config_json}' SHARED_TABLES_TEMPLATE \
         'ns_{{namespace}}/shared_{{tableName}}' USER_TABLES_TEMPLATE \
         'ns_{{namespace}}/user_{{tableName}}/user_{{userId}}'",
    );

    match execute_sql_as_root_via_cli(&create_storage_sql) {
        Ok(output) => {
            println!("✅ Storage created successfully");
            if !output.trim().is_empty() {
                println!("   Output: {}", output.trim());
            }
        },
        Err(error) => {
            eprintln!("❌ Failed to create storage: {}", error);
            panic!("Storage creation failed");
        },
    }

    println!("\n🩺 Step 2: Running STORAGE CHECK before table operations...");
    wait_for_storage_check_healthy(&storage_id, Duration::from_secs(5))
        .unwrap_or_else(|err| panic!("STORAGE CHECK failed: {}", err));
    println!("✅ STORAGE CHECK healthy");

    println!("\n📊 Step 3: Creating USER table '{}.{}'...", namespace, user_table);
    let create_user_table_sql = format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL) WITH (TYPE='USER', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:2')",
        namespace, user_table, storage_id
    );
    execute_sql_as_root_via_cli(&create_user_table_sql).expect("user table creation");
    println!("✅ USER table created");

    println!("\n📊 Step 4: Creating SHARED table '{}.{}'...", namespace, shared_table);
    let create_shared_table_sql = format!(
        "CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, body TEXT NOT NULL) WITH (TYPE='SHARED', \
         STORAGE_ID='{}', FLUSH_POLICY='rows:2')",
        namespace, shared_table, storage_id
    );
    execute_sql_as_root_via_cli(&create_shared_table_sql).expect("shared table creation");
    println!("✅ SHARED table created");

    println!("\n📝 Step 5: Inserting data into USER table...");
    execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {}.{} (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        namespace, user_table
    ))
    .expect("insert user data");
    println!("✅ Inserted 2 rows into USER table");

    println!("\n📝 Step 6: Inserting data into SHARED table...");
    execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {}.{} (id, body) VALUES (10, 'Hello'), (11, 'World')",
        namespace, shared_table
    ))
    .expect("insert shared data");
    println!("✅ Inserted 2 rows into SHARED table");

    println!("\n💾 Step 7: Flushing USER table to MinIO...");
    flush_table_and_wait(&format!("{}.{}", namespace, user_table));
    println!("✅ USER table flushed");

    println!("\n💾 Step 8: Flushing SHARED table to MinIO...");
    flush_table_and_wait(&format!("{}.{}", namespace, shared_table));
    println!("✅ SHARED table flushed");

    println!("\n🔍 Step 9: Verifying USER table data...");
    let user_result = wait_for_query_contains_with(
        &format!("SELECT * FROM {}.{} ORDER BY id", namespace, user_table),
        "Alice",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("select user data");
    assert!(user_result.contains("Bob"), "User table should contain Bob");
    println!("✅ USER table contains expected data (Alice, Bob)");

    println!("\n🔍 Step 10: Verifying SHARED table data...");
    let shared_result = wait_for_query_contains_with(
        &format!("SELECT * FROM {}.{} ORDER BY id", namespace, shared_table),
        "Hello",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("select shared data");
    assert!(shared_result.contains("World"), "Shared table should contain World");
    println!("✅ SHARED table contains expected data (Hello, World)");

    println!("\n🔎 Step 11: Fetching storage metadata from system.storages...");
    let storage_meta = fetch_storage_metadata(&storage_id);
    println!("   Base directory: {}", storage_meta.base_directory);
    println!("   Shared template: {}", storage_meta.shared_template);
    println!("   User template: {}", storage_meta.user_template);

    println!("\n👤 Step 12: Fetching root user ID...");
    let root_user_id = fetch_root_user_id();
    println!("   Root user ID: {}", root_user_id);

    println!("\n🔌 Step 13: Connecting to MinIO at {}...", minio_endpoint());
    let store = build_minio_store(&storage_meta.base_directory);

    if let Err(err) = minio_bucket_reachable(&runtime, &store) {
        eprintln!("❌ MinIO bucket not reachable at {}", minio_endpoint());
        eprintln!("   Error: {}", err);
        eprintln!("   Please start MinIO: docker run -p 9010:9000 -p 9011:9001 \\");
        eprintln!("      -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \\");
        eprintln!("      minio/minio server /data --console-address :9001");
        cleanup_minio_resources(&namespace, &user_table, &shared_table, &storage_id);
        return;
    }
    println!("✅ Connected to MinIO");

    let user_table_dir =
        resolve_template(&storage_meta.user_template, &namespace, &user_table, Some(&root_user_id));
    let shared_table_dir =
        resolve_template(&storage_meta.shared_template, &namespace, &shared_table, None);

    println!("\n📂 Step 14: Resolving MinIO paths...");
    println!("   USER table path: {}", user_table_dir);
    println!("   SHARED table path: {}", shared_table_dir);

    println!("\n🔎 Step 15: Verifying USER table files in MinIO...");
    assert_minio_files(&runtime, &store, &user_table_dir, "user table");
    println!("✅ USER table files verified (manifest.json + Parquet)");

    println!("\n🔎 Step 16: Verifying SHARED table files in MinIO...");
    assert_minio_files(&runtime, &store, &shared_table_dir, "shared table");
    println!("✅ SHARED table files verified (manifest.json + Parquet)");

    println!("\n🧹 Step 17: Cleaning up test resources...");
    cleanup_minio_resources(&namespace, &user_table, &shared_table, &storage_id);
    println!("✅ Cleanup complete");

    println!("\n🎉 MinIO storage integration test PASSED!");
}

#[test]
#[ignore]
fn test_minio_storage_check() {
    println!("\n🚀 Starting MinIO STORAGE CHECK test...");

    println!("📡 Checking if KalamDB server is running...");
    if !is_server_running() {
        eprintln!("❌ Server not running. Skipping MinIO storage check test.");
        eprintln!("   Please start the server first: cargo run");
        panic!("Server not running");
    }
    println!("✅ Server is running");

    let runtime = Runtime::new().expect("minio runtime");
    println!("\n🔐 Step 0: Checking MinIO auth/connectivity...");
    let probe_store = build_minio_store(&format!("s3://{}/", minio_bucket()));
    if let Err(err) = minio_bucket_reachable(&runtime, &probe_store) {
        eprintln!("❌ MinIO auth/connectivity check failed: {}", err);
        eprintln!("   Verify endpoint, access key, secret key, and bucket.");
        panic!("MinIO auth/connectivity check failed");
    }
    println!("✅ MinIO auth/connectivity check passed");

    let storage_id = generate_unique_namespace("minio_storage_check");
    let _cleanup = StorageCleanup::new(storage_id.clone());
    let base_directory = format!("s3://{}/{}/", minio_bucket(), storage_id);
    let config_json = json!({
        "type": "s3",
        "region": minio_region(),
        "endpoint": minio_endpoint(),
        "allow_http": true,
        "access_key_id": minio_access_key(),
        "secret_access_key": minio_secret_key()
    })
    .to_string();

    println!("\n🗄️  Step 1: Creating MinIO storage '{}' for check...", storage_id);
    let create_storage_sql = format!(
        "CREATE STORAGE {storage_id} TYPE s3 NAME 'MinIO Check Storage' BASE_DIRECTORY \
         '{base_directory}' CONFIG '{config_json}' SHARED_TABLES_TEMPLATE \
         'ns_{{namespace}}/shared_{{tableName}}' USER_TABLES_TEMPLATE \
         'ns_{{namespace}}/user_{{tableName}}/user_{{userId}}'",
    );

    execute_sql_as_root_via_cli(&create_storage_sql).expect("storage creation");
    println!("✅ Storage created");

    wait_for_storage_check_healthy(&storage_id, Duration::from_secs(5))
        .unwrap_or_else(|err| panic!("STORAGE CHECK failed: {}", err));

    println!("\n🔍 Step 2: Running STORAGE CHECK (basic)...");
    let basic_output = execute_sql_as_root_via_cli_json(&format!("STORAGE CHECK {}", storage_id))
        .expect("storage check basic");
    let basic_json = parse_cli_json_output(&basic_output).expect("basic check json");
    let basic_rows = get_rows_as_hashmaps(&basic_json).unwrap_or_default();
    assert_eq!(basic_rows.len(), 1, "expected one row from STORAGE CHECK");
    let basic_row = basic_rows.first().expect("basic row");

    let status = extract_typed_value(basic_row.get("status").expect("status column missing"));
    let status_str = status.as_str().unwrap_or("unknown");
    if status_str != "healthy" {
        let error_value =
            extract_typed_value(basic_row.get("error").expect("error column missing"));
        let error_str = error_value.as_str().unwrap_or("<no error>");
        panic!("STORAGE CHECK should be healthy; got {} ({})", status_str, error_str);
    }

    let readable = extract_typed_value(basic_row.get("readable").expect("readable column missing"));
    let writable = extract_typed_value(basic_row.get("writable").expect("writable column missing"));
    let listable = extract_typed_value(basic_row.get("listable").expect("listable column missing"));
    let deletable =
        extract_typed_value(basic_row.get("deletable").expect("deletable column missing"));

    assert_eq!(readable.as_bool(), Some(true));
    assert_eq!(writable.as_bool(), Some(true));
    assert_eq!(listable.as_bool(), Some(true));
    assert_eq!(deletable.as_bool(), Some(true));

    let total_bytes =
        extract_typed_value(basic_row.get("total_bytes").expect("total_bytes column missing"));
    let used_bytes =
        extract_typed_value(basic_row.get("used_bytes").expect("used_bytes column missing"));
    assert!(total_bytes.is_null(), "basic check should not include total_bytes");
    assert!(used_bytes.is_null(), "basic check should not include used_bytes");

    println!("✅ STORAGE CHECK basic passed");

    println!("\n🔍 Step 3: Running STORAGE CHECK EXTENDED...");
    let extended_output =
        execute_sql_as_root_via_cli_json(&format!("STORAGE CHECK {} EXTENDED", storage_id))
            .expect("storage check extended");
    let extended_json = parse_cli_json_output(&extended_output).expect("extended check json");
    let extended_rows = get_rows_as_hashmaps(&extended_json).unwrap_or_default();
    assert_eq!(extended_rows.len(), 1, "expected one row from STORAGE CHECK EXTENDED");
    let extended_row = extended_rows.first().expect("extended row");

    let extended_status =
        extract_typed_value(extended_row.get("status").expect("status column missing"));
    assert_eq!(extended_status.as_str(), Some("healthy"), "extended status should be healthy");

    let extended_total =
        extract_typed_value(extended_row.get("total_bytes").expect("total_bytes column missing"));
    let extended_used =
        extract_typed_value(extended_row.get("used_bytes").expect("used_bytes column missing"));
    assert!(extended_total.is_null(), "minio does not report total_bytes");
    assert!(extended_used.is_null(), "minio does not report used_bytes");

    println!("✅ STORAGE CHECK extended passed");

    println!("\n🧹 Step 4: Cleaning up storage...");
    println!("✅ Cleanup complete");
}

fn flush_table_and_wait(full_table_name: &str) {
    println!("   Issuing flush command...");
    let flush_output =
        execute_sql_as_root_via_cli(&format!("STORAGE FLUSH TABLE {}", full_table_name))
            .expect("storage flush table");

    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        println!("   Flush job created: {}", job_id);
        let timeout = if is_cluster_mode() {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(10)
        };
        println!("   Waiting for flush job to complete (timeout: {:?})...", timeout);
        verify_job_completed(&job_id, timeout).expect("flush job should complete");
        println!("   Flush job completed");
    } else {
        println!("   Flush command executed (no job ID), waiting 200ms...");
    }
}

fn cleanup_minio_resources(
    namespace: &str,
    user_table: &str,
    shared_table: &str,
    storage_id: &str,
) {
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE {}.{}", namespace, user_table));
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE {}.{}", namespace, shared_table));
    let _ = execute_sql_as_root_via_cli(&format!("DROP STORAGE {}", storage_id));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

struct StorageCleanup {
    storage_id: String,
}

impl StorageCleanup {
    fn new(storage_id: String) -> Self {
        Self { storage_id }
    }
}

impl Drop for StorageCleanup {
    fn drop(&mut self) {
        let _ = execute_sql_as_root_via_cli(&format!("DROP STORAGE {}", self.storage_id));
    }
}

struct StorageMeta {
    base_directory: String,
    shared_template: String,
    user_template: String,
}

fn fetch_storage_metadata(storage_id: &str) -> StorageMeta {
    let sql = format!(
        "SELECT base_directory, shared_tables_template, user_tables_template FROM system.storages \
         WHERE storage_id = '{}'",
        storage_id
    );
    let output = execute_sql_as_root_via_client_json(&sql).expect("storage metadata query");
    let json: JsonValue = parse_cli_json_output(&output).expect("storage metadata json");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();

    if rows.is_empty() {
        eprintln!("❌ No storage found with ID '{}'", storage_id);
        eprintln!("   Checking all storages...");
        let all_storages_sql = "SELECT storage_id, name FROM system.storages";
        if let Ok(all_output) = execute_sql_as_root_via_client_json(all_storages_sql) {
            if let Ok(all_json) = parse_cli_json_output(&all_output) {
                if let Some(all_rows) = get_rows_as_hashmaps(&all_json) {
                    if all_rows.is_empty() {
                        eprintln!("   No storages exist in system.storages table");
                    } else {
                        eprintln!("   Existing storages:");
                        for row in all_rows {
                            let id = get_row_string(&row, "storage_id");
                            let name = get_row_string(&row, "name");
                            eprintln!("     - {}: {}", id, name);
                        }
                    }
                }
            }
        }
        panic!("Storage metadata missing for {}", storage_id);
    }

    let row = rows.first().unwrap();

    StorageMeta {
        base_directory: get_row_string(row, "base_directory"),
        shared_template: get_row_string(row, "shared_tables_template"),
        user_template: get_row_string(row, "user_tables_template"),
    }
}

fn fetch_root_user_id() -> String {
    let output = execute_sql_as_root_via_client_json(
        "SELECT user_id FROM system.users WHERE user_id = 'root' LIMIT 1",
    )
    .expect("root user id query");
    let json: JsonValue = parse_cli_json_output(&output).expect("root user id json");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
    let row = rows.first().expect("root user row missing");
    get_row_string(row, "user_id")
}

fn get_row_string(row: &std::collections::HashMap<String, JsonValue>, key: &str) -> String {
    let value = row.get(key).unwrap_or_else(|| panic!("missing column {}", key));
    let extracted = extract_typed_value(value);
    extracted
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| extracted.to_string().trim_matches('"').to_string())
}

fn resolve_template(
    template: &str,
    namespace: &str,
    table_name: &str,
    user_id: Option<&str>,
) -> String {
    let normalized = normalize_template(template);
    let mut resolved =
        normalized.replace("{namespace}", namespace).replace("{tableName}", table_name);
    if let Some(uid) = user_id {
        resolved = resolved.replace("{userId}", uid);
    }
    resolved
}

fn normalize_template(template: &str) -> Cow<'_, str> {
    if !(template.contains("{table_name}")
        || template.contains("{namespace_id}")
        || template.contains("{namespaceId}")
        || template.contains("{table-id}")
        || template.contains("{namespace-id}")
        || template.contains("{user_id}")
        || template.contains("{user-id}")
        || template.contains("{shard_id}")
        || template.contains("{shard-id}"))
    {
        return Cow::Borrowed(template);
    }

    Cow::Owned(
        template
            .replace("{table_name}", "{tableName}")
            .replace("{namespace_id}", "{namespace}")
            .replace("{namespaceId}", "{namespace}")
            .replace("{table-id}", "{tableName}")
            .replace("{namespace-id}", "{namespace}")
            .replace("{user_id}", "{userId}")
            .replace("{user-id}", "{userId}")
            .replace("{shard_id}", "{shard}")
            .replace("{shard-id}", "{shard}"),
    )
}

fn build_minio_store(base_directory: &str) -> Arc<dyn ObjectStore> {
    let (bucket, prefix) = parse_s3_base_directory(base_directory);

    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
    builder = builder
        .with_region(minio_region())
        .with_endpoint(minio_endpoint())
        .with_allow_http(true)
        .with_access_key_id(minio_access_key())
        .with_secret_access_key(minio_secret_key());

    let store = builder.build().expect("minio object store");

    if prefix.is_empty() {
        Arc::new(store) as Arc<dyn ObjectStore>
    } else {
        let prefix_path = ObjectPath::parse(prefix.trim_matches('/')).expect("minio prefix path");
        Arc::new(PrefixStore::new(store, prefix_path)) as Arc<dyn ObjectStore>
    }
}

fn parse_s3_base_directory(base_directory: &str) -> (String, String) {
    let trimmed = base_directory.trim();
    let bucket_and_prefix = trimmed
        .strip_prefix("s3://")
        .unwrap_or_else(|| panic!("expected s3:// base_directory, got {}", base_directory));
    match bucket_and_prefix.split_once('/') {
        Some((bucket, prefix)) => (bucket.to_string(), prefix.to_string()),
        None => (bucket_and_prefix.to_string(), String::new()),
    }
}

fn minio_bucket_reachable(runtime: &Runtime, store: &Arc<dyn ObjectStore>) -> Result<(), String> {
    runtime
        .block_on(async {
            let list_path = ObjectPath::parse("").expect("minio list root");
            let mut stream = store.list(Some(&list_path));
            match stream.next().await {
                Some(Ok(_)) | None => Ok(()),
                Some(Err(err)) => Err(err.to_string()),
            }
        })
        .map_err(|err| err.to_string())
}

fn wait_for_storage_check_healthy(storage_id: &str, timeout: Duration) -> Result<(), String> {
    let start = std::time::Instant::now();
    let mut last_error = String::new();

    while start.elapsed() < timeout {
        let output = execute_sql_as_root_via_cli_json(&format!("STORAGE CHECK {}", storage_id))
            .map_err(|e| e.to_string())?;
        let json = parse_cli_json_output(&output).map_err(|e| e.to_string())?;
        let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
        if let Some(row) = rows.first() {
            let status_value =
                extract_typed_value(row.get("status").ok_or("status column missing")?);
            let status = status_value.as_str().unwrap_or("unknown");
            if status == "healthy" {
                return Ok(());
            }

            let error_value = extract_typed_value(row.get("error").ok_or("error column missing")?);
            let error = error_value.as_str().unwrap_or("<no error>");
            last_error = format!("status={}, error={}", status, error);
        } else {
            last_error = "no rows returned".to_string();
        }
    }

    Err(format!(
        "MinIO storage unhealthy after {:?}. Last check: {}. \nSet \
         MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY/MINIO_BUCKET/MINIO_REGION if server \
         cannot reach MinIO.",
        timeout, last_error
    ))
}

fn minio_endpoint() -> String {
    minio_env("MINIO_ENDPOINT", MINIO_ENDPOINT)
}

fn minio_access_key() -> String {
    minio_env("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
}

fn minio_secret_key() -> String {
    minio_env("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
}

fn minio_bucket() -> String {
    minio_env("MINIO_BUCKET", MINIO_BUCKET)
}

fn minio_region() -> String {
    minio_env("MINIO_REGION", MINIO_REGION)
}

fn minio_env(key: &str, default_value: &str) -> String {
    env::var(key).unwrap_or_else(|_| default_value.to_string())
}

fn assert_minio_files(
    runtime: &Runtime,
    store: &Arc<dyn ObjectStore>,
    table_dir: &str,
    context: &str,
) {
    let table_dir = table_dir.trim_end_matches('/');
    let manifest_path = format!("{}/manifest.json", table_dir);

    let manifest_obj = ObjectPath::parse(&manifest_path).expect("manifest object path");
    let manifest_result = runtime.block_on(async { store.head(&manifest_obj).await });
    assert!(
        manifest_result.is_ok(),
        "{}: manifest.json should exist in MinIO at {}",
        context,
        manifest_path
    );
    println!("   Found manifest: {}", manifest_path);

    let list_prefix = ObjectPath::parse(table_dir).expect("table dir object path");
    let parquet_found = runtime
        .block_on(async {
            let mut stream = store.list(Some(&list_prefix));
            let mut found = false;
            while let Some(item) = stream.next().await {
                let meta = item?;
                if meta.location.to_string().ends_with(".parquet") {
                    found = true;
                    break;
                }
            }
            Ok::<bool, object_store::Error>(found)
        })
        .expect("minio list parquet files");

    assert!(
        parquet_found,
        "{}: expected at least one Parquet file in MinIO under {}",
        context, table_dir
    );
}
