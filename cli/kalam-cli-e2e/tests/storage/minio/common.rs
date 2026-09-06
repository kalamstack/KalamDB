#![allow(dead_code, unused_imports)]

use std::{
    borrow::Cow,
    env,
    sync::{Arc, OnceLock},
    time::Duration,
};

use futures_util::StreamExt;
use object_store::{
    aws::AmazonS3Builder, path::Path as ObjectPath, prefix::PrefixStore, ObjectStore,
    ObjectStoreExt,
};
use reqwest::multipart;
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime;

use crate::common::{
    admin_username, default_password, default_username, execute_sql_as_root_via_cli,
    execute_sql_as_root_via_client_json, extract_typed_value, get_rows_as_hashmaps,
    is_cluster_mode, is_server_running, leader_or_server_url, parse_cli_json_output,
    parse_job_id_from_flush_output, parse_job_id_from_json_message, server_url, shared_http_client,
    verify_job_completed, wait_for_job_finished,
};

pub(super) const MINIO_ENDPOINT: &str = "http://127.0.0.1:9120";
pub(super) const MINIO_ACCESS_KEY: &str = "minioadmin";
pub(super) const MINIO_SECRET_KEY: &str = "minioadmin";
pub(super) const MINIO_BUCKET: &str = "kalamdb-test";
pub(super) const MINIO_REGION: &str = "us-east-1";

pub(super) fn minio_endpoint() -> String {
    minio_env("MINIO_ENDPOINT", MINIO_ENDPOINT)
}

pub(super) fn minio_access_key() -> String {
    minio_env("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
}

pub(super) fn minio_secret_key() -> String {
    minio_env("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
}

pub(super) fn minio_bucket() -> String {
    minio_env("MINIO_BUCKET", MINIO_BUCKET)
}

pub(super) fn minio_region() -> String {
    minio_env("MINIO_REGION", MINIO_REGION)
}

fn minio_env(key: &str, default_value: &str) -> String {
    env::var(key).unwrap_or_else(|_| default_value.to_string())
}

fn server_supports_s3_storage() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| {
        let probe_id = format!("_minio_s3_probe_{}", std::process::id());
        let config_json = json!({
            "type": "s3",
            "region": minio_region(),
            "endpoint": minio_endpoint(),
            "allow_http": true,
            "access_key_id": minio_access_key(),
            "secret_access_key": minio_secret_key()
        })
        .to_string();
        let sql = format!(
            "CREATE STORAGE {probe_id} TYPE 's3' NAME 'probe' BASE_DIRECTORY 's3://{}/probe/' \
             CONFIG '{config_json}'",
            minio_bucket()
        );
        match execute_sql_as_root_via_cli(&sql) {
            Ok(_) => {
                let _ = execute_sql_as_root_via_cli(&format!("DROP STORAGE {probe_id}"));
                true
            },
            Err(err) => {
                let msg = err.to_string();
                !msg.contains("not compiled in") && !msg.contains("cloud-* feature")
            },
        }
    })
}

pub(crate) fn should_run_minio_storage_tests() -> bool {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping MinIO storage test.");
        return false;
    }

    if !server_supports_s3_storage() {
        eprintln!(
            "⚠️  Server at {} does not include S3/object storage (build with --features \
             cloud-aws). Skipping MinIO storage test.",
            server_url()
        );
        eprintln!(
            "    Rebuild: cd backend && cargo build -p kalamdb-server --bin kalamdb-server \
             --features cloud-aws"
        );
        eprintln!(
            "    Or run: KALAMDB_SERVER_TYPE=fresh ./cli/run-tests.sh --package kalam-cli-e2e \
             --test-target storage"
        );
        return false;
    }

    let runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!(
                "⚠️  Failed to create tokio runtime for MinIO probe: {err}. Skipping MinIO \
                 storage test."
            );
            return false;
        },
    };
    let store = build_minio_store(&format!("s3://{}/", minio_bucket()));
    if let Err(err) = minio_bucket_reachable(&runtime, &store) {
        eprintln!(
            "⚠️  MinIO endpoint {} is not reachable ({err}). Skipping MinIO storage test.",
            minio_endpoint()
        );
        eprintln!("    Start MinIO: cd docker/utils && docker compose up -d minio");
        return false;
    }

    true
}

pub(super) fn setup_minio_storage(storage_id: &str, storage_name: &str) {
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

    let create_storage_sql = format!(
        "CREATE STORAGE {storage_id} TYPE 's3' NAME '{storage_name}' BASE_DIRECTORY \
         '{base_directory}' CONFIG '{config_json}' SHARED_TABLES_TEMPLATE \
         'ns_{{namespace}}/shared_{{tableName}}' USER_TABLES_TEMPLATE \
         'ns_{{namespace}}/user_{{tableName}}/user_{{userId}}'",
    );

    execute_sql_as_root_via_cli(&create_storage_sql).expect("storage creation");
    wait_for_storage_check_healthy(storage_id, Duration::from_secs(5))
        .unwrap_or_else(|err| panic!("STORAGE CHECK failed: {}", err));
}

pub(super) fn build_minio_store(base_directory: &str) -> Arc<dyn ObjectStore> {
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

pub(super) fn minio_bucket_reachable(
    runtime: &Runtime,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), String> {
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

pub(super) fn wait_for_storage_check_healthy(
    storage_id: &str,
    timeout: Duration,
) -> Result<(), String> {
    let start = std::time::Instant::now();
    let mut last_error = String::new();

    while start.elapsed() < timeout {
        let output = execute_sql_as_root_via_client_json(&format!("STORAGE CHECK {}", storage_id))
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
        "MinIO storage unhealthy after {:?}. Last check: {}.\nSet \
         MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY/MINIO_BUCKET/MINIO_REGION if server \
         cannot reach MinIO.",
        timeout, last_error
    ))
}

#[derive(Debug, Clone)]
pub(super) struct StorageMeta {
    pub base_directory:  String,
    pub shared_template: String,
    pub user_template:   String,
}

pub(super) fn fetch_storage_metadata(storage_id: &str) -> StorageMeta {
    let sql = format!(
        "SELECT base_directory, shared_tables_template, user_tables_template FROM system.storages \
         WHERE storage_id = '{}'",
        storage_id
    );
    let output = execute_sql_as_root_via_client_json(&sql).expect("storage metadata query");
    let json: JsonValue = parse_cli_json_output(&output).expect("storage metadata json");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();

    if rows.is_empty() {
        panic!("Storage metadata missing for {}", storage_id);
    }

    let row = rows.first().unwrap();

    StorageMeta {
        base_directory:  get_row_string(row, "base_directory"),
        shared_template: get_row_string(row, "shared_tables_template"),
        user_template:   get_row_string(row, "user_tables_template"),
    }
}

pub(super) fn resolve_template(
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

pub(super) fn assert_minio_files(
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

    let list_prefix = ObjectPath::parse(table_dir).expect("table dir object path");

    // Poll for the parquet file with retries: flush jobs complete their status before the
    // S3 multipart upload finishes, so under concurrent test load the file may lag slightly.
    let max_retries = 20;
    let retry_delay = Duration::from_millis(500);
    let mut parquet_found = false;
    for _ in 0..max_retries {
        parquet_found = runtime
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
        if parquet_found {
            break;
        }
        std::thread::sleep(retry_delay);
    }

    assert!(
        parquet_found,
        "{}: expected at least one Parquet file in MinIO under {}",
        context, table_dir
    );
}

pub(super) fn assert_minio_vix_file(
    runtime: &Runtime,
    store: &Arc<dyn ObjectStore>,
    table_dir: &str,
    context: &str,
) {
    let table_dir = table_dir.trim_end_matches('/');
    let list_prefix = ObjectPath::parse(table_dir).expect("table dir object path");
    let vix_found = runtime
        .block_on(async {
            let mut stream = store.list(Some(&list_prefix));
            let mut found = false;
            while let Some(item) = stream.next().await {
                let meta = item?;
                if meta.location.to_string().ends_with(".vix") {
                    found = true;
                    break;
                }
            }
            Ok::<bool, object_store::Error>(found)
        })
        .expect("minio list vix files");

    assert!(
        vix_found,
        "{}: expected at least one vector index (.vix) file in MinIO under {}",
        context, table_dir
    );
}

pub(super) fn manifest_segment_paths(namespace: &str, table_name: &str) -> Vec<String> {
    let sql = format!(
        "SELECT manifest_json FROM system.manifest WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table_name
    );
    let output = execute_sql_as_root_via_client_json(&sql).expect("query system.manifest");
    let parsed: JsonValue = serde_json::from_str(&output).expect("parse system.manifest JSON");
    let rows = get_rows_as_hashmaps(&parsed).unwrap_or_default();

    let mut paths = Vec::new();
    for row in rows {
        if let Some(value) = row.get("manifest_json") {
            let extracted = extract_typed_value(value);
            let manifest_json: Option<JsonValue> = if extracted.is_object() {
                Some(extracted)
            } else {
                extracted
                    .as_str()
                    .and_then(|manifest_str| serde_json::from_str::<JsonValue>(manifest_str).ok())
            };

            if let Some(manifest_json) = manifest_json {
                if let Some(segments) = manifest_json.get("segments").and_then(JsonValue::as_array)
                {
                    for segment in segments {
                        if let Some(path) = segment.get("path").and_then(JsonValue::as_str) {
                            paths.push(path.to_string());
                        }
                    }
                }
            }
        }
    }

    paths
}

pub(super) fn manifest_segment_count(namespace: &str, table_name: &str) -> usize {
    manifest_segment_paths(namespace, table_name).len()
}

pub(super) fn assert_manifest_segment_count(
    namespace: &str,
    table_name: &str,
    expected_count: usize,
) {
    let actual_count = manifest_segment_count(namespace, table_name);
    assert_eq!(
        actual_count, expected_count,
        "unexpected segment count for {}.{}",
        namespace, table_name
    );
}

pub(super) fn query_count(sql: &str) -> i64 {
    let output = execute_sql_as_root_via_client_json(sql).expect("count query");
    let json: JsonValue = serde_json::from_str(&output).expect("parse count json");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
    let row = rows.first().expect("count row missing");
    for key in ["c", "count", "COUNT(*)"] {
        if let Some(value) = row.get(key) {
            let extracted = extract_typed_value(value);
            if let Some(count) = extracted
                .as_i64()
                .or_else(|| extracted.as_u64().and_then(|v| i64::try_from(v).ok()))
            {
                return count;
            }
            if let Some(text) = extracted.as_str() {
                if let Ok(count) = text.parse::<i64>() {
                    return count;
                }
            }
        }
    }
    panic!("could not parse count from query output: {}", output);
}

pub(super) fn admin_access_token() -> String {
    let runtime = tokio::runtime::Runtime::new().expect("admin token runtime");
    runtime
        .block_on(async {
            crate::common::get_access_token(default_username(), default_password()).await
        })
        .expect("admin access token")
}

pub(super) fn http_get_with_token(
    url: &str,
    token: &str,
) -> Result<(u16, String, Vec<u8>), Box<dyn std::error::Error>> {
    let url = url.to_string();
    let token = token.to_string();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

    let result = rt.block_on(async move {
        let response = shared_http_client().get(&url).bearer_auth(&token).send().await?;

        let status = response.status().as_u16();
        let ct = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body: Vec<u8> = response.bytes().await?.to_vec();
        Ok::<_, reqwest::Error>((status, ct, body))
    })?;

    Ok(result)
}

pub(super) fn wait_for_terminal_job_status(
    job_id: &str,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let effective_timeout = std::cmp::max(timeout, Duration::from_secs(30));
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_millis(250);

    loop {
        if start.elapsed() > effective_timeout {
            return Err(format!(
                "Timeout waiting for job {} to reach a terminal state after {:?}",
                job_id, effective_timeout
            )
            .into());
        }

        let query =
            format!("SELECT job_id, status, message FROM system.jobs WHERE job_id = '{}'", job_id);

        let output = execute_sql_as_root_via_client_json(&query)?;
        let json: JsonValue = serde_json::from_str(&output)?;
        if let Some(rows) = get_rows_as_hashmaps(&json) {
            if let Some(row) = rows.first() {
                let status_value = row
                    .get("status")
                    .and_then(|value| Some(extract_typed_value(value)))
                    .or_else(|| row.get("status").cloned())
                    .unwrap_or(JsonValue::Null);
                let status = status_value
                    .as_str()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| status_value.to_string());
                let status_lower = status.to_lowercase();

                if status_lower.contains("completed")
                    || status_lower.contains("failed")
                    || status_lower.contains("skipped")
                    || status_lower.contains("cancelled")
                {
                    return Ok(status_lower);
                }
            }
        }

        std::thread::sleep(poll_interval);
    }
}

pub(super) fn start_table_export_sync(
    token: &str,
    namespace: &str,
    table: &str,
    table_type: &str,
    user_id: Option<&str>,
) -> Result<JsonValue, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let base = leader_or_server_url().trim_end_matches('/').to_string();
    let token = token.to_string();
    let namespace = namespace.to_string();
    let table = table.to_string();
    let table_type = table_type.to_string();
    let user_id = user_id.map(ToOwned::to_owned);

    let value: Result<JsonValue, Box<dyn std::error::Error>> = rt.block_on(async move {
        let mut body = serde_json::json!({
            "namespace_id": namespace,
            "table_name": table,
            "table_type": table_type,
        });
        if let Some(user_id) = user_id {
            body["user_id"] = JsonValue::String(user_id);
        }

        let response = shared_http_client()
            .post(format!("{}/v1/api/table-exports", base))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        let json = response.json::<JsonValue>().await?;
        if !status.is_success() {
            return Err(std::io::Error::other(format!(
                "table export failed ({}): {}",
                status, json
            ))
            .into());
        }

        Ok(json)
    });

    Ok(value?)
}

pub(super) fn start_table_import_sync(
    token: &str,
    namespace: &str,
    table: &str,
    table_type: &str,
    user_id: Option<&str>,
    filename: &str,
    zip_bytes: Vec<u8>,
) -> Result<JsonValue, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let base = leader_or_server_url().trim_end_matches('/').to_string();
    let token = token.to_string();
    let namespace = namespace.to_string();
    let table = table.to_string();
    let table_type = table_type.to_string();
    let user_id = user_id.map(ToOwned::to_owned);
    let filename = filename.to_string();

    let value: Result<JsonValue, Box<dyn std::error::Error>> = rt.block_on(async move {
        let mut form = multipart::Form::new()
            .text("namespace_id", namespace)
            .text("table_name", table)
            .text("table_type", table_type)
            .part(
                "file",
                multipart::Part::bytes(zip_bytes)
                    .file_name(filename)
                    .mime_str("application/zip")?,
            );
        if let Some(user_id) = user_id {
            form = form.text("user_id", user_id);
        }

        let response = shared_http_client()
            .post(format!("{}/v1/api/table-imports", base))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await?;

        let status = response.status();
        let json = response.json::<JsonValue>().await?;
        if !status.is_success() {
            return Err(std::io::Error::other(format!(
                "table import failed ({}): {}",
                status, json
            ))
            .into());
        }

        Ok(json)
    });

    Ok(value?)
}

pub(super) fn flush_table_and_wait(full_table_name: &str) {
    let flush_output =
        execute_sql_as_root_via_cli(&format!("STORAGE FLUSH TABLE {}", full_table_name))
            .expect("storage flush table");

    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        let timeout = if is_cluster_mode() {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(10)
        };
        verify_job_completed(&job_id, timeout).expect("flush job should complete");
    }
}

pub(super) fn cleanup_minio_resources(
    namespace: &str,
    user_table: &str,
    shared_table: &str,
    storage_id: &str,
) {
    let mut dropped_tables = Vec::new();
    for table_name in [user_table, shared_table] {
        if table_name.is_empty() || dropped_tables.iter().any(|dropped| *dropped == table_name) {
            continue;
        }

        let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE {}.{}", namespace, table_name));
        dropped_tables.push(table_name);
    }
    let _ = execute_sql_as_root_via_cli(&format!("DROP STORAGE {}", storage_id));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

fn get_row_string(row: &std::collections::HashMap<String, JsonValue>, key: &str) -> String {
    let value = row.get(key).unwrap_or_else(|| panic!("missing column {}", key));
    let extracted = extract_typed_value(value);
    extracted
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| extracted.to_string().trim_matches('"').to_string())
}
