//! Smoke tests for table export/import transfer APIs and SHOW EXPORT integration.

use std::time::Duration;

use reqwest::multipart;
use serde_json::Value;

use crate::common::*;

const TABLE_TRANSFER_TIMEOUT: Duration = Duration::from_secs(180);
const SHOW_EXPORT_ROW_TIMEOUT: Duration = Duration::from_secs(30);

fn parse_count_from_row(row: &std::collections::HashMap<String, Value>, key: &str) -> Option<i64> {
    let raw = row.get(key)?;
    let scalar = extract_arrow_value(raw).unwrap_or_else(|| raw.clone());
    match scalar {
        Value::Number(n) => n.as_i64().or_else(|| n.as_u64().and_then(|v| i64::try_from(v).ok())),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn get_token_sync(username: &str, password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let username = username.to_string();
    let password = password.to_string();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let token = rt.block_on(async move { get_access_token(&username, &password).await })?;
    Ok(token)
}

fn start_table_export_sync(
    token: &str,
    namespace: &str,
    table: &str,
    table_type: &str,
    user_id: Option<&str>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let base = server_url().trim_end_matches('/').to_string();
    let token = token.to_string();
    let namespace = namespace.to_string();
    let table = table.to_string();
    let table_type = table_type.to_string();
    let user_id = user_id.map(ToOwned::to_owned);

    let value: Result<Value, Box<dyn std::error::Error>> = rt.block_on(async move {
        let mut body = serde_json::json!({
            "namespace_id": namespace,
            "table_name": table,
            "table_type": table_type,
        });
        if let Some(user_id) = user_id {
            body["user_id"] = Value::String(user_id);
        }

        let response = shared_http_client()
            .post(format!("{}/v1/api/table-exports", base))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        let json = response.json::<Value>().await?;
        if !status.is_success() {
            return Err(std::io::Error::other(format!(
                "table export failed ({}): {}",
                status, json
            ))
            .into());
        }

        Ok(json)
    });

    let value = value?;

    Ok(value)
}

fn start_table_import_sync(
    token: &str,
    namespace: &str,
    table: &str,
    table_type: &str,
    user_id: Option<&str>,
    filename: &str,
    zip_bytes: Vec<u8>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let base = server_url().trim_end_matches('/').to_string();
    let token = token.to_string();
    let namespace = namespace.to_string();
    let table = table.to_string();
    let table_type = table_type.to_string();
    let user_id = user_id.map(ToOwned::to_owned);
    let filename = filename.to_string();

    let value: Result<Value, Box<dyn std::error::Error>> = rt.block_on(async move {
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
        let json = response.json::<Value>().await?;
        if !status.is_success() {
            return Err(std::io::Error::other(format!(
                "table import failed ({}): {}",
                status, json
            ))
            .into());
        }

        Ok(json)
    });

    let value = value?;

    Ok(value)
}

fn http_get_with_token(
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

fn wait_for_show_export_row(
    job_id: &str,
    timeout: Duration,
) -> Result<Value, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_millis(300);
    let mut last_rows: Vec<std::collections::HashMap<String, Value>> = Vec::new();

    while start.elapsed() <= timeout {
        let show_export = execute_sql_as_root_via_client_json("SHOW EXPORT")?;
        let show_json: Value = serde_json::from_str(&show_export)?;
        let rows = get_rows_as_hashmaps(&show_json).unwrap_or_default();

        if let Some(row) = rows.into_iter().find(|row| {
            row.get("job_id")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("job_id").cloned())
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .is_some_and(|candidate| candidate == job_id)
        }) {
            return Ok(serde_json::to_value(row)?);
        }

        last_rows = get_rows_as_hashmaps(&show_json).unwrap_or_default();
        std::thread::sleep(poll_interval);
    }

    Err(format!(
        "SHOW EXPORT did not contain job_id={} within {:?}; last rows={:?}",
        job_id, timeout, last_rows
    )
    .into())
}

#[ntest::timeout(300_000)]
#[test]
fn smoke_table_export_import_shared_via_api_and_show_export() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("tbl_xfer_ns");
    let source_table = generate_unique_table("tbl_xfer_src");
    let target_table = generate_unique_table("tbl_xfer_dst");
    let source_fqn = format!("{}.{}", namespace, source_table);
    let target_fqn = format!("{}.{}", namespace, target_table);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, note TEXT) WITH (TYPE='SHARED', \
         FLUSH_POLICY='rows:5')",
        source_fqn
    ))
    .expect("create source table");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, note TEXT) WITH (TYPE='SHARED')",
        target_fqn
    ))
    .expect("create target table");
    grant_public_shared_table_access(&source_fqn);
    grant_public_shared_table_access(&target_fqn);

    wait_for_table_ready(&source_fqn, Duration::from_secs(5)).expect("source table ready");
    wait_for_table_ready(&target_fqn, Duration::from_secs(5)).expect("target table ready");

    for index in 1..=8 {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (note) VALUES ('shared_row_{}')",
            source_fqn, index
        ))
        .expect("insert source row");
    }

    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", source_fqn))
            .expect("flush source table");
    let flush_job_id =
        parse_job_id_from_flush_output(&flush_output).expect("flush job id from output");
    verify_job_completed(&flush_job_id, Duration::from_secs(45)).expect("flush should complete");

    // Keep extra rows hot so export must cover both flushed and in-memory data.
    for index in 9..=12 {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (note) VALUES ('shared_row_hot_{}')",
            source_fqn, index
        ))
        .expect("insert hot source row");
    }

    let token = get_token_sync(default_username(), default_password()).expect("admin token");

    let export = start_table_export_sync(&token, &namespace, &source_table, "shared", None)
        .expect("start table export");
    let export_job_id = export
        .get("job_id")
        .and_then(|value| value.as_str())
        .expect("export job_id")
        .to_string();

    let export_status =
        wait_for_job_finished(&export_job_id, TABLE_TRANSFER_TIMEOUT).expect("wait export job");
    assert_eq!(export_status, "completed", "table export should complete");

    let row = wait_for_show_export_row(&export_job_id, SHOW_EXPORT_ROW_TIMEOUT)
        .expect("table export row in SHOW EXPORT");

    let download_path = row
        .get("download_url")
        .and_then(extract_arrow_value)
        .or_else(|| row.get("download_url").cloned())
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .expect("table export download_url");
    assert!(download_path.starts_with("/v1/table-exports/"));

    let download_url = format!("{}{}", server_url().trim_end_matches('/'), download_path);
    let (status_code, content_type, zip_body) =
        http_get_with_token(&download_url, &token).expect("download table export zip");
    assert_eq!(status_code, 200, "expected successful table export download");
    assert!(
        content_type.contains("application/zip")
            || content_type.contains("application/octet-stream")
    );
    assert!(zip_body.starts_with(&[0x50, 0x4b, 0x03, 0x04]), "download should be ZIP bytes");

    let import = start_table_import_sync(
        &token,
        &namespace,
        &target_table,
        "shared",
        None,
        "table-export.zip",
        zip_body,
    )
    .expect("start table import");
    let import_job_id = import
        .get("job_id")
        .and_then(|value| value.as_str())
        .expect("import job_id")
        .to_string();

    let import_status =
        wait_for_job_finished(&import_job_id, TABLE_TRANSFER_TIMEOUT).expect("wait import job");
    assert_eq!(import_status, "completed", "table import should complete");

    let source_count =
        execute_sql_as_root_via_client_json(&format!("SELECT COUNT(*) AS c FROM {}", source_fqn))
            .expect("source count");
    let source_count_json: Value = serde_json::from_str(&source_count).expect("parse source count");
    let source_rows = get_rows_as_hashmaps(&source_count_json).unwrap_or_default();
    let source_count_value = source_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("source count value");

    let target_count =
        execute_sql_as_root_via_client_json(&format!("SELECT COUNT(*) AS c FROM {}", target_fqn))
            .expect("target count");
    let target_count_json: Value = serde_json::from_str(&target_count).expect("parse target count");
    let target_rows = get_rows_as_hashmaps(&target_count_json).unwrap_or_default();
    let target_count_value = target_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("target count value");

    assert_eq!(target_count_value, source_count_value, "imported row count should match source");

    let hot_row_check = execute_sql_as_root_via_client_json(&format!(
        "SELECT COUNT(*) AS c FROM {} WHERE note = 'shared_row_hot_12'",
        target_fqn
    ))
    .expect("target hot row check");
    let hot_row_json: Value = serde_json::from_str(&hot_row_check).expect("parse hot row check");
    let hot_rows = get_rows_as_hashmaps(&hot_row_json).unwrap_or_default();
    let hot_row_count = hot_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("hot row count value");
    assert_eq!(hot_row_count, 1, "imported data should include hot rows");

    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", source_fqn));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", target_fqn));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(120_000)]
#[test]
fn smoke_table_export_user_requires_user_id() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("tbl_xfer_user_ns");
    let table = generate_unique_table("tbl_xfer_user");
    let table_fqn = format!("{}.{}", namespace, table);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, note TEXT) WITH (TYPE='USER')",
        table_fqn
    ))
    .expect("create user table");
    wait_for_table_ready(&table_fqn, Duration::from_secs(5)).expect("table ready");

    let token = get_token_sync(default_username(), default_password()).expect("admin token");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (status_code, response_json) = rt.block_on(async {
        let response = shared_http_client()
            .post(format!("{}/v1/api/table-exports", server_url().trim_end_matches('/')))
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "namespace_id": namespace,
                "table_name": table,
                "table_type": "user"
            }))
            .send()
            .await
            .expect("request");
        let status_code = response.status().as_u16();
        let payload = response.json::<Value>().await.expect("json");
        (status_code, payload)
    });

    assert_eq!(status_code, 400, "expected table export validation failure");

    let message = response_json
        .get("message")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_lowercase();
    assert!(
        message.contains("user_id"),
        "expected user_id validation error, got: {response_json}"
    );

    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", table_fqn));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

#[ntest::timeout(300_000)]
#[test]
fn smoke_table_export_import_user_table_flushed_and_hot_data() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("tbl_xfer_user_ns");
    let source_table = generate_unique_table("tbl_user_src");
    let target_table = generate_unique_table("tbl_user_dst");
    let source_fqn = format!("{}.{}", namespace, source_table);
    let target_fqn = format!("{}.{}", namespace, target_table);

    let export_user = generate_unique_namespace("tbl_xfer_user_actor");
    let export_pass = "TableTransfer123!";

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'service'",
        export_user, export_pass
    ))
    .expect("create transfer user");

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, note TEXT) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:5')",
        source_fqn
    ))
    .expect("create source user table");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, note TEXT) WITH (TYPE='USER')",
        target_fqn
    ))
    .expect("create target user table");

    wait_for_table_ready(&source_fqn, Duration::from_secs(5)).expect("source table ready");
    wait_for_table_ready(&target_fqn, Duration::from_secs(5)).expect("target table ready");

    for index in 1..=8 {
        execute_sql_via_client_as(
            &export_user,
            export_pass,
            &format!("INSERT INTO {} (note) VALUES ('user_row_{}')", source_fqn, index),
        )
        .expect("insert user source row");
    }

    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", source_fqn))
            .expect("flush user source table");
    let flush_job_id =
        parse_job_id_from_flush_output(&flush_output).expect("flush job id from output");
    verify_job_completed(&flush_job_id, Duration::from_secs(45)).expect("flush should complete");

    for index in 9..=12 {
        execute_sql_via_client_as(
            &export_user,
            export_pass,
            &format!("INSERT INTO {} (note) VALUES ('user_row_hot_{}')", source_fqn, index),
        )
        .expect("insert user hot row");
    }

    let token = get_token_sync(default_username(), default_password()).expect("admin token");

    let export =
        start_table_export_sync(&token, &namespace, &source_table, "user", Some(&export_user))
            .expect("start user table export");
    let export_job_id = export
        .get("job_id")
        .and_then(|value| value.as_str())
        .expect("export job_id")
        .to_string();

    let export_status =
        wait_for_job_finished(&export_job_id, TABLE_TRANSFER_TIMEOUT).expect("wait export job");
    assert_eq!(export_status, "completed", "user table export should complete");

    let row = wait_for_show_export_row(&export_job_id, SHOW_EXPORT_ROW_TIMEOUT)
        .expect("user table export row in SHOW EXPORT");
    let download_path = row
        .get("download_url")
        .and_then(extract_arrow_value)
        .or_else(|| row.get("download_url").cloned())
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .expect("table export download_url");
    assert!(download_path.starts_with("/v1/table-exports/"));

    let download_url = format!("{}{}", server_url().trim_end_matches('/'), download_path);
    let (status_code, _content_type, zip_body) =
        http_get_with_token(&download_url, &token).expect("download table export zip");
    assert_eq!(status_code, 200, "expected successful table export download");

    let import = start_table_import_sync(
        &token,
        &namespace,
        &target_table,
        "user",
        Some(&export_user),
        "table-export-user.zip",
        zip_body,
    )
    .expect("start table import");
    let import_job_id = import
        .get("job_id")
        .and_then(|value| value.as_str())
        .expect("import job_id")
        .to_string();

    let import_status =
        wait_for_job_finished(&import_job_id, TABLE_TRANSFER_TIMEOUT).expect("wait import job");
    assert_eq!(import_status, "completed", "user table import should complete");

    // Verify target table exists and imported rows are visible to the same user scope.
    let table_exists = execute_sql_as_root_via_client_json(&format!(
        "SELECT COUNT(*) AS c FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, target_table
    ))
    .expect("target table exists query");
    let table_exists_json: Value = serde_json::from_str(&table_exists).expect("parse table exists");
    let table_exists_rows = get_rows_as_hashmaps(&table_exists_json).unwrap_or_default();
    let table_exists_count = table_exists_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("table exists count");
    assert_eq!(table_exists_count, 1, "target table should exist");

    let source_count = execute_sql_via_client_as_json(
        &export_user,
        export_pass,
        &format!("SELECT COUNT(*) AS c FROM {}", source_fqn),
    )
    .expect("source count json");
    let source_count_json: Value = serde_json::from_str(&source_count).expect("parse source count");
    let source_rows = get_rows_as_hashmaps(&source_count_json).unwrap_or_default();
    let source_count_value = source_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("source count value");

    let target_count = execute_sql_via_client_as_json(
        &export_user,
        export_pass,
        &format!("SELECT COUNT(*) AS c FROM {}", target_fqn),
    )
    .expect("target count json");
    let target_count_json: Value = serde_json::from_str(&target_count).expect("parse target count");
    let target_rows = get_rows_as_hashmaps(&target_count_json).unwrap_or_default();
    let target_count_value = target_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("target count value");

    assert_eq!(
        target_count_value, source_count_value,
        "imported user row count should match source"
    );

    let hot_row_check = execute_sql_via_client_as_json(
        &export_user,
        export_pass,
        &format!("SELECT COUNT(*) AS c FROM {} WHERE note = 'user_row_hot_12'", target_fqn),
    )
    .expect("target user hot row check");
    let hot_row_json: Value = serde_json::from_str(&hot_row_check).expect("parse hot row check");
    let hot_rows = get_rows_as_hashmaps(&hot_row_json).unwrap_or_default();
    let hot_row_count = hot_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("hot row count value");
    assert_eq!(hot_row_count, 1, "imported user data should include hot rows");

    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", source_fqn));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", target_fqn));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", export_user));
}
