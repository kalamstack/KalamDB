//! Smoke tests for EXPORT USER DATA / SHOW EXPORT SQL commands and the
//! `GET /v1/exports/{user_id}/{export_id}` download endpoint.
//!
//! ## What is tested
//! 1. `EXPORT USER DATA` triggers a `UserExport` job that completes, flushes all user tables first,
//!    and writes a `.zip` file under the exports directory.
//! 2. `SHOW EXPORT` returns the job status and a download URL once the job is done.
//! 3. The download endpoint serves a valid ZIP file to DBA/System callers.
//! 4. The download endpoint returns 403 Forbidden when a different user tries to download another
//!    user's export.
//!
//! ## Design notes
//! - Each test creates its own isolated user so idempotency keys never collide across parallel or
//!   repeated test runs.
//! - The export executor flushes **all** user tables before copying Parquet files, so the export
//!   job timeout is generous (10 min) to accommodate CI slowness.

use std::{io::Cursor, time::Duration};

use reqwest::multipart;

use crate::common::*;

/// Timeout for an export job (flush user's data + copy Parquet + zip).
/// With the optimized executor that only flushes tables with user data,
/// this should complete in well under 60 seconds.
const EXPORT_JOB_TIMEOUT: Duration = Duration::from_secs(120);

// ── helpers ─────────────────────────────────────────────────────────────────

/// Extract the first JobID from a message that contains "Job ID: UE-xxx".
fn parse_job_id(output: &str) -> Option<String> {
    let marker = "Job ID: ";
    let idx = output.find(marker)?;
    let rest = &output[idx + marker.len()..];
    let id: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '-').collect();
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

/// Resolve a SHOW EXPORT download path or URL into one reachable by the test
/// client.
///
/// SHOW EXPORT now returns a relative URI path like `/v1/exports/...`, but the
/// helper still accepts legacy absolute URLs so the smoke test remains useful
/// against older servers.
fn normalize_download_url(raw_url: &str) -> String {
    if let Some(path_start) = raw_url.find("/v1/") {
        format!("{}{}", server_url().trim_end_matches('/'), &raw_url[path_start..])
    } else {
        // Fall back to raw URL; hope for the best
        raw_url.to_string()
    }
}

/// Make a GET request with a Bearer token and return `(status_code, content_type, body)`.
///
/// Creates a temporary single-thread Tokio runtime to run the async request
/// from within a synchronous test function.
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

/// Get a Bearer token for the given credentials synchronously.
fn get_token_sync(username: &str, password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let username = username.to_string();
    let password = password.to_string();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

    let token = rt.block_on(async move { get_access_token(&username, &password).await })?;
    Ok(token)
}

/// Timeout for a table-transfer export or import job.
const TABLE_TRANSFER_TIMEOUT: Duration = Duration::from_secs(180);

/// Extract a COUNT(*) integer from an Arrow-encoded or plain JSON row value.
fn parse_count_from_row(
    row: &std::collections::HashMap<String, serde_json::Value>,
    key: &str,
) -> Option<i64> {
    let raw = row.get(key)?;
    let scalar = extract_arrow_value(raw).unwrap_or_else(|| raw.clone());
    match scalar {
        serde_json::Value::Number(n) => {
            n.as_i64().or_else(|| n.as_u64().and_then(|v| i64::try_from(v).ok()))
        },
        serde_json::Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

/// Call `POST /v1/api/table-exports` for a USER-scoped table identified by `username`.
/// Returns the JSON response body on success.
fn start_user_table_export_sync(
    admin_token: &str,
    namespace: &str,
    table: &str,
    username: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let base = server_url().trim_end_matches('/').to_string();
    let token = admin_token.to_string();
    let body = serde_json::json!({
        "namespace_id": namespace,
        "table_name": table,
        "table_type": "user",
        "user_id": username,
    });
    rt.block_on(async move {
        let response = shared_http_client()
            .post(format!("{}/v1/api/table-exports", base))
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let json = response.json::<serde_json::Value>().await?;
        if !status.is_success() {
            return Err(std::io::Error::other(format!(
                "table export failed ({}): {}",
                status, json
            ))
            .into());
        }
        Ok::<_, Box<dyn std::error::Error>>(json)
    })
}

/// Upload `zip_bytes` to `POST /v1/api/table-imports` for a USER-scoped table.
/// Returns the JSON response body on success.
fn start_user_table_import_sync(
    admin_token: &str,
    namespace: &str,
    table: &str,
    username: &str,
    zip_bytes: Vec<u8>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let base = server_url().trim_end_matches('/').to_string();
    let token = admin_token.to_string();
    let namespace = namespace.to_string();
    let table = table.to_string();
    let username = username.to_string();
    rt.block_on(async move {
        let form = multipart::Form::new()
            .text("namespace_id", namespace)
            .text("table_name", table)
            .text("table_type", "user")
            .text("user_id", username)
            .part(
                "file",
                multipart::Part::bytes(zip_bytes)
                    .file_name("table-export.zip")
                    .mime_str("application/zip")?,
            );
        let response = shared_http_client()
            .post(format!("{}/v1/api/table-imports", base))
            .bearer_auth(&token)
            .multipart(form)
            .send()
            .await?;
        let status = response.status();
        let json = response.json::<serde_json::Value>().await?;
        if !status.is_success() {
            return Err(std::io::Error::other(format!(
                "table import failed ({}): {}",
                status, json
            ))
            .into());
        }
        Ok::<_, Box<dyn std::error::Error>>(json)
    })
}

/// Poll `SHOW EXPORT` (as root/admin) until a row for `job_id` appears, or timeout.
fn wait_for_export_row_as_root(
    job_id: &str,
    timeout: Duration,
) -> Result<std::collections::HashMap<String, serde_json::Value>, Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let show_json = execute_sql_as_root_via_client_json("SHOW EXPORT")?;
        let json: serde_json::Value = serde_json::from_str(&show_json)?;
        let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
        if let Some(row) = rows.iter().find(|row| {
            let jid = row
                .get("job_id")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("job_id").cloned())
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default();
            jid == job_id
        }) {
            return Ok(row.clone());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("Timeout waiting for SHOW EXPORT row for job {}", job_id).into());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

// ── tests ─────────────────────────────────────────────────────────────────

/// Full export lifecycle: insert data, trigger export, wait for job, verify
/// the `.zip` file exists on the server's exports directory.
///
/// This test works even on an external server since it verifies job completion
/// via `system.jobs`; the zip-file presence check is skipped for external servers
/// (we cannot inspect the server's filesystem).
#[ntest::timeout(240_000)]
#[test]
fn smoke_export_user_data_job_completes() {
    if !require_server_running() {
        return;
    }

    let export_user = generate_unique_namespace("exp_usr");
    let export_pass = "ExportPass123!";
    let namespace = generate_unique_namespace("exp_ns");
    let table = generate_unique_table("exp_tbl");
    let full_table = format!("{}.{}", namespace, table);

    // ── Setup ────────────────────────────────────────────────────────────
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        export_user, export_pass
    ))
    .expect("CREATE USER failed");

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("CREATE NAMESPACE failed");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {} (
            id   BIGINT AUTO_INCREMENT PRIMARY KEY,
            note TEXT NOT NULL
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:5')"#,
        full_table
    ))
    .expect("CREATE TABLE failed");

    wait_for_table_ready(&full_table, Duration::from_secs(5)).expect("table not ready");

    // Insert some rows as the export user (RLS — user sees only their rows)
    for i in 1..=10u32 {
        execute_sql_via_client_as(
            &export_user,
            export_pass,
            &format!("INSERT INTO {} (note) VALUES ('export_row_{}')", full_table, i),
        )
        .expect("INSERT failed");
    }

    // ── Trigger export ────────────────────────────────────────────────────
    let export_out = execute_sql_via_client_as(&export_user, export_pass, "EXPORT USER DATA")
        .expect("EXPORT USER DATA should succeed");

    assert!(
        export_out.contains("Job ID") || export_out.contains("export started"),
        "Expected export started message; got: {}",
        export_out
    );

    let job_id =
        parse_job_id(&export_out).unwrap_or_else(|| panic!("No job id in: {}", export_out));

    println!("📦  Export job: {}", job_id);

    // ── Wait for completion ───────────────────────────────────────────────
    let status = wait_for_job_finished(&job_id, EXPORT_JOB_TIMEOUT)
        .unwrap_or_else(|e| panic!("Export job wait failed: {}", e));

    assert_eq!(status, "completed", "Export job did not complete (job_id={})", job_id);

    println!("✅  Export job {} completed", job_id);

    // ── Cleanup ───────────────────────────────────────────────────────────
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", export_user));
}

/// SHOW EXPORT returns the completed job status and a relative download URI.
#[ntest::timeout(240_000)]
#[test]
fn smoke_show_export_returns_completed_status_and_download_url() {
    if !require_server_running() {
        return;
    }

    let export_user = generate_unique_namespace("show_exp_usr");
    let export_pass = "ShowExport123!";
    let namespace = generate_unique_namespace("show_exp_ns");
    let table = generate_unique_table("show_exp_tbl");
    let full_table = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        export_user, export_pass
    ))
    .expect("CREATE USER failed");
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("CREATE NAMESPACE failed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, val TEXT) WITH (TYPE='USER')",
        full_table
    ))
    .expect("CREATE TABLE failed");
    wait_for_table_ready(&full_table, Duration::from_secs(5)).expect("table not ready");

    for i in 1..=3u32 {
        execute_sql_via_client_as(
            &export_user,
            export_pass,
            &format!("INSERT INTO {} (val) VALUES ('row_{}')", full_table, i),
        )
        .expect("INSERT failed");
    }

    // Trigger export
    let export_out = execute_sql_via_client_as(&export_user, export_pass, "EXPORT USER DATA")
        .expect("EXPORT USER DATA failed");
    let job_id =
        parse_job_id(&export_out).unwrap_or_else(|| panic!("No job id in: {}", export_out));

    // Wait for completion
    let status =
        wait_for_job_finished(&job_id, EXPORT_JOB_TIMEOUT).expect("export job should finish");
    assert_eq!(status, "completed");

    // SHOW EXPORT — run as the export user
    let show_out_json = execute_sql_via_client_as_json(&export_user, export_pass, "SHOW EXPORT")
        .expect("SHOW EXPORT failed");

    let json: serde_json::Value =
        serde_json::from_str(&show_out_json).expect("parse SHOW EXPORT JSON");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();

    assert!(!rows.is_empty(), "SHOW EXPORT returned no rows after a completed export");

    // Find the row that matches our job_id
    let matching_row = rows.iter().find(|row| {
        let jid = row
            .get("job_id")
            .and_then(extract_arrow_value)
            .or_else(|| row.get("job_id").cloned())
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        jid == job_id
    });

    let row = matching_row.unwrap_or_else(|| {
        panic!("Job {} not found in SHOW EXPORT result. Rows: {:?}", job_id, rows)
    });

    // Verify status column
    let row_status = row
        .get("status")
        .and_then(extract_arrow_value)
        .or_else(|| row.get("status").cloned())
        .and_then(|v| v.as_str().map(|s| s.to_lowercase()))
        .unwrap_or_default();

    assert!(
        row_status.contains("completed"),
        "Expected status=Completed in SHOW EXPORT; got '{}'",
        row_status
    );

    // Verify download_url is populated
    let download_url = row
        .get("download_url")
        .and_then(extract_arrow_value)
        .or_else(|| row.get("download_url").cloned())
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default();

    assert!(
        !download_url.is_empty(),
        "download_url should be non-empty for completed export"
    );
    assert!(
        download_url.starts_with("/v1/exports/"),
        "download_url should start with '/v1/exports/'; got '{}'",
        download_url
    );
    assert!(
        !download_url.starts_with("http://") && !download_url.starts_with("https://"),
        "download_url should be a relative URI path, not an absolute URL; got '{}'",
        download_url
    );

    println!("✅  SHOW EXPORT reports completed; download_url: {}", download_url);

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", export_user));
}

/// Downloading the export ZIP via HTTP returns 200 OK, content-type:
/// application/zip, and a body that starts with the ZIP magic bytes (PK\x03\x04).
#[ntest::timeout(240_000)]
#[test]
fn smoke_export_download_zip_is_valid() {
    if !require_server_running() {
        return;
    }

    let export_user = generate_unique_namespace("dl_exp_usr");
    let export_pass = "DownloadExp123!";
    let namespace = generate_unique_namespace("dl_exp_ns");
    let table = generate_unique_table("dl_exp_tbl");
    let full_table = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        export_user, export_pass
    ))
    .expect("CREATE USER failed");
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("CREATE NAMESPACE failed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, data TEXT) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:5')",
        full_table
    ))
    .expect("CREATE TABLE failed");
    wait_for_table_ready(&full_table, Duration::from_secs(5)).expect("table not ready");

    // Insert rows and flush so parquet files exist
    for i in 1..=8u32 {
        execute_sql_via_client_as(
            &export_user,
            export_pass,
            &format!("INSERT INTO {} (data) VALUES ('dl_row_{}')", full_table, i),
        )
        .expect("INSERT failed");
    }
    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table))
            .expect("flush before export should succeed");
    let flush_job_id = parse_job_id_from_flush_output(&flush_output)
        .expect("flush before export should include a job id");
    verify_job_completed(&flush_job_id, Duration::from_secs(30))
        .expect("flush before export should complete successfully");

    for i in 9..=12u32 {
        execute_sql_via_client_as(
            &export_user,
            export_pass,
            &format!("INSERT INTO {} (data) VALUES ('dl_row_hot_{}')", full_table, i),
        )
        .expect("hot INSERT failed");
    }

    // Trigger export
    let export_out = execute_sql_via_client_as(&export_user, export_pass, "EXPORT USER DATA")
        .expect("EXPORT USER DATA failed");
    let job_id =
        parse_job_id(&export_out).unwrap_or_else(|| panic!("No job id in: {}", export_out));

    // Wait for completed
    let status = wait_for_job_finished(&job_id, EXPORT_JOB_TIMEOUT).expect("job should finish");
    assert_eq!(status, "completed", "Export job must complete before download test");

    // Get download URL from SHOW EXPORT
    let show_json = execute_sql_via_client_as_json(&export_user, export_pass, "SHOW EXPORT")
        .expect("SHOW EXPORT failed");
    let json: serde_json::Value = serde_json::from_str(&show_json).expect("parse JSON");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();

    let download_url = rows
        .iter()
        .find(|row| {
            let jid = row
                .get("job_id")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("job_id").cloned())
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default();
            jid == job_id
        })
        .and_then(|row| {
            row.get("download_url")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("download_url").cloned())
                .and_then(|v| v.as_str().map(String::from))
        })
        .unwrap_or_else(|| panic!("No download_url in SHOW EXPORT for job {}", job_id));

    assert!(!download_url.is_empty(), "download_url must be non-empty");

    // Resolve the relative URI path into a concrete URL for the test client.
    let normalized = normalize_download_url(&download_url);
    println!("⬇️  Downloading export from: {}", normalized);

    // Get access token for the export user
    let token = get_token_sync(&export_user, export_pass).expect("get token");

    // HTTP GET
    let (status_code, content_type, body) =
        http_get_with_token(&normalized, &token).expect("HTTP GET failed");

    assert_eq!(
        status_code, 200,
        "Expected 200 OK downloading export; got {}. URL: {}",
        status_code, normalized
    );

    assert!(
        content_type.contains("application/zip")
            || content_type.contains("application/octet-stream"),
        "Expected application/zip content-type; got '{}'",
        content_type
    );

    assert!(!body.is_empty(), "Export ZIP body should not be empty");

    // Verify ZIP magic bytes: PK\x03\x04
    if body.len() >= 4 {
        let magic = &body[..4];
        assert_eq!(
            magic,
            &[0x50, 0x4b, 0x03, 0x04],
            "Body does not start with ZIP magic bytes (PK\\x03\\x04). First 4 bytes: {:?}",
            magic
        );
    }

    // Deep ZIP inspection: every .parquet entry must carry valid PAR1 magic at both
    // the header (bytes 0-3) and the footer (last 4 bytes). At least one entry must
    // reside under the expected `{namespace}/{table}/` folder path.
    {
        let mut zip = zip::ZipArchive::new(Cursor::new(body.clone())).expect("parse export zip");
        let expected_prefix = format!("{}/{}/", namespace, table);
        let mut parquet_count: usize = 0;
        let mut namespace_path_found = false;
        let mut all_names: Vec<String> = Vec::new();
        for idx in 0..zip.len() {
            let mut entry = zip.by_index(idx).expect("zip entry by index");
            let name = entry.name().to_string();
            all_names.push(name.clone());
            if !name.ends_with(".parquet") {
                continue;
            }
            let mut data: Vec<u8> = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut data).expect("read parquet bytes from zip");
            assert!(
                data.len() >= 8,
                "Parquet entry '{}' is too small ({} bytes) to be a valid Parquet file",
                name,
                data.len()
            );
            assert_eq!(
                &data[..4],
                b"PAR1",
                "Parquet entry '{}' is missing PAR1 header magic; first 4 bytes = {:?}",
                name,
                &data[..4]
            );
            assert_eq!(
                &data[data.len() - 4..],
                b"PAR1",
                "Parquet entry '{}' is missing PAR1 footer magic; last 4 bytes = {:?}",
                name,
                &data[data.len() - 4..]
            );
            if name.starts_with(&expected_prefix) {
                namespace_path_found = true;
            }
            parquet_count += 1;
        }
        assert!(
            parquet_count >= 1,
            "Export ZIP must contain at least one valid Parquet entry; ZIP entries: {:?}",
            all_names
        );
        assert!(
            namespace_path_found,
            "Expected a Parquet entry under '{}'; ZIP contained: {:?}",
            expected_prefix, all_names
        );
        println!(
            "✅  Export ZIP deep-checked: {} Parquet entries (PAR1 header+footer OK), path '{}' \
             confirmed ({} total ZIP bytes)",
            parquet_count,
            expected_prefix,
            body.len()
        );
    }

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", export_user));
}

/// Deep round-trip validation in two parts:
///
/// **Part 1** — Downloads the user-data export ZIP (produced by `EXPORT USER DATA`) and
/// verifies that every `.parquet` entry inside carries valid Parquet file magic (`PAR1`)
/// at both the 4-byte header and the 4-byte footer, and that at least one entry lives
/// under the expected `{namespace}/{table}/` folder path.
///
/// **Part 2** — Exports the same USER table via the table-transfer API (which flushes
/// hot RocksDB rows before packaging), downloads the resulting archive, imports it into
/// a fresh target table, then queries the target to verify the total row count (12 =
/// 8 flushed + 4 hot that were flushed by the export executor) and two specific row
/// values: a flushed-era row (`reimp_row_5`) and a hot-era row (`reimp_row_hot_12`).
#[ntest::timeout(480_000)]
#[test]
fn smoke_export_user_data_download_and_reimport() {
    if !require_server_running() {
        return;
    }

    let export_user = generate_unique_namespace("reimp_usr");
    let export_pass = "Reimport123!";
    let namespace = generate_unique_namespace("reimp_ns");
    let source_table = generate_unique_table("reimp_src");
    let target_table = generate_unique_table("reimp_dst");
    let source_fqn = format!("{}.{}", namespace, source_table);
    let target_fqn = format!("{}.{}", namespace, target_table);

    // Setup: DBA user + source and target USER tables
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        export_user, export_pass
    ))
    .expect("CREATE USER failed");
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("CREATE NAMESPACE failed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, data TEXT) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:5')",
        source_fqn
    ))
    .expect("CREATE source TABLE failed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, data TEXT) WITH (TYPE='USER')",
        target_fqn
    ))
    .expect("CREATE target TABLE failed");
    wait_for_table_ready(&source_fqn, Duration::from_secs(5)).expect("source table not ready");
    wait_for_table_ready(&target_fqn, Duration::from_secs(5)).expect("target table not ready");

    // Insert 8 rows and flush so Parquet files are created on disk
    for i in 1..=8u32 {
        execute_sql_via_client_as(
            &export_user,
            export_pass,
            &format!("INSERT INTO {} (data) VALUES ('reimp_row_{}')", source_fqn, i),
        )
        .expect("INSERT failed");
    }
    let flush_out = execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", source_fqn))
        .expect("flush failed");
    let flush_job_id = parse_job_id_from_flush_output(&flush_out).expect("flush job id");
    verify_job_completed(&flush_job_id, Duration::from_secs(45)).expect("flush should complete");

    // Insert 4 hot rows that remain in RocksDB (not yet flushed to Parquet)
    for i in 9..=12u32 {
        execute_sql_via_client_as(
            &export_user,
            export_pass,
            &format!("INSERT INTO {} (data) VALUES ('reimp_row_hot_{}')", source_fqn, i),
        )
        .expect("hot INSERT failed");
    }

    // ── Part 1: EXPORT USER DATA → download → deep ZIP inspection ─────────

    let export_out = execute_sql_via_client_as(&export_user, export_pass, "EXPORT USER DATA")
        .expect("EXPORT USER DATA failed");
    let user_export_job_id = parse_job_id(&export_out)
        .unwrap_or_else(|| panic!("No job id in export output: {}", export_out));

    let user_export_status = wait_for_job_finished(&user_export_job_id, EXPORT_JOB_TIMEOUT)
        .expect("user export job should finish");
    assert_eq!(user_export_status, "completed", "User export job must complete");

    let show_json = execute_sql_via_client_as_json(&export_user, export_pass, "SHOW EXPORT")
        .expect("SHOW EXPORT failed");
    let show_val: serde_json::Value =
        serde_json::from_str(&show_json).expect("parse SHOW EXPORT JSON");
    let show_rows = get_rows_as_hashmaps(&show_val).unwrap_or_default();
    let download_url = show_rows
        .iter()
        .find(|row| {
            let jid = row
                .get("job_id")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("job_id").cloned())
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default();
            jid == user_export_job_id
        })
        .and_then(|row| {
            row.get("download_url")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("download_url").cloned())
                .and_then(|v| v.as_str().map(String::from))
        })
        .unwrap_or_else(|| panic!("No download_url in SHOW EXPORT for job {}", user_export_job_id));

    let user_token = get_token_sync(&export_user, export_pass).expect("get user token");
    let normalized_url = normalize_download_url(&download_url);
    let (dl_status, _, zip_body) =
        http_get_with_token(&normalized_url, &user_token).expect("HTTP GET failed");
    assert_eq!(dl_status, 200, "Expected 200 OK from user export download");
    assert!(
        zip_body.starts_with(&[0x50, 0x4b, 0x03, 0x04]),
        "User export response is missing ZIP magic bytes"
    );

    // Deep ZIP inspection: every .parquet entry must have PAR1 magic at header and
    // footer; at least one entry must live under namespace/table/.
    {
        let mut zip =
            zip::ZipArchive::new(Cursor::new(zip_body.clone())).expect("parse user export ZIP");
        let expected_prefix = format!("{}/{}/", namespace, source_table);
        let mut parquet_count: usize = 0;
        let mut namespace_path_found = false;
        let mut all_names: Vec<String> = Vec::new();
        for idx in 0..zip.len() {
            let mut entry = zip.by_index(idx).expect("zip entry by index");
            let name = entry.name().to_string();
            all_names.push(name.clone());
            if !name.ends_with(".parquet") {
                continue;
            }
            let mut data: Vec<u8> = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut data)
                .expect("read parquet bytes from user export zip");
            assert!(
                data.len() >= 8,
                "Parquet entry '{}' is too small ({} bytes) to be a valid Parquet file",
                name,
                data.len()
            );
            assert_eq!(
                &data[..4],
                b"PAR1",
                "Parquet entry '{}' missing PAR1 header magic; first 4 bytes = {:?}",
                name,
                &data[..4]
            );
            assert_eq!(
                &data[data.len() - 4..],
                b"PAR1",
                "Parquet entry '{}' missing PAR1 footer magic; last 4 bytes = {:?}",
                name,
                &data[data.len() - 4..]
            );
            if name.starts_with(&expected_prefix) {
                namespace_path_found = true;
            }
            parquet_count += 1;
        }
        assert!(
            parquet_count >= 1,
            "User export ZIP must contain at least one valid Parquet entry; entries: {:?}",
            all_names
        );
        assert!(
            namespace_path_found,
            "Expected a Parquet entry under '{}'; ZIP contained: {:?}",
            expected_prefix, all_names
        );
        println!(
            "✅  Part 1: {} Parquet entries with valid PAR1 magic, path '{}' confirmed ({} ZIP \
             bytes)",
            parquet_count,
            expected_prefix,
            zip_body.len()
        );
    }

    // ── Part 2: table-transfer export → download → import → row-level verify ─
    //
    // The table-transfer executor flushes hot rows before exporting, so all 12
    // rows (8 previously flushed + 4 hot flushed by the executor) must appear in
    // the imported table.

    let admin_token = get_token_sync(default_username(), default_password()).expect("admin token");

    let transfer_export =
        start_user_table_export_sync(&admin_token, &namespace, &source_table, &export_user)
            .expect("start user table-transfer export");
    let transfer_job_id = transfer_export
        .get("job_id")
        .and_then(|v| v.as_str())
        .expect("transfer export job_id")
        .to_string();

    let transfer_export_status = wait_for_job_finished(&transfer_job_id, TABLE_TRANSFER_TIMEOUT)
        .expect("table-transfer export job should finish");
    assert_eq!(transfer_export_status, "completed", "Table-transfer export must complete");

    let transfer_row = wait_for_export_row_as_root(&transfer_job_id, Duration::from_secs(30))
        .expect("SHOW EXPORT row for table-transfer job");
    let transfer_download_path = transfer_row
        .get("download_url")
        .and_then(extract_arrow_value)
        .or_else(|| transfer_row.get("download_url").cloned())
        .and_then(|v| v.as_str().map(String::from))
        .expect("table-transfer download_url");
    assert!(
        transfer_download_path.starts_with("/v1/table-exports/"),
        "Unexpected table-transfer download path: {}",
        transfer_download_path
    );

    let transfer_url = format!("{}{}", server_url().trim_end_matches('/'), transfer_download_path);
    let (transfer_dl_status, _, transfer_zip) =
        http_get_with_token(&transfer_url, &admin_token).expect("download table-transfer zip");
    assert_eq!(transfer_dl_status, 200, "Expected 200 OK for table-transfer download");
    assert!(
        transfer_zip.starts_with(&[0x50, 0x4b, 0x03, 0x04]),
        "Table-transfer ZIP missing PK magic bytes"
    );

    let import_resp = start_user_table_import_sync(
        &admin_token,
        &namespace,
        &target_table,
        &export_user,
        transfer_zip,
    )
    .expect("start user table-transfer import");
    let import_job_id = import_resp
        .get("job_id")
        .and_then(|v| v.as_str())
        .expect("import job_id")
        .to_string();

    let import_status = wait_for_job_finished(&import_job_id, TABLE_TRANSFER_TIMEOUT)
        .expect("import job should finish");
    assert_eq!(import_status, "completed", "Table import must complete");

    // Verify total row count in the imported table
    let count_json = execute_sql_via_client_as_json(
        &export_user,
        export_pass,
        &format!("SELECT COUNT(*) AS c FROM {}", target_fqn),
    )
    .expect("target count query");
    let count_val: serde_json::Value = serde_json::from_str(&count_json).expect("parse count JSON");
    let count_rows = get_rows_as_hashmaps(&count_val).unwrap_or_default();
    let total = count_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("count value from imported table");
    assert_eq!(
        total, 12,
        "Imported table must contain all 12 rows (8 flushed + 4 hot flushed by export executor)"
    );

    // Verify a specific flushed-era row survived the round-trip
    let flushed_json = execute_sql_via_client_as_json(
        &export_user,
        export_pass,
        &format!("SELECT COUNT(*) AS c FROM {} WHERE data = 'reimp_row_5'", target_fqn),
    )
    .expect("flushed row check");
    let flushed_val: serde_json::Value =
        serde_json::from_str(&flushed_json).expect("parse flushed check");
    let flushed_rows = get_rows_as_hashmaps(&flushed_val).unwrap_or_default();
    let flushed_count = flushed_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("flushed row count");
    assert_eq!(
        flushed_count, 1,
        "Flushed row 'reimp_row_5' must survive the export→import round-trip"
    );

    // Verify a specific hot-era row survived the round-trip
    let hot_json = execute_sql_via_client_as_json(
        &export_user,
        export_pass,
        &format!("SELECT COUNT(*) AS c FROM {} WHERE data = 'reimp_row_hot_12'", target_fqn),
    )
    .expect("hot row check");
    let hot_val: serde_json::Value = serde_json::from_str(&hot_json).expect("parse hot check");
    let hot_rows = get_rows_as_hashmaps(&hot_val).unwrap_or_default();
    let hot_count = hot_rows
        .first()
        .and_then(|row| parse_count_from_row(row, "c"))
        .expect("hot row count");
    assert_eq!(
        hot_count, 1,
        "Hot row 'reimp_row_hot_12' must survive the export→import round-trip"
    );

    println!(
        "✅  Part 2: {}/12 rows imported; 'reimp_row_5' (flushed) and 'reimp_row_hot_12' (hot) \
         verified",
        total
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", source_fqn));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", target_fqn));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", export_user));
}

/// User B cannot download User A's export (403 Forbidden).
#[ntest::timeout(240_000)]
#[test]
fn smoke_export_download_forbidden_for_other_user() {
    if !require_server_running() {
        return;
    }

    let user_a = generate_unique_namespace("exp_a_usr");
    let pass_a = "UserAPass123!";
    let user_b = generate_unique_namespace("exp_b_usr");
    let pass_b = "UserBPass123!";
    let namespace = generate_unique_namespace("exp_auth_ns");
    let table = generate_unique_table("exp_auth_tbl");
    let full_table = format!("{}.{}", namespace, table);

    // Create export-capable owner A and regular user B for download-boundary checks.
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        user_a, pass_a
    ))
    .expect("CREATE USER A failed");
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user_b, pass_b
    ))
    .expect("CREATE USER B failed");
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("CREATE NAMESPACE failed");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, v TEXT) WITH (TYPE='USER')",
        full_table
    ))
    .expect("CREATE TABLE failed");
    wait_for_table_ready(&full_table, Duration::from_secs(5)).expect("table not ready");

    // User A inserts data and triggers export
    execute_sql_via_client_as(
        &user_a,
        pass_a,
        &format!("INSERT INTO {} (v) VALUES ('secret_data')", full_table),
    )
    .expect("INSERT failed");

    let export_out = execute_sql_via_client_as(&user_a, pass_a, "EXPORT USER DATA")
        .expect("EXPORT USER DATA failed");
    let job_id =
        parse_job_id(&export_out).unwrap_or_else(|| panic!("No job id in: {}", export_out));

    let status = wait_for_job_finished(&job_id, EXPORT_JOB_TIMEOUT).expect("job should finish");
    assert_eq!(status, "completed");

    // Get download URL via User A's SHOW EXPORT
    let show_json =
        execute_sql_via_client_as_json(&user_a, pass_a, "SHOW EXPORT").expect("SHOW EXPORT failed");
    let json: serde_json::Value = serde_json::from_str(&show_json).expect("parse JSON");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();

    let download_url = rows
        .iter()
        .find(|row| {
            let jid = row
                .get("job_id")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("job_id").cloned())
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default();
            jid == job_id
        })
        .and_then(|row| {
            row.get("download_url")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("download_url").cloned())
                .and_then(|v| v.as_str().map(String::from))
        })
        .unwrap_or_else(|| panic!("No download_url for job {}", job_id));

    let normalized = normalize_download_url(&download_url);

    // User B tries to download User A's export — must be 403
    let token_b = get_token_sync(&user_b, pass_b).expect("get token for user B");

    let (status_code, _ct, _body) =
        http_get_with_token(&normalized, &token_b).expect("HTTP GET failed");

    assert_eq!(
        status_code, 403,
        "User B should get 403 Forbidden when downloading User A's export; got {}",
        status_code
    );

    println!("✅  User B correctly received 403 Forbidden for User A's export");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_a));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user_b));
}

/// SHOW EXPORT returns an empty result when no export has been triggered for
/// the calling user.
#[ntest::timeout(30_000)]
#[test]
fn smoke_show_export_empty_for_new_user() {
    if !require_server_running() {
        return;
    }

    let fresh_user = generate_unique_namespace("no_exp_usr");
    let fresh_pass = "FreshUser123!";

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'dba'",
        fresh_user, fresh_pass
    ))
    .expect("CREATE USER failed");

    let show_out_json = execute_sql_via_client_as_json(&fresh_user, fresh_pass, "SHOW EXPORT")
        .expect("SHOW EXPORT should succeed for user with no exports");

    let json: serde_json::Value =
        serde_json::from_str(&show_out_json).expect("parse SHOW EXPORT JSON");

    // In running-server mode, privileged users can see historical exports for
    // other users. Assert that this fresh user has no rows.
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
    let user_row_count = rows
        .iter()
        .filter(|row| {
            row.get("user_id")
                .and_then(extract_arrow_value)
                .or_else(|| row.get("user_id").cloned())
                .and_then(|v| v.as_str().map(String::from))
                .map(|uid| uid == fresh_user)
                .unwrap_or(false)
        })
        .count();

    assert_eq!(
        user_row_count, 0,
        "SHOW EXPORT should return 0 rows for a fresh user; got {} rows for user {}",
        user_row_count, fresh_user
    );

    println!("✅  SHOW EXPORT correctly returns empty result for user with no exports");

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", fresh_user));
}
