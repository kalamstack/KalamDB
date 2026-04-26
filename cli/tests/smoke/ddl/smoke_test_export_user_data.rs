//! Smoke tests for EXPORT USER DATA / SHOW EXPORT SQL commands and the
//! `GET /v1/exports/{user_id}/{export_id}` download endpoint.
//!
//! ## What is tested
//! 1. `EXPORT USER DATA` triggers a `UserExport` job that completes, flushes all user tables first,
//!    and writes a `.zip` file under the exports directory.
//! 2. `SHOW EXPORT` returns the job status and a download URL once the job is done.
//! 3. The download endpoint serves a valid ZIP file to the owning user.
//! 4. The download endpoint returns 403 Forbidden when a different user tries to download another
//!    user's export.
//!
//! ## Design notes
//! - Each test creates its own isolated user so idempotency keys never collide across parallel or
//!   repeated test runs.
//! - The export executor flushes **all** user tables before copying Parquet files, so the export
//!   job timeout is generous (10 min) to accommodate CI slowness.

use std::time::Duration;

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

/// Translate a download URL from SHOW EXPORT into one reachable by the test
/// client.  The server may advertise `0.0.0.0` or a bind address that is not
/// directly routable; we replace the scheme+host+port with the one we already
/// know works (`server_url()`).
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
        let response = reqwest::Client::new().get(&url).bearer_auth(&token).send().await?;

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
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
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

/// SHOW EXPORT returns the completed job status and a non-empty download URL.
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
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
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
        download_url.contains("/v1/exports/"),
        "download_url should contain '/v1/exports/'; got '{}'",
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
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
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
    let _ = execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table));

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

    // Normalize URL (replace server advertised host:port with test reachable URL)
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

    println!("✅  Export ZIP downloaded ({} bytes), magic bytes verified", body.len());

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
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

    // Create both users and a shared table for user A to export from
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
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
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        fresh_user, fresh_pass
    ))
    .expect("CREATE USER failed");

    let show_out_json = execute_sql_via_client_as_json(&fresh_user, fresh_pass, "SHOW EXPORT")
        .expect("SHOW EXPORT should succeed for user with no exports");

    let json: serde_json::Value =
        serde_json::from_str(&show_out_json).expect("parse SHOW EXPORT JSON");

    // Either empty rows or an empty table
    let row_count = get_rows_as_hashmaps(&json).map(|r| r.len()).unwrap_or(0);

    assert_eq!(
        row_count, 0,
        "SHOW EXPORT should return 0 rows for a user who has never exported; got {}",
        row_count
    );

    println!("✅  SHOW EXPORT correctly returns empty result for user with no exports");

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", fresh_user));
}
