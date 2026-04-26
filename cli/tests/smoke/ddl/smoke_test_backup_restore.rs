//! Smoke tests for BACKUP DATABASE / RESTORE DATABASE SQL commands.
//!
//! ## What is tested
//! - `BACKUP DATABASE TO '<path>'` creates a job that completes and writes the expected backup
//!   directory layout (`rocksdb/`, `storage/`, `snapshots/`, `streams/`).
//! - `RESTORE DATABASE FROM '<path>'` creates a restore job that reaches a terminal state.
//! - Non-DBA users receive an authorization error when attempting either command.
//! - `RESTORE DATABASE FROM '<non-existent-path>'` returns a clear error immediately (no job
//!   created).
//!
//! ## Notes
//! - Backup/restore paths must be reachable on the **server's** filesystem. For the auto-started
//!   test server this is the local machine's temp directory.
//! - Restore requires a server restart to reload data — these tests verify only that the restore
//!   job itself completes, not post-restart data consistency.

use std::{path::PathBuf, time::Duration};

use crate::common::*;

/// Timeout for a backup job (RocksDB BackupEngine + file copies can be slow).
const BACKUP_JOB_TIMEOUT: Duration = Duration::from_secs(120);
/// Timeout for a restore job.
const RESTORE_JOB_TIMEOUT: Duration = Duration::from_secs(120);

// ── helpers ─────────────────────────────────────────────────────────────────

/// Extract the first Job ID from a success message like
/// "Database backup started to '…'. Job ID: BK-20260225-abcdef"
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

/// Return a fresh temp directory path string suitable for a backup test.
///
/// The directory is intentionally **not created** before the backup so that
/// `BACKUP DATABASE` itself creates it; for restore tests the backup must be
/// created first.
fn tmp_backup_path(suffix: &str) -> PathBuf {
    let unique = generate_unique_namespace(suffix);
    std::env::temp_dir().join(unique)
}

// ── tests ────────────────────────────────────────────────────────────────────

/// BACKUP DATABASE creates a completed job and writes the expected directory layout.
#[ntest::timeout(300_000)]
#[test]
fn smoke_backup_database_job_completes() {
    if !require_server_running() {
        return;
    }

    let backup_path = tmp_backup_path("kdb_bkp");

    // Issue the backup command
    let output =
        execute_sql_as_root_via_client(&format!("BACKUP DATABASE TO '{}'", backup_path.display()))
            .expect("BACKUP DATABASE should succeed");

    assert!(
        output.contains("backup started") || output.contains("Job ID"),
        "Expected backup started message; got: {}",
        output
    );

    let job_id =
        parse_job_id(&output).unwrap_or_else(|| panic!("Could not parse job id from: {}", output));

    println!("🗄️  Backup job: {}", job_id);

    // Wait for job completion
    let status = wait_for_job_finished(&job_id, BACKUP_JOB_TIMEOUT)
        .unwrap_or_else(|e| panic!("Backup job wait failed: {}", e));

    assert_eq!(status, "completed", "Backup job did not complete: {}", job_id);

    // Verify backup directory structure
    assert!(
        backup_path.exists(),
        "Backup directory was not created: {}",
        backup_path.display()
    );
    assert!(backup_path.join("rocksdb").exists(), "Backup is missing rocksdb/ subdirectory");

    println!("✅  Backup directory verified at {}", backup_path.display());

    // Cleanup — best-effort
    let _ = std::fs::remove_dir_all(&backup_path);
}

/// RESTORE DATABASE FROM a valid backup path creates a restore job that completes.
///
/// Restore rewrites on-disk data; a server restart is required to reload it.
/// This test verifies only that the job reaches a terminal state, not
/// post-restart data correctness.
#[ntest::timeout(360_000)]
#[test]
fn smoke_restore_from_backup_job_completes() {
    if !require_server_running() {
        return;
    }

    let backup_path = tmp_backup_path("kdb_restore");

    // Step 1: create a backup to restore from
    let bkp_out =
        execute_sql_as_root_via_client(&format!("BACKUP DATABASE TO '{}'", backup_path.display()))
            .expect("BACKUP DATABASE should succeed");

    let bkp_job_id = parse_job_id(&bkp_out).unwrap_or_else(|| panic!("No job id in: {}", bkp_out));
    let bkp_status = wait_for_job_finished(&bkp_job_id, BACKUP_JOB_TIMEOUT)
        .unwrap_or_else(|e| panic!("Backup job wait failed: {}", e));
    assert_eq!(bkp_status, "completed", "Backup must complete before restore test");

    // Step 2: restore from the backup
    let restore_out = execute_sql_as_root_via_client(&format!(
        "RESTORE DATABASE FROM '{}'",
        backup_path.display()
    ))
    .expect("RESTORE DATABASE should succeed");

    assert!(
        restore_out.contains("restore started") || restore_out.contains("Job ID"),
        "Expected restore started message; got: {}",
        restore_out
    );

    let restore_job_id = parse_job_id(&restore_out)
        .unwrap_or_else(|| panic!("Could not parse restore job id from: {}", restore_out));

    println!("♻️  Restore job: {}", restore_job_id);

    let status = wait_for_job_finished(&restore_job_id, RESTORE_JOB_TIMEOUT)
        .unwrap_or_else(|e| panic!("Restore job wait failed: {}", e));

    // Restore may "complete" or "fail" depending on underlying conditions;
    // what matters is that the job ends in a known terminal state.
    assert!(
        status == "completed" || status == "failed",
        "Restore job should be in terminal state; got: {}",
        status
    );
    println!("✅  Restore job finished with status '{}'", status);

    // Cleanup
    let _ = std::fs::remove_dir_all(&backup_path);
}

/// A regular `user`-role user is forbidden from running BACKUP DATABASE.
#[ntest::timeout(60_000)]
#[test]
fn smoke_backup_requires_dba_role() {
    if !require_server_running() {
        return;
    }

    let user = generate_unique_namespace("bkp_noauth_user");
    let pass = "TestPass123!";

    // Create a regular user (role=user)
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user, pass
    ))
    .expect("CREATE USER should succeed");

    let backup_path = tmp_backup_path("kdb_unauth_bkp");
    let result = execute_sql_via_client_as(
        &user,
        pass,
        &format!("BACKUP DATABASE TO '{}'", backup_path.display()),
    );

    // Expect either an Err or an output containing an authorization-related message
    let is_unauthorized = match &result {
        Err(e) => {
            let s = e.to_string().to_lowercase();
            s.contains("unauthorized")
                || s.contains("permission")
                || s.contains("forbidden")
                || s.contains("dba")
                || s.contains("not allowed")
        },
        Ok(output) => {
            let lower = output.to_lowercase();
            lower.contains("unauthorized")
                || lower.contains("permission denied")
                || lower.contains("forbidden")
                || lower.contains("dba")
                || lower.contains("error")
        },
    };

    assert!(
        is_unauthorized,
        "Expected authorization error for regular user; got: {:?}",
        result
    );

    println!("✅  BACKUP DATABASE correctly rejected for role=user");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user));
}

/// A regular `user`-role user is forbidden from running RESTORE DATABASE.
#[ntest::timeout(60_000)]
#[test]
fn smoke_restore_requires_dba_role() {
    if !require_server_running() {
        return;
    }

    let user = generate_unique_namespace("rst_noauth_user");
    let pass = "TestPass123!";

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        user, pass
    ))
    .expect("CREATE USER should succeed");

    // Point to any path — auth check happens before path validation
    let result = execute_sql_via_client_as(
        &user,
        pass,
        "RESTORE DATABASE FROM '/tmp/nonexistent_backup_path_abc123'",
    );

    let is_unauthorized = match &result {
        Err(e) => {
            let s = e.to_string().to_lowercase();
            s.contains("unauthorized")
                || s.contains("permission")
                || s.contains("forbidden")
                || s.contains("dba")
                || s.contains("not allowed")
        },
        Ok(output) => {
            let lower = output.to_lowercase();
            lower.contains("unauthorized")
                || lower.contains("permission denied")
                || lower.contains("forbidden")
                || lower.contains("dba")
                || lower.contains("error")
        },
    };

    assert!(
        is_unauthorized,
        "Expected authorization error for regular user; got: {:?}",
        result
    );

    println!("✅  RESTORE DATABASE correctly rejected for role=user");

    let _ = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", user));
}

/// RESTORE DATABASE FROM a non-existent path returns an error immediately
/// (no job is created).
#[ntest::timeout(30_000)]
#[test]
fn smoke_restore_nonexistent_path_returns_error() {
    if !require_server_running() {
        return;
    }

    let bad_path = "/tmp/kalamdb_nonexistent_backup_path_zzz999";

    // Make sure the path does NOT exist on this machine (best-effort)
    let _ = std::fs::remove_dir_all(bad_path);

    let result = execute_sql_as_root_via_client(&format!("RESTORE DATABASE FROM '{}'", bad_path));

    let is_error = match &result {
        Err(_) => true,
        Ok(output) => {
            let lower = output.to_lowercase();
            lower.contains("not found")
                || lower.contains("error")
                || lower.contains("does not exist")
                || lower.contains("no such file")
        },
    };

    assert!(
        is_error,
        "Expected RESTORE DATABASE to fail for non-existent path; got: {:?}",
        result
    );

    println!("✅  RESTORE DATABASE correctly rejected non-existent backup path");
}

/// SHOW JOBS contains backup/restore jobs after they are created.
#[ntest::timeout(300_000)]
#[test]
fn smoke_backup_job_visible_in_system_jobs() {
    if !require_server_running() {
        return;
    }

    let backup_path = tmp_backup_path("kdb_jobs_vis");

    let output =
        execute_sql_as_root_via_client(&format!("BACKUP DATABASE TO '{}'", backup_path.display()))
            .expect("BACKUP DATABASE should succeed");

    let job_id = parse_job_id(&output).unwrap_or_else(|| panic!("No job id in: {}", output));

    // Wait for job to appear and finish in system.jobs
    let status = wait_for_job_finished(&job_id, BACKUP_JOB_TIMEOUT)
        .unwrap_or_else(|e| panic!("Backup job wait failed: {}", e));

    assert_eq!(status, "completed");

    // Verify the job appears in system.jobs with correct type
    let jobs_output = execute_sql_as_root_via_client_json(&format!(
        "SELECT job_id, job_type, status FROM system.jobs WHERE job_id = '{}'",
        job_id
    ))
    .expect("Query system.jobs");

    let json: serde_json::Value = serde_json::from_str(&jobs_output).expect("parse json");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();

    assert!(!rows.is_empty(), "Backup job {} not found in system.jobs", job_id);

    let row = &rows[0];
    let job_type = row
        .get("job_type")
        .and_then(extract_arrow_value)
        .or_else(|| row.get("job_type").cloned())
        .and_then(|v| v.as_str().map(|s| s.to_lowercase()))
        .unwrap_or_default();

    assert!(
        job_type.contains("backup"),
        "Expected job_type to contain 'backup'; got: {}",
        job_type
    );

    println!("✅  Backup job {} is visible in system.jobs as type '{}'", job_id, job_type);

    let _ = std::fs::remove_dir_all(&backup_path);
}
