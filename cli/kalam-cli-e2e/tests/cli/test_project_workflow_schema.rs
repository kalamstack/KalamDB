use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
};

use tempfile::TempDir;

use crate::common::*;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedSqlRequest {
    sql:          String,
    namespace_id: Option<String>,
}

fn start_recording_sql_server(
) -> (String, Arc<Mutex<Vec<RecordedSqlRequest>>>, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind recording server");
    let addr = listener.local_addr().expect("recording server addr");
    let url = format!("http://{}", addr);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_handle = Arc::clone(&requests);

    let handle = std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                break;
            };

            let mut buffer = Vec::new();
            let mut chunk = [0_u8; 4096];
            let header_end;
            loop {
                let read = stream.read(&mut chunk).expect("read request chunk");
                if read == 0 {
                    return;
                }
                buffer.extend_from_slice(&chunk[..read]);
                if let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                    header_end = pos + 4;
                    break;
                }
            }

            let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("content-length") {
                        value.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            while buffer.len() < header_end + content_length {
                let read = stream.read(&mut chunk).expect("read request body");
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
            }

            let request_line = headers.lines().next().unwrap_or_default().to_string();
            let body =
                &buffer[header_end..header_end + content_length.min(buffer.len() - header_end)];

            let response = if request_line.starts_with("GET /health") {
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 52\r\nConnection: close\r\n\r\n{\"status\":\"ok\",\"version\":\"test\",\"api_version\":\"v1\"}".to_vec()
            } else if request_line.starts_with("GET /v1/api/auth/me") {
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 30\r\nConnection: close\r\n\r\n{\"user\":\"test\",\"role\":\"admin\"}".to_vec()
            } else if request_line.starts_with("POST /v1/api/sql") {
                let payload: serde_json::Value =
                    serde_json::from_slice(body).expect("parse sql request body");
                let sql = payload
                    .get("sql")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                requests_handle.lock().expect("lock requests").push(RecordedSqlRequest {
                    sql:          sql.clone(),
                    namespace_id: payload
                        .get("namespace_id")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string),
                });
                let body = if sql.to_uppercase().contains("FROM SYSTEM.MIGRATIONS") {
                    r#"{"status":"success","results":[{"schema":[],"rows":[],"row_count":0}]}"#
                } else if sql.to_uppercase().contains("SYSTEM.MIGRATIONS") {
                    r#"{"status":"success","results":[{"schema":[],"rows":[],"row_count":1}]}"#
                } else {
                    r#"{"status":"success","results":[]}"#
                };
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \
                     {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .into_bytes()
            } else {
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
            };

            let _ = stream.write_all(&response);
            let _ = stream.flush();
        }
    });

    (url, requests, handle)
}

#[test]
fn test_project_workflow_schema_command_help_surface() {
    let cases = [
        ("schema", "Generate"),
        ("migration", "migration"),
        ("db", "database"),
    ];

    for (command, expected_snippet) in cases {
        let mut cmd = create_cli_command();
        cmd.args([command, "--help"]);

        let output = cmd.output().expect("run workflow schema help");
        assert!(
            output.status.success(),
            "{} help should succeed\nstdout: {}\nstderr: {}",
            command,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.to_lowercase().contains(&expected_snippet.to_lowercase()),
            "{} help should contain '{}'\nstdout: {}",
            command,
            expected_snippet,
            stdout
        );
    }
}

fn scaffold_sql_project(
    temp: &TempDir,
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let project_dir = temp.path().join("schema-app");
    let isolated_home = temp.path().join("home");
    fs::create_dir_all(&project_dir).expect("create project dir");
    fs::create_dir_all(isolated_home.join(".kalam")).expect("create isolated home");
    let credentials_path = isolated_home.join(".kalam/credentials.toml");
    // Schema gen uses a fake ORM client and must not bind credentials to the Fresh
    // ephemeral URL — init still defaults the project to http://localhost:2900.
    store_test_kalam_dev_credentials(&credentials_path);

    let mut cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    cmd.current_dir(&project_dir).args([
        "init",
        "--yes",
        "--name",
        "schema-app",
        "--schema-mode",
        "sql",
        "--languages",
        "typescript,dart",
    ]);
    let output = cmd.output().expect("init project");
    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    (project_dir, isolated_home, credentials_path)
}

#[test]
fn test_project_workflow_schema_gen_from_sql() {
    let temp = TempDir::new().expect("temp dir");
    let (project_dir, isolated_home, credentials_path) = scaffold_sql_project(&temp);

    let mut cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    cmd.current_dir(&project_dir).args(["schema", "gen"]);

    let output = cmd.output().expect("schema gen");
    assert!(
        output.status.success(),
        "schema gen should succeed\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let ts_output = project_dir.join("src/generated/kalam.ts");
    let dart_output = project_dir.join("lib/generated/kalam.dart");
    assert!(ts_output.is_file(), "typescript output missing");
    assert!(dart_output.is_file(), "dart output missing");

    let ts = fs::read_to_string(ts_output).expect("read ts");
    assert!(
        ts.contains("Generated by kalam schema gen"),
        "expected locally generated typescript"
    );
    assert!(ts.contains("contract_hash:"));
    assert!(ts.contains("import { kTable } from '@kalamdb/orm';"));
    assert!(ts.contains("export const users"), "expected users table export");

    let dart = fs::read_to_string(dart_output).expect("read dart");
    assert!(dart.contains("Generated by kalam schema gen"), "expected generated dart header");
    assert!(dart.contains("contract_hash:"));
    assert!(dart.contains("import 'package:kalam_sync/kalam_sync.dart';"));
    assert!(dart.contains("KalamTableSpec<Users>"), "expected Users table spec");
    assert!(dart.contains("tableId: 'users'"));
    assert!(
        !dart.to_lowercase().contains("placeholder"),
        "dart output should not be a placeholder"
    );
}

#[test]
fn test_project_workflow_migration_create_and_status() {
    let temp = TempDir::new().expect("temp dir");
    let project_dir = temp.path().join("schema-status-app");
    fs::create_dir_all(&project_dir).expect("create project dir");
    let isolated_home = temp.path().join("home");
    fs::create_dir_all(isolated_home.join(".kalam")).expect("create isolated home");
    let credentials_path = isolated_home.join(".kalam/credentials.toml");
    store_test_kalam_dev_credentials(&credentials_path);
    let (server_url, _requests, _server_handle) = start_recording_sql_server();

    let mut init_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    init_cmd.current_dir(&project_dir).args([
        "init",
        "--yes",
        "--name",
        "schema-status-app",
        "--schema-mode",
        "sql",
        "--server-mode",
        "remote",
        "--server-url",
        &server_url,
        "--languages",
        "typescript",
    ]);
    let init_output = init_cmd.output().expect("init project");
    assert!(
        init_output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&init_output.stderr)
    );

    let mut create_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    create_cmd
        .current_dir(&project_dir)
        .args(["migration", "create", "add_profile"]);

    let create_output = create_cmd.output().expect("migration create");
    assert!(
        create_output.status.success(),
        "migration create failed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let migrations_dir = project_dir.join("kalam/migrations");
    let sql_files: Vec<_> = fs::read_dir(&migrations_dir)
        .expect("read migrations")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sql"))
        .collect();
    assert_eq!(sql_files.len(), 1, "expected one migration file");

    let migration_sql = fs::read_to_string(sql_files[0].path()).expect("read migration file");
    assert!(migration_sql.contains("-- UP"), "migration should include UP section");
    assert!(migration_sql.contains("-- DOWN"), "migration should include DOWN section");
    assert!(
        migration_sql.contains("-- Generated KalamDB schema evolution"),
        "migration UP should come from kalam-schema-diff semantic diff"
    );
    assert!(migration_sql.contains("CREATE TABLE users"));

    let mut status_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    status_cmd.current_dir(&project_dir).args(["migration", "status"]);
    let status_output = status_cmd.output().expect("migration status");
    assert!(
        status_output.status.success(),
        "migration status failed: {}",
        String::from_utf8_lossy(&status_output.stderr)
    );

    let stderr = String::from_utf8_lossy(&status_output.stderr);
    assert!(
        stderr.to_ascii_lowercase().contains("pending"),
        "expected pending migration in status\nstderr: {stderr}"
    );
}

#[test]
fn test_project_workflow_db_migrate_local_state() {
    let temp = TempDir::new().expect("temp dir");
    let project_dir = temp.path().join("db-migrate-app");
    fs::create_dir_all(&project_dir).expect("create project dir");
    let isolated_home = temp.path().join("home");
    fs::create_dir_all(isolated_home.join(".kalam")).expect("create isolated home");
    let credentials_path = isolated_home.join(".kalam/credentials.toml");
    store_test_kalam_dev_credentials(&credentials_path);
    let (server_url, _requests, _server_handle) =
        crate::test_project_workflow_dev::start_migration_tracking_server();

    let mut init_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    init_cmd.current_dir(&project_dir).args([
        "init",
        "--yes",
        "--name",
        "db-migrate-app",
        "--schema-mode",
        "sql",
        "--server-mode",
        "remote",
        "--server-url",
        &server_url,
        "--languages",
        "typescript",
    ]);
    assert!(init_cmd.output().expect("init").status.success());

    let mut create_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    create_cmd.current_dir(&project_dir).args(["migration", "create", "initial"]);

    let create_output = create_cmd.output().expect("migration create");
    assert!(create_output.status.success());

    let mut migrate_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    migrate_cmd.current_dir(&project_dir).args(["db", "migrate"]);
    let migrate_output = migrate_cmd.output().expect("db migrate");
    assert!(
        migrate_output.status.success(),
        "db migrate failed: {}",
        String::from_utf8_lossy(&migrate_output.stderr)
    );

    let state_path = project_dir.join("kalam/migrations/.kalam-state.json");
    assert!(
        !state_path.exists(),
        "migration state should be stored in system.migrations, not a local file"
    );

    let mut status_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    status_cmd.current_dir(&project_dir).args(["migration", "status"]);
    let status_output = status_cmd.output().expect("migration status");
    let stderr = String::from_utf8_lossy(&status_output.stderr);
    assert!(
        stderr.to_ascii_lowercase().contains("applied"),
        "expected applied migration\nstderr: {stderr}"
    );
}

fn create_isolated_workflow_command(
    home: &std::path::Path,
    credentials_path: &std::path::Path,
) -> std::process::Command {
    let mut cmd = std::process::Command::new(kalam_bin());
    cmd.env("HOME", home)
        .env("USERPROFILE", home)
        .env("KALAMDB_CREDENTIALS_PATH", credentials_path)
        .env("KALAM_TEST_SKIP_PACKAGE_INSTALL", "1");
    clear_workflow_url_env_overrides(&mut cmd);
    cmd
}

#[test]
fn test_project_workflow_db_migrate_executes_pending_sql_against_server() {
    let temp = TempDir::new().expect("temp dir");
    let project_dir = temp.path().join("remote-migrate-app");
    fs::create_dir_all(&project_dir).expect("create dir");
    let isolated_home = temp.path().join("home");
    fs::create_dir_all(isolated_home.join(".kalam")).expect("create isolated home");
    let credentials_path = isolated_home.join(".kalam/credentials.toml");
    store_test_kalam_dev_credentials(&credentials_path);
    let (server_url, requests, _server_handle) = start_recording_sql_server();

    let mut init_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    init_cmd.current_dir(&project_dir).args([
        "init",
        "--yes",
        "--name",
        "remote-migrate-app",
        "--schema-mode",
        "sql",
        "--server-mode",
        "remote",
        "--server-url",
        &server_url,
        "--languages",
        "typescript",
    ]);
    assert!(init_cmd.output().expect("init").status.success());

    fs::write(
        project_dir.join("kalam/migrations/20250101120000_add_users.sql"),
        "-- UP\nCREATE TABLE users_from_migration (id INTEGER PRIMARY KEY);\n\n-- DOWN\nDROP \
         TABLE users_from_migration;\n",
    )
    .expect("write migration");

    let mut migrate_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    migrate_cmd.current_dir(&project_dir).args(["db", "migrate"]);
    let migrate_output = migrate_cmd.output().expect("db migrate");
    assert!(
        migrate_output.status.success(),
        "db migrate failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&migrate_output.stdout),
        String::from_utf8_lossy(&migrate_output.stderr)
    );

    let recorded = requests.lock().expect("lock requests").clone();
    assert!(
        recorded
            .iter()
            .any(|request| request.sql.contains("CREATE TABLE users_from_migration")
                && request.namespace_id.as_deref() == Some("remote_migrate_app")),
        "expected pending migration SQL to execute against linked server\nrequests: {recorded:?}"
    );
    assert!(
        recorded
            .iter()
            .all(|request| !request.sql.contains("DROP TABLE users_from_migration")),
        "expected db migrate to execute only the UP section\nrequests: {recorded:?}"
    );

    let stderr = String::from_utf8_lossy(&migrate_output.stderr);
    assert!(
        stderr.contains("applying migration") || stderr.contains("executing migration"),
        "expected migration progress logging\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("20250101120000_add_users.sql"),
        "expected migration filename in logs\nstderr: {stderr}"
    );
}

#[test]
fn test_project_workflow_schema_pull_requires_server() {
    let temp = TempDir::new().expect("temp dir");
    let project_dir = temp.path().join("remote-app");
    let isolated_home = temp.path().join("home");
    let credentials_path = isolated_home.join(".kalam/credentials.toml");
    fs::create_dir_all(credentials_path.parent().expect("credentials parent"))
        .expect("create credentials dir");
    fs::create_dir_all(&project_dir).expect("create dir");

    let mut init_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    init_cmd.current_dir(&project_dir).args([
        "init",
        "--yes",
        "--name",
        "remote-app",
        "--schema-mode",
        "sql",
        "--languages",
        "typescript",
    ]);
    let init_output = init_cmd.output().expect("init");
    assert!(
        init_output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&init_output.stderr)
    );

    // Remote schema mode is not available during init yet; emulate a pull-based project.
    let kalam_toml_path = project_dir.join("kalam.toml");
    let kalam_toml = fs::read_to_string(&kalam_toml_path).expect("read kalam.toml");
    fs::write(&kalam_toml_path, kalam_toml.replace("mode = \"sql\"", "mode = \"remote\""))
        .expect("write kalam.toml");

    let mut pull_cmd = create_isolated_workflow_command(&isolated_home, &credentials_path);
    pull_cmd.current_dir(&project_dir).args(["schema", "pull"]);
    let pull_output = pull_cmd.output().expect("schema pull");
    assert!(!pull_output.status.success(), "schema pull should fail without server");

    let stderr = String::from_utf8_lossy(&pull_output.stderr);
    assert!(
        stderr.to_lowercase().contains("server"),
        "expected clear server error, got: {stderr}"
    );
}
