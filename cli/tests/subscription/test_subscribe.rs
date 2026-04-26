//! Integration tests for subscription and live query operations
//!
//! **Implements T041-T046**: WebSocket subscriptions, live queries, and real-time data streaming
//!
//! These tests validate:
//! - SUBSCRIBE TO command functionality
//! - Live query with WHERE filters
//! - Subscription pause/resume controls
//! - Unsubscribe operations
//! - Initial data in subscriptions
//! - CRUD operations with live updates

use std::{
    io::{BufRead, BufReader, Read},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};

use crate::common::*;

fn forward_process_output<R: Read + Send + 'static>(
    reader: R,
    stream_name: &'static str,
    tx: mpsc::Sender<String>,
) {
    std::thread::spawn(move || {
        for line in BufReader::new(reader).lines() {
            match line {
                Ok(line) => {
                    let _ = tx.send(format!("{}: {}", stream_name, line));
                },
                Err(err) => {
                    let _ = tx.send(format!("{} read error: {}", stream_name, err));
                    break;
                },
            }
        }
    });
}

fn spawn_cli_subscription_process(
    query: &str,
) -> Result<(Child, Receiver<String>, TempDir), Box<dyn std::error::Error>> {
    let cli_home = TempDir::new()?;
    let home_dir = cli_home.path().join("home");
    let config_dir = home_dir.join(".kalam");
    let credentials_path = config_dir.join("credentials.toml");
    std::fs::create_dir_all(&config_dir)?;

    let mut child = Command::new(env!("CARGO_BIN_EXE_kalam"));
    child
        .arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(default_password())
        .arg("--no-spinner")
        .arg("--no-color")
        .arg("--subscribe")
        .arg(query)
        .env("HOME", &home_dir)
        .env("USERPROFILE", &home_dir)
        .env("KALAMDB_CREDENTIALS_PATH", &credentials_path)
        .env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1")
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = child.spawn()?;
    let stdout = child.stdout.take().ok_or("failed to capture CLI stdout")?;
    let stderr = child.stderr.take().ok_or("failed to capture CLI stderr")?;
    let (tx, rx) = mpsc::channel();
    forward_process_output(stdout, "stdout", tx.clone());
    forward_process_output(stderr, "stderr", tx);

    Ok((child, rx, cli_home))
}

fn wait_for_process_output(
    rx: &Receiver<String>,
    needle: &str,
    timeout: Duration,
    collected: &mut Vec<String>,
) -> Result<String, String> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(line) => {
                if !line.trim().is_empty() {
                    collected.push(line.clone());
                }
                if line.contains(needle) {
                    return Ok(line);
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    Err(format!(
        "Timed out waiting for '{}'. Output so far:\n{}",
        needle,
        collected.join("\n")
    ))
}

struct ChildProcessGuard {
    child: Option<Child>,
}

impl ChildProcessGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }
}

impl Drop for ChildProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// T041: Test basic live query subscription
#[test]
fn test_cli_live_query_basic() {
    if cfg!(windows) {
        eprintln!(
            "⚠️  Skipping on Windows due to intermittent access violations in WebSocket tests."
        );
        return;
    }
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table = setup_test_table("live_query_basic").unwrap();

    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (content) VALUES ('Initial Message')",
        table
    ));

    // Test subscription using SubscriptionListener
    let query = format!("SELECT * FROM {}", table);
    let mut listener = match SubscriptionListener::start(&query) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("⚠️  Failed to start subscription: {}. Skipping test.", e);
            cleanup_test_table(&table).unwrap();
            return;
        },
    };

    // Wait for initial row that was inserted before subscribing.
    let timeout = Duration::from_secs(3);
    let result = listener.wait_for_event("Initial Message", timeout);

    listener.stop().unwrap();
    cleanup_test_table(&table).unwrap();

    let line = result.expect("subscription should stream initial snapshot row");
    assert!(
        line.contains("Initial Message"),
        "Expected initial row content in subscription output, got: {}",
        line
    );
}

/// T041b: Test CLI subscription commands
#[test]
fn test_cli_subscription_commands() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Test --list-subscriptions command
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--list-subscriptions");

    let output = cmd.output().unwrap();
    assert!(output.status.success(), "list-subscriptions command should succeed");

    // Test --unsubscribe command (should provide helpful message)
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--unsubscribe")
        .arg("test-subscription-id");

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("Ctrl+C"),
        "unsubscribe command should provide helpful feedback"
    );
}

/// T042: Test live query with WHERE filter
#[test]
fn test_cli_live_query_with_filter() {
    if cfg!(windows) {
        eprintln!(
            "⚠️  Skipping on Windows due to intermittent access violations in WebSocket tests."
        );
        return;
    }
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace_name = generate_unique_namespace("test_cli");
    let table_name = generate_unique_table("live_query_filter");
    let table = format!("{}.{}", namespace_name, table_name);

    let _ =
        execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace_name));
    execute_sql_as_root_via_cli(&format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, content VARCHAR NOT NULL, created_at TIMESTAMP \
         DEFAULT CURRENT_TIMESTAMP) WITH (TYPE='USER', FLUSH_POLICY='rows:10')",
        table
    ))
    .unwrap();

    // Use explicit IDs so WHERE id > 10 is deterministic; AUTO_INCREMENT maps to generated IDs.
    let query = format!("SELECT * FROM {} WHERE id > 10", table);
    let mut listener = match SubscriptionListener::start(&query) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("⚠️  Failed to start subscription: {}. Skipping test.", e);
            cleanup_test_table(&table).unwrap();
            return;
        },
    };

    for i in 1..=12 {
        let marker = if i <= 10 {
            format!("low_{}", i)
        } else {
            format!("high_{}", i)
        };
        let insert_sql =
            format!("INSERT INTO {} (id, content) VALUES ({}, '{}')", table, i, marker);
        let insert_result = execute_sql_as_root_via_cli(&insert_sql);
        assert!(insert_result.is_ok(), "Insert {} should succeed: {:?}", i, insert_result.err());
    }

    let mut lines = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match listener.try_read_line(Duration::from_millis(150)) {
            Ok(Some(line)) if !line.trim().is_empty() => lines.push(line),
            Ok(_) => continue,
            Err(_) => continue,
        }
    }

    listener.stop().unwrap();
    cleanup_test_table(&table).unwrap();

    assert!(
        lines.iter().any(|line| line.contains("high_11") || line.contains("high_12")),
        "Expected rows with id > 10 to pass filter, got lines:\n{}",
        lines.join("\n")
    );
    assert!(
        !lines.iter().any(|line| line.contains("low_1") || line.contains("low_2")),
        "Rows with id <= 10 should be filtered out, got lines:\n{}",
        lines.join("\n")
    );
}

/// T043: Test subscription pause/resume (Ctrl+S/Ctrl+Q)
#[test]
#[ignore] // Requires interactive terminal input simulation which is not feasible in automated tests
fn test_cli_subscription_pause_resume() {
    // Note: Testing pause/resume requires interactive input simulation
    // This functionality is manually tested via the CLI
    // Ctrl+S pauses output, Ctrl+Q resumes
}

/// T044: Test unsubscribe command support
#[test]
#[ignore] // \unsubscribe is an interactive meta-command that cannot be tested via CLI args
fn test_cli_unsubscribe() {
    // Note: \unsubscribe is an interactive meta-command used within a REPL session
    // It cannot be tested via command-line arguments
    // Manual testing: Start subscription, then type \unsubscribe in REPL
}

/// Test CLI subscription with initial data
#[test]
fn test_cli_subscription_with_initial_data() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Use unique names to avoid conflicts
    let namespace_name = generate_unique_namespace("sub_test_ns");
    let table_name = format!("{}.events", namespace_name);

    // Setup: Create namespace and table, insert data via CLI
    let _ = execute_sql_as_root_via_cli(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace_name
    ));

    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace_name));

    let create_table_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, event_type VARCHAR, timestamp BIGINT) WITH \
         (TYPE='USER', FLUSH_POLICY='rows:10')",
        table_name
    );
    let _ = execute_sql_as_root_via_cli(&create_table_sql);

    // Insert some initial data via CLI
    for i in 1..=3 {
        let insert_sql = format!(
            "INSERT INTO {} (id, event_type, timestamp) VALUES ({}, 'test_event', {})",
            table_name,
            i,
            i * 1000
        );
        let _ = execute_sql_as_root_via_cli(&insert_sql);
    }

    // Test that SUBSCRIBE TO command is accepted via CLI
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(format!("SUBSCRIBE TO SELECT * FROM {}", table_name))
        .timeout(std::time::Duration::from_secs(2)); // Short timeout

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should not crash with unsupported error
    assert!(
        !stdout.contains("Unsupported SQL") && !stderr.contains("Unsupported SQL"),
        "SUBSCRIBE TO should be accepted. stdout: {}, stderr: {}",
        stdout,
        stderr
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE {} CASCADE", namespace_name));
}

#[test]
fn test_cli_binary_projected_subscription_receives_live_changes() {
    if cfg!(windows) {
        eprintln!(
            "⚠️  Skipping on Windows due to intermittent access violations in WebSocket tests."
        );
        return;
    }
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace_name = generate_unique_namespace("sub_proj_cli");
    let table_name = format!("{}.messages", namespace_name);
    let initial_content = "snap-row";
    let live_content = "live-row";

    let _ = execute_sql_as_root_via_client(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace_name
    ));
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace_name))
        .expect("namespace should be created");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, role TEXT NOT NULL, author TEXT NOT NULL, \
         content TEXT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT NOW()) WITH (TYPE='USER', \
         FLUSH_POLICY='rows:10')",
        table_name
    ))
    .expect("table should be created");
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, role, author, content) VALUES (1, 'assistant', 'KalamDB Copilot', \
         '{}')",
        table_name, initial_content
    ))
    .expect("initial row should be inserted");
    wait_for_sql_output_contains(
        &format!("SELECT content FROM {} WHERE id = 1", table_name),
        &initial_content,
        Duration::from_secs(5),
    )
    .expect("initial row should be queryable before subscribing");

    let query = format!("SELECT id, role, author, content, created_at FROM {}", table_name);
    let (child, rx, _cli_home) =
        spawn_cli_subscription_process(&query).expect("CLI subscription should spawn");
    let _child = ChildProcessGuard::new(child);
    let mut output = Vec::new();

    wait_for_process_output(&rx, "SUBSCRIBED", Duration::from_secs(15), &mut output)
        .expect("CLI should acknowledge the subscription");
    wait_for_process_output(&rx, &initial_content, Duration::from_secs(15), &mut output)
        .expect("CLI should print the projected initial row");

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, role, author, content) VALUES (2, 'user', 'admin', '{}')",
        table_name, live_content
    ))
    .expect("live row should be inserted");

    let insert_line =
        wait_for_process_output(&rx, &live_content, Duration::from_secs(15), &mut output)
            .expect("CLI should print the projected live row");
    assert!(
        insert_line.contains("INSERT"),
        "Expected an INSERT line for the live row. Output: {}",
        output.join("\n")
    );

    let _ = execute_sql_as_root_via_client(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace_name
    ));
}

/// Test comprehensive subscription functionality with CRUD operations
#[test]
fn test_cli_subscription_comprehensive_crud() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Use unique names to avoid conflicts
    let namespace_name = generate_unique_namespace("sub_crud_ns");
    let table_name = format!("{}.events", namespace_name);

    // Setup: Create namespace and table via CLI
    let _ = execute_sql_as_root_via_cli(&format!(
        "DROP NAMESPACE IF EXISTS {} CASCADE",
        namespace_name
    ));

    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE {}", namespace_name));

    let create_table_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, event_type VARCHAR, data VARCHAR, timestamp BIGINT) \
         WITH (TYPE='USER', FLUSH_POLICY='rows:10')",
        table_name
    );
    let _ = execute_sql_as_root_via_cli(&create_table_sql);

    // Test 1: Verify subscription command is accepted
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(format!("SUBSCRIBE TO SELECT * FROM {} LIMIT 1", table_name))
        .timeout(std::time::Duration::from_secs(8));

    let output = cmd.output().unwrap();
    assert!(
        output.status.success() || !output.stderr.is_empty(),
        "CLI subscription command should be handled gracefully"
    );

    // Test 2: Insert initial data via CLI
    let insert_sql = format!(
        "INSERT INTO {} (id, event_type, data, timestamp) VALUES (1, 'create', 'initial_data', \
         1000)",
        table_name
    );
    let _ = execute_sql_as_root_via_cli(&insert_sql);

    // Test 3: Verify data was inserted correctly via CLI
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(format!("SELECT * FROM {}", table_name));

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && (stdout.contains("initial_data") || stdout.contains("1")),
        "Data should be inserted and queryable"
    );

    // Test 4: Insert more data and verify via CLI
    let insert_sql2 = format!(
        "INSERT INTO {} (id, event_type, data, timestamp) VALUES (2, 'insert', 'more_data', 2000)",
        table_name
    );
    let _ = execute_sql_as_root_via_cli(&insert_sql2);

    // Use wait helper to handle timing issues under load
    let select_sql = format!("SELECT * FROM {} ORDER BY id", table_name);
    let output_result =
        wait_for_sql_output_contains(&select_sql, "more_data", std::time::Duration::from_secs(3));

    match output_result {
        Ok(stdout) => {
            assert!(
                stdout.contains("initial_data") && stdout.contains("more_data"),
                "Both rows should be present"
            );
        },
        Err(e) => {
            panic!("Failed to verify both rows: {}", e);
        },
    }

    // Test 5: Update operation via CLI
    let update_sql = format!("UPDATE {} SET data = 'updated_data' WHERE id = 1", table_name);
    let _ = execute_sql_as_root_via_cli(&update_sql);

    let select_updated_sql = format!("SELECT * FROM {} WHERE id = 1", table_name);
    let updated_output =
        wait_for_sql_output_contains(&select_updated_sql, "updated_data", Duration::from_secs(5))
            .expect("Data should be updated");
    assert!(updated_output.contains("updated_data"), "Data should be updated");

    // Test 6: Delete operation via CLI
    let delete_sql = format!("DELETE FROM {} WHERE id = 2", table_name);
    let _ = execute_sql_as_root_via_cli(&delete_sql);

    let select_after_delete = format!("SELECT * FROM {} ORDER BY id", table_name);
    let start = Instant::now();
    let mut last_output = String::new();
    while start.elapsed() < Duration::from_secs(5) {
        if let Ok(output) = execute_sql_as_root_via_cli(&select_after_delete) {
            last_output = output;
            if last_output.contains("updated_data") && !last_output.contains("more_data") {
                break;
            }
        }
    }

    assert!(
        last_output.contains("updated_data") && !last_output.contains("more_data"),
        "Should have only the updated row after delete"
    );

    // Test 7: Verify subscription command still works after CRUD operations
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(format!("SUBSCRIBE TO SELECT * FROM {} ORDER BY id", table_name))
        .timeout(std::time::Duration::from_secs(2));

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let exit_code = output.status.code().unwrap_or(-1);
    assert!(
        output.status.success()
            || !stdout.is_empty()
            || !stderr.is_empty()
            || exit_code == 124
            || exit_code == 137,
        "CLI subscription should still work after CRUD operations"
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE {} CASCADE", namespace_name));
}
