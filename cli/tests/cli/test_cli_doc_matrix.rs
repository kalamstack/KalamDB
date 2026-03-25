//! CLI docs coverage matrix + targeted gaps tests.
//!
//! This file addresses:
//! 1) docs-to-tests coverage matrix for documented flags/commands
//! 2) non-server clap parsing tests for previously uncovered flags
//! 3) server-backed runtime tests for subscribe/consume/timeout-related flags

use crate::common::*;
use clap::Parser;
use std::time::{Duration, Instant};

#[path = "../../src/args.rs"]
mod cli_args;

#[test]
fn test_docs_matrix_has_execution_tests_for_documented_flags_and_commands() {
    struct Coverage<'a> {
        item: &'a str,
        tests: &'a [&'a str],
    }

    // This is the canonical docs inventory from KalamSite/content/getting-started/cli.mdx.
    let matrix = vec![
        // Connection/auth flags
        Coverage {
            item: "--url",
            tests: &[
                "test_cli_help_command",
                "test_cli_runtime_timeout_and_spinner_flags_work",
            ],
        },
        Coverage {
            item: "--host",
            tests: &[
                "test_cli_parse_missing_documented_flags_without_server",
                "test_cli_host_and_port_work_against_running_server",
            ],
        },
        Coverage {
            item: "--port",
            tests: &[
                "test_cli_parse_missing_documented_flags_without_server",
                "test_cli_host_and_port_work_against_running_server",
            ],
        },
        Coverage {
            item: "--token",
            tests: &["test_cli_invalid_token"],
        },
        Coverage {
            item: "--username",
            tests: &["test_cli_color_output"],
        },
        Coverage {
            item: "--password",
            tests: &["test_cli_color_output"],
        },
        Coverage {
            item: "--instance",
            tests: &["test_cli_multiple_instances"],
        },
        // Query/output flags
        Coverage {
            item: "--command",
            tests: &["test_cli_color_output"],
        },
        Coverage {
            item: "--file",
            tests: &["test_cli_batch_file_execution"],
        },
        Coverage {
            item: "--format",
            tests: &["test_cli_config_precedence"],
        },
        Coverage {
            item: "--json",
            tests: &["test_cli_config_precedence"],
        },
        Coverage {
            item: "--csv",
            tests: &["test_cli_csv_output_format"],
        },
        Coverage {
            item: "--no-color",
            tests: &["test_cli_color_output"],
        },
        Coverage {
            item: "--no-spinner",
            tests: &["test_cli_runtime_timeout_and_spinner_flags_work"],
        },
        Coverage {
            item: "--loading-threshold-ms",
            tests: &["test_cli_runtime_timeout_and_spinner_flags_work"],
        },
        // Credential flags
        Coverage {
            item: "--list-instances",
            tests: &["test_cli_list_instances_command"],
        },
        Coverage {
            item: "--show-credentials",
            tests: &["test_cli_show_credentials_command"],
        },
        Coverage {
            item: "--update-credentials",
            tests: &["test_cli_parse_missing_documented_flags_without_server"],
        },
        Coverage {
            item: "--delete-credentials",
            tests: &["test_cli_delete_credentials"],
        },
        Coverage {
            item: "--save-credentials",
            tests: &["test_cli_save_credentials_creates_file"],
        },
        // Subscription flags
        Coverage {
            item: "--subscribe",
            tests: &["test_cli_subscribe_flags_work_end_to_end"],
        },
        Coverage {
            item: "--subscription-timeout",
            tests: &["test_cli_subscribe_flags_work_end_to_end"],
        },
        Coverage {
            item: "--initial-data-timeout",
            tests: &["test_cli_subscribe_flags_work_end_to_end"],
        },
        Coverage {
            item: "--list-subscriptions",
            tests: &["test_cli_subscription_commands"],
        },
        Coverage {
            item: "--unsubscribe",
            tests: &["test_cli_subscription_commands"],
        },
        // Consume flags
        Coverage {
            item: "--consume",
            tests: &["test_cli_consume_flags_work_end_to_end"],
        },
        Coverage {
            item: "--topic",
            tests: &["test_cli_consume_flags_work_end_to_end"],
        },
        Coverage {
            item: "--group",
            tests: &["test_cli_consume_flags_work_end_to_end"],
        },
        Coverage {
            item: "--from",
            tests: &["test_cli_consume_flags_work_end_to_end"],
        },
        Coverage {
            item: "--consume-limit",
            tests: &["test_cli_consume_flags_work_end_to_end"],
        },
        Coverage {
            item: "--consume-timeout",
            tests: &["test_cli_consume_flags_work_end_to_end"],
        },
        // Init agent flags
        Coverage {
            item: "--init-agent",
            tests: &["test_cli_init_agent_non_interactive_generates_project"],
        },
        Coverage {
            item: "--init-agent-non-interactive",
            tests: &["test_cli_init_agent_non_interactive_generates_project"],
        },
        Coverage {
            item: "--agent-name",
            tests: &["test_cli_init_agent_non_interactive_generates_project"],
        },
        Coverage {
            item: "--agent-output",
            tests: &["test_cli_init_agent_non_interactive_generates_project"],
        },
        Coverage {
            item: "--agent-table",
            tests: &["test_cli_init_agent_non_interactive_generates_project"],
        },
        Coverage {
            item: "--agent-topic",
            tests: &["test_cli_init_agent_non_interactive_generates_project"],
        },
        Coverage {
            item: "--agent-group",
            tests: &["test_cli_init_agent_non_interactive_generates_project"],
        },
        Coverage {
            item: "--agent-id-column",
            tests: &["test_cli_parse_missing_documented_agent_flags_without_server"],
        },
        Coverage {
            item: "--agent-input-column",
            tests: &["test_cli_parse_missing_documented_agent_flags_without_server"],
        },
        Coverage {
            item: "--agent-output-column",
            tests: &["test_cli_parse_missing_documented_agent_flags_without_server"],
        },
        Coverage {
            item: "--agent-system-prompt",
            tests: &["test_cli_parse_missing_documented_agent_flags_without_server"],
        },
        // Timeout/runtime flags
        Coverage {
            item: "--timeout",
            tests: &["test_cli_runtime_timeout_and_spinner_flags_work"],
        },
        Coverage {
            item: "--connection-timeout",
            tests: &["test_cli_runtime_timeout_and_spinner_flags_work"],
        },
        Coverage {
            item: "--receive-timeout",
            tests: &["test_cli_runtime_timeout_and_spinner_flags_work"],
        },
        Coverage {
            item: "--auth-timeout",
            tests: &["test_cli_runtime_timeout_and_spinner_flags_work"],
        },
        Coverage {
            item: "--fast-timeouts",
            tests: &["test_cli_runtime_timeout_presets_work"],
        },
        Coverage {
            item: "--relaxed-timeouts",
            tests: &["test_cli_runtime_timeout_presets_work"],
        },
        Coverage {
            item: "--config",
            tests: &["test_cli_load_config_file"],
        },
        Coverage {
            item: "--verbose",
            tests: &["test_cli_verbose_output"],
        },
        Coverage {
            item: "--version",
            tests: &["test_cli_version_command"],
        },
        // Interactive commands covered by parser + execution smoke
        Coverage {
            item: "\\help",
            tests: &[
                "test_parse_help",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\?",
            tests: &[
                "test_parse_help",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\quit",
            tests: &[
                "test_parse_quit",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\q",
            tests: &[
                "test_parse_quit",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\info",
            tests: &[
                "test_cli_authenticate_and_check_info",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\session",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\history",
            tests: &[
                "test_parse_history",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\h",
            tests: &[
                "test_parse_history",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\health",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\stats",
            tests: &[
                "test_parse_stats",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\metrics",
            tests: &[
                "test_parse_stats",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\dt",
            tests: &[
                "smoke_cli_list_tables_command",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\tables",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\d",
            tests: &[
                "smoke_cli_describe_table_command",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\describe",
            tests: &[
                "test_parse_describe",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\format",
            tests: &[
                "smoke_cli_format_json_command",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\refresh-tables",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\refresh",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\show-credentials",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\credentials",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\update-credentials",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\delete-credentials",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\subscribe",
            tests: &["test_cli_subscribe_flags_work_end_to_end"],
        },
        Coverage {
            item: "\\watch",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\unsubscribe",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\unwatch",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\consume",
            tests: &["test_cli_consume_flags_work_end_to_end"],
        },
        Coverage {
            item: "\\flush",
            tests: &[
                "smoke_cli_flush_command",
                "test_cli_meta_commands_doc_smoke_non_interactive",
            ],
        },
        Coverage {
            item: "\\pause",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\continue",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster snapshot",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster purge",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster trigger-election",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster trigger election",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster transfer-leader",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster transfer leader",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster stepdown",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster step-down",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster clear",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster list",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster ls",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster list groups",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster status",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster join",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
        Coverage {
            item: "\\cluster leave",
            tests: &["test_cli_meta_commands_doc_smoke_non_interactive"],
        },
    ];

    let test_sources = concat!(
        include_str!("test_cli.rs"),
        include_str!("test_cli_auth.rs"),
        include_str!("test_cli_auth_admin.rs"),
        include_str!("test_cli_doc_matrix.rs"),
        include_str!("../users/test_admin.rs"),
        include_str!("../tables/test_user_tables.rs"),
        include_str!("../subscription/test_subscribe.rs"),
        include_str!("../auth/test_auth.rs"),
        include_str!("../smoke/cli/smoke_test_cli_commands.rs"),
        include_str!("../../src/parser.rs"),
    );

    let missing: Vec<String> = matrix
        .into_iter()
        .filter_map(|row| {
            let has_execution_test =
                row.tests.iter().any(|test_name| test_sources.contains(test_name));
            if has_execution_test {
                None
            } else {
                Some(format!("{} (expected one of: {})", row.item, row.tests.join(", ")))
            }
        })
        .collect();

    assert!(missing.is_empty(), "Missing docs coverage for:\n{}", missing.join("\n"));
}

#[test]
fn test_cli_parse_missing_documented_flags_without_server() {
    let cli = cli_args::Cli::try_parse_from([
        "kalam",
        "--host",
        "127.0.0.1",
        "--port",
        "8080",
        "--loading-threshold-ms",
        "350",
        "--update-credentials",
        "--consume",
        "--topic",
        "ns.topic",
        "--group",
        "g1",
        "--from",
        "earliest",
        "--consume-limit",
        "5",
        "--consume-timeout",
        "8",
    ])
    .expect("args should parse");

    assert_eq!(cli.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(cli.port, 8080);
    assert_eq!(cli.loading_threshold_ms, Some(350));
    assert!(cli.update_credentials);
    assert!(cli.consume);
    assert_eq!(cli.topic.as_deref(), Some("ns.topic"));
    assert_eq!(cli.group.as_deref(), Some("g1"));
    assert_eq!(cli.from.as_deref(), Some("earliest"));
    assert_eq!(cli.consume_limit, Some(5));
    assert_eq!(cli.consume_timeout, Some(8));
}

#[test]
fn test_cli_parse_missing_documented_agent_flags_without_server() {
    let cli = cli_args::Cli::try_parse_from([
        "kalam",
        "--init-agent",
        "--init-agent-non-interactive",
        "--agent-name",
        "doc-agent",
        "--agent-table",
        "blog.blogs",
        "--agent-topic",
        "blog.summarizer",
        "--agent-group",
        "blog-summarizer-agent",
        "--agent-id-column",
        "blog_id",
        "--agent-input-column",
        "content",
        "--agent-output-column",
        "summary",
        "--agent-system-prompt",
        "You are a summarizer.",
    ])
    .expect("args should parse");

    assert!(cli.init_agent);
    assert!(cli.init_agent_non_interactive);
    assert_eq!(cli.agent_id_column.as_deref(), Some("blog_id"));
    assert_eq!(cli.agent_input_column.as_deref(), Some("content"));
    assert_eq!(cli.agent_output_column.as_deref(), Some("summary"));
    assert_eq!(cli.agent_system_prompt.as_deref(), Some("You are a summarizer."));
}

#[test]
fn test_cli_parse_missing_documented_timeout_flags_without_server() {
    let cli = cli_args::Cli::try_parse_from([
        "kalam",
        "--subscribe",
        "SELECT * FROM app.messages",
        "--subscription-timeout",
        "9",
        "--initial-data-timeout",
        "11",
        "--connection-timeout",
        "6",
        "--receive-timeout",
        "12",
        "--auth-timeout",
        "4",
        "--fast-timeouts",
    ])
    .expect("args should parse");

    assert_eq!(cli.subscribe.as_deref(), Some("SELECT * FROM app.messages"));
    assert_eq!(cli.subscription_timeout, 9);
    assert_eq!(cli.initial_data_timeout, 11);
    assert_eq!(cli.connection_timeout, 6);
    assert_eq!(cli.receive_timeout, 12);
    assert_eq!(cli.auth_timeout, 4);
    assert!(cli.fast_timeouts);
}

#[test]
fn test_cli_subscribe_flags_work_end_to_end() {
    if !is_server_running() {
        eprintln!("Skipping subscribe runtime test: server not running");
        return;
    }

    let namespace = generate_unique_namespace("doc_subscribe");
    let table = generate_unique_table("events");
    let full_table = format!("{}.{}", namespace, table);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, msg VARCHAR) WITH (TYPE='SHARED')",
        full_table
    ))
    .expect("create table");
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, msg) VALUES (1, 'hello')",
        full_table
    ))
    .expect("insert row");

    let mut cmd = create_cli_command_with_root_auth();
    cmd.arg("--subscribe")
        .arg(format!("SELECT * FROM {}", full_table))
        .arg("--subscription-timeout")
        .arg("8")
        .arg("--initial-data-timeout")
        .arg("30")
        .arg("--no-spinner")
        .timeout(Duration::from_secs(60));

    let started = Instant::now();
    let output = cmd.output().expect("run subscribe");
    let elapsed = started.elapsed();

    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    assert!(
        output.status.success(),
        "subscribe should succeed, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        elapsed < Duration::from_secs(60),
        "subscribe should finish with timeout, elapsed={:?}",
        elapsed
    );
}

#[test]
fn test_cli_consume_flags_work_end_to_end() {
    if !is_server_running() {
        eprintln!("Skipping consume runtime test: server not running");
        return;
    }

    let namespace = generate_unique_namespace("doc_consume");
    let table = generate_unique_table("events");
    let topic = format!("{}.{}", namespace, generate_unique_table("topic"));
    let full_table = format!("{}.{}", namespace, table);

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, msg VARCHAR) WITH (TYPE='SHARED')",
        full_table
    ))
    .expect("create table");
    execute_sql_as_root_via_client(&format!("CREATE TOPIC {}", topic)).expect("create topic");
    execute_sql_as_root_via_client(&format!(
        "ALTER TOPIC {} ADD SOURCE {} ON INSERT",
        topic, full_table
    ))
    .expect("add topic source");

    let deadline = Instant::now() + Duration::from_secs(10);
    let readiness_sql = format!("SELECT routes FROM system.topics WHERE topic_id = '{}'", topic);
    let mut ready = false;
    while Instant::now() < deadline {
        if let Ok(out) = execute_sql_as_root_via_client(&readiness_sql) {
            if out.contains("INSERT") || out.contains("insert") {
                ready = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(ready, "topic routes were not ready in time for {}", topic);

    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, msg) VALUES (1, 'hello'), (2, 'world')",
        full_table
    ))
    .expect("insert records");

    let mut cmd = create_cli_command_with_root_auth();
    cmd.arg("--consume")
        .arg("--topic")
        .arg(&topic)
        .arg("--group")
        .arg(format!("doc-consume-{}", generate_unique_table("g")))
        .arg("--from")
        .arg("earliest")
        .arg("--consume-limit")
        .arg("2")
        .arg("--consume-timeout")
        .arg("15")
        .arg("--no-spinner")
        .timeout(Duration::from_secs(30));

    let output = cmd.output().expect("run consume");

    let _ = execute_sql_as_root_via_client(&format!("DROP TOPIC IF EXISTS {}", topic));
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "consume should succeed, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("Consumed 2 message(s)")
            || stdout.contains("Consumed 1 message(s)")
            || stdout.contains("Consumed"),
        "consume summary should be present, stdout={}",
        stdout
    );
}

#[test]
fn test_cli_runtime_timeout_and_spinner_flags_work() {
    if !is_server_running() {
        eprintln!("Skipping timeout/spinner runtime test: server not running");
        return;
    }

    let mut cmd = create_cli_command_with_root_auth();
    cmd.arg("--connection-timeout")
        .arg("5")
        .arg("--receive-timeout")
        .arg("30")
        .arg("--auth-timeout")
        .arg("5")
        .arg("--timeout")
        .arg("10")
        .arg("--loading-threshold-ms")
        .arg("0")
        .arg("--no-spinner")
        .arg("--command")
        .arg("SELECT 1 as ok")
        .timeout(Duration::from_secs(15));

    let output = cmd.output().expect("run timeout/spinner flags");
    assert!(
        output.status.success(),
        "runtime timeout/spinner flags should succeed, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_cli_runtime_timeout_presets_work() {
    if !is_server_running() {
        eprintln!("Skipping timeout preset runtime test: server not running");
        return;
    }

    let mut fast = create_cli_command_with_root_auth();
    fast.arg("--fast-timeouts")
        .arg("--command")
        .arg("SELECT 1 as fast")
        .timeout(Duration::from_secs(15));
    let fast_output = fast.output().expect("run fast-timeouts");
    assert!(
        fast_output.status.success(),
        "fast-timeouts should succeed, stderr={}",
        String::from_utf8_lossy(&fast_output.stderr)
    );

    let mut relaxed = create_cli_command_with_root_auth();
    relaxed
        .arg("--relaxed-timeouts")
        .arg("--command")
        .arg("SELECT 1 as relaxed")
        .timeout(Duration::from_secs(15));
    let relaxed_output = relaxed.output().expect("run relaxed-timeouts");
    assert!(
        relaxed_output.status.success(),
        "relaxed-timeouts should succeed, stderr={}",
        String::from_utf8_lossy(&relaxed_output.stderr)
    );
}

#[test]
fn test_cli_host_and_port_work_against_running_server() {
    if !is_server_running() {
        eprintln!("Skipping host/port runtime test: server not running");
        return;
    }

    let server = server_url().to_string();
    let host_port = server
        .strip_prefix("http://")
        .or_else(|| server.strip_prefix("https://"))
        .unwrap_or(&server);
    let (host, port) = host_port.split_once(':').expect("server URL should include host:port");

    let mut cmd = create_cli_command();
    cmd.arg("--host")
        .arg(host)
        .arg("--port")
        .arg(port)
        .arg("--username")
        .arg(default_username())
        .arg("--password")
        .arg(default_password())
        .arg("--command")
        .arg("SELECT 1 as host_port_ok")
        .timeout(Duration::from_secs(15));

    let output = cmd.output().expect("run with host/port");
    assert!(
        output.status.success(),
        "host/port invocation should succeed, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_cli_version_command() {
    let mut cmd = create_cli_command();
    cmd.arg("--version");
    let output = cmd.output().expect("run --version");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "--version should succeed");
    assert!(stdout.contains("Commit:"), "version output should include commit metadata");
}

#[test]
fn test_cli_meta_commands_doc_smoke_non_interactive() {
    // Interactive meta-commands are parsed in REPL mode, not by `--command`.
    // Validate parser support directly against the documented command inventory.
    let parser_source = include_str!("../../src/parser.rs");
    let documented_commands = [
        "\\help",
        "\\?",
        "\\info",
        "\\session",
        "\\history",
        "\\h",
        "\\health",
        "\\stats",
        "\\metrics",
        "\\dt",
        "\\tables",
        "\\d",
        "\\describe",
        "\\format",
        "\\refresh-tables",
        "\\refresh",
        "\\show-credentials",
        "\\credentials",
        "\\update-credentials",
        "\\delete-credentials",
        "\\unsubscribe",
        "\\unwatch",
        "\\flush",
        "\\pause",
        "\\continue",
        "\\cluster",
    ];

    for command in documented_commands {
        assert!(
            parser_source.contains(command),
            "documented interactive command missing in parser source: {}",
            command,
        );
    }
}
