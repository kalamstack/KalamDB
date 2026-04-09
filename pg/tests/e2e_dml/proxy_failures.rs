use std::time::{Duration, Instant};
use std::{env, ops::{Deref, DerefMut}};

use serde_json::Value;
use tokio_postgres::{Config, NoTls};

use super::common::{
    kalamdb_grpc_target, pg_backend_pid, postgres_error_text, unique_name, TestEnv,
};
use crate::e2e_common::tcp_proxy::TcpDisconnectProxy;

const PG_HOST: &str = "127.0.0.1";
const PG_PORT: u16 = 28816;
const TEST_DB: &str = "kalamdb_test";

struct OwnedPgClient {
    client: tokio_postgres::Client,
    connection_task: Option<tokio::task::JoinHandle<()>>,
}

impl OwnedPgClient {
    async fn connect() -> Self {
        let pg_user = env::var("USER").unwrap_or_else(|_| "postgres".to_string());
        let (client, connection) = Config::new()
            .host(PG_HOST)
            .port(PG_PORT)
            .user(&pg_user)
            .dbname(TEST_DB)
            .connect(NoTls)
            .await
            .expect("connect to pgrx PostgreSQL (is it running on port 28816?)");

        let connection_task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("pg connection error: {error}");
            }
        });

        Self {
            client,
            connection_task: Some(connection_task),
        }
    }

    async fn disconnect(mut self) {
        drop(self.client);
        if let Some(connection_task) = self.connection_task.take() {
            connection_task.abort();
            let _ = connection_task.await;
        }
    }
}

impl Deref for OwnedPgClient {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for OwnedPgClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

#[derive(Clone, Copy)]
enum TerminalAction {
    Commit,
    Rollback,
}

impl TerminalAction {
    fn label(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Rollback => "rollback",
        }
    }
}

fn sql_rows(result: &Value) -> Vec<Vec<Value>> {
    result["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|result| result["rows"].as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.as_array().map(|columns| columns.to_vec()))
                .collect()
        })
        .unwrap_or_default()
}

fn string_cell(row: &[Value], index: usize) -> Option<String> {
    row.get(index).and_then(Value::as_str).map(ToString::to_string)
}

async fn wait_for_pg_backend_exit(backend_pid: u32, timeout: Duration) {
    let env = TestEnv::global().await;
    let observer = env.pg_connect().await;
    let deadline = Instant::now() + timeout;

    loop {
        let row = observer
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)",
                &[&(backend_pid as i32)],
            )
            .await
            .expect("query pg_stat_activity");
        let still_running: bool = row.get(0);
        if !still_running {
            return;
        }

        if Instant::now() >= deadline {
            panic!("backend pid {backend_pid} remained in pg_stat_activity past disconnect timeout");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn create_proxy_shared_foreign_table(
    client: &tokio_postgres::Client,
    server_name: &str,
    table: &str,
    host: &str,
    port: u16,
) {
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS e2e;")
        .await
        .expect("create e2e schema");
    client
        .batch_execute(&format!(
            "DROP FOREIGN TABLE IF EXISTS e2e.{table}; \
             DROP SERVER IF EXISTS {server_name} CASCADE; \
             CREATE SERVER {server_name} \
                 FOREIGN DATA WRAPPER pg_kalam \
                 OPTIONS (host '{host}', port '{port}'); \
             CREATE FOREIGN TABLE e2e.{table} ( \
                 id TEXT, \
                 title TEXT, \
                 value INTEGER \
             ) SERVER {server_name} \
             OPTIONS (namespace 'e2e', \"table\" '{table}', table_type 'shared');"
        ))
        .await
        .expect("create proxy foreign table");

    TestEnv::global()
        .await
        .wait_for_kalamdb_table_exists("e2e", table)
        .await;
}

async fn fetch_session_rows(env: &TestEnv, backend_pid: u32) -> Vec<Vec<Value>> {
    sql_rows(
        &env.kalamdb_sql(&format!(
            "SELECT session_id, state, transaction_id, transaction_state \
             FROM system.sessions \
             WHERE backend_pid = {backend_pid} \
             ORDER BY last_seen_at DESC"
        ))
        .await,
    )
}

async fn fetch_transaction_rows(env: &TestEnv, transaction_id: &str) -> Vec<Vec<Value>> {
    sql_rows(
        &env.kalamdb_sql(&format!(
            "SELECT transaction_id, owner_id, origin, state, write_count \
             FROM system.transactions \
             WHERE transaction_id = '{transaction_id}'"
        ))
        .await,
    )
}

async fn wait_for_transaction_row(
    env: &TestEnv,
    transaction_id: &str,
    timeout: Duration,
) -> Vec<Value> {
    let deadline = Instant::now() + timeout;

    loop {
        let rows = fetch_transaction_rows(env, transaction_id).await;
        if let Some(row) = rows.first() {
            return row.clone();
        }
        if Instant::now() >= deadline {
            panic!("transaction {transaction_id} did not appear in system.transactions");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_session_rows(env: &TestEnv, backend_pid: u32, timeout: Duration) -> Vec<Vec<Value>> {
    let deadline = Instant::now() + timeout;

    loop {
        let rows = fetch_session_rows(env, backend_pid).await;
        if !rows.is_empty() {
            return rows;
        }
        if Instant::now() >= deadline {
            panic!("backend pid {backend_pid} did not appear in system.sessions within timeout");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_session_cleanup(env: &TestEnv, backend_pid: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;

    loop {
        if fetch_session_rows(env, backend_pid).await.is_empty() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("backend pid {backend_pid} remained in system.sessions past cleanup timeout");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_transaction_cleanup(env: &TestEnv, transaction_id: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;

    loop {
        if fetch_transaction_rows(env, transaction_id).await.is_empty() {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "transaction {transaction_id} remained in system.transactions past cleanup timeout"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn proxy_host_port(base_url: &str) -> (String, u16) {
    let address = base_url.trim_start_matches("http://").trim_start_matches("https://");
    let mut parts = address.split(':');
    let host = parts.next().unwrap_or("127.0.0.1").to_string();
    let port = parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(9188);
    (host, port)
}

async fn run_terminal_proxy_cleanup_scenario(action: TerminalAction) {
    let env = TestEnv::global().await;
    let mut pg = OwnedPgClient::connect().await;
    let (grpc_host, grpc_port) = kalamdb_grpc_target();
    let proxy = TcpDisconnectProxy::start(&format!("http://{grpc_host}:{grpc_port}")).await;
    let server_name = unique_name(&format!("proxy_server_{}", action.label()));
    let table = unique_name(&format!("proxy_{}", action.label()));
    let qualified_table = format!("e2e.{table}");
    let (proxy_host, proxy_port) = proxy_host_port(proxy.base_url());

    create_proxy_shared_foreign_table(&pg, &server_name, &table, &proxy_host, proxy_port).await;

    let backend_pid = pg_backend_pid(&pg).await;
    let tx = pg.transaction().await.expect("begin transaction through proxy");
    tx.execute(
        &format!(
            "INSERT INTO {qualified_table} (id, title, value) VALUES ($1, $2, $3)"
        ),
        &[&format!("{}-1", action.label()), &format!("{} row", action.label()), &7_i32],
    )
    .await
    .expect("stage row through proxy-backed foreign table");

    assert!(
        proxy
            .wait_for_active_connections(1, Duration::from_secs(3))
            .await,
        "proxy should observe the gRPC connection before transport failure"
    );

    let session_rows = wait_for_session_rows(env, backend_pid, Duration::from_secs(3)).await;
    assert_eq!(session_rows.len(), 1);
    assert_eq!(string_cell(&session_rows[0], 1).as_deref(), Some("idle in transaction"));
    assert_eq!(string_cell(&session_rows[0], 3).as_deref(), Some("active"));
    let transaction_id = string_cell(&session_rows[0], 2).expect("transaction id in system.sessions");
    let transaction_row = wait_for_transaction_row(env, &transaction_id, Duration::from_secs(3)).await;
    assert_eq!(string_cell(&transaction_row, 2).as_deref(), Some("PgRpc"));
    assert!(matches!(
        string_cell(&transaction_row, 3).as_deref(),
        Some("open_read") | Some("open_write")
    ));

    proxy.simulate_server_down().await;

    match action {
        TerminalAction::Commit => {
            let terminal_error = tx
                .commit()
                .await
                .expect_err("commit should fail while proxy is down");
            let message = postgres_error_text(&terminal_error);
            assert!(
                message.contains("db error")
                    || message.contains("Connection reset")
                    || message.contains("broken pipe")
                    || message.contains("transport")
                    || message.contains("connection closed"),
                "unexpected proxy failure error for {}: {message}",
                action.label()
            );

            let stuck_session_rows =
                wait_for_session_rows(env, backend_pid, Duration::from_secs(2)).await;
            assert_eq!(stuck_session_rows.len(), 1);
            assert_eq!(string_cell(&stuck_session_rows[0], 2), Some(transaction_id.clone()));
            let stuck_transaction_rows = fetch_transaction_rows(env, &transaction_id).await;
            assert_eq!(stuck_transaction_rows.len(), 1);
        },
        TerminalAction::Rollback => {
            if let Err(terminal_error) = tx.rollback().await {
                let message = postgres_error_text(&terminal_error);
                assert!(
                    message.contains("db error")
                        || message.contains("Connection reset")
                        || message.contains("broken pipe")
                        || message.contains("transport")
                        || message.contains("connection closed")
                        || message.contains("operation was canceled")
                        || message.contains("operation was cancelled"),
                    "unexpected proxy failure error for {}: {message}",
                    action.label()
                );
            }
        },
    }

    proxy.simulate_server_up();
    pg.disconnect().await;

    wait_for_session_cleanup(env, backend_pid, Duration::from_secs(5)).await;
    wait_for_transaction_cleanup(env, &transaction_id, Duration::from_secs(5)).await;

    let final_rows = env
        .kalamdb_sql(&format!(
            "SELECT id FROM {qualified_table} WHERE id = '{}-1'",
            action.label()
        ))
        .await;
    let final_text = serde_json::to_string(&final_rows).unwrap_or_default();
    assert!(
        !final_text.contains(&format!("{}-1", action.label())),
        "proxy-interrupted {} should not leave committed rows behind: {final_text}",
        action.label()
    );

    let cleanup = env.pg_connect().await;
    cleanup
        .batch_execute(&format!(
            "DROP FOREIGN TABLE IF EXISTS e2e.{table}; DROP SERVER IF EXISTS {server_name} CASCADE;"
        ))
        .await
        .ok();
    cleanup.disconnect().await;
    env.kalamdb_sql(&format!("DROP SHARED TABLE IF EXISTS e2e.{table}")).await;

    proxy.shutdown().await;
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_proxy_commit_failure_eventually_cleans_remote_transaction_state() {
    run_terminal_proxy_cleanup_scenario(TerminalAction::Commit).await;
}

#[tokio::test]
#[ntest::timeout(15000)]
async fn e2e_proxy_rollback_failure_eventually_cleans_remote_transaction_state() {
    run_terminal_proxy_cleanup_scenario(TerminalAction::Rollback).await;
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn owned_pg_client_disconnect_terminates_backend() {
    let pg = OwnedPgClient::connect().await;
    let backend_pid = pg_backend_pid(&pg).await;

    pg.disconnect().await;

    wait_for_pg_backend_exit(backend_pid, Duration::from_secs(3)).await;
}