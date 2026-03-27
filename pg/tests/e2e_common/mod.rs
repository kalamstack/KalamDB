// pg/tests/e2e_common/mod.rs
//
// Shared helpers for pg_kalam end-to-end tests using a local pgrx PostgreSQL
// instance and a locally running KalamDB server.
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;
use std::process::Command;
use std::{env, fmt};

use reqwest::Client;
use serde_json::Value;
use tokio_postgres::{Config, NoTls};
use tokio_postgres::error::SqlState;

// ---------------------------------------------------------------------------
// Constants — pgrx-managed PostgreSQL + local KalamDB
// ---------------------------------------------------------------------------

const PG_HOST: &str = "127.0.0.1";
const PG_PORT: u16 = 28816;
const TEST_DB: &str = "kalamdb_test";

const DEFAULT_KALAMDB_SERVER_URL: &str = "http://127.0.0.1:8080";
const KALAMDB_GRPC_HOST: &str = "127.0.0.1";
const KALAMDB_GRPC_PORT: u16 = 9188;

const DEFAULT_KALAMDB_USER: &str = "root";
const DEFAULT_KALAMDB_PASSWORD: &str = "kalamdb123";
const DEFAULT_SETUP_USER: &str = "admin";

struct KalamDbAuthConfig {
    base_url: String,
    login_username: String,
    login_password: String,
    setup_username: String,
    setup_password: String,
    root_password: String,
}

impl fmt::Display for KalamDbAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "server={}, login_user={}, setup_user={}",
            self.base_url, self.login_username, self.setup_username
        )
    }
}

fn kalamdb_auth_config() -> KalamDbAuthConfig {
    let base_url = env::var("KALAMDB_SERVER_URL")
        .unwrap_or_else(|_| DEFAULT_KALAMDB_SERVER_URL.to_string())
        .trim_end_matches('/')
        .to_string();
    let root_password = env::var("KALAMDB_ROOT_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_KALAMDB_PASSWORD.to_string());
    let login_username = env::var("KALAMDB_USER")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_KALAMDB_USER.to_string());
    let login_password = env::var("KALAMDB_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| root_password.clone());
    let setup_username = env::var("KALAMDB_SETUP_USER")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_SETUP_USER.to_string());
    let setup_password = env::var("KALAMDB_SETUP_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| login_password.clone());

    KalamDbAuthConfig {
        base_url,
        login_username,
        login_password,
        setup_username,
        setup_password,
        root_password,
    }
}

// ---------------------------------------------------------------------------
// TestEnv — shared, singleton local test environment
// ---------------------------------------------------------------------------

pub struct TestEnv {
    pub bearer_token: String,
    http_client: Client,
    pg_user: String,
}

static ENV: OnceLock<TestEnv> = OnceLock::new();

impl TestEnv {
    pub async fn global() -> &'static TestEnv {
        if let Some(env) = ENV.get() {
            return env;
        }
        let env = Self::start().await;
        ENV.get_or_init(|| env)
    }

    pub async fn pg_connect(&self) -> tokio_postgres::Client {
        self.pg_connect_to(TEST_DB)
            .await
            .expect("connect to pgrx PostgreSQL (is it running on port 28816?)")
    }

    pub async fn kalamdb_sql(&self, sql: &str) -> Value {
        let base_url = kalamdb_auth_config().base_url;
        let url = format!("{base_url}/v1/api/sql");
        let body = serde_json::json!({ "sql": sql });
        let resp = self
            .http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .json(&body)
            .send()
            .await
            .expect("KalamDB SQL request");
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        assert!(
            status.is_success(),
            "KalamDB SQL failed ({status}): {text}\n  SQL: {sql}"
        );
        serde_json::from_str(&text).unwrap_or(Value::Null)
    }

    pub async fn kalamdb_table_exists(&self, namespace: &str, table: &str) -> bool {
        let base_url = kalamdb_auth_config().base_url;
        let url = format!("{base_url}/v1/api/sql");
        let sql = format!("SELECT * FROM {namespace}.{table} LIMIT 0");
        let body = serde_json::json!({ "sql": sql });
        let resp = self
            .http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .json(&body)
            .send()
            .await
            .expect("KalamDB table exists check");
        resp.status().is_success()
    }

    pub async fn kalamdb_columns(&self, namespace: &str, table: &str) -> Vec<String> {
        let base_url = kalamdb_auth_config().base_url;
        let url = format!("{base_url}/v1/api/sql");
        let sql = format!("SELECT * FROM {namespace}.{table} LIMIT 0");
        let body = serde_json::json!({ "sql": sql });
        let resp = self
            .http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.bearer_token))
            .json(&body)
            .send()
            .await
            .expect("KalamDB columns check");
        if !resp.status().is_success() {
            return Vec::new();
        }
        let text = resp.text().await.unwrap_or_default();
        let val: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        val["results"][0]["schema"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v["name"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn start() -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();

        Self::wait_for_kalamdb(&http_client).await;
        let bearer_token = Self::authenticate(&http_client).await;
        let pg_user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());

        let env = Self {
            bearer_token,
            http_client,
            pg_user,
        };

        env.ensure_test_db().await;
        env.wait_for_pg().await;
        env.ensure_extension_bootstrap().await;

        env
    }

    async fn wait_for_kalamdb(client: &Client) {
        let config = kalamdb_auth_config();
        let url = format!("{}/health", config.base_url);
        for i in 0..10 {
            if client
                .get(&url)
                .send()
                .await
                .map(|response| response.status().is_success())
                .unwrap_or(false)
            {
                return;
            }
            if i == 0 {
                eprintln!("  waiting for KalamDB at {}...", config.base_url);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!(
            "KalamDB not reachable at {}\n\
             Start with: cd backend && cargo run"
            , config.base_url
        );
    }

    async fn authenticate(client: &Client) -> String {
        let config = kalamdb_auth_config();

        if let Some(token) = try_login(
            client,
            &config.base_url,
            &config.login_username,
            &config.login_password,
        )
        .await
        {
            return token;
        }

        let _ = client
            .post(format!("{}/v1/api/auth/setup", config.base_url))
            .json(&serde_json::json!({
                "username": config.setup_username,
                "password": config.setup_password,
                "root_password": config.root_password,
            }))
            .send()
            .await;

        if let Some(token) = try_login(
            client,
            &config.base_url,
            &config.login_username,
            &config.login_password,
        )
        .await
        {
            return token;
        }

        if config.setup_username != config.login_username {
            if let Some(token) = try_login(
                client,
                &config.base_url,
                &config.setup_username,
                &config.setup_password,
            )
            .await
            {
                return token;
            }
        }

        panic!(
            "Failed to authenticate KalamDB test environment ({config}). Set KALAMDB_USER/KALAMDB_PASSWORD or KALAMDB_ROOT_PASSWORD to match the running server."
        );
    }

    async fn ensure_test_db(&self) {
        let postgres = self
            .pg_connect_to("postgres")
            .await
            .expect("connect to postgres database");

        let exists = postgres
            .query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&TEST_DB])
            .await
            .expect("query test database")
            .is_some();
        if !exists {
            postgres
                .batch_execute(&format!("CREATE DATABASE {TEST_DB};"))
                .await
                .expect("create test database");
        }
    }

    async fn ensure_extension_bootstrap(&self) {
        let pg = self.pg_connect().await;

        pg.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_kalam;")
            .await
            .expect("create extension pg_kalam");
        ensure_schema_exists(&pg, "e2e").await;
        pg.batch_execute(&format!(
            "CREATE SERVER IF NOT EXISTS kalam_server
                 FOREIGN DATA WRAPPER pg_kalam
                 OPTIONS (host '{KALAMDB_GRPC_HOST}', port '{KALAMDB_GRPC_PORT}');"
        ))
        .await
        .expect("create kalam_server foreign server");
    }

    async fn wait_for_pg(&self) {
        for i in 0..10 {
            match self.pg_connect_to("postgres").await {
                Ok(_client) => return,
                Err(_) => {
                    if i == 0 {
                        eprintln!("  waiting for PostgreSQL on port {PG_PORT}...");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        panic!(
            "PostgreSQL not reachable at {PG_HOST}:{PG_PORT}\n\
             Start with: ./pg/scripts/pgrx-test-setup.sh --start"
        );
    }

    async fn pg_connect_to(
        &self,
        dbname: &str,
    ) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
        let (client, conn) = Config::new()
            .host(PG_HOST)
            .port(PG_PORT)
            .user(&self.pg_user)
            .dbname(dbname)
            .connect(NoTls)
            .await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("pg connection error: {e}");
            }
        });
        Ok(client)
    }
}

async fn try_login(client: &Client, base_url: &str, username: &str, password: &str) -> Option<String> {
    let resp = client
        .post(format!("{base_url}/v1/api/auth/login"))
        .json(&serde_json::json!({
            "username": username,
            "password": password,
        }))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: Value = resp.json().await.ok()?;
    body["access_token"].as_str().map(ToString::to_string)
}

// ---------------------------------------------------------------------------
// Foreign Table helpers
// ---------------------------------------------------------------------------

pub async fn create_shared_foreign_table(
    client: &tokio_postgres::Client,
    table: &str,
    columns: &str,
) {
    ensure_schema_exists(client, "e2e").await;
    let drop = format!("DROP FOREIGN TABLE IF EXISTS e2e.{table};");
    client.batch_execute(&drop).await.expect("drop old table");
    let sql = format!(
        "CREATE FOREIGN TABLE e2e.{table} ({columns}) \
         SERVER kalam_server \
         OPTIONS (namespace 'e2e', \"table\" '{table}', table_type 'shared');"
    );
    client.batch_execute(&sql).await.expect("create foreign table");
}

pub async fn create_user_foreign_table(
    client: &tokio_postgres::Client,
    table: &str,
    columns: &str,
) {
    ensure_schema_exists(client, "e2e").await;
    let drop = format!("DROP FOREIGN TABLE IF EXISTS e2e.{table};");
    client.batch_execute(&drop).await.expect("drop old table");
    let sql = format!(
        "CREATE FOREIGN TABLE e2e.{table} ({columns}) \
         SERVER kalam_server \
         OPTIONS (namespace 'e2e', \"table\" '{table}', table_type 'user');"
    );
    client.batch_execute(&sql).await.expect("create foreign table");
}

pub async fn set_user_id(client: &tokio_postgres::Client, user_id: &str) {
    let sql = format!("SET kalam.user_id = '{user_id}';");
    client.batch_execute(&sql).await.expect("set user_id");
}

pub async fn wait_for_table_queryable(client: &tokio_postgres::Client, table: &str) {
    let sql = format!("SELECT 1 FROM {table} LIMIT 0");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

    loop {
        match client.simple_query(&sql).await {
            Ok(_) => return,
            Err(err) => {
                if std::time::Instant::now() >= deadline {
                    panic!("table {table} did not become queryable within timeout: {err}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            },
        }
    }
}

pub async fn count_rows(
    client: &tokio_postgres::Client,
    table: &str,
    where_clause: Option<&str>,
) -> i64 {
    let sql = match where_clause {
        Some(w) => format!("SELECT COUNT(*) FROM {table} WHERE {w}"),
        None => format!("SELECT COUNT(*) FROM {table}"),
    };
    let row = client.query_one(&sql, &[]).await.expect("count query");
    row.get::<_, i64>(0)
}

pub async fn bulk_delete_all(client: &tokio_postgres::Client, table: &str, pk_col: &str) {
    let sql = format!("DELETE FROM {table} WHERE {pk_col} IS NOT NULL");
    client.batch_execute(&sql).await.ok();
}

pub async fn delete_all(client: &tokio_postgres::Client, table: &str, pk_col: &str) {
    let sql = format!("SELECT {pk_col} FROM {table}");
    let rows = client.query(&sql, &[]).await.expect("select pks");
    for row in &rows {
        let pk: String = row.get(0);
        let del = format!("DELETE FROM {table} WHERE {pk_col} = $1");
        client.execute(&del, &[&pk]).await.expect("delete row");
    }
}

pub async fn timed_execute(client: &tokio_postgres::Client, sql: &str) -> (u64, f64) {
    let start = std::time::Instant::now();
    let rows_affected = client.execute(sql, &[]).await.expect("timed_execute");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    (rows_affected, elapsed_ms)
}

pub async fn timed_batch_execute(client: &tokio_postgres::Client, sql: &str) -> f64 {
    let start = std::time::Instant::now();
    client.batch_execute(sql).await.expect("timed_batch_execute");
    start.elapsed().as_secs_f64() * 1000.0
}

pub async fn timed_count(
    client: &tokio_postgres::Client,
    table: &str,
    where_clause: Option<&str>,
) -> (i64, f64) {
    let sql = match where_clause {
        Some(w) => format!("SELECT COUNT(*) FROM {table} WHERE {w}"),
        None => format!("SELECT COUNT(*) FROM {table}"),
    };
    let start = std::time::Instant::now();
    let row = client.query_one(&sql, &[]).await.expect("timed count");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    (row.get::<_, i64>(0), elapsed_ms)
}

pub async fn timed_query(client: &tokio_postgres::Client, sql: &str) -> (Vec<tokio_postgres::Row>, f64) {
    let start = std::time::Instant::now();
    let rows = client.query(sql, &[]).await.expect("timed_query");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    (rows, elapsed_ms)
}

pub fn kalamdb_pid() -> u32 {
    let output = Command::new("lsof")
        .args(["-nP", "-iTCP:8080", "-sTCP:LISTEN", "-t"])
        .output()
        .expect("run lsof for KalamDB pid");
    assert!(
        output.status.success(),
        "failed to resolve KalamDB pid via lsof: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("parse lsof pid output");
    stdout
        .lines()
        .find_map(|line| line.trim().parse::<u32>().ok())
        .expect("find KalamDB pid listening on 8080")
}

pub fn process_rss_kb(pid: u32) -> u64 {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("run ps for rss");
    assert!(
        output.status.success(),
        "failed to read rss for pid {pid}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("parse ps rss output");
    stdout
        .trim()
        .parse::<u64>()
        .expect("parse rss value")
}

pub async fn sample_process_peak_rss_kb(
    pid: u32,
    sample_interval_ms: u64,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> u64 {
    use std::sync::atomic::Ordering;

    let mut peak = process_rss_kb(pid);
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(sample_interval_ms)).await;
        peak = peak.max(process_rss_kb(pid));
    }
    peak.max(process_rss_kb(pid))
}

async fn ensure_schema_exists(client: &tokio_postgres::Client, schema: &str) {
    let sql = format!("CREATE SCHEMA IF NOT EXISTS {schema};");
    if let Err(error) = client.batch_execute(&sql).await {
        let duplicate = error
            .as_db_error()
            .map(|db_error| {
                db_error.code() == &SqlState::UNIQUE_VIOLATION
                    || db_error.code() == &SqlState::DUPLICATE_SCHEMA
            })
            .unwrap_or(false);
        if !duplicate {
            panic!("create schema {schema}: {error}");
        }
    }
}
