// pg/tests/e2e_ddl_common/mod.rs
//
// Shared helpers for DDL propagation e2e tests using pgrx-managed PostgreSQL.
//
// Unlike e2e_common (Docker-based), this targets the local pgrx PG16 on port
// 28816 and a locally running KalamDB server on port 8080 (HTTP) / 9188 (gRPC).
//
// Prerequisites (run once):
//   1. KalamDB server running:  cd backend && cargo run
//   2. Extension installed:     pg/scripts/pgrx-test-setup.sh
//
// Run tests:
//   cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e_ddl)'
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;
use std::{env, fmt};

use reqwest::Client;
use serde_json::Value;
use tokio_postgres::{Config, NoTls};

// ---------------------------------------------------------------------------
// Constants — pgrx-managed PostgreSQL + local KalamDB
// ---------------------------------------------------------------------------

/// pgrx PG16 port (default from `cargo pgrx init`).
const PG_PORT: u16 = 28816;
const PG_HOST: &str = "127.0.0.1";

/// KalamDB HTTP API (local server by default, overridable via KALAMDB_SERVER_URL).
const DEFAULT_KALAMDB_SERVER_URL: &str = "http://127.0.0.1:8080";
const DEFAULT_KALAMDB_GRPC_HOST: &str = "127.0.0.1";
const DEFAULT_KALAMDB_GRPC_PORT: u16 = 9188;
const DEFAULT_KALAMDB_USER: &str = "root";
const DEFAULT_KALAMDB_PASSWORD: &str = "kalamdb123";
const DEFAULT_SETUP_USER: &str = "admin";

const TEST_DB: &str = "kalamdb_test";

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

fn kalamdb_grpc_target() -> (String, u16) {
    let host = env::var("KALAMDB_GRPC_HOST")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_KALAMDB_GRPC_HOST.to_string());
    let port = env::var("KALAMDB_GRPC_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_KALAMDB_GRPC_PORT);

    (host, port)
}

// ---------------------------------------------------------------------------
// DdlTestEnv — lightweight test environment (no Docker)
// ---------------------------------------------------------------------------

pub struct DdlTestEnv {
    pub bearer_token: String,
    http_client: Client,
    pg_user: String,
}

static ENV: OnceLock<DdlTestEnv> = OnceLock::new();

impl DdlTestEnv {
    /// Return a reference to the global test environment.
    /// Panics if the pgrx PG or KalamDB server is not reachable.
    pub async fn global() -> &'static DdlTestEnv {
        if let Some(env) = ENV.get() {
            return env;
        }
        let env = Self::start().await;
        ENV.get_or_init(|| env)
    }

    /// Open a new `tokio_postgres::Client` connected to the pgrx test PG.
    pub async fn pg_connect(&self) -> tokio_postgres::Client {
        let (client, conn) = Config::new()
            .host(PG_HOST)
            .port(PG_PORT)
            .user(&self.pg_user)
            .dbname(TEST_DB)
            .connect(NoTls)
            .await
            .expect("connect to pgrx PostgreSQL (is it running on port 28816?)");
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("pg connection error: {e}");
            }
        });
        client
    }

    /// Execute a SQL statement on KalamDB via its HTTP API.
    pub async fn kalamdb_sql(&self, sql: &str) -> Value {
        self.kalamdb_sql_maybe(sql)
            .await
            .unwrap_or_else(|error| panic!("KalamDB SQL failed: {error}\n  SQL: {sql}"))
    }

    pub async fn kalamdb_sql_maybe(&self, sql: &str) -> Result<Value, String> {
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
        if !status.is_success() {
            return Err(format!("({status}): {text}"));
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    /// Check if a table exists in KalamDB by querying it.
    /// Returns true if the query succeeds, false if it errors.
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

    /// Get column names for a KalamDB table (returns empty vec on error).
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
        // Response format: { "results": [{ "schema": [{"name": "col1", ...}, ...], "rows": [...] }] }
        val["results"][0]["schema"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v["name"].as_str().map(String::from)).collect())
            .unwrap_or_default()
    }

    pub async fn wait_for_kalamdb_table_exists(&self, namespace: &str, table: &str) {
        self.wait_for_kalamdb_table_state(namespace, table, true).await;
    }

    pub async fn wait_for_kalamdb_table_absent(&self, namespace: &str, table: &str) {
        self.wait_for_kalamdb_table_state(namespace, table, false).await;
    }

    pub async fn wait_for_kalamdb_columns<F>(
        &self,
        namespace: &str,
        table: &str,
        description: &str,
        predicate: F,
    ) -> Vec<String>
    where
        F: Fn(&[String]) -> bool,
    {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);

        loop {
            let columns = self.kalamdb_columns(namespace, table).await;
            if predicate(&columns) {
                return columns;
            }

            if std::time::Instant::now() >= deadline {
                panic!(
                    "KalamDB columns for {namespace}.{table} did not satisfy {description} within timeout: {columns:?}"
                );
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn wait_for_kalamdb_table_state(&self, namespace: &str, table: &str, should_exist: bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);

        loop {
            let exists = self.kalamdb_table_exists(namespace, table).await;
            if exists == should_exist {
                return;
            }

            if std::time::Instant::now() >= deadline {
                let expectation = if should_exist { "exist" } else { "be removed" };
                panic!(
                    "KalamDB table {namespace}.{table} did not {expectation} within timeout"
                );
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // -- lifecycle ----------------------------------------------------------

    async fn start() -> Self {
        let http_client = Client::builder().timeout(Duration::from_secs(15)).build().unwrap();

        // 1. Verify KalamDB is reachable
        Self::wait_for_kalamdb(&http_client).await;

        // 2. Authenticate
        let bearer_token = Self::authenticate(&http_client).await;

        // 3. Detect current OS user (pgrx uses trust auth with $USER)
        let pg_user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());

        let env = Self {
            bearer_token,
            http_client,
            pg_user,
        };

        // 4. Verify PG is reachable and bootstrap the FDW test server.
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
             Start with: cd backend && cargo run",
            config.base_url
        );
    }

    async fn ensure_test_db(&self) {
        let postgres = self.pg_connect_to("postgres").await.expect("connect to postgres database");

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
        let (grpc_host, grpc_port) = kalamdb_grpc_target();
        const BOOTSTRAP_LOCK_ID: i64 = 8_271_604_221;

        pg.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_kalam;")
            .await
            .expect("create extension pg_kalam");

        let shared_preload = pg
            .query_one("SHOW shared_preload_libraries", &[])
            .await
            .expect("show shared_preload_libraries");
        let shared_preload: String = shared_preload.get(0);
        assert!(
            shared_preload.split(',').any(|entry| entry.trim() == "pg_kalam"),
            "shared_preload_libraries must include pg_kalam for DDL propagation tests, got: {shared_preload}"
        );

        pg.execute("SELECT pg_advisory_lock($1)", &[&BOOTSTRAP_LOCK_ID])
            .await
            .expect("acquire pg_kalam bootstrap advisory lock");

        let bootstrap_result = async {
            pg.batch_execute(&format!(
                "CREATE SERVER IF NOT EXISTS kalam_server
                     FOREIGN DATA WRAPPER pg_kalam
                     OPTIONS (host '{grpc_host}', port '{grpc_port}');"
            ))
            .await
            .expect("create kalam_server foreign server");

            pg.batch_execute(&format!(
                "ALTER SERVER kalam_server OPTIONS (SET host '{grpc_host}', SET port '{grpc_port}');"
            ))
            .await
            .expect("repoint kalam_server foreign server");
        }
        .await;

        pg.execute("SELECT pg_advisory_unlock($1)", &[&BOOTSTRAP_LOCK_ID])
            .await
            .expect("release pg_kalam bootstrap advisory lock");

        bootstrap_result
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
                },
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
            if let Err(error) = conn.await {
                eprintln!("pg connection error: {error}");
            }
        });
        Ok(client)
    }

    async fn authenticate(client: &Client) -> String {
        let config = kalamdb_auth_config();

        if let Some(token) =
            try_login(client, &config.base_url, &config.login_username, &config.login_password)
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

        if let Some(token) =
            try_login(client, &config.base_url, &config.login_username, &config.login_password)
                .await
        {
            return token;
        }

        if config.setup_username != config.login_username {
            if let Some(token) =
                try_login(client, &config.base_url, &config.setup_username, &config.setup_password)
                    .await
            {
                return token;
            }
        }

        panic!(
            "Failed to authenticate KalamDB test environment ({config}). Set KALAMDB_USER/KALAMDB_PASSWORD or KALAMDB_ROOT_PASSWORD to match the running server."
        );
    }
}

async fn try_login(
    client: &Client,
    base_url: &str,
    username: &str,
    password: &str,
) -> Option<String> {
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
