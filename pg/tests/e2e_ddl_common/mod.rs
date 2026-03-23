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

use reqwest::Client;
use serde_json::Value;
use tokio_postgres::{Config, NoTls};

// ---------------------------------------------------------------------------
// Constants — pgrx-managed PostgreSQL + local KalamDB
// ---------------------------------------------------------------------------

/// pgrx PG16 port (default from `cargo pgrx init`).
const PG_PORT: u16 = 28816;
const PG_HOST: &str = "127.0.0.1";

/// KalamDB HTTP API (local server).
const KALAMDB_HTTP_PORT: u16 = 8080;

const KALAMDB_ADMIN_USER: &str = "admin";
const KALAMDB_ADMIN_PASSWORD: &str = "kalamdb123";

const TEST_DB: &str = "kalamdb_test";

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
        let url = format!("http://127.0.0.1:{KALAMDB_HTTP_PORT}/v1/api/sql");
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

    /// Check if a table exists in KalamDB by querying it.
    /// Returns true if the query succeeds, false if it errors.
    pub async fn kalamdb_table_exists(&self, namespace: &str, table: &str) -> bool {
        let url = format!("http://127.0.0.1:{KALAMDB_HTTP_PORT}/v1/api/sql");
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
        let url = format!("http://127.0.0.1:{KALAMDB_HTTP_PORT}/v1/api/sql");
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
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v["name"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    }

    // -- lifecycle ----------------------------------------------------------

    async fn start() -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();

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

        // 4. Verify PG is reachable
        let _ = env.pg_connect().await;

        env
    }

    async fn wait_for_kalamdb(client: &Client) {
        let url = format!("http://127.0.0.1:{KALAMDB_HTTP_PORT}/health");
        for i in 0..10 {
            if client.get(&url).send().await.is_ok() {
                return;
            }
            if i == 0 {
                eprintln!("  waiting for KalamDB on port {KALAMDB_HTTP_PORT}...");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!(
            "KalamDB not reachable at http://127.0.0.1:{KALAMDB_HTTP_PORT}\n\
             Start with: cd backend && cargo run"
        );
    }

    async fn authenticate(client: &Client) -> String {
        let base = format!("http://127.0.0.1:{KALAMDB_HTTP_PORT}");

        // Try setup first (idempotent).
        let _ = client
            .post(format!("{base}/v1/api/auth/setup"))
            .json(&serde_json::json!({
                "username": KALAMDB_ADMIN_USER,
                "password": KALAMDB_ADMIN_PASSWORD,
                "root_password": KALAMDB_ADMIN_PASSWORD,
            }))
            .send()
            .await;

        // Login
        let resp = client
            .post(format!("{base}/v1/api/auth/login"))
            .json(&serde_json::json!({
                "username": KALAMDB_ADMIN_USER,
                "password": KALAMDB_ADMIN_PASSWORD,
            }))
            .send()
            .await
            .expect("KalamDB login request");
        let body: Value = resp.json().await.expect("parse login response");
        body["access_token"]
            .as_str()
            .expect("access_token in login response")
            .to_string()
    }
}
