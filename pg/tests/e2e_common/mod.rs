// pg/tests/e2e_common/mod.rs
//
// Shared helpers for pg_kalam end-to-end tests using a local pgrx PostgreSQL
// instance and a locally running KalamDB server.
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;

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

const KALAMDB_HTTP_PORT: u16 = 8080;
const KALAMDB_GRPC_HOST: &str = "127.0.0.1";
const KALAMDB_GRPC_PORT: u16 = 9188;

const KALAMDB_ADMIN_USER: &str = "admin";
const KALAMDB_ADMIN_PASSWORD: &str = "kalamdb123";

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

        let _ = client
            .post(format!("{base}/v1/api/auth/setup"))
            .json(&serde_json::json!({
                "username": KALAMDB_ADMIN_USER,
                "password": KALAMDB_ADMIN_PASSWORD,
                "root_password": KALAMDB_ADMIN_PASSWORD,
            }))
            .send()
            .await;

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
