// pg/tests/e2e_common/mod.rs
//
// Shared helpers for pg_kalam end-to-end tests.
#![allow(dead_code)]
//
// Architecture:
//   - `TestEnv::start()` spins up docker-compose.test.yml, bootstraps KalamDB
//     (auth + namespace + tables), and returns a reusable handle.
//   - `TestEnv::pg_connect()` returns a tokio-postgres client connected to the
//     test PostgreSQL instance.
//   - `TestEnv` implements `Drop` to tear down the compose stack.
//   - A global `OnceLock<TestEnv>` lets multiple `#[tokio::test]` in the same
//     binary share the same environment (Docker is started once).

use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use reqwest::Client;
use serde_json::Value;
use tokio_postgres::{Config, NoTls};

// ---------------------------------------------------------------------------
// Constants (aligned with docker-compose.test.yml)
// ---------------------------------------------------------------------------

/// KalamDB HTTP API on the host.
const KALAMDB_HTTP_PORT: u16 = 18088;
/// PostgreSQL port on the host.
const PG_PORT: u16 = 15433;

const PG_USER: &str = "kalamdb";
const PG_PASSWORD: &str = "kalamdb123";
const PG_DB: &str = "kalamdb";

const KALAMDB_ADMIN_USER: &str = "admin";
const KALAMDB_ADMIN_PASSWORD: &str = "kalamdb123";

const COMPOSE_FILE: &str = "pg/docker/docker-compose.test.yml";

// ---------------------------------------------------------------------------
// TestEnv — shared, singleton test environment
// ---------------------------------------------------------------------------

pub struct TestEnv {
    /// Bearer token obtained from KalamDB after login.
    pub bearer_token: String,
    http_client: Client,
}

/// Global singleton so the compose stack is started only once per test binary.
static ENV: OnceLock<TestEnv> = OnceLock::new();

impl TestEnv {
    // -- public API ---------------------------------------------------------

    /// Return a reference to the global test environment, starting Docker
    /// Compose on first call.
    pub async fn global() -> &'static TestEnv {
        if let Some(env) = ENV.get() {
            return env;
        }
        let env = Self::start().await;
        ENV.get_or_init(|| env)
    }

    /// Open a new `tokio_postgres::Client` connected to the test PostgreSQL.
    pub async fn pg_connect(&self) -> tokio_postgres::Client {
        let (client, conn) = Config::new()
            .host("127.0.0.1")
            .port(PG_PORT)
            .user(PG_USER)
            .password(PG_PASSWORD)
            .dbname(PG_DB)
            .connect(NoTls)
            .await
            .expect("connect to test PostgreSQL");
        // Spawn the connection driver so queries can execute.
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

    // -- lifecycle ----------------------------------------------------------

    async fn start() -> Self {
        // 1. Start compose
        Self::compose_up();

        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();

        // 2. Wait for KalamDB health
        Self::wait_for_kalamdb(&http_client).await;

        // 3. Authenticate (setup + login)
        let bearer_token = Self::authenticate(&http_client).await;

        let env = Self {
            bearer_token,
            http_client,
        };

        // 4. Bootstrap test namespace + tables in KalamDB
        env.bootstrap().await;

        // 5. Wait for PostgreSQL
        env.wait_for_pg().await;

        // 6. Set the auth_header on the foreign server so the pg_kalam FDW
        //    can authenticate its gRPC calls to KalamDB.
        env.setup_pg_auth().await;

        env
    }

    fn compose_up() {
        let repo_root = Self::repo_root();
        let status = Command::new("docker")
            .args([
                "compose",
                "-f",
                COMPOSE_FILE,
                "-p",
                "kalam-e2e",
                "up",
                "-d",
                "--wait",
            ])
            .current_dir(&repo_root)
            .status()
            .expect("docker compose up");
        assert!(status.success(), "docker compose up failed");
    }

    /// Tear down the test stack. Called on process exit via atexit if needed;
    /// tests can also call this explicitly.
    #[allow(dead_code)]
    pub fn compose_down() {
        let repo_root = Self::repo_root();
        let _ = Command::new("docker")
            .args([
                "compose",
                "-f",
                COMPOSE_FILE,
                "-p",
                "kalam-e2e",
                "down",
                "-v",
                "--remove-orphans",
            ])
            .current_dir(&repo_root)
            .status();
    }

    // -- helpers ------------------------------------------------------------

    fn repo_root() -> String {
        // The test binary runs with cwd = crate root (pg/).
        // Go up one level to reach the repo root.
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .unwrap_or_else(|_| ".".to_string());
        let path = std::path::Path::new(&manifest);
        // pg/Cargo.toml → repo root is parent
        path.parent()
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    }

    async fn wait_for_kalamdb(client: &Client) {
        let url = format!("http://127.0.0.1:{KALAMDB_HTTP_PORT}/health");
        for i in 0..60 {
            if client.get(&url).send().await.is_ok() {
                return;
            }
            if i % 10 == 0 {
                eprintln!("  waiting for KalamDB health ({i}s) ...");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!("KalamDB did not become healthy within 60s");
    }

    async fn authenticate(client: &Client) -> String {
        let base = format!("http://127.0.0.1:{KALAMDB_HTTP_PORT}");

        // Try setup first (idempotent on an already-initialized server).
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

    async fn bootstrap(&self) {
        self.kalamdb_sql("CREATE NAMESPACE IF NOT EXISTS e2e").await;

        self.kalamdb_sql(
            "CREATE SHARED TABLE IF NOT EXISTS e2e.items \
             (id TEXT PRIMARY KEY, title TEXT, value INTEGER)",
        )
        .await;

        self.kalamdb_sql(
            "CREATE USER TABLE IF NOT EXISTS e2e.profiles \
             (id TEXT PRIMARY KEY, name TEXT, age INTEGER)",
        )
        .await;

        // Performance test tables
        for (name, cols) in [
            ("perf_batch", "id TEXT PRIMARY KEY, payload TEXT, seq_num INTEGER"),
            ("perf_seq", "id TEXT PRIMARY KEY, value INTEGER"),
            ("perf_seq100", "id TEXT PRIMARY KEY, value INTEGER"),
            ("perf_scan", "id TEXT PRIMARY KEY, title TEXT, value INTEGER"),
            ("perf_point", "id TEXT PRIMARY KEY, payload TEXT, value INTEGER"),
            ("perf_update", "id TEXT PRIMARY KEY, value INTEGER"),
            ("perf_delete", "id TEXT PRIMARY KEY, value INTEGER"),
            ("perf_xv", "id TEXT PRIMARY KEY, value INTEGER"),
        ] {
            self.kalamdb_sql(&format!(
                "CREATE SHARED TABLE IF NOT EXISTS e2e.{name} ({cols})"
            ))
            .await;
        }

        self.kalamdb_sql(
            "CREATE USER TABLE IF NOT EXISTS e2e.perf_user \
             (id TEXT PRIMARY KEY, data TEXT)",
        )
        .await;
    }

    /// Verify the `kalam_server` foreign server has a valid auth_header configured.
    /// The pre-shared token is set statically in init-test.sql (matching KALAMDB_PG_AUTH_TOKEN).
    async fn setup_pg_auth(&self) {
        let pg = self.pg_connect().await;
        // Quick sanity check: the foreign server should already exist with auth_header set.
        let row = pg
            .query_one(
                "SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'kalam_server'",
                &[],
            )
            .await
            .expect("query kalam_server options");
        let opts: Vec<String> = row.get(0);
        assert!(
            opts.iter().any(|o| o.starts_with("auth_header=")),
            "init-test.sql should have set auth_header on kalam_server; got: {opts:?}"
        );
    }

    async fn wait_for_pg(&self) {
        for i in 0..60 {
            match self.pg_connect_attempt().await {
                Ok(_client) => return,
                Err(_) => {
                    if i % 10 == 0 {
                        eprintln!("  waiting for PostgreSQL ({i}s) ...");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        panic!("PostgreSQL did not become ready within 60s");
    }

    async fn pg_connect_attempt(
        &self,
    ) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
        let (client, conn) = Config::new()
            .host("127.0.0.1")
            .port(PG_PORT)
            .user(PG_USER)
            .password(PG_PASSWORD)
            .dbname(PG_DB)
            .connect(NoTls)
            .await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(client)
    }
}

// ---------------------------------------------------------------------------
// Foreign Table helpers
// ---------------------------------------------------------------------------

/// Create the `e2e` schema and a foreign table for a KalamDB shared table.
pub async fn create_shared_foreign_table(
    client: &tokio_postgres::Client,
    table: &str,
    columns: &str,
) {
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS e2e;")
        .await
        .expect("create schema");
    let drop = format!("DROP FOREIGN TABLE IF EXISTS e2e.{table};");
    client.batch_execute(&drop).await.expect("drop old table");
    let sql = format!(
        "CREATE FOREIGN TABLE e2e.{table} ({columns}) \
         SERVER kalam_server \
         OPTIONS (namespace 'e2e', \"table\" '{table}', table_type 'shared');"
    );
    client.batch_execute(&sql).await.expect("create foreign table");
}

/// Create the `e2e` schema and a foreign table for a KalamDB user table.
pub async fn create_user_foreign_table(
    client: &tokio_postgres::Client,
    table: &str,
    columns: &str,
) {
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS e2e;")
        .await
        .expect("create schema");
    let drop = format!("DROP FOREIGN TABLE IF EXISTS e2e.{table};");
    client.batch_execute(&drop).await.expect("drop old table");
    let sql = format!(
        "CREATE FOREIGN TABLE e2e.{table} ({columns}) \
         SERVER kalam_server \
         OPTIONS (namespace 'e2e', \"table\" '{table}', table_type 'user');"
    );
    client.batch_execute(&sql).await.expect("create foreign table");
}

/// Set the kalam.user_id GUC for the current session.
pub async fn set_user_id(client: &tokio_postgres::Client, user_id: &str) {
    let sql = format!("SET kalam.user_id = '{user_id}';");
    client.batch_execute(&sql).await.expect("set user_id");
}

/// Count rows in a foreign table (optionally with a WHERE clause).
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

/// Delete all rows from a foreign table using bulk WHERE clause.
/// Much faster than row-by-row `delete_all` for large tables.
pub async fn bulk_delete_all(client: &tokio_postgres::Client, table: &str, pk_col: &str) {
    let sql = format!("DELETE FROM {table} WHERE {pk_col} IS NOT NULL");
    client.batch_execute(&sql).await.ok();
}

/// Delete all rows from a foreign table (requires user_id set for user tables).
pub async fn delete_all(client: &tokio_postgres::Client, table: &str, pk_col: &str) {
    // FDW DELETE requires a WHERE clause. Select all PKs and delete each.
    let sql = format!("SELECT {pk_col} FROM {table}");
    let rows = client.query(&sql, &[]).await.expect("select pks");
    for row in &rows {
        let pk: String = row.get(0);
        let del = format!("DELETE FROM {table} WHERE {pk_col} = $1");
        client.execute(&del, &[&pk]).await.expect("delete row");
    }
}

/// Measure the elapsed time of a SQL statement in milliseconds.
pub async fn timed_execute(client: &tokio_postgres::Client, sql: &str) -> (u64, f64) {
    let start = std::time::Instant::now();
    let rows_affected = client.execute(sql, &[]).await.expect("timed_execute");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    (rows_affected, elapsed_ms)
}

/// Measure the elapsed time of a batch_execute in milliseconds.
pub async fn timed_batch_execute(client: &tokio_postgres::Client, sql: &str) -> f64 {
    let start = std::time::Instant::now();
    client.batch_execute(sql).await.expect("timed_batch_execute");
    start.elapsed().as_secs_f64() * 1000.0
}

/// Measure the elapsed time of a SELECT COUNT(*) in milliseconds.
pub async fn timed_count(client: &tokio_postgres::Client, table: &str, where_clause: Option<&str>) -> (i64, f64) {
    let sql = match where_clause {
        Some(w) => format!("SELECT COUNT(*) FROM {table} WHERE {w}"),
        None => format!("SELECT COUNT(*) FROM {table}"),
    };
    let start = std::time::Instant::now();
    let row = client.query_one(&sql, &[]).await.expect("timed count");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    (row.get::<_, i64>(0), elapsed_ms)
}

/// Measure the elapsed time of a SELECT query in milliseconds.
pub async fn timed_query(client: &tokio_postgres::Client, sql: &str) -> (Vec<tokio_postgres::Row>, f64) {
    let start = std::time::Instant::now();
    let rows = client.query(sql, &[]).await.expect("timed_query");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    (rows, elapsed_ms)
}
