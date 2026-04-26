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

#[path = "../support/http_client.rs"]
mod http_client;

use std::{
    env, fmt,
    ops::{Deref, DerefMut},
    sync::OnceLock,
    time::Duration,
};

/// Reason DDL tests are being skipped (set once during init).
static SKIP_REASON: OnceLock<Option<String>> = OnceLock::new();

use http_client::TestHttpClient;
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
    login_user: String,
    login_password: String,
    setup_user: String,
    setup_password: String,
    root_password: String,
}

impl fmt::Display for KalamDbAuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "server={}, login_user={}, setup_user={}",
            self.base_url, self.login_user, self.setup_user
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
    let login_user = env::var("KALAMDB_USER")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_KALAMDB_USER.to_string());
    let login_password = env::var("KALAMDB_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| root_password.clone());
    let setup_user = env::var("KALAMDB_SETUP_USER")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_SETUP_USER.to_string());
    let setup_password = env::var("KALAMDB_SETUP_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| login_password.clone());

    KalamDbAuthConfig {
        base_url,
        login_user,
        login_password,
        setup_user,
        setup_password,
        root_password,
    }
}

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn kalamdb_account_login_server_options(host: &str, port: u16) -> String {
    let config = kalamdb_auth_config();

    format!(
        "host '{}', port '{}', auth_mode 'account_login', login_user '{}', login_password '{}'",
        sql_literal(host),
        port,
        sql_literal(&config.login_user),
        sql_literal(&config.login_password)
    )
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

async fn server_option_exists(
    pg: &tokio_postgres::Client,
    server_name: &str,
    option_name: &str,
) -> bool {
    pg.query_opt(
        "SELECT 1 FROM pg_foreign_server AS server CROSS JOIN LATERAL \
         pg_options_to_table(server.srvoptions) AS option_entry WHERE server.srvname = $1 AND \
         option_entry.option_name = $2",
        &[&server_name, &option_name],
    )
    .await
    .expect("query foreign server option")
    .is_some()
}

async fn ensure_server_option(
    pg: &tokio_postgres::Client,
    server_name: &str,
    option_name: &str,
    value: &str,
) {
    let action = if server_option_exists(pg, server_name, option_name).await {
        "SET"
    } else {
        "ADD"
    };
    let statement = format!(
        "ALTER SERVER {server_name} OPTIONS ({action} {option_name} '{}');",
        sql_literal(value)
    );

    pg.batch_execute(&statement)
        .await
        .unwrap_or_else(|error| panic!("configure foreign server option {option_name}: {error}"));
}

async fn drop_server_option_if_present(
    pg: &tokio_postgres::Client,
    server_name: &str,
    option_name: &str,
) {
    if !server_option_exists(pg, server_name, option_name).await {
        return;
    }

    let statement = format!("ALTER SERVER {server_name} OPTIONS (DROP {option_name});");
    pg.batch_execute(&statement)
        .await
        .unwrap_or_else(|error| panic!("drop foreign server option {option_name}: {error}"));
}

// ---------------------------------------------------------------------------
// DdlTestEnv — lightweight test environment (no Docker)
// ---------------------------------------------------------------------------

pub struct DdlTestEnv {
    pub bearer_token: String,
    http_client: TestHttpClient,
    pg_user: String,
}

pub struct OwnedPgClient {
    client: Option<tokio_postgres::Client>,
    connection_task: Option<tokio::task::JoinHandle<()>>,
}

impl OwnedPgClient {
    fn new(client: tokio_postgres::Client, connection_task: tokio::task::JoinHandle<()>) -> Self {
        Self {
            client: Some(client),
            connection_task: Some(connection_task),
        }
    }

    pub async fn disconnect(mut self) {
        self.client.take();
        if let Some(connection_task) = self.connection_task.take() {
            let _ = connection_task.await;
        }
    }
}

impl Deref for OwnedPgClient {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("pg client already disconnected")
    }
}

impl DerefMut for OwnedPgClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("pg client already disconnected")
    }
}

impl Drop for OwnedPgClient {
    fn drop(&mut self) {
        self.client.take();
        let _ = self.connection_task.take();
    }
}

static ENV: OnceLock<DdlTestEnv> = OnceLock::new();

impl DdlTestEnv {
    /// Return a reference to the global test environment.
    /// Returns `None` (and prints a skip message) if pgrx PostgreSQL is not
    /// reachable or `shared_preload_libraries` does not include `pg_kalam`.
    pub async fn global() -> Option<&'static DdlTestEnv> {
        if let Some(env) = ENV.get() {
            // Already initialised — check whether we decided to skip.
            if SKIP_REASON.get().and_then(|r| r.as_ref()).is_some() {
                return None;
            }
            return Some(env);
        }
        match Self::try_start().await {
            Ok(env) => {
                SKIP_REASON.get_or_init(|| None);
                Some(ENV.get_or_init(|| env))
            },
            Err(reason) => {
                eprintln!("  [SKIP] DDL tests skipped: {reason}");
                SKIP_REASON.get_or_init(|| Some(reason));
                // Store a dummy env so subsequent calls don't re-init.
                None
            },
        }
    }

    /// Open a new `tokio_postgres::Client` connected to the pgrx test PG.
    pub async fn pg_connect(&self) -> OwnedPgClient {
        self.pg_connect_to(TEST_DB)
            .await
            .expect("connect to pgrx PostgreSQL (is it running on port 28816?)")
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
            .post_json(&url, &body, Some(&self.bearer_token))
            .await
            .expect("KalamDB SQL request");
        let status = resp.status;
        let text = resp.body;
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
            .post_json(&url, &body, Some(&self.bearer_token))
            .await
            .expect("KalamDB table exists check");
        resp.status.is_success()
    }

    /// Get column names for a KalamDB table (returns empty vec on error).
    pub async fn kalamdb_columns(&self, namespace: &str, table: &str) -> Vec<String> {
        let base_url = kalamdb_auth_config().base_url;
        let url = format!("{base_url}/v1/api/sql");
        let sql = format!("SELECT * FROM {namespace}.{table} LIMIT 0");
        let body = serde_json::json!({ "sql": sql });
        let resp = self
            .http_client
            .post_json(&url, &body, Some(&self.bearer_token))
            .await
            .expect("KalamDB columns check");
        if !resp.status.is_success() {
            return Vec::new();
        }
        let text = resp.body;
        let val: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        // Response format: { "results": [{ "schema": [{"name": "col1", ...}, ...], "rows": [...] }]
        // }
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
                    "KalamDB columns for {namespace}.{table} did not satisfy {description} within \
                     timeout: {columns:?}"
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
                panic!("KalamDB table {namespace}.{table} did not {expectation} within timeout");
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // -- lifecycle ----------------------------------------------------------

    async fn try_start() -> Result<Self, String> {
        let http_client = TestHttpClient::new(Duration::from_secs(15));

        // 1. Verify KalamDB is reachable
        Self::wait_for_kalamdb(&http_client).await?;

        // 2. Authenticate
        let bearer_token = Self::authenticate(&http_client).await?;

        // 3. Detect current OS user (pgrx uses trust auth with $USER)
        let pg_user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());

        let env = Self {
            bearer_token,
            http_client,
            pg_user,
        };

        // 4. Verify PG is reachable and bootstrap the FDW test server.
        env.ensure_test_db().await?;
        env.wait_for_pg().await?;
        env.ensure_extension_bootstrap().await?;

        Ok(env)
    }

    async fn wait_for_kalamdb(client: &TestHttpClient) -> Result<(), String> {
        let config = kalamdb_auth_config();
        let url = format!("{}/health", config.base_url);
        for i in 0..10 {
            if client
                .get(&url)
                .await
                .map(|response| response.status.is_success())
                .unwrap_or(false)
            {
                return Ok(());
            }
            if i == 0 {
                eprintln!("  waiting for KalamDB at {}...", config.base_url);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(format!(
            "KalamDB not reachable at {}. Start with: cd backend && cargo run",
            config.base_url
        ))
    }

    async fn ensure_test_db(&self) -> Result<(), String> {
        let postgres = self
            .pg_connect_to("postgres")
            .await
            .map_err(|e| format!("connect to postgres database: {e}"))?;

        let exists = postgres
            .query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&TEST_DB])
            .await
            .map_err(|e| format!("query test database: {e}"))?
            .is_some();
        if !exists {
            postgres
                .batch_execute(&format!("CREATE DATABASE {TEST_DB};"))
                .await
                .map_err(|e| format!("create test database: {e}"))?;
        }

        postgres.disconnect().await;
        Ok(())
    }

    async fn ensure_extension_bootstrap(&self) -> Result<(), String> {
        let pg = self.pg_connect().await;
        let (grpc_host, grpc_port) = kalamdb_grpc_target();
        let auth_config = kalamdb_auth_config();
        let server_options = kalamdb_account_login_server_options(&grpc_host, grpc_port);
        const BOOTSTRAP_LOCK_ID: i64 = 8_271_604_221;

        pg.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_kalam;")
            .await
            .map_err(|e| format!("create extension pg_kalam: {e}"))?;

        let shared_preload = pg
            .query_one("SHOW shared_preload_libraries", &[])
            .await
            .map_err(|e| format!("show shared_preload_libraries: {e}"))?;
        let shared_preload: String = shared_preload.get(0);
        if !shared_preload.split(',').any(|entry| entry.trim() == "pg_kalam") {
            pg.disconnect().await;
            return Err(format!(
                "shared_preload_libraries does not include pg_kalam (got: '{shared_preload}'). \
                 Run: pg/scripts/pgrx-test-setup.sh"
            ));
        }

        pg.execute("SELECT pg_advisory_lock($1)", &[&BOOTSTRAP_LOCK_ID])
            .await
            .map_err(|e| format!("acquire pg_kalam bootstrap advisory lock: {e}"))?;

        let bootstrap_result = async {
            pg.batch_execute(&format!(
                "CREATE SERVER IF NOT EXISTS kalam_server
                     FOREIGN DATA WRAPPER pg_kalam
                     OPTIONS ({server_options});"
            ))
            .await
            .map_err(|e| format!("create kalam_server foreign server: {e}"))?;

            pg.batch_execute(&format!(
                "ALTER SERVER kalam_server OPTIONS (SET host '{grpc_host}', SET port \
                 '{grpc_port}');"
            ))
            .await
            .map_err(|e| format!("repoint kalam_server foreign server: {e}"))?;

            drop_server_option_if_present(&pg, "kalam_server", "auth_header").await;
            ensure_server_option(&pg, "kalam_server", "auth_mode", "account_login").await;
            ensure_server_option(&pg, "kalam_server", "login_user", &auth_config.login_user).await;
            ensure_server_option(
                &pg,
                "kalam_server",
                "login_password",
                &auth_config.login_password,
            )
            .await;

            Ok::<(), String>(())
        }
        .await;

        let _ = pg.execute("SELECT pg_advisory_unlock($1)", &[&BOOTSTRAP_LOCK_ID]).await;

        pg.disconnect().await;

        bootstrap_result
    }

    async fn wait_for_pg(&self) -> Result<(), String> {
        for i in 0..10 {
            match self.pg_connect_to("postgres").await {
                Ok(client) => {
                    client.disconnect().await;
                    return Ok(());
                },
                Err(_) => {
                    if i == 0 {
                        eprintln!("  waiting for PostgreSQL on port {PG_PORT}...");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                },
            }
        }
        Err(format!(
            "PostgreSQL not reachable at {PG_HOST}:{PG_PORT}. Start with: \
             ./pg/scripts/pgrx-test-setup.sh --start"
        ))
    }

    async fn pg_connect_to(&self, dbname: &str) -> Result<OwnedPgClient, tokio_postgres::Error> {
        let (client, conn) = Config::new()
            .host(PG_HOST)
            .port(PG_PORT)
            .user(&self.pg_user)
            .dbname(dbname)
            .connect(NoTls)
            .await?;
        let connection_task = tokio::spawn(async move {
            if let Err(error) = conn.await {
                eprintln!("pg connection error: {error}");
            }
        });
        Ok(OwnedPgClient::new(client, connection_task))
    }

    async fn authenticate(client: &TestHttpClient) -> Result<String, String> {
        let config = kalamdb_auth_config();

        if let Some(token) =
            try_login(client, &config.base_url, &config.login_user, &config.login_password).await
        {
            return Ok(token);
        }

        let _ = client
            .post_json(
                &format!("{}/v1/api/auth/setup", config.base_url),
                &serde_json::json!({
                    "user": config.setup_user,
                    "password": config.setup_password,
                    "root_password": config.root_password,
                }),
                None,
            )
            .await;

        if let Some(token) =
            try_login(client, &config.base_url, &config.login_user, &config.login_password).await
        {
            return Ok(token);
        }

        if config.setup_user != config.login_user {
            if let Some(token) =
                try_login(client, &config.base_url, &config.setup_user, &config.setup_password)
                    .await
            {
                return Ok(token);
            }
        }

        Err(format!(
            "Failed to authenticate KalamDB test environment ({config}). Set \
             KALAMDB_USER/KALAMDB_PASSWORD or KALAMDB_ROOT_PASSWORD."
        ))
    }
}

async fn try_login(
    client: &TestHttpClient,
    base_url: &str,
    user: &str,
    password: &str,
) -> Option<String> {
    let resp = client
        .post_json(
            &format!("{base_url}/v1/api/auth/login"),
            &serde_json::json!({
                "user": user,
                "password": password,
            }),
            None,
        )
        .await
        .ok()?;
    if !resp.status.is_success() {
        return None;
    }
    let body: Value = serde_json::from_str(&resp.body).ok()?;
    body["access_token"].as_str().map(ToString::to_string)
}
