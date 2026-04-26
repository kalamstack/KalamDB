// pg/tests/e2e_common/mod.rs
//
// Shared helpers for pg_kalam end-to-end tests using a local pgrx PostgreSQL
// instance and a locally running KalamDB server.
#![allow(dead_code)]

pub mod tcp_proxy;

#[path = "../support/http_client.rs"]
mod http_client;

use std::{
    env, fmt,
    future::Future,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    process::Command,
    sync::OnceLock,
    time::Duration,
};

use http_client::TestHttpClient;
use serde_json::Value;
use tokio_postgres::{error::SqlState, Config, NoTls};

pub fn unique_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}_{ts}_{n}")
}

pub fn postgres_error_text(error: &tokio_postgres::Error) -> String {
    if let Some(db_error) = error.as_db_error() {
        let mut parts = vec![db_error.message().to_string()];
        if let Some(detail) = db_error.detail() {
            parts.push(detail.to_string());
        }
        if let Some(hint) = db_error.hint() {
            parts.push(hint.to_string());
        }
        parts.join(" | ")
    } else {
        error.to_string()
    }
}

// ---------------------------------------------------------------------------
// Constants — configurable via env vars, with sensible defaults
// ---------------------------------------------------------------------------

// Default: pgrx local postgres. Override with KALAMDB_PG_HOST / KALAMDB_PG_PORT.
const DEFAULT_PG_HOST: &str = "127.0.0.1";
const DEFAULT_PG_PORT: u16 = 28816;
const DEFAULT_TEST_DB: &str = "kalamdb_test";

fn pg_connection_config() -> (String, u16) {
    let host = env::var("KALAMDB_PG_HOST")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_PG_HOST.to_string());
    let port = env::var("KALAMDB_PG_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PG_PORT);
    (host, port)
}

const DEFAULT_KALAMDB_SERVER_URL: &str = "http://127.0.0.1:8080";
const DEFAULT_KALAMDB_GRPC_HOST: &str = "127.0.0.1";
const DEFAULT_KALAMDB_GRPC_PORT: u16 = 9188;

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

fn sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

pub fn kalamdb_account_login_server_options(host: &str, port: u16) -> String {
    let config = kalamdb_auth_config();

    format!(
        "host '{}', port '{}', auth_mode 'account_login', login_user '{}', login_password '{}'",
        sql_literal(host),
        port,
        sql_literal(&config.login_username),
        sql_literal(&config.login_password)
    )
}

pub fn kalamdb_grpc_target() -> (String, u16) {
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

/// gRPC target as seen from *inside* the postgres process.
/// Defaults to the same as kalamdb_grpc_target(), but can be overridden
/// with KALAMDB_PG_GRPC_HOST / KALAMDB_PG_GRPC_PORT when postgres runs
/// inside Docker and kalamdb is reachable via its Docker service name.
fn kalamdb_pg_grpc_target() -> (String, u16) {
    let (default_host, default_port) = kalamdb_grpc_target();
    let host = env::var("KALAMDB_PG_GRPC_HOST")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or(default_host);
    let port = env::var("KALAMDB_PG_GRPC_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default_port);
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

fn pg_user_from_env() -> String {
    env::var("KALAMDB_PG_USER")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| env::var("USER").unwrap_or_else(|_| "postgres".to_string()))
}

fn pg_password_from_env() -> Option<String> {
    env::var("KALAMDB_PG_PASSWORD").ok().filter(|value| !value.is_empty())
}

// ---------------------------------------------------------------------------
// TestEnv — shared, singleton local test environment
// ---------------------------------------------------------------------------

pub struct TestEnv {
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

    pub async fn disconnect_and_wait_for_session_cleanup(mut self) {
        let backend_pid = if let Some(client) = self.client.as_ref() {
            Some(pg_backend_pid(client).await)
        } else {
            None
        };

        self.client.take();
        if let Some(connection_task) = self.connection_task.take() {
            let _ = connection_task.await;
        }

        if let Some(backend_pid) = backend_pid {
            wait_for_remote_pg_session_cleanup(backend_pid, Duration::from_secs(5)).await;
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

static ENV: OnceLock<TestEnv> = OnceLock::new();

impl TestEnv {
    pub async fn global() -> &'static TestEnv {
        if let Some(env) = ENV.get() {
            return env;
        }
        let env = Self::start().await;
        ENV.get_or_init(|| env)
    }

    fn pg_database_from_env() -> String {
        env::var("KALAMDB_PG_DATABASE")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_TEST_DB.to_string())
    }

    pub async fn pg_connect(&self) -> OwnedPgClient {
        let test_db = Self::pg_database_from_env();
        let (pg_host, pg_port) = pg_connection_config();
        self.pg_connect_to(&test_db)
            .await
            .unwrap_or_else(|e| panic!("connect to PostgreSQL at {pg_host}:{pg_port}: {e}"))
    }

    pub async fn kalamdb_sql(&self, sql: &str) -> Value {
        let text = self.kalamdb_sql_text(sql).await;
        serde_json::from_str(&text).unwrap_or(Value::Null)
    }

    pub async fn kalamdb_sql_text(&self, sql: &str) -> String {
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
        assert!(status.is_success(), "KalamDB SQL failed ({status}): {text}\n  SQL: {sql}");
        text
    }

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

    pub async fn wait_for_kalamdb_table_exists(&self, namespace: &str, table: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);

        loop {
            if self.kalamdb_table_exists(namespace, table).await {
                return;
            }

            if std::time::Instant::now() >= deadline {
                panic!("KalamDB table {namespace}.{table} did not become available within timeout");
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

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
        val["results"][0]["schema"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v["name"].as_str().map(String::from)).collect())
            .unwrap_or_default()
    }

    async fn start() -> Self {
        let http_client = TestHttpClient::new(Duration::from_secs(15));

        Self::wait_for_kalamdb(&http_client).await;
        let bearer_token = Self::authenticate(&http_client).await;
        let pg_user = pg_user_from_env();

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

    async fn wait_for_kalamdb(client: &TestHttpClient) {
        let config = kalamdb_auth_config();
        let url = format!("{}/health", config.base_url);
        for i in 0..10 {
            if client
                .get(&url)
                .await
                .map(|response| {
                    response.status.is_success() || matches!(response.status.as_u16(), 401 | 403)
                })
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
            "KalamDB not reachable at {}\nStart with: cd backend && cargo run",
            config.base_url
        );
    }

    async fn authenticate(client: &TestHttpClient) -> String {
        let config = kalamdb_auth_config();

        if let Some(token) =
            try_login(client, &config.base_url, &config.login_username, &config.login_password)
                .await
        {
            return token;
        }

        let _ = client
            .post_json(
                &format!("{}/v1/api/auth/setup", config.base_url),
                &serde_json::json!({
                    "user": config.setup_username,
                    "password": config.setup_password,
                    "root_password": config.root_password,
                }),
                None,
            )
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
            "Failed to authenticate KalamDB test environment ({config}). Set \
             KALAMDB_USER/KALAMDB_PASSWORD or KALAMDB_ROOT_PASSWORD to match the running server."
        );
    }

    async fn ensure_test_db(&self) {
        let test_db = Self::pg_database_from_env();
        let postgres = self.pg_connect_to("postgres").await.expect("connect to postgres database");

        let exists = postgres
            .query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&test_db])
            .await
            .expect("query test database")
            .is_some();
        if !exists {
            postgres
                .batch_execute(&format!("CREATE DATABASE {test_db};"))
                .await
                .expect("create test database");
        }

        postgres.disconnect().await;
    }

    async fn ensure_extension_bootstrap(&self) {
        let pg = self.pg_connect().await;
        let (grpc_host, grpc_port) = kalamdb_pg_grpc_target();
        let auth_config = kalamdb_auth_config();
        let server_options = kalamdb_account_login_server_options(&grpc_host, grpc_port);
        const BOOTSTRAP_LOCK_ID: i64 = 8_271_604_221;

        pg.batch_execute("CREATE EXTENSION IF NOT EXISTS pg_kalam;")
            .await
            .expect("create extension pg_kalam");
        ensure_schema_exists(&pg, "e2e").await;

        pg.execute("SELECT pg_advisory_lock($1)", &[&BOOTSTRAP_LOCK_ID])
            .await
            .expect("acquire pg_kalam bootstrap advisory lock");

        let bootstrap_result = async {
            pg.batch_execute(&format!(
                "CREATE SERVER IF NOT EXISTS kalam_server
                     FOREIGN DATA WRAPPER pg_kalam
                     OPTIONS ({server_options});"
            ))
            .await
            .expect("create kalam_server foreign server");

            pg.batch_execute(&format!(
                "ALTER SERVER kalam_server OPTIONS (SET host '{grpc_host}', SET port \
                 '{grpc_port}');"
            ))
            .await
            .expect("repoint kalam_server foreign server");

            drop_server_option_if_present(&pg, "kalam_server", "auth_header").await;
            ensure_server_option(&pg, "kalam_server", "auth_mode", "account_login").await;
            ensure_server_option(&pg, "kalam_server", "login_user", &auth_config.login_username)
                .await;
            ensure_server_option(
                &pg,
                "kalam_server",
                "login_password",
                &auth_config.login_password,
            )
            .await;
        }
        .await;

        pg.execute("SELECT pg_advisory_unlock($1)", &[&BOOTSTRAP_LOCK_ID])
            .await
            .expect("release pg_kalam bootstrap advisory lock");

        pg.disconnect().await;

        bootstrap_result
    }

    async fn wait_for_pg(&self) {
        let (pg_host, pg_port) = pg_connection_config();
        for i in 0..10 {
            match self.pg_connect_to("postgres").await {
                Ok(client) => {
                    client.disconnect().await;
                    return;
                },
                Err(_) => {
                    if i == 0 {
                        eprintln!("  waiting for PostgreSQL on {pg_host}:{pg_port}...");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                },
            }
        }
        panic!(
            "PostgreSQL not reachable at {pg_host}:{pg_port}\nStart with: \
             ./pg/scripts/pgrx-test-setup.sh --start"
        );
    }

    async fn pg_connect_to(&self, dbname: &str) -> Result<OwnedPgClient, tokio_postgres::Error> {
        let (pg_host, pg_port) = pg_connection_config();
        let mut config = Config::new();
        config.host(&pg_host).port(pg_port).user(&self.pg_user).dbname(dbname);
        if let Some(password) = pg_password_from_env() {
            config.password(password);
        }
        let (client, conn) = config.connect(NoTls).await?;
        let connection_task = tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("pg connection error: {e}");
            }
        });

        client.batch_execute("LOAD 'pg_kalam';").await?;

        Ok(OwnedPgClient::new(client, connection_task))
    }
}

async fn try_login(
    client: &TestHttpClient,
    base_url: &str,
    username: &str,
    password: &str,
) -> Option<String> {
    let resp = client
        .post_json(
            &format!("{base_url}/v1/api/auth/login"),
            &serde_json::json!({
                "user": username,
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

// ---------------------------------------------------------------------------
// Kalam table helpers
// ---------------------------------------------------------------------------

pub async fn create_shared_kalam_table(
    client: &tokio_postgres::Client,
    table: &str,
    columns: &str,
) {
    create_shared_kalam_table_in_schema(client, "e2e", table, columns).await;
    TestEnv::global().await.wait_for_kalamdb_table_exists("e2e", table).await;
    wait_for_table_queryable(client, &format!("e2e.{table}")).await;
}

pub async fn create_user_kalam_table(client: &tokio_postgres::Client, table: &str, columns: &str) {
    create_user_kalam_table_in_schema(client, "e2e", table, columns).await;
    TestEnv::global().await.wait_for_kalamdb_table_exists("e2e", table).await;
}

pub async fn create_shared_kalam_table_in_schema(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    columns: &str,
) {
    create_kalam_table_in_schema(
        client,
        schema,
        table,
        columns,
        "shared",
        "create shared Kalam table",
    )
    .await;
}

pub async fn create_user_kalam_table_in_schema(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    columns: &str,
) {
    create_kalam_table_in_schema(client, schema, table, columns, "user", "create user Kalam table")
        .await;
}

pub async fn drop_kalam_tables(client: &tokio_postgres::Client, schema: &str, tables: &[String]) {
    for table in tables {
        client
            .batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {schema}.{table};"))
            .await
            .ok();
    }
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE;"))
        .await
        .ok();
}

pub async fn set_user_id(client: &tokio_postgres::Client, user_id: &str) {
    let sql = format!("SET kalam.user_id = '{user_id}';");
    client.batch_execute(&sql).await.expect("set user_id");
}

fn sql_row_count(result: &Value) -> i64 {
    result["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|entry| entry["row_count"].as_i64())
        .unwrap_or_default()
}

fn sql_first_cell_i64(result: &Value) -> Option<i64> {
    result["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|entry| entry["rows"].as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|columns| columns.first())
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|raw| i64::try_from(raw).ok()))
                .or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok()))
        })
}

pub async fn wait_for_remote_pg_session_cleanup(backend_pid: u32, timeout: Duration) {
    let env = TestEnv::global().await;
    let deadline = std::time::Instant::now() + timeout;

    loop {
        let result = env
            .kalamdb_sql(&format!(
                "SELECT COUNT(*) AS session_count FROM system.sessions WHERE backend_pid = \
                 {backend_pid} LIMIT 1"
            ))
            .await;
        let count = sql_first_cell_i64(&result).unwrap_or_default();
        if count == 0 {
            return;
        }

        if std::time::Instant::now() >= deadline {
            panic!(
                "remote pg session for backend_pid {backend_pid} remained visible in \
                 system.sessions past timeout"
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn cluster_user_shard_count(env: &TestEnv) -> u32 {
    let result = env
        .kalamdb_sql("SELECT group_id FROM system.cluster_groups WHERE group_type = 'user_data'")
        .await;
    let count = sql_row_count(&result);
    if count <= 0 {
        panic!("system.cluster_groups did not report any user shards");
    }
    count as u32
}

fn user_shard_group_id(user_id: &str, num_user_shards: u32) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    user_id.hash(&mut hasher);
    let shard = (hasher.finish() % num_user_shards as u64) as u32;
    100 + shard as u64
}

pub async fn await_user_shard_leader(user_id: &str) {
    let env = TestEnv::global().await;
    let num_user_shards = cluster_user_shard_count(env).await;
    let group_id = user_shard_group_id(user_id, num_user_shards);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut triggered_elections = 0;

    loop {
        let result = env
            .kalamdb_sql(&format!(
                "SELECT group_id FROM system.cluster_groups WHERE group_id = {group_id} AND \
                 current_leader IS NOT NULL"
            ))
            .await;

        if sql_row_count(&result) == 1 {
            return;
        }

        if std::time::Instant::now() >= deadline {
            panic!("user shard leader not ready for {user_id} ({group_id})");
        }

        if triggered_elections < 3 {
            env.kalamdb_sql("CLUSTER TRIGGER ELECTION").await;
            triggered_elections += 1;
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub async fn same_user_shard_pair(first_user_id: &str, second_prefix: &str) -> (String, String) {
    let env = TestEnv::global().await;
    let num_user_shards = cluster_user_shard_count(env).await;
    let target_group = user_shard_group_id(first_user_id, num_user_shards);

    for index in 0..1024 {
        let candidate = format!("{second_prefix}-{index}");
        if candidate != first_user_id
            && user_shard_group_id(&candidate, num_user_shards) == target_group
        {
            return (first_user_id.to_string(), candidate);
        }
    }

    panic!(
        "failed to find a user id with prefix '{second_prefix}' on the same shard as \
         '{first_user_id}'"
    );
}

pub fn is_transient_user_leader_error(error: &tokio_postgres::Error) -> bool {
    let mut message = if let Some(db_error) = error.as_db_error() {
        db_error.message().to_string()
    } else {
        error.to_string()
    };

    if let Some(db_error) = error.as_db_error() {
        if let Some(detail) = db_error.detail() {
            message.push_str(" | ");
            message.push_str(detail);
        }
        if let Some(hint) = db_error.hint() {
            message.push_str(" | ");
            message.push_str(hint);
        }
    }

    message.contains("Not leader for group data:user")
        || message.contains("leader is node None")
        || message.contains("Leader unknown")
}

pub async fn retry_transient_user_leader_error<T, F, Fut>(description: &str, mut op: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, tokio_postgres::Error>>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    loop {
        match op().await {
            Ok(value) => return value,
            Err(error) if is_transient_user_leader_error(&error) => {
                if std::time::Instant::now() >= deadline {
                    panic!(
                        "{description} did not succeed before user-shard leader was ready: {error}"
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            },
            Err(error) => panic!("{description} failed: {error}"),
        }
    }
}

pub async fn wait_for_table_queryable(client: &tokio_postgres::Client, table: &str) {
    let sql = format!("SELECT 1 FROM {table} LIMIT 0");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);

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

pub async fn timed_query(
    client: &tokio_postgres::Client,
    sql: &str,
) -> (Vec<tokio_postgres::Row>, f64) {
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
    stdout.trim().parse::<u64>().expect("parse rss value")
}

pub fn process_group_rss_kb(pids: &[u32]) -> u64 {
    if pids.is_empty() {
        return 0;
    }

    let pid_list = pids.iter().map(u32::to_string).collect::<Vec<_>>().join(",");
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid_list])
        .output()
        .expect("run ps for process group rss");
    assert!(
        output.status.success(),
        "failed to read rss for pids {pid_list}: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("parse process group rss output");
    stdout.lines().filter_map(|line| line.trim().parse::<u64>().ok()).sum()
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

pub async fn sample_process_group_peak_rss_kb(
    pids: Vec<u32>,
    sample_interval_ms: u64,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> u64 {
    use std::sync::atomic::Ordering;

    let mut peak = process_group_rss_kb(&pids);
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(sample_interval_ms)).await;
        peak = peak.max(process_group_rss_kb(&pids));
    }
    peak.max(process_group_rss_kb(&pids))
}

pub async fn pg_backend_pid(client: &tokio_postgres::Client) -> u32 {
    let row = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("query pg_backend_pid");
    let pid: i32 = row.get(0);
    pid as u32
}

pub async fn ensure_schema_exists(client: &tokio_postgres::Client, schema: &str) {
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

async fn create_kalam_table_in_schema(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    columns: &str,
    table_type: &str,
    description: &str,
) {
    ensure_schema_exists(client, schema).await;
    client
        .batch_execute(&format!("DROP FOREIGN TABLE IF EXISTS {schema}.{table};"))
        .await
        .ok();
    client
        .batch_execute(&format!(
            "CREATE TABLE {schema}.{table} ({columns}) USING kalamdb WITH (type = '{table_type}');"
        ))
        .await
        .expect(description);
}
