#![allow(unused_imports)]

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
};

use anyhow::{Context, Result};
use kalam_client::{
    models::{QueryResponse, ResponseStatus},
    AuthProvider, KalamLinkClient, KalamLinkTimeouts,
};
use kalamdb_commons::{NamespaceId, Role, UserId};
use kalamdb_core::app_context::AppContext;
use once_cell::sync::{Lazy, OnceCell as SyncOnceCell};
use serde_json::Value as JsonValue;
use tokio::{
    sync::{Mutex, OnceCell},
    time::{sleep, Duration, Instant},
};

use super::cluster::ClusterTestServer;

static GLOBAL_HTTP_TEST_RUNTIME: SyncOnceCell<Arc<tokio::runtime::Runtime>> = SyncOnceCell::new();

static HTTP_TEST_SERVER_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
static HTTP_TEST_SUITE_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Global singleton server instance for tests that want to reuse the same server.
///
/// Use `get_global_server()` to get a reference to the shared server instance.
/// The server is started once on first access and reused across tests.
///
/// **Note**: Tests using the global server share state (tables, data), so they
/// should use unique table names or be designed to handle shared state.
static GLOBAL_HTTP_TEST_SERVER: OnceCell<HttpTestServer> = OnceCell::const_new();

/// Global cluster server instance for tests that need a 3-node cluster.
///
/// Use `get_cluster_server()` to get a reference to the shared cluster.
/// The cluster is started once on first access and reused across tests.
static GLOBAL_CLUSTER_SERVER: OnceCell<ClusterTestServer> = OnceCell::const_new();

pub async fn acquire_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    HTTP_TEST_SUITE_LOCK.lock().await
}

fn root_jwt_auth_header(jwt_secret: &str) -> String {
    let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
        &UserId::root(),
        &Role::System,
        None,
        Some(1),
        jwt_secret,
    )
    .expect("Failed to generate root JWT token");

    format!("Bearer {}", token)
}

/// Get a reference to the global shared HTTP test server.
///
/// The server is started once on first access and reused across all tests.
/// This is more efficient than starting a new server for each test.
///
/// **Note**: Tests using the global server share state (tables, data), so they
/// should use unique table names or be designed to handle shared state.
///
/// # Example
/// ```ignore
/// #[tokio::test]
/// async fn test_something() {
///     let server = get_global_server().await;
///     let response = server.execute_sql("SELECT 1").await.unwrap();
///     assert_eq!(response.status.to_string(), "success");
/// }
/// ```
pub async fn get_global_server() -> &'static HttpTestServer {
    GLOBAL_HTTP_TEST_SERVER
        .get_or_init(|| async {
            start_http_test_server_on_global_runtime()
                .await
                .expect("Failed to start global HTTP test server")
        })
        .await
}

async fn start_http_test_server_on_global_runtime() -> Result<HttpTestServer> {
    let runtime = GLOBAL_HTTP_TEST_RUNTIME
        .get_or_init(|| {
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("kalamdb-test-runtime")
                    .build()
                    .expect("Failed to build global test runtime"),
            )
        })
        .clone();

    tokio::task::spawn_blocking(move || runtime.block_on(start_http_test_server()))
        .await
        .expect("Failed to join global test server task")
}

/// Get a reference to the global 3-node cluster for testing.
///
/// The cluster is started once on first access and reused across tests.
/// Access individual nodes via their indices (0, 1, 2).
///
/// # Example
/// ```ignore
/// #[tokio::test]
/// async fn test_cluster() {
///     let cluster = get_cluster_server().await;
///     let node1 = cluster.get_node(0).await;
///     let response = node1.execute_sql("CREATE NAMESPACE test").await.unwrap();
///     assert!(response.success());
/// }
/// ```
pub async fn get_cluster_server() -> &'static ClusterTestServer {
    GLOBAL_CLUSTER_SERVER
        .get_or_init(|| async {
            start_cluster_server().await.expect("Failed to start global cluster server")
        })
        .await
}

/// A near-production HTTP server instance for tests.
///
/// Uses the real `kalamdb_server::lifecycle::bootstrap()` and `run_for_tests()` wiring.
pub struct HttpTestServer {
    // Cross-process lock to avoid running multiple near-production servers concurrently.
    // Some subsystems (notably Raft/bootstrap) are not safe to initialize concurrently
    // across integration test binaries.
    _global_lock: Option<std::fs::File>,
    pub base_url: String,
    #[allow(dead_code)]
    data_path: PathBuf,
    root_auth_header: String,
    jwt_secret: String,
    link_client_cache: Mutex<HashMap<String, KalamLinkClient>>,
    /// Cache of username -> user_id mappings for proper JWT token generation
    /// Uses std::sync::Mutex since it's accessed from sync code (link_client)
    user_id_cache: std::sync::Mutex<HashMap<String, String>>,
    /// Cache of username -> password for basic auth test clients
    user_password_cache: std::sync::Mutex<HashMap<String, String>>,
    /// Cache of username -> bearer token to avoid regenerating per request
    user_token_cache: std::sync::Mutex<HashMap<String, String>>,
    running: Option<kalamdb_server::lifecycle::RunningTestHttpServer>,
    skip_raft_leader_check: bool,
    // Keep temp dir last so it is dropped after server resources.
    _temp_dir: Option<tempfile::TempDir>,
}

fn acquire_global_http_test_server_lock() -> Result<Option<std::fs::File>> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;

        let lock_path = std::env::temp_dir().join("kalamdb-http-test-server.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("Failed to open global lock file: {}", lock_path.display()))?;

        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            return Err(anyhow::anyhow!(
                "Failed to acquire global lock (flock) on {}",
                lock_path.display()
            ));
        }

        Ok(Some(file))
    }

    #[cfg(not(unix))]
    {
        Ok(None)
    }
}

impl HttpTestServer {
    fn auth_provider_from_header(auth_header: &str) -> Result<AuthProvider> {
        let auth_header = auth_header.trim();

        if auth_header.is_empty() {
            return Ok(AuthProvider::none());
        }

        if let Some(token) = auth_header.strip_prefix("Bearer ") {
            return Ok(AuthProvider::jwt_token(token.to_string()));
        }

        Err(anyhow::anyhow!("Unsupported Authorization header format: {}", auth_header))
    }

    /// Build a Bearer auth header value for a user.
    ///
    /// Example: `Authorization: Bearer <jwt>`
    pub fn bearer_auth_header(&self, username: &str) -> Result<String> {
        if let Some(token) = self.get_cached_user_token(username) {
            return Ok(format!("Bearer {}", token));
        }

        let users = self.app_context().system_tables().users();
        let user_id = UserId::new(username);
        let user = users
            .get_user_by_id(&user_id)
            .context("Failed to load user for bearer auth header")?
            .ok_or_else(|| anyhow::anyhow!("User '{}' not found", username))?;

        let token = self.create_jwt_token_with_id(&user.user_id, &user.role);
        self.cache_user_token(username, &token);
        Ok(format!("Bearer {}", token))
    }

    /// Returns the server's isolated data directory for this test instance.
    #[allow(dead_code)]
    pub fn data_path(&self) -> &Path {
        &self.data_path
    }

    /// Access the underlying AppContext for tests that need direct access.
    pub fn app_context(&self) -> Arc<AppContext> {
        self.running
            .as_ref()
            .expect("HTTP test server has been shut down")
            .app_context
            .clone()
    }

    /// Returns the storage root directory (e.g. `<data_path>/storage`).
    #[allow(dead_code)]
    pub fn storage_root(&self) -> PathBuf {
        self.data_path.join("storage")
    }

    /// Returns the WebSocket URL for this server (ws://localhost:port/v1/ws)
    pub fn websocket_url(&self) -> String {
        self.base_url.replace("http://", "ws://") + "/v1/ws"
    }

    /// Returns the base URL without trailing slash
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Creates a JWT token for the specified user with explicit user_id.
    pub fn create_jwt_token_with_id(&self, user_id: &UserId, role: &Role) -> String {
        let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
            user_id,
            role,
            None,
            Some(1), // 1 hour expiry
            &self.jwt_secret,
        )
        .expect("Failed to generate JWT token");

        token
    }

    /// Creates a JWT token for the specified user.
    /// For test purposes, assumes role is 'system' for root, and 'user' for others.
    pub fn create_jwt_token(&self, username: &str) -> String {
        let role = if username == "root" {
            Role::System
        } else {
            Role::User
        };
        // Use the real built-in root user id, and otherwise mirror the test user id.
        let user_id = if username == "root" {
            UserId::root()
        } else {
            UserId::new(username)
        };

        let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
            &user_id,
            &role,
            None,
            Some(1), // 1 hour expiry
            &self.jwt_secret,
        )
        .expect("Failed to generate JWT token");

        token
    }

    /// Register a user_id in the cache for later use by link_client
    pub fn cache_user_id(&self, username: &str, user_id: &str) {
        let mut cache = self.user_id_cache.lock().expect("user_id_cache mutex poisoned");
        cache.insert(username.to_string(), user_id.to_string());
    }

    /// Register a user password in the cache for later basic-auth requests
    pub fn cache_user_password(&self, username: &str, password: &str) {
        let mut cache =
            self.user_password_cache.lock().expect("user_password_cache mutex poisoned");
        cache.insert(username.to_string(), password.to_string());
    }

    /// Register a bearer token in the cache for reuse.
    pub fn cache_user_token(&self, username: &str, token: &str) {
        let mut cache = self.user_token_cache.lock().expect("user_token_cache mutex poisoned");
        cache.insert(username.to_string(), token.to_string());
    }

    /// Get cached user_id for a username, if available
    pub fn get_cached_user_id(&self, username: &str) -> Option<String> {
        let cache = self.user_id_cache.lock().expect("user_id_cache mutex poisoned");
        cache.get(username).cloned()
    }

    /// Get cached password for a username, if available
    pub fn get_cached_user_password(&self, username: &str) -> Option<String> {
        let cache = self.user_password_cache.lock().expect("user_password_cache mutex poisoned");
        cache.get(username).cloned()
    }

    /// Get cached bearer token for a username, if available.
    pub fn get_cached_user_token(&self, username: &str) -> Option<String> {
        let cache = self.user_token_cache.lock().expect("user_token_cache mutex poisoned");
        cache.get(username).cloned()
    }

    /// Create a new user if it doesnt exists already and cache the user_id
    pub async fn ensure_user_exists(
        &self,
        username: &str,
        password: &str,
        role: &Role,
    ) -> Result<String> {
        let check_sql = format!(
            "SELECT user_id, COUNT(*) AS user_count FROM system.users WHERE user_id = '{}' GROUP \
             BY user_id",
            username
        );
        let resp = self.execute_sql(&check_sql).await?;

        // Check if user exists and get their user_id
        if let Some(rows) = resp.rows_as_maps().first() {
            if let Some(user_id) = rows.get("user_id").and_then(|v| v.as_str()) {
                self.cache_user_id(username, user_id);
                self.cache_user_password(username, password);
                return Ok(user_id.to_string());
            }
        }

        // User doesn't exist, create them
        let create_sql = format!(
            "CREATE USER {} WITH PASSWORD '{}' ROLE '{}'",
            username,
            password,
            role.as_str()
        );
        self.execute_sql(&create_sql).await?;

        // Now fetch the user_id
        let get_id_sql = format!("SELECT user_id FROM system.users WHERE user_id = '{}'", username);
        let resp = self.execute_sql(&get_id_sql).await?;
        let user_id = resp
            .rows_as_maps()
            .first()
            .and_then(|r| r.get("user_id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("Failed to get user_id for newly created user {}", username)
            })?
            .to_string();

        self.cache_user_id(username, &user_id);
        self.cache_user_password(username, password);
        Ok(user_id)
    }

    /// Returns a pre-configured KalamLinkClient authenticated as specified user using JWT.
    pub fn link_client_with_id(
        &self,
        _user_id: &str,
        username: &str,
        _role: &Role,
    ) -> KalamLinkClient {
        if username == "root" {
            let token = self.create_jwt_token("root");
            return KalamLinkClient::builder()
                .base_url(self.base_url())
                .auth(AuthProvider::jwt_token(token))
                .timeouts(
                    KalamLinkTimeouts::builder()
                        .connection_timeout_secs(5)
                        .receive_timeout_secs(120)
                        .send_timeout_secs(30)
                        .subscribe_timeout_secs(15)
                        .auth_timeout_secs(10)
                        .initial_data_timeout(Duration::from_secs(120))
                        .build(),
                )
                .build()
                .expect("Failed to build KalamLinkClient");
        }

        // Use JWT for all non-root users
        let users = self.app_context().system_tables().users();
        let uid = UserId::new(username);
        let user = users
            .get_user_by_id(&uid)
            .expect("Failed to load user by id")
            .unwrap_or_else(|| panic!("User '{}' not found for link_client_with_id", username));
        let token = self.create_jwt_token_with_id(&user.user_id, &user.role);

        KalamLinkClient::builder()
            .base_url(self.base_url())
            .auth(AuthProvider::jwt_token(token))
            .timeouts(
                KalamLinkTimeouts::builder()
                    .connection_timeout_secs(5)
                    .receive_timeout_secs(120)
                    .send_timeout_secs(30)
                    .subscribe_timeout_secs(15)
                    .auth_timeout_secs(10)
                    .initial_data_timeout(Duration::from_secs(120))
                    .build(),
            )
            .build()
            .expect("Failed to build KalamLinkClient")
    }

    /// Returns a pre-configured KalamLinkClient authenticated as specified user using JWT.
    ///
    /// **Note**: For USER tables with RLS to work correctly, the user must have been created
    /// via `ensure_user_exists` (or `create_user_and_client` in helpers.rs) first, so that
    /// their real user_id is cached. If the user_id is not cached, this method falls back
    /// to using the username as user_id (which won't work for USER table RLS).
    pub fn link_client(&self, username: &str) -> KalamLinkClient {
        let role = if username == "root" {
            Role::System
        } else {
            Role::User
        };

        // Try to get cached user_id, fall back to username
        let user_id = self.get_cached_user_id(username).unwrap_or_else(|| {
            if username != "root" {
                eprintln!(
                    "WARNING: link_client('{}') called without cached user_id. USER table RLS may \
                     not work correctly.",
                    username
                );
            }
            username.to_string()
        });

        self.link_client_with_id(&user_id, username, &role)
    }

    /// Execute SQL via the real HTTP API as the localhost `root` user.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResponse> {
        self.execute_sql_with_auth(sql, &self.root_auth_header).await
    }

    /// Execute a parameterized SQL query via the real HTTP API as the localhost `root` user.
    pub async fn execute_sql_with_params(
        &self,
        sql: &str,
        params: Vec<JsonValue>,
    ) -> Result<QueryResponse> {
        self.execute_sql_with_auth_and_params(sql, &self.root_auth_header, params).await
    }

    /// Execute SQL via the real HTTP API using an explicit `Authorization` header.
    pub async fn execute_sql_with_auth(
        &self,
        sql: &str,
        auth_header: &str,
    ) -> Result<QueryResponse> {
        self.execute_sql_with_auth_and_params(sql, auth_header, Vec::new()).await
    }

    /// Execute SQL (optionally parameterized) via the real HTTP API using an explicit
    /// `Authorization` header.
    pub async fn execute_sql_with_auth_and_params(
        &self,
        sql: &str,
        auth_header: &str,
        params: Vec<JsonValue>,
    ) -> Result<QueryResponse> {
        let mut attempt = 0;
        let resp = loop {
            match self
                .execute_sql_raw_with_auth_and_params_no_wait(sql, auth_header, params.clone())
                .await
            {
                Ok(resp) => break resp,
                Err(err) => {
                    let err_str = err.to_string();
                    let is_transient = err_str.contains("KalamLink execute_query failed")
                        || err_str.to_lowercase().contains("connection")
                        || err_str.to_lowercase().contains("timeout")
                        || err_str.to_lowercase().contains("timed out");

                    attempt += 1;
                    if is_transient && attempt < 6 {
                        let backoff = 50u64.saturating_mul(attempt as u64);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                        continue;
                    }

                    return Err(err);
                },
            }
        };

        let mut resp = resp;

        if attempt > 0 && resp.status == ResponseStatus::Error {
            if let Some(error) = resp.error.as_ref() {
                let msg = error.message.to_lowercase();
                let sql_lower = sql.trim_start().to_lowercase();
                let is_strict =
                    sql_lower.contains("/* strict */") || sql_lower.contains("/*strict*/");
                let is_pk_violation =
                    msg.contains("primary key violation") || msg.contains("duplicate");
                let is_alter_missing =
                    sql_lower.starts_with("alter table") && msg.contains("does not exist");
                let is_idempotent_create = msg.contains("already exists") && !is_pk_violation;

                if !is_strict {
                    let is_idempotent_insert = is_pk_violation && sql_lower.starts_with("insert");
                    if is_idempotent_create || is_idempotent_insert || is_alter_missing {
                        resp.status = ResponseStatus::Success;
                        resp.error = None;
                    }
                }
            }
        }

        // Tests frequently issue DDL immediately followed by DML. In near-production mode
        // (Raft + multi-worker HTTP server), table registration can lag very slightly.
        // Add a short, targeted wait to prevent flaky "Table does not exist" failures.
        if resp.status == ResponseStatus::Success {
            if let Some((namespace_id, table_name)) = Self::try_parse_create_table_target(sql) {
                self.wait_for_table_queryable(&namespace_id, &table_name, auth_header).await?;
            }
        }

        Ok(resp)
    }

    async fn execute_sql_raw_with_auth_and_params_no_wait(
        &self,
        sql: &str,
        auth_header: &str,
        params: Vec<JsonValue>,
    ) -> Result<QueryResponse> {
        // Important: reuse HTTP connections across calls.
        // Actix selects a worker per TCP connection; creating a fresh client per
        // query can hit different workers and expose worker-local state.
        let client = {
            let mut cache = self.link_client_cache.lock().await;
            if let Some(existing) = cache.get(auth_header) {
                existing.clone()
            } else {
                let auth = Self::auth_provider_from_header(auth_header)?;
                let built = KalamLinkClient::builder()
                    .base_url(self.base_url())
                    .auth(auth)
                    .timeouts(
                        KalamLinkTimeouts::builder()
                            .connection_timeout_secs(5)
                            .receive_timeout_secs(120)
                            .send_timeout_secs(60)
                            .subscribe_timeout_secs(15)
                            .auth_timeout_secs(10)
                            .initial_data_timeout(Duration::from_secs(120))
                            .build(),
                    )
                    .build()
                    .context("Failed to build KalamLinkClient")?;
                cache.insert(auth_header.to_string(), built.clone());
                built
            }
        };

        let resp = match client
            .execute_query(
                sql,
                None,
                if params.is_empty() {
                    None
                } else {
                    Some(params)
                },
                None,
            )
            .await
        {
            Ok(r) => r,
            Err(kalam_client::KalamLinkError::ServerError {
                status_code: _,
                message,
            }) => {
                // Try to parse the error message as QueryResponse JSON
                if let Ok(error_response) = serde_json::from_str::<QueryResponse>(&message) {
                    error_response
                } else {
                    // If parsing fails, return the error
                    return Err(anyhow::anyhow!("Server error: {}", message));
                }
            },
            Err(e) => return Err(e).context("KalamLink execute_query failed"),
        };

        if resp.status == ResponseStatus::Error {
            eprintln!("HTTP SQL Error: sql={:?} error={:?}", sql, resp.error);
        }

        Ok(resp)
    }
    fn try_parse_create_table_target(sql: &str) -> Option<(NamespaceId, String)> {
        // Best-effort parse for statements like:
        //   CREATE TABLE ns.table ( ... ) WITH (...)
        // We keep this intentionally simple for tests; if parsing fails we just skip the wait.
        let upper = sql.trim_start().to_ascii_uppercase();
        if !upper.starts_with("CREATE TABLE") {
            return None;
        }

        let after = sql.trim_start().get("CREATE TABLE".len()..)?.trim_start();

        let ident = after.split_whitespace().next()?.trim_end_matches('(').trim();

        let mut parts = ident.splitn(2, '.');
        let namespace_id_str = parts.next()?.trim().trim_matches('"').to_string();
        let table_name = parts.next()?.trim().trim_matches('"').to_string();

        if namespace_id_str.is_empty() || table_name.is_empty() {
            return None;
        }

        Some((NamespaceId::new(namespace_id_str), table_name))
    }

    #[allow(unused_assignments)]
    async fn wait_for_table_queryable(
        &self,
        namespace_id: &NamespaceId,
        table_name: &str,
        auth_header: &str,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let probe = format!("SELECT 1 AS ok FROM {}.{} LIMIT 1", namespace_id.as_str(), table_name);
        let mut last_error: Option<String> = None;
        let system_probe = format!(
            "SELECT COUNT(*) AS cnt FROM system.schemas WHERE namespace_id='{}' AND \
             table_name='{}'",
            namespace_id.as_str(),
            table_name
        );
        let mut last_system_cnt: Option<u64> = None;

        loop {
            match self
                .execute_sql_raw_with_auth_and_params_no_wait(&probe, auth_header, Vec::new())
                .await
            {
                Ok(resp) if resp.status == ResponseStatus::Success => return Ok(()),
                Ok(resp) => {
                    last_error = resp.error.as_ref().map(|e| e.message.clone());

                    let is_missing = resp
                        .error
                        .as_ref()
                        .map(|e| {
                            let m = e.message.to_lowercase();
                            m.contains("does not exist") || m.contains("not found")
                        })
                        .unwrap_or(false);

                    if !is_missing {
                        return Err(anyhow::anyhow!(
                            "CREATE TABLE probe failed with non-missing error ({}.{}): {:?}",
                            namespace_id.as_str(),
                            table_name,
                            resp.error
                        ));
                    }

                    // Check if the table definition is visible in system.schemas yet.
                    if let Ok(sys_resp) = self
                        .execute_sql_raw_with_auth_and_params_no_wait(
                            &system_probe,
                            &self.root_auth_header,
                            Vec::new(),
                        )
                        .await
                    {
                        if sys_resp.status == ResponseStatus::Success {
                            last_system_cnt = sys_resp
                                .results
                                .first()
                                .and_then(|r| r.row_as_map(0))
                                .and_then(|row| row.get("cnt").cloned())
                                .and_then(|v| {
                                    v.as_u64()
                                        .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                                });
                        }
                    }
                },
                Err(e) => {
                    last_error = Some(format!("{:#}", e));
                },
            }

            if Instant::now() >= deadline {
                return Err(anyhow::anyhow!(
                    "CREATE TABLE did not become queryable in time ({}.{}): last_error={:?} \
                     system.schemas_cnt={:?}",
                    namespace_id.as_str(),
                    table_name,
                    last_error,
                    last_system_cnt
                ));
            }

            sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn shutdown(mut self) {
        // Actix `stop(true)` is graceful: it waits for existing keep-alive connections.
        if let Some((running, temp_dir)) = self.take_shutdown_parts() {
            running.shutdown().await;
            drop(temp_dir);
        }
    }

    fn take_shutdown_parts(
        &mut self,
    ) -> Option<(kalamdb_server::lifecycle::RunningTestHttpServer, tempfile::TempDir)> {
        let running = self.running.take()?;
        let temp_dir = self._temp_dir.take()?;
        Some((running, temp_dir))
    }
    #[allow(unused_assignments)]
    async fn wait_until_ready(&self) -> Result<()> {
        // The server may bind before subsystems (notably Raft/bootstrap) are fully ready.
        // For tests, "ready" means:
        // - SQL HTTP API is accepting requests
        // - Raft single-node cluster has completed initialization and elected a leader
        //
        // Avoid DDL-based probes here.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last_error: Option<String> = None;

        loop {
            if self.skip_raft_leader_check {
                let select_probe = self.execute_sql("SELECT 1 AS ok").await;
                if matches!(
                    select_probe,
                    Ok(ref r) if r.status == kalam_client::models::ResponseStatus::Success
                ) {
                    return Ok(());
                }
                last_error =
                    select_probe.err().map(|e| format!("{:#}", e)).or_else(|| last_error.take());
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "HTTP test server did not become ready in time (last_error={:?})",
                        last_error
                    ));
                }

                sleep(Duration::from_millis(50)).await;
                continue;
            }

            // Prefer a probe that confirms Raft leadership is established.
            // system.cluster is registered during AppContext init.
            let cluster_probe = "SELECT is_self, is_leader FROM system.cluster";
            match self.execute_sql(cluster_probe).await {
                Ok(resp) if resp.status == kalam_client::models::ResponseStatus::Success => {
                    let is_ready = resp
                        .results
                        .first()
                        .map(|r| {
                            r.rows_as_maps().iter().any(|row| {
                                let is_self = row
                                    .get("is_self")
                                    .and_then(|v| {
                                        v.as_bool().or_else(|| v.as_str().map(|s| s == "true"))
                                    })
                                    .unwrap_or(false);
                                let is_leader = row
                                    .get("is_leader")
                                    .and_then(|v| {
                                        v.as_bool().or_else(|| v.as_str().map(|s| s == "true"))
                                    })
                                    .unwrap_or(false);
                                is_self && is_leader
                            })
                        })
                        .unwrap_or(false);

                    if is_ready {
                        return Ok(());
                    }

                    last_error = Some("cluster not ready (no self leader row yet)".to_string());
                },
                Ok(resp) => {
                    // Fall back to a trivial read-only probe if system.cluster isn't available yet.
                    last_error = resp.error.as_ref().map(|e| e.message.clone());
                    let select_probe = self.execute_sql("SELECT 1 AS ok").await;
                    if matches!(
                        select_probe,
                        Ok(ref r) if r.status == kalam_client::models::ResponseStatus::Success
                    ) {
                        // SQL is up, but cluster leadership isn't confirmed yet.
                    }
                },
                Err(e) => {
                    last_error = Some(format!("{:#}", e));
                },
            }

            if Instant::now() >= deadline {
                return Err(anyhow::anyhow!(
                    "HTTP test server did not become ready in time (last_error={:?})",
                    last_error
                ));
            }

            sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for HttpTestServer {
    fn drop(&mut self) {
        let Some((running, temp_dir)) = self.take_shutdown_parts() else {
            return;
        };

        let (tx, rx) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();

            if let Ok(runtime) = runtime {
                runtime.block_on(async {
                    running.shutdown().await;
                    drop(temp_dir);
                });
            }

            let _ = tx.send(());
        });

        let _ = rx.recv();
    }
}

fn reserve_local_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

async fn wait_for_cluster_ready(nodes: &[HttpTestServer]) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let mut meta_leader_count = 0usize;
        let mut shared_leader_count = 0usize;
        let mut known_meta_leader = None;
        let mut known_shared_leader = None;
        let mut ready = true;

        for node in nodes {
            let executor = node.app_context().executor();
            let meta_leader = executor.get_leader(kalamdb_raft::GroupId::Meta).await;
            let shared_leader =
                executor.get_leader(kalamdb_raft::GroupId::DataSharedShard(0)).await;

            if meta_leader.is_none() || shared_leader.is_none() {
                ready = false;
                break;
            }

            if executor.is_leader(kalamdb_raft::GroupId::Meta).await {
                meta_leader_count += 1;
            }

            if executor.is_leader(kalamdb_raft::GroupId::DataSharedShard(0)).await {
                shared_leader_count += 1;
            }

            if let Some(current_meta_leader) = known_meta_leader {
                if Some(current_meta_leader) != meta_leader {
                    ready = false;
                    break;
                }
            } else {
                known_meta_leader = meta_leader;
            }

            if let Some(current_shared_leader) = known_shared_leader {
                if Some(current_shared_leader) != shared_leader {
                    ready = false;
                    break;
                }
            } else {
                known_shared_leader = shared_leader;
            }
        }

        if ready && meta_leader_count == 1 && shared_leader_count == 1 {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "Timed out waiting for 3-node cluster to converge (meta_leaders={}, \
                 shared_leaders={})",
                meta_leader_count,
                shared_leader_count
            ));
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn start_http_test_server_with_setup(
    override_config: impl FnOnce(&mut kalamdb_configs::ServerConfig),
    initialize_cluster: bool,
    skip_raft_leader_check: bool,
) -> Result<HttpTestServer> {
    let _global_lock = acquire_global_http_test_server_lock()?;
    let temp_dir = tempfile::TempDir::new()?;
    let data_path = temp_dir.path().to_path_buf();

    let mut config = kalamdb_configs::ServerConfig::default();
    config.server.host = "127.0.0.1".to_string();
    config.server.port = 0;
    config.server.ui_path = None;
    config.storage.data_path = data_path.to_string_lossy().into_owned();
    config.rate_limit.max_queries_per_sec = 100000;
    config.rate_limit.max_messages_per_sec = 10000;
    override_config(&mut config);

    kalamdb_auth::services::unified::init_auth_config(&config.auth, &config.oauth);

    let (components, app_context) = if initialize_cluster {
        kalamdb_server::lifecycle::bootstrap_isolated(&config).await?
    } else {
        kalamdb_server::lifecycle::bootstrap_isolated_without_cluster_init(&config).await?
    };

    let running = if config.server.port == 0 {
        kalamdb_server::lifecycle::run_for_tests(&config, components, app_context).await?
    } else {
        kalamdb_server::lifecycle::run_detached(&config, components, app_context).await?
    };

    let base_url = running.base_url.clone();
    let jwt_secret = config.auth.jwt_secret.clone();

    let server = HttpTestServer {
        _temp_dir: Some(temp_dir),
        _global_lock: None,
        base_url,
        data_path,
        root_auth_header: root_jwt_auth_header(&jwt_secret),
        jwt_secret,
        link_client_cache: Mutex::new(HashMap::new()),
        user_id_cache: std::sync::Mutex::new(HashMap::new()),
        user_password_cache: std::sync::Mutex::new(HashMap::new()),
        user_token_cache: std::sync::Mutex::new(HashMap::new()),
        running: Some(running),
        skip_raft_leader_check,
    };

    server.wait_until_ready().await?;
    Ok(server)
}

/// Start a near-production HTTP server on a random available port.
///
/// This is intended for integration tests that want to use `reqwest`/WebSocket
/// clients against a real server instance.
#[allow(dead_code)]
pub async fn start_http_test_server() -> Result<HttpTestServer> {
    start_http_test_server_with_setup(|_| {}, true, false).await
}

/// Start a near-production HTTP server with a config override.
#[allow(dead_code)]
pub async fn start_http_test_server_with_config(
    override_config: impl FnOnce(&mut kalamdb_configs::ServerConfig),
) -> Result<HttpTestServer> {
    start_http_test_server_with_setup(override_config, true, false).await
}

/// Start a 3-node cluster for testing
async fn start_cluster_server() -> Result<ClusterTestServer> {
    let _guard = HTTP_TEST_SERVER_LOCK.lock().await;

    eprintln!("🚀 Starting 3-node cluster for testing...");

    let cluster_id = format!("http-test-cluster-{}", std::process::id());
    let node_specs = (1u64..=3)
        .map(|node_id| Ok((node_id, reserve_local_port()?, reserve_local_port()?)))
        .collect::<Result<Vec<_>>>()?;

    let mut nodes = Vec::new();

    for (index, (node_id, rpc_port, api_port)) in node_specs.iter().copied().enumerate() {
        eprintln!("  📍 Starting node {} of 3...", index + 1);

        let peers = node_specs
            .iter()
            .filter(|(peer_node_id, _, _)| *peer_node_id != node_id)
            .map(|(peer_node_id, peer_rpc_port, peer_api_port)| kalamdb_configs::PeerConfig {
                node_id: *peer_node_id,
                rpc_addr: format!("127.0.0.1:{}", peer_rpc_port),
                api_addr: format!("http://127.0.0.1:{}", peer_api_port),
                rpc_server_name: None,
            })
            .collect::<Vec<_>>();

        let cluster_id = cluster_id.clone();
        let initialize_cluster = node_id == 1;
        let server = start_http_test_server_with_setup(
            move |config| {
                config.server.port = api_port;
                config.cluster = Some(kalamdb_configs::ClusterConfig {
                    cluster_id: cluster_id.clone(),
                    node_id,
                    rpc_addr: format!("127.0.0.1:{}", rpc_port),
                    api_addr: format!("http://127.0.0.1:{}", api_port),
                    peers,
                    user_shards: 1,
                    shared_shards: 1,
                    heartbeat_interval_ms: 50,
                    election_timeout_ms: (150, 300),
                    snapshot_policy: "LogsSinceLast(1000)".to_string(),
                    max_snapshots_to_keep: 3,
                    replication_timeout_ms: 5_000,
                    reconnect_interval_ms: 250,
                    peer_wait_max_retries: Some(80),
                    peer_wait_initial_delay_ms: Some(100),
                    peer_wait_max_delay_ms: Some(500),
                });
            },
            initialize_cluster,
            !initialize_cluster,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start cluster node {}: {}", index, e))?;
        nodes.push(server);
    }

    wait_for_cluster_ready(&nodes).await?;

    eprintln!("✅ 3-node cluster started successfully");

    Ok(ClusterTestServer::new(nodes))
}
// ============================================================================
// LEGACY API (DEPRECATED) - Use get_global_server() instead
// ============================================================================
// The following functions are kept only for test_user_sql_commands_http.rs
// which requires a config override (enforce_password_complexity = true).
// All other tests should use get_global_server() for better performance.
// ============================================================================

/// Run a test closure against a freshly started HTTP test server (with config override), then shut
/// it down.
///
/// **DEPRECATED**: Only use this if you need a config override. Otherwise use
/// `get_global_server()`.
#[allow(dead_code)]
pub async fn with_http_test_server_config<T, F>(
    override_config: impl FnOnce(&mut kalamdb_configs::ServerConfig),
    f: F,
) -> Result<T>
where
    F: for<'a> FnOnce(
        &'a HttpTestServer,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + 'a>>,
{
    let _guard = HTTP_TEST_SERVER_LOCK.lock().await;

    let server = start_http_test_server_with_config(override_config).await?;
    let result = f(&server).await;
    server.shutdown().await;
    result
}
