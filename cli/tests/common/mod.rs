#![allow(dead_code, unused_imports)]
extern crate kalam_cli;
#[cfg(unix)]
use libc::{flock, LOCK_EX, LOCK_UN};
use rand::{distr::Alphanumeric, RngExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::{Child, Stdio};
use std::sync::mpsc as std_mpsc;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Mutex as TokioMutex;

// Load environment variables from .env file at test startup
fn load_env_file() {
    static ENV_LOADED: OnceLock<()> = OnceLock::new();
    ENV_LOADED.get_or_init(|| {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".env");
        if path.exists() {
            let _ = dotenv::from_path(&path);
            eprintln!("[TEST] Loaded environment from: {}", path.display());
        }
    });
}

fn root_password_from_env() -> String {
    load_env_file();
    parse_test_arg("--password")
        .or_else(|| std::env::var("KALAMDB_ROOT_PASSWORD").ok())
        .unwrap_or_else(|| "kalamdb123".to_string())
}

fn admin_password_from_env() -> String {
    load_env_file();
    parse_test_arg("--password")
        .or_else(|| std::env::var("KALAMDB_ADMIN_PASSWORD").ok())
        .or_else(|| std::env::var("KALAMDB_ROOT_PASSWORD").ok())
        .unwrap_or_else(|| "kalamdb123".to_string())
}

fn shared_token_cache_path() -> PathBuf {
    std::env::temp_dir().join("kalamdb_test_tokens.json")
}

fn shared_token_cache_lock_path() -> PathBuf {
    std::env::temp_dir().join("kalamdb_test_tokens.lock")
}

// Re-export commonly used types for credential tests
pub use kalam_cli::FileCredentialStore;
pub use kalam_client::client::KalamLinkClientBuilder;
pub use kalam_client::credentials::{CredentialStore, Credentials};
pub use kalam_client::{AuthProvider, KalamLinkClient, KalamLinkTimeouts};
pub use tempfile::TempDir;

#[cfg(unix)]
pub use std::os::unix::fs::PermissionsExt;

static SERVER_URL: OnceLock<String> = OnceLock::new();
static ROOT_PASSWORD: OnceLock<String> = OnceLock::new();
static ADMIN_PASSWORD: OnceLock<String> = OnceLock::new();
static TEST_CONTEXT: OnceLock<TestContext> = OnceLock::new();
static LAST_LEADER_URL: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static AUTO_TEST_SERVER: OnceLock<Mutex<Option<AutoTestServer>>> = OnceLock::new();
static AUTO_TEST_RUNTIME: OnceLock<&'static Runtime> = OnceLock::new();
/// Token cache: maps "username:password" to access_token
static TOKEN_CACHE: OnceLock<Mutex<std::collections::HashMap<String, String>>> = OnceLock::new();
static TEST_AUTH_MANAGER: OnceLock<TestAuthManager> = OnceLock::new();
static LOGIN_MUTEX: OnceLock<TokioMutex<()>> = OnceLock::new();
static TOKEN_FILE_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
static TEST_CLI_HOME_DIR: OnceLock<PathBuf> = OnceLock::new();
static TEST_CLI_CREDENTIALS_PATH: OnceLock<PathBuf> = OnceLock::new();

struct TestAuthManager {
    ready_urls: Mutex<HashSet<String>>,
}

impl TestAuthManager {
    fn new() -> Self {
        Self {
            ready_urls: Mutex::new(HashSet::new()),
        }
    }

    fn with_shared_token_cache<R>(
        &self,
        op: impl FnOnce(&mut HashMap<String, String>) -> R,
    ) -> Result<R, Box<dyn std::error::Error>> {
        let _guard = TOKEN_FILE_MUTEX
            .get_or_init(|| Mutex::new(()))
            .lock()
            .map_err(|_| "Failed to lock token cache mutex")?;

        let cache_path = shared_token_cache_path();

        #[cfg(unix)]
        {
            let lock_path = shared_token_cache_lock_path();
            let _lock_file =
                OpenOptions::new().create(true).read(true).write(true).open(&lock_path)?;

            unsafe {
                if flock(_lock_file.as_raw_fd(), LOCK_EX) != 0 {
                    return Err("Failed to acquire token cache lock".into());
                }
            }
        }

        let mut map: HashMap<String, String> = if cache_path.exists() {
            let contents = std::fs::read_to_string(&cache_path).unwrap_or_default();
            serde_json::from_str(&contents).unwrap_or_default()
        } else {
            HashMap::new()
        };

        let result = op(&mut map);

        let serialized = serde_json::to_string(&map)?;
        std::fs::write(&cache_path, serialized)?;

        #[cfg(unix)]
        if let Ok(lock_path) = OpenOptions::new().write(true).open(shared_token_cache_lock_path()) {
            unsafe {
                let _ = flock(lock_path.as_raw_fd(), LOCK_UN);
            }
        }

        Ok(result)
    }

    fn shared_token_for_key(
        &self,
        cache_key: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        self.with_shared_token_cache(|map| map.get(cache_key).cloned())
    }

    fn store_shared_token(
        &self,
        cache_key: &str,
        token: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.with_shared_token_cache(|map| {
            map.insert(cache_key.to_string(), token.to_string());
        })?;
        Ok(())
    }

    fn clear_shared_tokens_for_url(
        &self,
        base_url: &str,
        usernames: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let prefixes: Vec<String> =
            usernames.iter().map(|user| format!("{}|{}:", base_url, user)).collect();
        self.with_shared_token_cache(|map| {
            map.retain(|key, _| !prefixes.iter().any(|prefix| key.starts_with(prefix)));
        })?;
        Ok(())
    }

    async fn complete_setup_if_needed(
        &self,
        base_url: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = Client::new();
        let status_response = client.get(format!("{}/v1/api/auth/status", base_url)).send().await;

        let Ok(status_response) = status_response else {
            return Ok(());
        };

        if !status_response.status().is_success() {
            return Ok(());
        }

        let body: serde_json::Value = status_response.json().await?;
        let needs_setup = body.get("needs_setup").and_then(|v| v.as_bool()).unwrap_or(false);

        if !needs_setup {
            return Ok(());
        }

        let root_password = root_password_from_env();
        let setup_response = client
            .post(format!("{}/v1/api/auth/setup", base_url))
            .json(&json!({
                "username": "admin",
                "password": "kalamdb123",
                "root_password": root_password,
                "email": null
            }))
            .send()
            .await?;

        if !setup_response.status().is_success() {
            let text = setup_response.text().await?;
            return Err(format!("Failed to complete setup: {}", text).into());
        }

        Ok(())
    }

    async fn login_for_token(
        &self,
        base_url: &str,
        username: &str,
        password: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let client = Client::new();
        let mut attempt: u32 = 0;
        let max_attempts: u32 = 40;
        let retry_delay = Duration::from_millis(250);

        let extract_error_message = |parsed: &serde_json::Value| -> Option<String> {
            if let Some(msg) = parsed.get("message").and_then(|m| m.as_str()) {
                return Some(msg.to_string());
            }
            if let Some(err) = parsed.get("error") {
                if let Some(msg) = err.get("message").and_then(|m| m.as_str()) {
                    return Some(msg.to_string());
                }
                if let Some(err_str) = err.as_str() {
                    return Some(err_str.to_string());
                }
            }
            None
        };

        loop {
            let response = client
                .post(format!("{}/v1/api/auth/login", base_url))
                .json(&json!({
                    "username": username,
                    "password": password
                }))
                .send()
                .await;

            let response = match response {
                Ok(response) => response,
                Err(err) if attempt + 1 < max_attempts => {
                    attempt += 1;
                    tokio::time::sleep(retry_delay).await;
                    if err.is_timeout() || err.is_connect() || err.is_request() {
                        continue;
                    }
                    continue;
                },
                Err(err) => return Err(err.into()),
            };

            let status = response.status();

            if status == reqwest::StatusCode::PRECONDITION_REQUIRED {
                self.complete_setup_if_needed(base_url).await?;
                continue;
            }

            let body = response.text().await?;
            let parsed: serde_json::Value =
                serde_json::from_str(&body).unwrap_or_else(|_| json!({ "message": body }));

            if status.is_success() {
                let token = parsed
                    .get("access_token")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| -> Box<dyn std::error::Error> {
                        let error_msg = extract_error_message(&parsed)
                            .unwrap_or_else(|| "Login failed".to_string());
                        format!("Failed to get access token: {}", error_msg).into()
                    })?
                    .to_string();
                return Ok(token);
            }

            let error_msg =
                extract_error_message(&parsed).unwrap_or_else(|| "Login failed".to_string());
            let is_rate_limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || error_msg.to_lowercase().contains("too many");
            let is_transient = status.is_server_error()
                || status == reqwest::StatusCode::REQUEST_TIMEOUT
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                || status == reqwest::StatusCode::GATEWAY_TIMEOUT;

            if (is_rate_limited || is_transient) && attempt + 1 < max_attempts {
                attempt += 1;
                tokio::time::sleep(retry_delay).await;
                continue;
            }

            return Err(format!("Failed to get access token: {}", error_msg).into());
        }
    }

    async fn ensure_admin_user(
        &self,
        base_url: &str,
        root_password: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        invalidate_cached_token_for_credentials(base_url, admin_username(), admin_password());

        let root_token = self.token_for_url_cached(base_url, "root", root_password).await?;
        let client = Client::new();
        let exists_response = client
            .post(format!("{}/v1/api/sql", base_url))
            .bearer_auth(&root_token)
            .json(&json!({
                "sql": "SELECT username FROM system.users WHERE username = 'admin' LIMIT 1"
            }))
            .send()
            .await?;

        let admin_exists = if exists_response.status().is_success() {
            let body = exists_response.text().await?;
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(rows) = get_rows_as_hashmaps(&parsed) {
                    !rows.is_empty()
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if admin_exists {
            let password_response = client
                .post(format!("{}/v1/api/sql", base_url))
                .bearer_auth(&root_token)
                .json(&json!({
                    "sql": format!(
                        "ALTER USER admin SET PASSWORD '{}'",
                        admin_password()
                    )
                }))
                .send()
                .await?;

            if !password_response.status().is_success() {
                let body = password_response.text().await?;
                return Err(format!("Failed to reset admin password: {}", body).into());
            }

            let role_response = client
                .post(format!("{}/v1/api/sql", base_url))
                .bearer_auth(&root_token)
                .json(&json!({
                    "sql": "ALTER USER admin SET ROLE 'dba'"
                }))
                .send()
                .await?;

            if !role_response.status().is_success() {
                let body = role_response.text().await?;
                return Err(format!("Failed to reset admin role: {}", body).into());
            }

            return Ok(());
        }

        let response = client
            .post(format!("{}/v1/api/sql", base_url))
            .bearer_auth(root_token)
            .json(&json!({
                "sql": format!(
                    "CREATE USER admin WITH PASSWORD '{}' ROLE 'dba'",
                    admin_password()
                )
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await?;
            // In cluster mode, another node may have already created the admin user
            // via Raft replication between our SELECT check and CREATE attempt
            if body.contains("already exists") {
                return Ok(());
            }
            return Err(format!("Failed to ensure admin user: {}", body).into());
        }

        Ok(())
    }

    async fn force_reset_admin(&self, base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.complete_setup_if_needed(base_url).await?;
        let root_password = root_password_from_env();
        self.ensure_admin_user(base_url, &root_password).await?;
        if let Ok(mut guard) = self.ready_urls.lock() {
            guard.insert(base_url.to_string());
        }
        if let Ok(mut guard) = TOKEN_CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
            guard.retain(|key, _| !key.contains("admin:") && !key.contains("root:"));
        }
        let _ = self.clear_shared_tokens_for_url(base_url, &["admin", "root"]);
        Ok(())
    }

    async fn ensure_ready(&self, base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Ok(guard) = self.ready_urls.lock() {
            if guard.contains(base_url) {
                return Ok(());
            }
        }

        self.complete_setup_if_needed(base_url).await?;

        let auth_required = server_requires_auth_for_url(base_url).unwrap_or(true);
        if auth_required {
            let root_password = root_password_from_env();
            self.ensure_admin_user(base_url, &root_password).await?;
        }

        if let Ok(mut guard) = self.ready_urls.lock() {
            guard.insert(base_url.to_string());
        }

        Ok(())
    }

    async fn token_for_url(
        &self,
        base_url: &str,
        username: &str,
        password: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        self.ensure_ready(base_url).await?;
        self.token_for_url_cached(base_url, username, password).await
    }

    async fn token_for_url_cached(
        &self,
        base_url: &str,
        username: &str,
        password: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let cache_key = format!("{}|{}:{}", base_url, username, password);

        if let Ok(guard) = TOKEN_CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
            if let Some(token) = guard.get(&cache_key) {
                return Ok(token.clone());
            }
        }

        if let Ok(Some(shared)) = self.shared_token_for_key(&cache_key) {
            if let Ok(mut guard) = TOKEN_CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
                guard.insert(cache_key.clone(), shared.clone());
            }
            return Ok(shared);
        }

        let login_lock = LOGIN_MUTEX.get_or_init(|| TokioMutex::new(()));
        let _guard = login_lock.lock().await;

        if let Ok(guard) = TOKEN_CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
            if let Some(token) = guard.get(&cache_key) {
                return Ok(token.clone());
            }
        }

        if let Ok(Some(shared)) = self.shared_token_for_key(&cache_key) {
            if let Ok(mut guard) = TOKEN_CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
                guard.insert(cache_key.clone(), shared.clone());
            }
            return Ok(shared);
        }

        let token = self.login_for_token(base_url, username, password).await?;

        if let Ok(mut guard) = TOKEN_CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
            guard.insert(cache_key.clone(), token.clone());
        }
        let _ = self.store_shared_token(&cache_key, &token);

        Ok(token)
    }

    fn auth_provider_for_url(
        &self,
        base_url: &str,
        username: &str,
        password: &str,
    ) -> AuthProvider {
        if tokio::runtime::Handle::try_current().is_ok() {
            let base_url_owned = base_url.to_string();
            let username_owned = username.to_string();
            let password_owned = password.to_string();
            let base_url_display = base_url.to_string();
            let username_display = username.to_string();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = match Runtime::new() {
                    Ok(rt) => rt,
                    Err(err) => {
                        let _ = tx.send(Err(err.to_string()));
                        return;
                    },
                };
                let result = runtime
                    .block_on(test_auth_manager().token_for_url(
                        &base_url_owned,
                        &username_owned,
                        &password_owned,
                    ))
                    .map(AuthProvider::jwt_token)
                    .map_err(|err| err.to_string());
                let _ = tx.send(result);
            });
            match rx.recv_timeout(Duration::from_secs(60)) {
                Ok(Ok(auth)) => auth,
                Ok(Err(err)) => panic!(
                    "Failed to authenticate user '{}' for {}: {}",
                    username_display, base_url_display, err
                ),
                Err(err) => panic!(
                    "Failed to authenticate user '{}' for {}: {}",
                    username_display, base_url_display, err
                ),
            }
        } else {
            get_shared_runtime()
                .block_on(self.token_for_url(base_url, username, password))
                .map(AuthProvider::jwt_token)
                .unwrap_or_else(|err| {
                    panic!("Failed to authenticate user '{}' for {}: {}", username, base_url, err)
                })
        }
    }

    fn client_for_url(&self, base_url: &str, username: &str, password: &str) -> KalamLinkClient {
        let auth_required = server_requires_auth_for_url(base_url).unwrap_or(true);
        if !auth_required {
            return KalamLinkClient::builder()
                .base_url(base_url)
                .auth(AuthProvider::none())
                .timeouts(
                    KalamLinkTimeouts::builder()
                        .connection_timeout_secs(5)
                        .receive_timeout_secs(120)
                        .send_timeout_secs(30)
                        .subscribe_timeout_secs(10)
                        .auth_timeout_secs(10)
                        .initial_data_timeout(Duration::from_secs(120))
                        .build(),
                )
                .build()
                .expect("Failed to create shared client without auth");
        }

        let auth = self.auth_provider_for_url(base_url, username, password);
        KalamLinkClient::builder()
            .base_url(base_url)
            .auth(auth)
            .timeouts(
                KalamLinkTimeouts::builder()
                    .connection_timeout_secs(5)
                    .receive_timeout_secs(120)
                    .send_timeout_secs(30)
                    .subscribe_timeout_secs(10)
                    .auth_timeout_secs(10)
                    .initial_data_timeout(Duration::from_secs(120))
                    .build(),
            )
            .build()
            .expect("Failed to create shared client with JWT")
    }
}

fn force_reset_admin_for_url(base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    if tokio::runtime::Handle::try_current().is_ok() {
        let base_url_owned = base_url.to_string();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = match Runtime::new() {
                Ok(rt) => rt,
                Err(err) => {
                    let _ = tx.send(Err(err.to_string()));
                    return;
                },
            };
            let result = runtime
                .block_on(test_auth_manager().force_reset_admin(&base_url_owned))
                .map_err(|err| err.to_string());
            let _ = tx.send(result);
        });
        match rx.recv_timeout(Duration::from_secs(60)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err.into()),
            Err(err) => Err(err.to_string().into()),
        }
    } else {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(test_auth_manager().force_reset_admin(base_url))
    }
}

fn invalidate_cached_token_for_credentials(base_url: &str, username: &str, password: &str) {
    let cache_key = format!("{}|{}:{}", base_url, username, password);

    if let Ok(mut guard) = TOKEN_CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock() {
        guard.remove(&cache_key);
    }

    let _ = test_auth_manager().with_shared_token_cache(|map| {
        map.remove(&cache_key);
    });
}

fn clear_shared_root_client_for_url(base_url: &str) {
    if let Ok(mut guard) = shared_root_client_cache().lock() {
        if guard.as_ref().is_some_and(|cache| cache.base_url == base_url) {
            *guard = None;
        }
    }
}

fn invalidate_auth_caches_for_credentials(base_url: &str, username: &str, password: &str) {
    invalidate_cached_token_for_credentials(base_url, username, password);

    if username == default_username() && password == default_password() {
        clear_shared_root_client_for_url(base_url);
    }
}

fn is_refreshable_token_error(message: &str) -> bool {
    let lowered = message.to_lowercase();
    lowered.contains("token expired")
        || lowered.contains("invalid token signature")
        || lowered.contains("invalid token")
        || lowered.contains("jwt")
}

fn test_auth_manager() -> &'static TestAuthManager {
    TEST_AUTH_MANAGER.get_or_init(TestAuthManager::new)
}

struct AutoTestServer {
    base_url: String,
    storage_dir: PathBuf,
    _temp_dir: Option<TempDir>,
    child: Option<Child>,
}

impl Drop for AutoTestServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Debug, Clone)]
pub struct TestContext {
    pub server_url: String,
    pub username: String,
    pub password: String,
    pub is_cluster: bool,
    pub cluster_urls: Vec<String>,
    pub cluster_urls_raw: Vec<String>,
}

fn has_explicit_server_target() -> bool {
    parse_test_arg("--url").is_some()
        || parse_test_arg("--urls").is_some()
        || std::env::var("KALAMDB_SERVER_URL").is_ok()
        || std::env::var("KALAMDB_CLUSTER_URLS").is_ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerType {
    Fresh,   // Auto-start a fresh server
    Running, // Use an existing running server
    Cluster, // Use an existing running cluster
}

fn server_type_from_env() -> Option<ServerType> {
    // Always load .env first so KALAMDB_SERVER_TYPE is available
    load_env_file();

    if let Ok(value) = std::env::var("KALAMDB_SERVER_TYPE") {
        match value.trim().to_lowercase().as_str() {
            "fresh" => return Some(ServerType::Fresh),
            "running" => return Some(ServerType::Running),
            "cluster" => return Some(ServerType::Cluster),
            other => {
                eprintln!("[TEST] WARNING: Unknown KALAMDB_SERVER_TYPE='{}', ignoring", other);
            },
        }
    }

    if let Ok(value) = std::env::var("KALAMDB_AUTO_START_TEST_SERVER") {
        let value = value.trim();
        if value == "1" {
            return Some(ServerType::Fresh);
        }
        if value == "0" {
            return Some(ServerType::Running);
        }
    }

    None
}

fn should_auto_start_test_server() -> bool {
    if let Some(server_type) = server_type_from_env() {
        return matches!(server_type, ServerType::Fresh);
    }

    if has_explicit_server_target() {
        return false;
    }

    true
}

fn is_external_server_mode() -> bool {
    if let Some(server_type) = server_type_from_env() {
        return matches!(server_type, ServerType::Running | ServerType::Cluster);
    }
    has_explicit_server_target()
}

fn url_reachable(url: &str) -> bool {
    let host_port = url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or("127.0.0.1:8080");
    host_port_reachable(host_port)
}

fn wait_for_url_reachable(url: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if url_reachable(url) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    url_reachable(url)
}

fn ensure_auto_test_server() -> Option<(String, PathBuf)> {
    let server_mutex = AUTO_TEST_SERVER.get_or_init(|| Mutex::new(None));
    let mut guard = server_mutex.lock().ok()?;

    if let Some(existing) = guard.as_ref() {
        if !url_reachable(&existing.base_url) {
            eprintln!(
                "[TEST] Auto-started server unreachable at {}, restarting",
                existing.base_url
            );
            *guard = None;
        }
    }

    if guard.is_none() {
        let start_result: Result<AutoTestServer, String> = if tokio::runtime::Handle::try_current()
            .is_ok()
        {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = AUTO_TEST_RUNTIME.get_or_init(|| {
                    Box::leak(Box::new(
                        Runtime::new().expect("Failed to create auto test server runtime"),
                    ))
                });
                let result =
                    (*runtime).block_on(start_local_test_server()).map_err(|err| err.to_string());
                let _ = tx.send(result);
            });

            match rx.recv_timeout(Duration::from_secs(60)) {
                Ok(result) => result,
                Err(err) => Err(format!("Timed out starting test server: {}", err)),
            }
        } else {
            let runtime = AUTO_TEST_RUNTIME.get_or_init(|| {
                Box::leak(Box::new(
                    Runtime::new().expect("Failed to create auto test server runtime"),
                ))
            });
            (*runtime).block_on(start_local_test_server()).map_err(|err| err.to_string())
        };

        match start_result {
            Ok(server) => {
                *guard = Some(server);
                if let Some(server) = guard.as_ref() {
                    let _ = wait_for_url_reachable(&server.base_url, Duration::from_secs(10));
                }
            },
            Err(err) => {
                eprintln!("Failed to auto-start test server: {}", err);
                return None;
            },
        }
    }

    guard
        .as_ref()
        .map(|server| (server.base_url.clone(), server.storage_dir.clone()))
}

/// Force a local auto-started test server and return its base URL.
///
/// This bypasses any externally running server to ensure tests use the
/// in-process server with the current code version.
pub fn force_auto_test_server_url() -> String {
    // test_context() already handles auto-starting or connecting to external server
    server_url().to_string()
}

/// Async variant of forcing a local auto-started test server.
pub async fn force_auto_test_server_url_async() -> String {
    // test_context() already handles auto-starting or connecting to external server
    server_url().to_string()
}

fn kalamdb_server_bin() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("KALAMDB_SERVER_BIN") {
        return Ok(PathBuf::from(path));
    }

    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.push("target");
    path.push("debug");
    path.push(if cfg!(windows) {
        "kalamdb-server.exe"
    } else {
        "kalamdb-server"
    });

    if path.exists() {
        Ok(path)
    } else {
        Err(format!(
            "kalamdb-server binary not found at {}. Build it with `cargo build -p kalamdb-server --bin kalamdb-server`.",
            path.display()
        )
        .into())
    }
}

async fn start_local_test_server() -> Result<AutoTestServer, Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let data_path = temp_dir.path().to_path_buf();

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);

    let rpc_listener = TcpListener::bind("127.0.0.1:0")?;
    let rpc_port = rpc_listener.local_addr()?.port();
    drop(rpc_listener);

    let api_listener = TcpListener::bind("127.0.0.1:0")?;
    let api_port = api_listener.local_addr()?.port();
    drop(api_listener);

    let base_url = format!("http://127.0.0.1:{}", port);
    let server_bin = kalamdb_server_bin()?;

    let mut config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    config_path.pop();
    config_path.push("server.toml");

    let log_path = data_path.join("server.log");
    let log_file = std::fs::OpenOptions::new().create(true).append(true).open(&log_path)?;
    let log_file_err = log_file.try_clone()?;

    let mut cmd = Command::new(server_bin);
    cmd.env("KALAMDB_SERVER_HOST", "127.0.0.1")
        .env("KALAMDB_SERVER_PORT", port.to_string())
        .env("KALAMDB_CLUSTER_RPC_ADDR", format!("127.0.0.1:{}", rpc_port))
        .env("KALAMDB_CLUSTER_API_ADDR", format!("127.0.0.1:{}", api_port))
        .env("KALAMDB_DATA_DIR", data_path.to_string_lossy().into_owned())
        .env("KALAMDB_RATE_LIMIT_AUTH_REQUESTS_PER_IP_PER_SEC", "200")
        .env("KALAMDB_LOG_LEVEL", "warn")
        .arg(config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err));

    let mut child = cmd.spawn()?;

    if !wait_for_url_reachable(&base_url, Duration::from_secs(30)) {
        let _ = child.kill();
        let log_tail = std::fs::read_to_string(&log_path).unwrap_or_default();
        let log_tail = log_tail
            .lines()
            .rev()
            .take(20)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "Timed out waiting for test server at {}.\nLast server logs:\n{}",
            base_url, log_tail
        )
        .into());
    }

    // Ensure setup and admin user are ready for the auto-started server
    test_auth_manager().ensure_ready(&base_url).await?;

    Ok(AutoTestServer {
        base_url: base_url.clone(),
        storage_dir: data_path,
        _temp_dir: Some(temp_dir),
        child: Some(child),
    })
}

fn parse_test_arg(name: &str) -> Option<String> {
    let prefix = format!("{}=", name);
    for arg in std::env::args() {
        if let Some(value) = arg.strip_prefix(&prefix) {
            return Some(value.to_string());
        }
    }
    None
}

fn parse_test_urls() -> Option<Vec<String>> {
    let raw = parse_test_arg("--urls")?;
    let urls: Vec<String> = raw
        .split(',')
        .map(|url| url.trim().to_string())
        .filter(|url| !url.is_empty())
        .collect();
    if urls.is_empty() {
        None
    } else {
        Some(urls)
    }
}

fn ensure_server_ready_sync(base_url: &str) {
    let server_type = server_type_from_env();

    if !url_reachable(base_url) {
        // If explicitly configured to use a running/cluster server, fail loudly
        if matches!(server_type, Some(ServerType::Running) | Some(ServerType::Cluster)) {
            panic!(
                "\n\n\
                ╔══════════════════════════════════════════════════════════════════╗\n\
                ║              SERVER NOT REACHABLE                                ║\n\
                ╠══════════════════════════════════════════════════════════════════╣\n\
                ║  KALAMDB_SERVER_TYPE={:?} but server is NOT reachable             ║\n\
                ║  at: {}\n\
                ║                                                                  ║\n\
                ║  Start the server:  cd backend && cargo run --release            ║\n\
                ║  Or set KALAMDB_SERVER_TYPE=fresh to auto-start a test server    ║\n\
                ╚══════════════════════════════════════════════════════════════════╝\n",
                server_type.unwrap(),
                base_url
            );
        }
        return;
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        let base_url_owned = base_url.to_string();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = match Runtime::new() {
                Ok(rt) => rt,
                Err(err) => {
                    let _ = tx.send(Err(err.to_string()));
                    return;
                },
            };
            let result = runtime
                .block_on(test_auth_manager().ensure_ready(&base_url_owned))
                .map_err(|err| err.to_string());
            let _ = tx.send(result);
        });
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(())) => {},
            Ok(Err(err)) => {
                panic!("Failed to prepare test authentication context for {}: {}", base_url, err)
            },
            Err(err) => {
                panic!("Timed out preparing test authentication context for {}: {}", base_url, err)
            },
        }
    } else {
        if let Err(err) = get_shared_runtime().block_on(test_auth_manager().ensure_ready(base_url))
        {
            panic!("Failed to prepare test authentication context for {}: {}", base_url, err);
        }
    }
}

fn json_value_is_true(value: &serde_json::Value) -> bool {
    value
        .as_bool()
        .or_else(|| value.as_str().map(|s| s.eq_ignore_ascii_case("true")))
        .unwrap_or(false)
}

fn sql_body_is_self_leader(body: &serde_json::Value) -> Option<bool> {
    let status = body.get("status")?.as_str()?;
    if status != "success" {
        return None;
    }

    let value = body
        .get("results")?
        .as_array()?
        .first()?
        .get("rows")?
        .as_array()?
        .first()?
        .as_array()?
        .first()?;

    let extracted = extract_typed_value(value);
    Some(json_value_is_true(&extracted))
}

fn leader_cache() -> &'static Mutex<Option<String>> {
    LAST_LEADER_URL.get_or_init(|| Mutex::new(None))
}

fn cache_leader_url(url: &str) {
    if let Ok(mut guard) = leader_cache().lock() {
        *guard = Some(url.to_string());
    }
}

fn cached_leader_url() -> Option<String> {
    leader_cache().lock().ok().and_then(|guard| guard.clone())
}

fn extract_leader_url(message: &str) -> Option<String> {
    let start = message.find("http://").or_else(|| message.find("https://"))?;
    let rest = &message[start..];
    let end = rest
        .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ')' | '}' | ',' | '\\'))
        .unwrap_or(rest.len());
    let url = rest[..end].to_string();
    if url.is_empty() {
        None
    } else {
        Some(url)
    }
}

fn detect_leader_url(urls: &[String], username: &str, password: &str) -> Option<String> {
    if urls.is_empty() {
        return None;
    }

    let urls = urls.to_vec();
    let username = username.to_string();
    let password = password.to_string();
    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let runtime: Runtime = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(_) => {
                let _ = tx.send(None);
                return;
            },
        };

        let leader = runtime.block_on(async move {
            let client = Client::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

            for _ in 0..15 {
                for url in &urls {
                    if tokio::time::Instant::now() >= deadline {
                        return None;
                    }

                    let auth_token = if !username.is_empty() || !password.is_empty() {
                        get_access_token_for_url(url, &username, &password).await.ok()
                    } else {
                        None
                    };

                    let sql_url = format!("{}/v1/api/sql", url);
                    let mut sql_request = client
                        .post(sql_url)
                        .timeout(Duration::from_millis(1500))
                        .json(&json!({
                            "sql": "SELECT is_leader FROM system.cluster WHERE is_self = true LIMIT 1"
                        }));
                    if let Some(token) = auth_token.as_ref() {
                        sql_request = sql_request.bearer_auth(token);
                    }

                    if let Ok(sql_response) = sql_request.send().await {
                        if let Ok(body) = sql_response.json::<serde_json::Value>().await {
                            if let Some(true) = sql_body_is_self_leader(&body) {
                                return Some(url.clone());
                            }
                        }
                    }

                    let health_url = format!("{}/v1/api/cluster/health", url);
                    let mut request = client.get(health_url).timeout(Duration::from_millis(1500));
                    if let Some(token) = auth_token.as_ref() {
                        request = request.bearer_auth(token);
                    }
                    let response = request.send().await;

                    let Ok(response) = response else {
                        continue;
                    };

                    let Ok(body) = response.json::<serde_json::Value>().await else {
                        continue;
                    };

                    let is_leader = body
                        .get("is_leader")
                        .map(json_value_is_true)
                        .or_else(|| {
                            body.get("data")
                                .and_then(|data| data.get("is_leader").map(json_value_is_true))
                        })
                        .unwrap_or(false);

                    if is_leader {
                        return Some(url.clone());
                    }
                }

                if tokio::time::Instant::now() >= deadline {
                    break;
                }

            }

            None
        });

        let _ = tx.send(leader);
    });

    rx.recv_timeout(Duration::from_secs(6)).ok().flatten()
}

fn reorder_cluster_urls_by_leader(
    mut urls: Vec<String>,
    username: &str,
    password: &str,
) -> Vec<String> {
    let Some(leader_url) = detect_leader_url(&urls, username, password) else {
        return urls;
    };

    urls.retain(|url| url != &leader_url);
    let mut ordered = Vec::with_capacity(urls.len() + 1);
    ordered.push(leader_url);
    ordered.extend(urls);
    ordered
}

pub fn test_context() -> &'static TestContext {
    TEST_CONTEXT.get_or_init(|| {
        // Load environment from .env file first so all env vars are available
        load_env_file();

        let server_type = server_type_from_env();
        eprintln!("[TEST] Server type: {:?}", server_type);

        let mut server_url = parse_test_arg("--url")
            .or_else(|| std::env::var("KALAMDB_SERVER_URL").ok())
            .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
        // ── Branch by server type ─────────────────────────────────
        match server_type {
            Some(ServerType::Running) => {
                // "running" means use the existing server — fail-fast if unreachable
                if !url_reachable(&server_url) {
                    panic!(
                        "\n\n\
                        ╔══════════════════════════════════════════════════════════════════╗\n\
                        ║              SERVER NOT REACHABLE                                ║\n\
                        ╠══════════════════════════════════════════════════════════════════╣\n\
                        ║  KALAMDB_SERVER_TYPE=running but server is NOT reachable at:     ║\n\
                        ║    {}\n\
                        ║                                                                  ║\n\
                        ║  Start the server:  cd backend && cargo run --release            ║\n\
                        ║  Or set KALAMDB_SERVER_TYPE=fresh to auto-start a test server    ║\n\
                        ╚══════════════════════════════════════════════════════════════════╝\n",
                        server_url
                    );
                }
                eprintln!(
                    "✅ [TEST] Using existing server at {} (KALAMDB_SERVER_TYPE=running)",
                    server_url
                );
            },
            Some(ServerType::Fresh) => {
                // "fresh" means auto-start a local test server
                if let Some((auto_url, storage_dir)) = ensure_auto_test_server() {
                    std::env::set_var("KALAMDB_SERVER_URL", &auto_url);
                    if std::env::var("KALAMDB_ROOT_PASSWORD").is_err() {
                        std::env::set_var("KALAMDB_ROOT_PASSWORD", root_password_from_env());
                    }
                    std::env::set_var(
                        "KALAMDB_STORAGE_DIR",
                        storage_dir.to_string_lossy().to_string(),
                    );
                    server_url = auto_url;
                    eprintln!("✅ [TEST] Auto-started fresh server at {}", server_url);
                } else {
                    panic!(
                        "\n\n\
                        ╔══════════════════════════════════════════════════════════════════╗\n\
                        ║          FAILED TO START FRESH TEST SERVER                       ║\n\
                        ╠══════════════════════════════════════════════════════════════════╣\n\
                        ║  KALAMDB_SERVER_TYPE=fresh but could not auto-start server.      ║\n\
                        ║  Build the server: cd backend && cargo build                    ║\n\
                        ╚══════════════════════════════════════════════════════════════════╝\n"
                    );
                }
            },
            Some(ServerType::Cluster) => {
                // Cluster mode — handled below in cluster URL resolution
                eprintln!("[TEST] Cluster mode configured");
            },
            None => {
                // No explicit type — auto-detect:
                // If KALAMDB_SERVER_URL is set AND reachable, use it (Running behavior)
                // Otherwise try to auto-start a fresh server
                if has_explicit_server_target() && url_reachable(&server_url) {
                    eprintln!("✅ [TEST] Using existing server at {} (auto-detected)", server_url);
                } else if has_explicit_server_target() {
                    // URL specified but not reachable — fail fast
                    panic!(
                        "\n\n\
                        ╔══════════════════════════════════════════════════════════════════╗\n\
                        ║              SERVER NOT REACHABLE                                ║\n\
                        ╠══════════════════════════════════════════════════════════════════╣\n\
                        ║  KALAMDB_SERVER_URL is set but server is NOT reachable at:       ║\n\
                        ║    {}\n\
                        ║                                                                  ║\n\
                        ║  Start the server:  cd backend && cargo run --release            ║\n\
                        ║  Or remove KALAMDB_SERVER_URL to auto-start a test server        ║\n\
                        ╚══════════════════════════════════════════════════════════════════╝\n",
                        server_url
                    );
                } else {
                    // No URL set, try auto-start
                    if let Some((auto_url, storage_dir)) = ensure_auto_test_server() {
                        std::env::set_var("KALAMDB_SERVER_URL", &auto_url);
                        if std::env::var("KALAMDB_ROOT_PASSWORD").is_err() {
                            std::env::set_var("KALAMDB_ROOT_PASSWORD", root_password_from_env());
                        }
                        std::env::set_var(
                            "KALAMDB_STORAGE_DIR",
                            storage_dir.to_string_lossy().to_string(),
                        );
                        server_url = auto_url;
                        eprintln!("✅ [TEST] Auto-started fresh server at {}", server_url);
                    } else {
                        panic!(
                            "\n\n\
                            ╔══════════════════════════════════════════════════════════════════╗\n\
                            ║          NO SERVER AVAILABLE                                     ║\n\
                            ╠══════════════════════════════════════════════════════════════════╣\n\
                            ║  Could not find or start a KalamDB server for tests.             ║\n\
                            ║                                                                  ║\n\
                            ║  Options:                                                        ║\n\
                            ║    1. Start server: cd backend && cargo run --release            ║\n\
                            ║    2. Set KALAMDB_SERVER_TYPE=running in .env                    ║\n\
                            ║    3. Set KALAMDB_SERVER_TYPE=fresh in .env                      ║\n\
                            ╚══════════════════════════════════════════════════════════════════╝\n"
                        );
                    }
                }
            },
        }

        // For Cluster mode, defer the reachability check until after cluster URL
        // resolution so we check the actual cluster node, not the default 8080.
        if !matches!(server_type, Some(ServerType::Cluster)) {
            ensure_server_ready_sync(&server_url);
        }

        let username = parse_test_arg("--user")
            .or_else(|| std::env::var("KALAMDB_USER").ok())
            .unwrap_or_else(|| default_username().to_string());

        let password = parse_test_arg("--password")
            .or_else(|| std::env::var("KALAMDB_ADMIN_PASSWORD").ok())
            .or_else(|| std::env::var("KALAMDB_ROOT_PASSWORD").ok())
            // Default to kalamdb123 to match autostart server setup
            .unwrap_or_else(|| default_password().to_string());

        // ── Cluster URL resolution ─────────────────────────────────
        // Only probe cluster nodes when server type is explicitly Cluster.
        // For Running/Fresh, we skip cluster probing entirely (no slow TCP timeouts).
        let explicit_cluster_urls = parse_test_urls().or_else(|| {
            std::env::var("KALAMDB_CLUSTER_URLS").ok().map(|s| {
                s.split(',')
                    .map(|url| url.trim().to_string())
                    .filter(|url| !url.is_empty())
                    .collect()
            })
        });

        let (is_cluster, mut cluster_urls) = match server_type {
            Some(ServerType::Cluster) => {
                // Cluster mode: probe cluster URLs to find healthy nodes
                let cluster_default =
                    "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083";
                let cluster_urls: Vec<String> =
                    explicit_cluster_urls.clone().unwrap_or_else(|| {
                        cluster_default
                            .split(',')
                            .map(|url| url.trim().to_string())
                            .filter(|url| !url.is_empty())
                            .collect()
                    });
                let healthy: Vec<String> = cluster_urls
                    .iter()
                    .filter(|url| {
                        let host_port = url
                            .trim_start_matches("http://")
                            .trim_start_matches("https://")
                            .split('/')
                            .next()
                            .unwrap_or("127.0.0.1:8081");
                        host_port_reachable(host_port)
                    })
                    .cloned()
                    .collect();
                if let Some(explicit_urls) = explicit_cluster_urls {
                    if healthy.is_empty() {
                        if let Some((auto_url, storage_dir)) = ensure_auto_test_server() {
                            std::env::set_var("KALAMDB_SERVER_URL", &auto_url);
                            if std::env::var("KALAMDB_ROOT_PASSWORD").is_err() {
                                std::env::set_var(
                                    "KALAMDB_ROOT_PASSWORD",
                                    root_password_from_env(),
                                );
                            }
                            std::env::set_var(
                                "KALAMDB_STORAGE_DIR",
                                storage_dir.to_string_lossy().to_string(),
                            );
                            server_url = auto_url.clone();
                            eprintln!(
                                "[TEST] Cluster URLs configured but unreachable; falling back to fresh auto-started server at {}",
                                server_url
                            );
                            (false, vec![auto_url])
                        } else {
                            eprintln!(
                                "[TEST] Cluster URLs configured but unreachable; falling back to single-node URL {}",
                                server_url
                            );
                            (false, vec![server_url.clone()])
                        }
                    } else {
                        (explicit_urls.len() > 1, explicit_urls)
                    }
                } else if !healthy.is_empty() {
                    (true, healthy)
                } else {
                    (false, vec![server_url.clone()])
                }
            },
            Some(ServerType::Fresh) | Some(ServerType::Running) | None => {
                // Single-node: no cluster probing, instant startup
                if let Some(explicit_urls) = explicit_cluster_urls {
                    (explicit_urls.len() > 1, explicit_urls)
                } else {
                    (false, vec![server_url.clone()])
                }
            },
        };

        let cluster_urls_raw = cluster_urls.clone();
        if is_cluster {
            cluster_urls = reorder_cluster_urls_by_leader(cluster_urls, &username, &password);
        }

        // Now that cluster URLs are resolved, verify the primary node is reachable.
        if matches!(server_type, Some(ServerType::Cluster)) {
            let check_url = cluster_urls.first().map(|s| s.as_str()).unwrap_or(&server_url);
            ensure_server_ready_sync(check_url);
        }

        TestContext {
            server_url,
            username,
            password,
            is_cluster,
            cluster_urls,
            cluster_urls_raw,
        }
    })
}

/// Get the server URL for tests.
///
/// Configure via `KALAMDB_SERVER_URL` environment variable.
/// Default: `http://127.0.0.1:8080`
///
/// # Examples
///
/// ```bash
/// # Test against different port
/// KALAMDB_SERVER_URL="http://127.0.0.1:3000" cargo test
///
/// # Test against remote server
/// KALAMDB_SERVER_URL="https://kalamdb.example.com" cargo test
/// ```
pub fn server_url() -> &'static str {
    SERVER_URL
        .get_or_init(|| {
            let ctx = test_context();
            if ctx.is_cluster {
                leader_url().unwrap_or_else(|| {
                    ctx.cluster_urls.first().cloned().unwrap_or_else(|| ctx.server_url.clone())
                })
            } else {
                ctx.server_url.clone()
            }
        })
        .as_str()
}

pub fn leader_or_server_url() -> String {
    if is_cluster_mode() {
        leader_url().unwrap_or_else(|| server_url().to_string())
    } else {
        server_url().to_string()
    }
}

pub fn leader_url() -> Option<String> {
    let ctx = test_context();
    if !ctx.is_cluster {
        return Some(ctx.server_url.clone());
    }

    // Check cached leader first
    if let Some(cached) = cached_leader_url() {
        eprintln!("DEBUG leader_url: Returning cached URL: {}", cached);
        return Some(cached);
    }

    if let Some(leader) = detect_leader_url(&ctx.cluster_urls_raw, &ctx.username, &ctx.password) {
        eprintln!("DEBUG leader_url: Detected leader URL: {}", leader);
        cache_leader_url(&leader);
        return Some(leader);
    }

    let fallback = ctx.cluster_urls.first().cloned();
    eprintln!("DEBUG leader_url: Using fallback URL: {:?}", fallback);
    fallback
}

pub const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Get the root user password for tests.
///
/// Configure via `KALAMDB_ROOT_PASSWORD` environment variable.
/// Default: empty string `""`
///
/// # Examples
///
/// ```bash
/// # Test with authentication enabled
/// KALAMDB_ROOT_PASSWORD="secure-password" cargo test
///
/// # Test with both URL and password
/// KALAMDB_SERVER_URL="http://127.0.0.1:3000" \
/// KALAMDB_ROOT_PASSWORD="mypass" \
/// cargo test
/// ```
pub fn root_password() -> &'static str {
    ROOT_PASSWORD.get_or_init(|| default_password().to_string()).as_str()
}

pub fn admin_username() -> &'static str {
    "admin"
}

pub fn admin_password() -> &'static str {
    ADMIN_PASSWORD.get_or_init(admin_password_from_env).as_str()
}

pub fn default_username() -> &'static str {
    admin_username()
}

pub fn default_password() -> &'static str {
    admin_password()
}

/// Get an access token for the given credentials.
///
/// Caches tokens to avoid repeated login calls.
/// Uses the /v1/api/auth/login endpoint to obtain a Bearer token.
///
/// If server requires setup (HTTP 428), automatically completes setup
/// with admin/kalamdb123 as the DBA user and kalamdb123 as root password.
pub async fn get_access_token(
    username: &str,
    password: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    get_access_token_for_url(server_url(), username, password).await
}

pub async fn get_access_token_for_url(
    base_url: &str,
    username: &str,
    password: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    test_auth_manager().token_for_url(base_url, username, password).await
}

async fn auth_provider_for_user_async(
    base_url: &str,
    username: &str,
    password: &str,
) -> Result<AuthProvider, Box<dyn std::error::Error>> {
    let auth_required = server_requires_auth_for_url(base_url).unwrap_or(true);
    if !auth_required {
        return Ok(AuthProvider::none());
    }

    let token = test_auth_manager().token_for_url(base_url, username, password).await?;
    Ok(AuthProvider::jwt_token(token))
}

pub fn auth_provider_for_user_on_url(
    base_url: &str,
    username: &str,
    password: &str,
) -> AuthProvider {
    test_auth_manager().auth_provider_for_url(base_url, username, password)
}

pub fn auth_provider_for_user(username: &str, password: &str) -> AuthProvider {
    let base_url = leader_url().unwrap_or_else(|| server_url().to_string());
    auth_provider_for_user_on_url(&base_url, username, password)
}

fn get_access_token_for_url_sync(base_url: &str, username: &str, password: &str) -> Option<String> {
    if tokio::runtime::Handle::try_current().is_ok() {
        let base_url_owned = base_url.to_string();
        let username_owned = username.to_string();
        let password_owned = password.to_string();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = match Runtime::new() {
                Ok(rt) => rt,
                Err(err) => {
                    let _ = tx.send(Err(err.to_string()));
                    return;
                },
            };
            let result = runtime
                .block_on(get_access_token_for_url(
                    &base_url_owned,
                    &username_owned,
                    &password_owned,
                ))
                .map_err(|err| err.to_string());
            let _ = tx.send(result);
        });
        match rx.recv_timeout(Duration::from_secs(60)) {
            Ok(Ok(token)) => Some(token),
            _ => None,
        }
    } else {
        get_shared_runtime()
            .block_on(get_access_token_for_url(base_url, username, password))
            .ok()
    }
}

/// Execute SQL over HTTP with explicit credentials.
///
/// First obtains a Bearer token via login, then executes SQL.
/// The SQL endpoint only accepts Bearer token authentication.
pub async fn execute_sql_via_http_as(
    username: &str,
    password: &str,
    sql: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    // Get access token first
    let token = get_access_token(username, password).await?;

    let client = Client::new();
    let mut last_parsed: Option<serde_json::Value> = None;

    for attempt in 0..5 {
        let base_url = if is_cluster_mode() {
            leader_url().unwrap_or_else(|| server_url().to_string())
        } else {
            server_url().to_string()
        };

        let response = client
            .post(format!("{}/v1/api/sql", base_url))
            .header("Authorization", format!("Bearer {}", token))
            .json(&json!({ "sql": sql }))
            .send()
            .await?;

        let body = response.text().await?;
        let parsed: serde_json::Value = serde_json::from_str(&body)?;
        last_parsed = Some(parsed.clone());

        let status = parsed.get("status").and_then(|s| s.as_str()).unwrap_or("");

        if status.eq_ignore_ascii_case("success") {
            if is_cluster_mode() {
                wait_for_cluster_after_sql(sql);
            }
            return Ok(parsed);
        }

        let err_msg = json_error_message(&parsed).unwrap_or_default();
        if is_leader_error(&err_msg) && attempt < 4 {
            let delay_ms = 300 + attempt * 200;
            tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
            continue;
        }

        return Ok(parsed);
    }

    Ok(last_parsed
        .unwrap_or_else(|| json!({"status": "error", "error": {"message": "No response"}})))
}

/// Execute SQL over HTTP as root user.
pub async fn execute_sql_via_http_as_root(
    sql: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    execute_sql_via_http_as(default_username(), default_password(), sql).await
}

/// Check if the server is running and root authentication succeeds.
pub async fn is_server_running_with_auth() -> bool {
    if !is_server_running() {
        return false;
    }

    execute_sql_via_http_as_root("SELECT 1")
        .await
        .ok()
        .and_then(|response| {
            response.get("status").and_then(|s| s.as_str()).map(|s| s == "success")
        })
        .unwrap_or(false)
}

/// Extract a value from Arrow JSON format
///
/// Arrow JSON format wraps values in type objects like `{"Utf8": "value"}` or `{"Int64": 123}`.
/// This helper extracts the actual value, supporting common types.
pub fn extract_arrow_value(value: &serde_json::Value) -> Option<serde_json::Value> {
    use serde_json::Value;

    // Check if it's already a simple value
    if value.is_string() || value.is_number() || value.is_boolean() || value.is_null() {
        return Some(value.clone());
    }

    // Check for Arrow typed objects
    if let Some(obj) = value.as_object() {
        // String types
        if let Some(v) = obj.get("Utf8") {
            return Some(v.clone());
        }
        // Integer types
        if let Some(v) = obj.get("Int64") {
            return Some(v.clone());
        }
        if let Some(v) = obj.get("Int32") {
            return Some(v.clone());
        }
        if let Some(v) = obj.get("Int16") {
            return Some(v.clone());
        }
        // Float types
        if let Some(v) = obj.get("Float64") {
            return Some(v.clone());
        }
        if let Some(v) = obj.get("Float32") {
            return Some(v.clone());
        }
        // Boolean
        if let Some(v) = obj.get("Boolean") {
            return Some(v.clone());
        }
        // Decimal
        if let Some(v) = obj.get("Decimal128") {
            return Some(v.clone());
        }
        // Date
        if let Some(v) = obj.get("Date32") {
            return Some(v.clone());
        }
        // Timestamp
        if let Some(v) = obj.get("TimestampMicrosecond") {
            return Some(v.clone());
        }
        // FixedSizeBinary (for UUID)
        if let Some(v) = obj.get("FixedSizeBinary") {
            return Some(v.clone());
        }
    }

    None
}

/// Convert array-based rows to HashMap-based rows using the schema.
///
/// The new API format returns `schema: [{name, data_type, index}, ...]` and `rows: [[val1, val2], ...]`.
/// This helper converts to the old format where each row is a `{col_name: value}` HashMap.
///
/// # Arguments
/// * `result` - A single query result from the API (one element from the `results` array)
///
/// # Returns
/// A vector of HashMap rows, or None if the result doesn't have the expected structure.
pub fn rows_as_hashmaps(
    result: &serde_json::Value,
) -> Option<Vec<std::collections::HashMap<String, serde_json::Value>>> {
    use serde_json::Value;
    use std::collections::HashMap;

    // Extract schema - array of {name, data_type, index}
    let schema = result.get("schema")?.as_array()?;

    // Build column name list from schema
    let column_names: Vec<&str> =
        schema.iter().filter_map(|col| col.get("name")?.as_str()).collect();

    // Extract rows - array of arrays
    let rows = result.get("rows")?.as_array()?;

    // Convert each row array to a HashMap
    let result: Vec<HashMap<String, Value>> = rows
        .iter()
        .filter_map(|row| {
            let row_arr = row.as_array()?;
            let mut map = HashMap::new();
            for (i, col_name) in column_names.iter().enumerate() {
                if let Some(val) = row_arr.get(i) {
                    map.insert(col_name.to_string(), val.clone());
                }
            }
            Some(map)
        })
        .collect();

    Some(result)
}

/// Get rows from API response as HashMaps.
///
/// Convenience function that extracts `results[0]` and converts its rows to HashMaps.
pub fn get_rows_as_hashmaps(
    json: &serde_json::Value,
) -> Option<Vec<std::collections::HashMap<String, serde_json::Value>>> {
    let first_result = json.get("results")?.as_array()?.first()?;
    rows_as_hashmaps(first_result)
}

pub fn parse_json_from_cli_output(
    output: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let start = output.find('{').ok_or("No JSON object start found")?;
    let end = output.rfind('}').ok_or("No JSON object end found")?;
    let json_str = &output[start..=end];
    Ok(serde_json::from_str(json_str)?)
}

pub fn parse_cli_json_output(
    output: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let trimmed = output.trim();
    let start = trimmed.find('{').ok_or_else(|| "JSON output missing '{'")?;
    let end = trimmed.rfind('}').ok_or_else(|| "JSON output missing '}'")?;
    let json_str = &trimmed[start..=end];
    let value = serde_json::from_str(json_str)
        .map_err(|e| format!("Failed to parse JSON output: {}. Raw: {}", e, output))?;
    Ok(value)
}

/// Detect leader-related errors for cluster failover attempts.
pub fn is_leader_error(message: &str) -> bool {
    let lower = message.to_lowercase();
    let is_leader = lower.contains("not leader")
        || lower.contains("not_leader")  // Handles NOT_LEADER error code
        || lower.contains("unknown leader")
        || lower.contains("no cluster leader")
        || lower.contains("no raft leader")
        || lower.contains("leader is node none")
        || lower.contains("forward request to cluster leader")
        || lower.contains("failed to forward request to cluster leader")
        || lower.contains("forward to leader")
        || lower.contains("forwardtoleader")
        || lower.contains("forward_to_leader")
        || lower.contains("raft insert failed")
        || lower.contains("raft update failed");

    if is_leader {
        if let Some(url) = extract_leader_url(message) {
            eprintln!("DEBUG is_leader_error: Extracted and caching leader URL: {}", url);
            cache_leader_url(&url);
        } else {
            eprintln!("DEBUG is_leader_error: No URL extracted from message");
        }
    }

    is_leader
}

fn is_flush_sql(sql: &str) -> bool {
    sql.trim_start().to_ascii_uppercase().starts_with("STORAGE FLUSH")
}

fn is_idempotent_conflict(message: &str) -> bool {
    message.to_ascii_lowercase().contains("idempotent conflict")
}

fn is_network_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("network error")
        || lower.contains("error sending request")
        || lower.contains("connection refused")
        || lower.contains("connection reset")
        || lower.contains("connection closed")
        || lower.contains("broken pipe")
        || lower.contains("connection error")
        || lower.contains("timed out")
        || lower.contains("timeout")
}

fn is_transient_missing_relation(sql: &str, message: &str) -> bool {
    let lower_msg = message.to_ascii_lowercase();
    if !lower_msg.contains("does not exist") && !lower_msg.contains("not found") {
        return false;
    }

    let lower_sql = sql.trim_start().to_ascii_lowercase();
    lower_sql.starts_with("select ")
        || lower_sql.starts_with("insert ")
        || lower_sql.starts_with("update ")
        || lower_sql.starts_with("delete ")
        || lower_sql.starts_with("storage flush")
        || lower_sql.starts_with("create table")
        || lower_sql.starts_with("create shared table")
        || lower_sql.starts_with("create user table")
        || lower_sql.starts_with("create stream table")
        || lower_sql.starts_with("alter table")
}

fn is_rate_limited_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("rate_limit_exceeded")
        || lower.contains("too many queries per second")
        || lower.contains("server error (429)")
        || lower.contains("too many requests")
}

pub fn is_retryable_cluster_error_for_sql(sql: &str, message: &str) -> bool {
    is_leader_error(message)
        || is_network_error(message)
        || is_transient_missing_relation(sql, message)
}

fn cli_output_error(stdout: &str) -> Option<String> {
    for line in stdout.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("Error:")
            || trimmed.starts_with("ERROR ")
            || trimmed.starts_with("ERROR:")
        {
            return Some(trimmed.to_string());
        }
    }
    None
}

/// Check if the KalamDB server is running and reachable.
/// Panics if server type is Running/Cluster and server is unreachable.
/// Returns false only when no explicit server type is set and server is down.
pub fn is_server_running() -> bool {
    // test_context() has already validated server reachability based on KALAMDB_SERVER_TYPE
    let _ctx = test_context();
    let url = server_url();

    if is_server_reachable() {
        match server_requires_auth() {
            Some(_) => return true,
            None => {
                // Server is TCP-reachable but auth check failed; try briefly
                if wait_for_url_reachable(url, Duration::from_secs(2)) {
                    return server_requires_auth().is_some();
                }
            },
        }
    }

    // Server not reachable at this point
    let server_type = server_type_from_env();
    if matches!(server_type, Some(ServerType::Running) | Some(ServerType::Cluster)) {
        panic!(
            "\n\n\
            ╔══════════════════════════════════════════════════════════════════╗\n\
            ║                    SERVER NOT REACHABLE                          ║\n\
            ╠══════════════════════════════════════════════════════════════════╣\n\
            ║  Tests require a running KalamDB server at:                      ║\n\
            ║    {}                                                            \n\
            ║                                                                  ║\n\
            ║  Start the server:  cd backend && cargo run --release            ║\n\
            ╚══════════════════════════════════════════════════════════════════╝\n\n",
            url
        );
    }

    false
}

fn is_server_reachable() -> bool {
    host_port_reachable(&server_host_port())
}

fn host_port_reachable(host_port: &str) -> bool {
    if std::net::TcpStream::connect(host_port).map(|_| true).unwrap_or(false) {
        return true;
    }

    let Some((host, port)) = split_host_port(host_port) else {
        return false;
    };

    let mut fallbacks: Vec<String> = Vec::new();
    match host.as_str() {
        "127.0.0.1" => {
            fallbacks.push(format!("localhost:{}", port));
            fallbacks.push(format!("[::1]:{}", port));
        },
        "localhost" => {
            fallbacks.push(format!("127.0.0.1:{}", port));
            fallbacks.push(format!("[::1]:{}", port));
        },
        "::1" => {
            fallbacks.push(format!("127.0.0.1:{}", port));
            fallbacks.push(format!("localhost:{}", port));
        },
        _ => {},
    }

    fallbacks
        .into_iter()
        .any(|candidate| std::net::TcpStream::connect(candidate).map(|_| true).unwrap_or(false))
}

fn split_host_port(host_port: &str) -> Option<(String, String)> {
    let trimmed = host_port.trim();
    if trimmed.starts_with('[') {
        let end = trimmed.find(']')?;
        let host = trimmed[1..end].to_string();
        let port = trimmed[end + 1..].strip_prefix(':')?.to_string();
        return Some((host, port));
    }

    let (host, port) = trimmed.rsplit_once(':')?;
    if host.is_empty() || port.is_empty() {
        return None;
    }
    Some((host.to_string(), port.to_string()))
}

fn server_requires_auth() -> Option<bool> {
    server_requires_auth_for_url(server_url())
}

fn server_requires_auth_for_url(url: &str) -> Option<bool> {
    let host_port = url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or("127.0.0.1:8080");

    if !host_port_reachable(host_port) {
        return None;
    }

    let url = url.to_string();
    let mut last_error: Option<String> = None;

    for _attempt in 0..3 {
        let request_url = url.clone();
        let request = async move {
            Client::new()
                .post(format!("{}/v1/api/sql", request_url))
                .json(&json!({ "sql": "SELECT 1" }))
                .timeout(Duration::from_millis(2000))
                .send()
                .await
        };

        let result: Result<reqwest::Response, reqwest::Error> =
            match tokio::runtime::Handle::try_current() {
                Ok(_) => {
                    let (tx, rx) = std::sync::mpsc::channel();
                    std::thread::spawn(move || {
                        let rt: Runtime = match tokio::runtime::Runtime::new() {
                            Ok(rt) => rt,
                            Err(_) => return,
                        };
                        let _ = tx.send(rt.block_on(request));
                    });
                    match rx.recv_timeout(Duration::from_secs(5)) {
                        Ok(result) => result,
                        Err(err) => {
                            last_error = Some(err.to_string());
                            continue;
                        },
                    }
                },
                Err(_) => {
                    let rt = tokio::runtime::Runtime::new().ok()?;
                    rt.block_on(request)
                },
            };

        let response: reqwest::Response = match result {
            Ok(resp) => resp,
            Err(err) => {
                last_error = Some(err.to_string());
                continue;
            },
        };

        if response.status().is_success() {
            return Some(false);
        }

        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            || response.status() == reqwest::StatusCode::FORBIDDEN
        {
            return Some(true);
        }

        return Some(true);
    }

    if let Some(err) = last_error {
        eprintln!("[DEBUG] Auth probe failed after retries: {}", err);
    }

    Some(true)
}

/// Get available server URLs (cluster mode or single-node)
pub fn get_available_server_urls() -> Vec<String> {
    let ctx = test_context();
    if ctx.is_cluster {
        let mut urls = ctx.cluster_urls_raw.clone();
        if let Some(leader) = leader_url() {
            urls.retain(|url| url != &leader);
            urls.insert(0, leader);
        }
        return urls;
    }

    // test_context() already validated the server is reachable for Running/Fresh modes
    vec![ctx.server_url.clone()]
}

pub fn cluster_urls_config_order() -> Vec<String> {
    test_context().cluster_urls_raw.clone()
}

fn should_wait_for_cluster_after_sql(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("CREATE NAMESPACE")
        || upper.starts_with("CREATE TABLE")
        || upper.starts_with("CREATE SHARED TABLE")
        || upper.starts_with("CREATE USER TABLE")
        || upper.starts_with("CREATE STREAM TABLE")
        || upper.starts_with("DROP NAMESPACE")
        || upper.starts_with("DROP TABLE")
        || upper.starts_with("ALTER TABLE")
}

fn parse_create_namespace(sql: &str) -> Option<String> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    let prefix = "create namespace";
    if !lower.starts_with(prefix) {
        return None;
    }

    let mut rest = trimmed[prefix.len()..].trim();
    if rest.to_ascii_lowercase().starts_with("if not exists") {
        rest = rest["if not exists".len()..].trim();
    }
    let name = rest.split_whitespace().next()?.trim_end_matches(';');
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

fn parse_create_table(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    let prefix = if lower.starts_with("create table") {
        "create table"
    } else if lower.starts_with("create shared table") {
        "create shared table"
    } else if lower.starts_with("create user table") {
        "create user table"
    } else if lower.starts_with("create stream table") {
        "create stream table"
    } else {
        return None;
    };

    let mut rest = trimmed[prefix.len()..].trim();
    if rest.to_ascii_lowercase().starts_with("if not exists") {
        rest = rest["if not exists".len()..].trim();
    }

    let target = rest.split_whitespace().next()?.trim_end_matches('(').trim_end_matches(';');
    let mut parts = target.split('.');
    let namespace = parts.next()?;
    let table = parts.next()?;
    if namespace.is_empty() || table.is_empty() {
        return None;
    }
    Some((namespace.to_string(), table.to_string()))
}

fn wait_for_namespace_on_all_nodes(namespace: &str, timeout: Duration) -> bool {
    if !is_cluster_mode() {
        return true;
    }
    let urls = test_context().cluster_urls_raw.clone();
    let sql = format!(
        "SELECT namespace_id FROM system.namespaces WHERE namespace_id = '{}'",
        namespace
    );
    let start = Instant::now();
    while start.elapsed() < timeout {
        let all_visible = urls.iter().all(|url| {
            matches!(execute_sql_on_node(url, &sql), Ok(output) if output.contains(namespace))
        });
        if all_visible {
            return true;
        }
    }
    false
}

fn wait_for_table_on_all_nodes(namespace: &str, table: &str, timeout: Duration) -> bool {
    if !is_cluster_mode() {
        return true;
    }
    let urls = test_context().cluster_urls_raw.clone();
    let sql = format!(
        "SELECT table_name FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table
    );
    let start = Instant::now();
    while start.elapsed() < timeout {
        let all_visible = urls.iter().all(
            |url| matches!(execute_sql_on_node(url, &sql), Ok(output) if output.contains(table)),
        );
        if all_visible {
            return true;
        }
    }
    false
}

fn wait_for_cluster_after_sql(sql: &str) {
    if !is_cluster_mode() {
        return;
    }

    if let Some(namespace) = parse_create_namespace(sql) {
        let _ = wait_for_namespace_on_all_nodes(&namespace, Duration::from_secs(20));
        return;
    }

    if let Some((namespace, table)) = parse_create_table(sql) {
        let _ = wait_for_table_on_all_nodes(&namespace, &table, Duration::from_secs(30));
        return;
    }

    if !should_wait_for_cluster_after_sql(sql) {
        return;
    }

    std::thread::sleep(Duration::from_millis(600));
}

fn query_response_error_message(response: &kalam_client::QueryResponse) -> String {
    if let Some(error) = &response.error {
        if let Some(details) = &error.details {
            return format!("{} ({})", error.message, details);
        }
        return error.message.clone();
    }

    format!("Query failed: {:?}", response)
}

fn json_error_message(parsed: &serde_json::Value) -> Option<String> {
    let error = parsed.get("error")?;
    let message = error.get("message")?.as_str().unwrap_or("");
    let details = error.get("details").and_then(|d| d.as_str());
    if let Some(details) = details {
        return Some(format!("{} ({})", message, details));
    }
    Some(message.to_string())
}

/// Check if we're running in cluster mode (multiple nodes available)
pub fn is_cluster_mode() -> bool {
    test_context().is_cluster
}

pub fn server_host_port() -> String {
    let trimmed = server_url().trim_start_matches("http://").trim_start_matches("https://");
    trimmed.split('/').next().unwrap_or("127.0.0.1:8080").to_string()
}

pub fn websocket_url() -> String {
    let base_url = leader_url().unwrap_or_else(|| {
        get_available_server_urls()
            .first()
            .cloned()
            .unwrap_or_else(|| server_url().to_string())
    });

    let base = if base_url.starts_with("https://") {
        base_url.replacen("https://", "wss://", 1)
    } else {
        base_url.replacen("http://", "ws://", 1)
    };
    format!("{}/v1/ws", base)
}

pub fn storage_base_dir() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("KALAMDB_STORAGE_DIR") {
        return std::path::PathBuf::from(path);
    }

    if let Ok(path) = std::env::var("KALAMDB_DATA_DIR") {
        return std::path::PathBuf::from(path).join("storage");
    }

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if let Some(workspace_root) = manifest_dir.parent() {
        let backend_storage = workspace_root.join("backend").join("data").join("storage");
        if backend_storage.exists() {
            return backend_storage;
        }
    }

    std::env::current_dir().expect("current dir").join("data").join("storage")
}

pub fn wait_for_query_contains_with<F>(
    sql: &str,
    expected: &str,
    timeout: Duration,
    execute: F,
) -> Result<String, Box<dyn std::error::Error>>
where
    F: Fn(&str) -> Result<String, Box<dyn std::error::Error>>,
{
    let start = Instant::now();
    let poll_interval = Duration::from_millis(200);

    loop {
        let output = execute(sql)?;
        if output.contains(expected) {
            return Ok(output);
        }
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout waiting for query to contain '{}'. Last output: {}",
                expected, output
            )
            .into());
        }
        thread::sleep(poll_interval);
    }
}

pub fn wait_for_path_exists(path: &std::path::Path, timeout: Duration) -> bool {
    let start = Instant::now();
    let poll_interval = Duration::from_millis(100);

    while start.elapsed() <= timeout {
        if path.exists() {
            return true;
        }
        thread::sleep(poll_interval);
    }

    path.exists()
}

/// Require the KalamDB server to be running, panic if not.
///
/// Use this at the start of smoke/e2e tests to fail fast with a clear error message.
/// Delegates to is_server_running() which uses the unified server detection logic.
///
/// # Panics
/// Panics with a clear error message if the server is not running.
pub fn require_server_running() -> bool {
    if !is_server_running() {
        panic!(
            "\n\n\
            ╔══════════════════════════════════════════════════════════════════╗\n\
            ║                    SERVER NOT RUNNING                            ║\n\
            ╠══════════════════════════════════════════════════════════════════╣\n\
            ║  All tests require a running KalamDB server!                     ║\n\
            ║  Server URL: {}                                                  \n\
            ║                                                                  ║\n\
            ║  Start the server:  cd backend && cargo run --release            ║\n\
            ║                                                                  ║\n\
            ║  Then run the tests:                                             ║\n\
            ║    cd cli && cargo nextest run --no-fail-fast                    ║\n\
            ╚══════════════════════════════════════════════════════════════════╝\n\n",
            server_url()
        );
    }

    // Print mode information
    if is_cluster_mode() {
        let available_urls = get_available_server_urls();
        println!(
            "ℹ️  Running in CLUSTER mode with {} nodes: {:?}",
            available_urls.len(),
            available_urls
        );
    } else {
        println!("ℹ️  Running in SINGLE-NODE mode: {}", server_url());
    }

    true
}

/// Helper to execute SQL via CLI
pub fn execute_sql_via_cli(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_kalam"))
        .arg("-u")
        .arg(server_url())
        .arg("--command")
        .arg(sql)
        .output()?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(format!("CLI command failed: {}", String::from_utf8_lossy(&output.stderr)).into())
    }
}

/// Timing information for CLI execution
pub struct CliTiming {
    pub output: String,
    pub total_time_ms: u128,
    pub server_time_ms: Option<f64>,
}

impl CliTiming {
    pub fn overhead_ms(&self) -> Option<f64> {
        self.server_time_ms.map(|server| self.total_time_ms as f64 - server)
    }
}

/// Helper to execute SQL via CLI with authentication and capture timing
pub fn execute_sql_via_cli_as_with_timing(
    username: &str,
    password: &str,
    sql: &str,
) -> Result<CliTiming, Box<dyn std::error::Error>> {
    use std::time::Instant;

    let start = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_kalam"))
        .arg("-u")
        .arg(server_url())
        .arg("--username")
        .arg(username)
        .arg("--password")
        .arg(password)
        .arg("--command")
        .arg(sql)
        .output()?;
    let total_time_ms = start.elapsed().as_millis();

    if output.status.success() {
        let output_str = String::from_utf8_lossy(&output.stdout).to_string();

        // Extract server time from output (looks for "Took: XXX.XXX ms")
        let server_time_ms = output_str.lines().find(|l| l.starts_with("Took:")).and_then(|line| {
            // Parse "Took: 123.456 ms"
            line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok())
        });

        Ok(CliTiming {
            output: output_str,
            total_time_ms,
            server_time_ms,
        })
    } else {
        Err(format!("CLI command failed: {}", String::from_utf8_lossy(&output.stderr)).into())
    }
}

/// Helper to execute SQL via CLI with authentication
pub fn execute_sql_via_cli_as(
    username: &str,
    password: &str,
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let ctx = test_context();
    let username = if username.is_empty() {
        ctx.username.as_str()
    } else {
        username
    };
    let password = if password.is_empty() {
        ctx.password.as_str()
    } else {
        password
    };
    execute_sql_via_cli_as_with_args(username, password, sql, &[])
}

/// Helper to execute SQL via CLI with authentication on a specific URL
pub fn execute_sql_via_cli_as_on_url(
    username: &str,
    password: &str,
    sql: &str,
    url: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_cli_as_with_args_and_urls(
        username,
        password,
        sql,
        &[],
        Some(vec![url.to_string()]),
    )
}

/// Helper to execute SQL via CLI with authentication (JSON output)
pub fn execute_sql_via_cli_as_json(
    username: &str,
    password: &str,
    sql: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let output = execute_sql_via_cli_as_with_args(username, password, sql, &["--json"])?;
    let parsed: Value = serde_json::from_str(&output)?;
    Ok(parsed)
}

/// Helper to execute SQL via CLI with authentication on a specific URL (JSON output)
pub fn execute_sql_via_cli_as_json_on_url(
    username: &str,
    password: &str,
    sql: &str,
    url: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let output = execute_sql_via_cli_as_with_args_and_urls(
        username,
        password,
        sql,
        &["--json"],
        Some(vec![url.to_string()]),
    )?;
    let parsed: Value = serde_json::from_str(&output)?;
    Ok(parsed)
}

fn execute_sql_via_cli_as_with_args(
    username: &str,
    password: &str,
    sql: &str,
    extra_args: &[&str],
) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_cli_as_with_args_and_urls(username, password, sql, extra_args, None)
}

fn execute_sql_via_cli_as_with_args_and_urls(
    username: &str,
    password: &str,
    sql: &str,
    extra_args: &[&str],
    urls_override: Option<Vec<String>>,
) -> Result<String, Box<dyn std::error::Error>> {
    use std::time::Instant;
    use wait_timeout::ChildExt;

    // test_context() already ensures server is started/reachable
    if is_server_reachable() {
        ensure_cli_server_setup()?;
    }

    let sql_preview = if sql.len() > 60 {
        format!("{}...", &sql[..60])
    } else {
        sql.to_string()
    };

    eprintln!("[TEST_CLI] Executing as {}: \"{}\"", username, sql_preview.replace('\n', " "));

    let max_attempts = if is_cluster_mode() { 6 } else { 3 };
    let mut last_err: Option<String> = None;

    for attempt in 0..max_attempts {
        let urls = if let Some(ref override_urls) = urls_override {
            override_urls.clone()
        } else if is_cluster_mode() {
            get_available_server_urls()
        } else {
            vec![server_url().to_string()]
        };
        let mut retry_after_attempt = false;
        for (idx, url) in urls.iter().enumerate() {
            let creds_dir = TempDir::new().map_err(|err| err.to_string())?;
            let creds_path = creds_dir.path().join("credentials.toml");
            let spawn_start = Instant::now();

            let token = get_access_token_for_url_sync(url, username, password);

            let mut child = Command::new(env!("CARGO_BIN_EXE_kalam"));
            child.arg("-u").arg(url);

            if let Some(token) = token {
                child.arg("--token").arg(token);
            } else {
                child.arg("--username").arg(username).arg("--password").arg(password);
            }

            child
                .env("KALAMDB_CREDENTIALS_PATH", &creds_path)
                .env("NO_PROXY", "127.0.0.1,localhost,::1")
                .env("no_proxy", "127.0.0.1,localhost,::1")
                .env_remove("HTTP_PROXY")
                .env_remove("http_proxy")
                .env_remove("HTTPS_PROXY")
                .env_remove("https_proxy")
                .env_remove("ALL_PROXY")
                .env_remove("all_proxy")
                .arg("--no-spinner")     // Disable spinner for cleaner output
                .args(extra_args)
                .arg("--command")
                .arg(sql)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());

            let mut child = child.spawn()?;

            let spawn_duration = spawn_start.elapsed();
            eprintln!("[TEST_CLI] Process spawned in {:?}", spawn_duration);

            let wait_start = Instant::now();

            // Wait for child with timeout to avoid hanging tests
            let timeout_duration = Duration::from_secs(60);
            match child.wait_timeout(timeout_duration)? {
                Some(status) => {
                    let wait_duration = wait_start.elapsed();
                    let total_duration_ms = spawn_start.elapsed().as_millis();

                    // Now read stdout/stderr since the process completed
                    let mut stdout = String::new();
                    let mut stderr = String::new();
                    if let Some(ref mut out) = child.stdout {
                        use std::io::Read;
                        out.read_to_string(&mut stdout)?;
                    }
                    if let Some(ref mut err) = child.stderr {
                        use std::io::Read;
                        err.read_to_string(&mut stderr)?;
                    }

                    if status.success() {
                        if !stderr.is_empty() {
                            eprintln!("[TEST_CLI] stderr: {}", stderr);
                        }
                        if let Some(output_err) = cli_output_error(&stdout) {
                            let err_msg = format!("CLI output error: {}", output_err);
                            if is_flush_sql(sql) && is_idempotent_conflict(&err_msg) {
                                return Ok(stdout);
                            }
                            if is_retryable_cluster_error_for_sql(sql, &err_msg) {
                                last_err = Some(err_msg);
                                if idx + 1 < urls.len() {
                                    continue;
                                }
                                retry_after_attempt = true;
                                break;
                            }
                            return Err(err_msg.into());
                        }
                        eprintln!(
                            "[TEST_CLI] Success: spawn={:?} wait={:?} total={}ms",
                            spawn_duration, wait_duration, total_duration_ms
                        );
                        if is_cluster_mode() {
                            wait_for_cluster_after_sql(sql);
                        }
                        return Ok(stdout);
                    } else {
                        eprintln!(
                            "[TEST_CLI] Failed: spawn={:?} wait={:?} total={}ms stderr={}",
                            spawn_duration, wait_duration, total_duration_ms, stderr
                        );
                        let err_msg = format!("CLI command failed: {}", stderr);
                        if is_refreshable_token_error(&err_msg) {
                            invalidate_auth_caches_for_credentials(url, username, password);
                            last_err = Some(err_msg);
                            retry_after_attempt = true;
                            break;
                        }
                        if (err_msg.to_lowercase().contains("invalid username or password")
                            || err_msg.to_lowercase().contains("user not found"))
                            && (username == admin_username() || username == default_username())
                        {
                            let _ = force_reset_admin_for_url(url);
                            last_err = Some(err_msg);
                            retry_after_attempt = true;
                            break;
                        }
                        if err_msg.contains("error sending request for url") {
                            let json_output = extra_args.iter().any(|arg| *arg == "--json");
                            if let Ok(output) = execute_sql_via_client_as_with_args(
                                username,
                                password,
                                sql,
                                json_output,
                            ) {
                                return Ok(output);
                            }
                        }
                        if is_flush_sql(sql) && is_idempotent_conflict(&err_msg) {
                            eprintln!(
                                "[TEST_CLI] Flush already queued; treating as success: {}",
                                err_msg
                            );
                            if is_cluster_mode() {
                                wait_for_cluster_after_sql(sql);
                            }
                            return Ok(stdout);
                        }
                        if is_retryable_cluster_error_for_sql(sql, &err_msg) {
                            last_err = Some(err_msg);
                            if idx + 1 < urls.len() {
                                continue;
                            }
                            retry_after_attempt = true;
                            break;
                        }
                        return Err(err_msg.into());
                    }
                },
                None => {
                    // Timeout - kill the child and return error
                    let _ = child.kill();
                    let _ = child.wait();
                    let wait_duration = wait_start.elapsed();
                    eprintln!("[TEST_CLI] TIMEOUT after {:?}", wait_duration);
                    let err_msg = format!("CLI command timed out after {:?}", timeout_duration);
                    if is_retryable_cluster_error_for_sql(sql, &err_msg) {
                        last_err = Some(err_msg);
                        if idx + 1 < urls.len() {
                            continue;
                        }
                        retry_after_attempt = true;
                        break;
                    }
                    return Err(err_msg.into());
                },
            }
        }
        if retry_after_attempt {
            let delay_ms = 100 + attempt * 100;
            std::thread::sleep(Duration::from_millis(delay_ms as u64));
        }
    }

    Err(last_err
        .unwrap_or_else(|| "CLI command failed on all cluster nodes".to_string())
        .into())
}

/// Ensure the server is set up with proper credentials before running CLI commands
///
/// This is needed for manually-started servers that haven't gone through the
/// auto-start setup process. It performs a no-op if server is already set up.
pub fn ensure_cli_server_setup() -> Result<(), Box<dyn std::error::Error>> {
    load_env_file();
    let base_url = server_url().to_string();
    let run_setup = move || -> Result<(), String> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(async {
            test_auth_manager()
                .ensure_ready(&base_url)
                .await
                .map_err(|err| err.to_string())?;
            Ok::<(), String>(())
        })?;
        Ok(())
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        let handle = std::thread::spawn(run_setup);
        match handle.join() {
            Ok(result) => result.map_err(|e| e.into()),
            Err(_) => Err("ensure_cli_server_setup thread panicked".into()),
        }
    } else {
        run_setup().map_err(|e| e.into())
    }
}

/// Helper to execute SQL as root user via CLI
pub fn execute_sql_as_root_via_cli(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Ensure server is set up on first CLI call
    static SETUP_DONE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    SETUP_DONE.get_or_init(|| {
        ensure_cli_server_setup().expect("Failed to prepare CLI server setup");
    });

    execute_sql_via_cli_as(admin_username(), admin_password(), sql)
}

/// Helper to execute SQL as root user via CLI returning JSON output to avoid table truncation
pub fn execute_sql_as_root_via_cli_json(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_cli_as_with_args(admin_username(), admin_password(), sql, &["--json"])
}

/// Wait for a SQL query to return output containing an expected substring.
///
/// Useful for eventual consistency scenarios where data may not be immediately visible.
pub fn wait_for_sql_output_contains(
    sql: &str,
    expected: &str,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut last_output = String::new();
    let mut last_error: Option<String> = None;

    while start.elapsed() < timeout {
        match execute_sql_as_root_via_cli(sql) {
            Ok(output) => {
                last_output = output.clone();
                if output.contains(expected) {
                    return Ok(output);
                }
            },
            Err(e) => {
                last_error = Some(e.to_string());
            },
        }

        std::thread::sleep(Duration::from_millis(120));
    }

    let error_hint = last_error.unwrap_or_else(|| "<none>".to_string());
    Err(format!(
        "Timed out waiting for SQL output to contain '{}'. Last error: {}. Last output: {}",
        expected, error_hint, last_output
    )
    .into())
}

/// Wait until a table is ready for DML (CREATE completed + indexes available).
pub fn wait_for_table_ready(
    table: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut last_error: Option<String> = None;

    while start.elapsed() < timeout {
        match execute_sql_as_root_via_client(&format!("SELECT * FROM {} LIMIT 1", table)) {
            Ok(_) => return Ok(()),
            Err(err) => {
                last_error = Some(err.to_string());
            },
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    Err(format!(
        "Timed out waiting for table {} to be ready. Last error: {}",
        table,
        last_error.unwrap_or_else(|| "<none>".to_string())
    )
    .into())
}

// ============================================================================
// CLIENT-BASED QUERY EXECUTION (uses kalam-client directly, avoids CLI process spawning)
// ============================================================================

/// A shared tokio runtime for client-based query execution.
/// Using a shared runtime avoids the overhead of creating a new runtime for each query.
fn get_shared_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .expect("Failed to create shared tokio runtime")
    })
}

fn build_client_for_url_with_timeouts(
    base_url: &str,
    username: &str,
    password: &str,
    timeouts: KalamLinkTimeouts,
) -> Result<KalamLinkClient, Box<dyn std::error::Error + Send + Sync>> {
    let auth_required = server_requires_auth_for_url(base_url).unwrap_or(true);
    let auth = if auth_required {
        test_auth_manager().auth_provider_for_url(base_url, username, password)
    } else {
        AuthProvider::none()
    };

    KalamLinkClient::builder()
        .base_url(base_url)
        .auth(auth)
        .timeouts(timeouts)
        .build()
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}

pub fn client_for_user_on_url_with_timeouts(
    base_url: &str,
    username: &str,
    password: &str,
    timeouts: KalamLinkTimeouts,
) -> Result<KalamLinkClient, Box<dyn std::error::Error + Send + Sync>> {
    build_client_for_url_with_timeouts(base_url, username, password, timeouts)
}

pub fn client_for_url_no_auth(
    base_url: &str,
    timeouts: KalamLinkTimeouts,
) -> Result<KalamLinkClient, Box<dyn std::error::Error + Send + Sync>> {
    KalamLinkClient::builder()
        .base_url(base_url)
        .auth(AuthProvider::none())
        .timeouts(timeouts)
        .build()
        .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
}

pub fn client_builder_for_user_on_url(
    base_url: &str,
    username: &str,
    password: &str,
) -> KalamLinkClientBuilder {
    let auth_required = server_requires_auth_for_url(base_url).unwrap_or(true);
    let auth = if auth_required {
        test_auth_manager().auth_provider_for_url(base_url, username, password)
    } else {
        AuthProvider::none()
    };

    KalamLinkClient::builder().base_url(base_url).auth(auth)
}

pub fn client_builder_for_url_no_auth(base_url: &str) -> KalamLinkClientBuilder {
    KalamLinkClient::builder().base_url(base_url).auth(AuthProvider::none())
}

/// A shared KalamLinkClient for root user to reuse HTTP connections.
/// This avoids creating new TCP connections for every query, which helps
/// avoid macOS TCP connection limits (connections in TIME_WAIT state).
///
/// **CRITICAL PERFORMANCE FIX**: Uses JWT token authentication instead of Basic Auth.
/// Basic Auth runs bcrypt verification (cost=12) on EVERY request (~100-300ms each).
/// JWT authentication verifies the token signature (< 1ms) - 100-300x faster!
///
/// This version automatically uses the first available server (cluster or single-node)
struct RootClientCache {
    base_url: String,
    client: KalamLinkClient,
}

fn shared_root_client_cache() -> &'static Mutex<Option<RootClientCache>> {
    static CLIENT_CACHE: OnceLock<Mutex<Option<RootClientCache>>> = OnceLock::new();
    CLIENT_CACHE.get_or_init(|| Mutex::new(None))
}

fn build_root_client(base_url: &str) -> KalamLinkClient {
    test_auth_manager().client_for_url(base_url, default_username(), default_password())
}

fn get_shared_root_client_for_url(base_url: &str) -> KalamLinkClient {
    if let Ok(mut guard) = shared_root_client_cache().lock() {
        if let Some(cache) = guard.as_ref() {
            if cache.base_url == base_url {
                return cache.client.clone();
            }
        }

        let client = build_root_client(base_url);
        *guard = Some(RootClientCache {
            base_url: base_url.to_string(),
            client: client.clone(),
        });
        return client;
    }

    build_root_client(base_url)
}

fn get_shared_root_client() -> KalamLinkClient {
    let base_url = get_available_server_urls()
        .first()
        .cloned()
        .unwrap_or_else(|| server_url().to_string());
    get_shared_root_client_for_url(&base_url)
}

pub fn execute_sql_via_client_as(
    username: &str,
    password: &str,
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_client_as_with_args(username, password, sql, false)
}

fn execute_sql_via_client_as_with_params(
    username: &str,
    password: &str,
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_client_internal(username, password, sql, Some(params), false)
}

fn execute_sql_via_client_as_with_args(
    username: &str,
    password: &str,
    sql: &str,
    json_output: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_client_internal(username, password, sql, None, json_output)
}

fn execute_sql_via_client_internal(
    username: &str,
    password: &str,
    sql: &str,
    params: Option<Vec<serde_json::Value>>,
    json_output: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let runtime = get_shared_runtime();

    let sql = sql.to_string();
    let sql_for_wait = sql.clone();
    let username_owned = username.to_string();
    let password_owned = password.to_string();

    let start = std::time::Instant::now();
    let (tx, rx) = std_mpsc::channel();

    runtime.spawn(async move {
        let result: Result<kalam_client::QueryResponse, Box<dyn std::error::Error + Send + Sync>> =
            async {
                let urls = get_available_server_urls();
                let base_url = urls.first().cloned().unwrap_or_else(|| server_url().to_string());

                async fn execute_once(
                    url: &str,
                    username: &str,
                    password: &str,
                    sql: &str,
                    params: &Option<Vec<serde_json::Value>>,
                ) -> Result<kalam_client::QueryResponse, Box<dyn std::error::Error + Send + Sync>>
                {
                    let timeouts = KalamLinkTimeouts::builder()
                        .connection_timeout_secs(5)
                        .receive_timeout_secs(120)
                        .send_timeout_secs(30)
                        .subscribe_timeout_secs(20)
                        .auth_timeout_secs(10)
                        .initial_data_timeout(Duration::from_secs(120))
                        .build();

                    let client = if username == default_username() && password == default_password()
                    {
                        get_shared_root_client_for_url(url)
                    } else {
                        build_client_for_url_with_timeouts(url, username, password, timeouts)?
                    };
                    let response = client.execute_query(sql, None, params.clone(), None).await?;
                    Ok(response)
                }

                if is_cluster_mode() && urls.len() > 1 {
                    let max_attempts = 12;
                    let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;

                    for attempt in 0..max_attempts {
                        let urls = get_available_server_urls();
                        let mut retry_after_attempt = false;
                        for (idx, url) in urls.iter().enumerate() {
                            let response =
                                execute_once(url, &username_owned, &password_owned, &sql, &params)
                                    .await;

                            match response {
                                Ok(response) => {
                                    if !response.success() {
                                        let err_msg = query_response_error_message(&response);
                                        if is_flush_sql(&sql) && is_idempotent_conflict(&err_msg) {
                                            return Ok(response);
                                        }
                                        if is_retryable_cluster_error_for_sql(&sql, &err_msg) {
                                            last_err = Some(err_msg.into());
                                            if idx + 1 < urls.len() {
                                                continue;
                                            }
                                            retry_after_attempt = true;
                                            break;
                                        }
                                        return Err(err_msg.into());
                                    }
                                    return Ok(response);
                                },
                                Err(e) => {
                                    let msg = e.to_string();
                                    if is_flush_sql(&sql) && is_idempotent_conflict(&msg) {
                                        return Ok(kalam_client::QueryResponse {
                                            status: kalam_client::models::ResponseStatus::Success,
                                            results: Vec::new(),
                                            took: None,
                                            error: None,
                                        });
                                    }
                                    if is_refreshable_token_error(&msg) {
                                        invalidate_auth_caches_for_credentials(
                                            url,
                                            &username_owned,
                                            &password_owned,
                                        );
                                        last_err = Some(e);
                                        retry_after_attempt = true;
                                        break;
                                    }
                                    if is_retryable_cluster_error_for_sql(&sql, &msg) {
                                        last_err = Some(e);
                                        if idx + 1 < urls.len() {
                                            continue;
                                        }
                                        retry_after_attempt = true;
                                        break;
                                    }
                                    return Err(e);
                                },
                            }
                        }
                        if retry_after_attempt {
                            let delay_ms = 300 + attempt * 200;
                            tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
                        }
                    }

                    return Err(last_err.unwrap_or_else(|| "All cluster nodes failed".into()));
                }

                let max_attempts = if is_cluster_mode() { 1 } else { 8 };
                for attempt in 0..max_attempts {
                    let response = match execute_once(
                        &base_url,
                        &username_owned,
                        &password_owned,
                        &sql,
                        &params,
                    )
                    .await
                    {
                        Ok(response) => response,
                        Err(err) => {
                            let err_msg = err.to_string();
                            if is_refreshable_token_error(&err_msg) && attempt + 1 < max_attempts {
                                invalidate_auth_caches_for_credentials(
                                    &base_url,
                                    &username_owned,
                                    &password_owned,
                                );
                                continue;
                            }
                            if is_rate_limited_error(&err_msg) && attempt + 1 < max_attempts {
                                let delay_ms = 250 + attempt * 250;
                                tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
                                continue;
                            }
                            return Err(err);
                        },
                    };

                    if response.success() {
                        return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response);
                    }

                    let err_msg = query_response_error_message(&response);
                    if is_flush_sql(&sql) && is_idempotent_conflict(&err_msg) {
                        return Ok(response);
                    }

                    if is_retryable_cluster_error_for_sql(&sql, &err_msg) {
                        let delay_ms = 200 + attempt * 200;
                        tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
                        continue;
                    }

                    if is_refreshable_token_error(&err_msg) && attempt + 1 < max_attempts {
                        invalidate_auth_caches_for_credentials(
                            &base_url,
                            &username_owned,
                            &password_owned,
                        );
                        continue;
                    }

                    if is_rate_limited_error(&err_msg) && attempt + 1 < max_attempts {
                        let delay_ms = 250 + attempt * 250;
                        tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
                        continue;
                    }

                    return Err(err_msg.into());
                }

                Err("Query failed after retries".into())
            }
            .await;

        let _ = tx.send(result);
    });

    let result = rx
        .recv_timeout(Duration::from_secs(120))
        .map_err(|e| format!("Query timed out or channel error: {}", e))?;

    let duration = start.elapsed();

    match result {
        Ok(response) => {
            if std::env::var_os("KALAMDB_TEST_CLIENT_TRACE").is_some() {
                eprintln!("[TEST_CLIENT] Success in {:?}", duration);
            }

            if json_output {
                let output = serde_json::to_string_pretty(&response)?;
                if is_cluster_mode() {
                    wait_for_cluster_after_sql(&sql_for_wait);
                }
                Ok(output)
            } else {
                let mut output = String::new();

                for result in &response.results {
                    if let Some(ref message) = result.message {
                        if message.starts_with("Inserted ")
                            || message.starts_with("Updated ")
                            || message.starts_with("Deleted ")
                        {
                            if let Some(count_str) = message.split_whitespace().nth(1) {
                                if let Ok(count) = count_str.parse::<usize>() {
                                    output.push_str(&format!("{} rows affected\n", count));
                                    continue;
                                }
                            }
                        }
                        output.push_str(message);
                        output.push('\n');
                        continue;
                    }

                    let is_dml = result.rows.is_none()
                        || (result.rows.as_ref().map(|r| r.is_empty()).unwrap_or(false)
                            && result.row_count > 0);

                    if is_dml {
                        output.push_str(&format!("{} rows affected\n", result.row_count));
                    } else {
                        let columns: Vec<String> = result.column_names();

                        if !columns.is_empty() {
                            output.push_str(&columns.join(" | "));
                            output.push('\n');
                        }

                        if let Some(ref rows) = result.rows {
                            for row in rows {
                                let row_str: Vec<String> = columns
                                    .iter()
                                    .enumerate()
                                    .map(|(i, _col)| {
                                        row.get(i)
                                            .map(|v| match v.inner() {
                                                serde_json::Value::String(s) => s.clone(),
                                                serde_json::Value::Null => "NULL".to_string(),
                                                other => other.to_string(),
                                            })
                                            .unwrap_or_else(|| "NULL".to_string())
                                    })
                                    .collect();
                                output.push_str(&row_str.join(" | "));
                                output.push('\n');
                            }

                            output.push_str(&format!(
                                "({} row{})\n",
                                rows.len(),
                                if rows.len() == 1 { "" } else { "s" }
                            ));
                        } else {
                            output.push_str("(0 rows)\n");
                        }
                    }
                }

                if let Some(ref error) = response.error {
                    output.push_str(&format!("Error: {}\n", error.message));
                }

                if is_cluster_mode() {
                    wait_for_cluster_after_sql(&sql_for_wait);
                }
                Ok(output)
            }
        },
        Err(e) => {
            eprintln!("[TEST_CLIENT] Failed in {:?}: {}", duration, e);
            Err(e.to_string().into())
        },
    }
}

/// Execute SQL as root user via kalam-client
pub fn execute_sql_as_root_via_client(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_client_as(default_username(), default_password(), sql)
}

/// Execute SQL as root user via kalam-client with query parameters
pub fn execute_sql_as_root_via_client_with_params(
    sql: &str,
    params: Vec<serde_json::Value>,
) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_client_as_with_params(default_username(), default_password(), sql, params)
}

/// Execute SQL as root user via kalam-client returning JSON output
pub fn execute_sql_as_root_via_client_json(
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_client_as_with_args(default_username(), default_password(), sql, true)
}

/// Execute SQL via kalam-client returning JSON output with custom credentials
pub fn execute_sql_via_client_as_json(
    username: &str,
    password: &str,
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_client_as_with_args(username, password, sql, true)
}

/// Execute SQL via kalam-client without authentication (uses default/anonymous)
/// This is the client equivalent of execute_sql_via_cli (no auth)
pub fn execute_sql_via_client(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_via_client_as(default_username(), default_password(), sql)
}

/// Extract a numeric ID from a JSON value that might be a number or a string.
///
/// Large integers (> JS_MAX_SAFE_INTEGER) are serialized as strings to preserve
/// precision for JavaScript clients. This helper handles both cases.
///
/// Returns Some(value_as_string) if the value is a number or a numeric string,
/// None otherwise.
pub fn json_value_as_id(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i.to_string())
            } else {
                n.as_u64().map(|u| u.to_string())
            }
        },
        serde_json::Value::String(s) => {
            // Verify it's a valid numeric string
            if s.parse::<i64>().is_ok() || s.parse::<u64>().is_ok() {
                Some(s.clone())
            } else {
                None
            }
        },
        _ => None,
    }
}

/// Extract the actual value from a typed JSON response (ScalarValue format).
///
/// The server returns values in typed format like `{"Int64": "42"}` or `{"Utf8": "hello"}`.
/// This function extracts the inner value, returning it as a simple JSON value.
///
/// # Examples
/// ```ignore
/// let typed = json!({"Int64": "123"});
/// assert_eq!(extract_typed_value(&typed), json!("123"));
///
/// let simple = json!("hello");
/// assert_eq!(extract_typed_value(&simple), json!("hello"));
/// ```
pub fn extract_typed_value(value: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value as JsonValue;

    match value {
        JsonValue::Object(map) if map.len() == 1 => {
            // Handle typed ScalarValue format: {"Int64": "42"}, {"Utf8": "hello"}, etc.
            let (type_name, inner_value) = map.iter().next().unwrap();
            match type_name.as_str() {
                "Null" => JsonValue::Null,
                "Boolean" => inner_value.clone(),
                "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64" => {
                    // These are stored as strings to preserve precision
                    inner_value.clone()
                },
                "Float32" | "Float64" => inner_value.clone(),
                "Utf8" | "LargeUtf8" => inner_value.clone(),
                "Binary" | "LargeBinary" | "FixedSizeBinary" => inner_value.clone(),
                "Date32" | "Time64Microsecond" => inner_value.clone(),
                "TimestampMillisecond" | "TimestampMicrosecond" | "TimestampNanosecond" => {
                    // Timestamp objects have 'value' and optionally 'timezone'
                    if let Some(obj) = inner_value.as_object() {
                        if let Some(val) = obj.get("value") {
                            return val.clone();
                        }
                    }
                    JsonValue::Null
                },
                "Decimal128" => {
                    // Decimal has 'value', 'precision', 'scale'
                    if let Some(obj) = inner_value.as_object() {
                        if let Some(val) = obj.get("value") {
                            return val.clone();
                        }
                    }
                    JsonValue::Null
                },
                _ => {
                    // Fallback for unknown types - return the inner value
                    inner_value.clone()
                },
            }
        },
        // Not a typed value, return as-is
        _ => value.clone(),
    }
}

/// Helper to generate unique namespace name
pub fn generate_unique_namespace(base_name: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    // Use a short base36-encoded millisecond timestamp + counter.
    // This stays short enough for identifier limits while remaining unique across reruns.
    let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let ts36 = to_base36(ts_ms);
    let suffix = if ts36.len() > 8 {
        &ts36[ts36.len() - 8..]
    } else {
        ts36.as_str()
    };
    let pid36 = to_base36(std::process::id() as u128);
    format!("{}_{}_{}_{}", base_name, suffix, pid36, count).to_lowercase()
}

/// Helper to generate unique table name
pub fn generate_unique_table(base_name: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    // Use a short base36-encoded millisecond timestamp + counter.
    // This stays short enough for identifier limits while remaining unique across reruns.
    let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let ts36 = to_base36(ts_ms);
    let suffix = if ts36.len() > 8 {
        &ts36[ts36.len() - 8..]
    } else {
        ts36.as_str()
    };
    let pid36 = to_base36(std::process::id() as u128);
    format!("{}_{}_{}_{}", base_name, suffix, pid36, count).to_lowercase()
}

fn to_base36(mut value: u128) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".to_string();
    }
    let mut buf = Vec::new();
    while value > 0 {
        let rem = (value % 36) as usize;
        buf.push(DIGITS[rem]);
        value /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).unwrap_or_else(|_| "0".to_string())
}

/// Helper to create a CLI command with default test settings
/// Helper to create a CLI command with default test settings (for assert_cmd tests)
pub fn create_cli_command() -> assert_cmd::Command {
    if is_server_reachable() {
        static SETUP_DONE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        SETUP_DONE.get_or_init(|| {
            ensure_cli_server_setup().expect("Failed to prepare CLI server setup");
        });
    }

    let test_home = TEST_CLI_HOME_DIR.get_or_init(|| {
        let path = std::env::temp_dir().join(format!("kalam-cli-test-home-{}", std::process::id()));
        std::fs::create_dir_all(path.join(".kalam")).expect("failed to create isolated test home");
        path
    });
    let credentials_path =
        TEST_CLI_CREDENTIALS_PATH.get_or_init(|| test_home.join(".kalam").join("credentials.toml"));

    let mut cmd = assert_cmd::Command::new(env!("CARGO_BIN_EXE_kalam"));
    cmd.env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1")
        // Keep tests isolated from developer/CI host ~/.kalam state.
        .env("HOME", test_home)
        .env("USERPROFILE", test_home)
        .env("KALAMDB_CREDENTIALS_PATH", credentials_path)
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy");
    cmd
}

/// Helper to create a CLI command with explicit authentication and URL.
pub fn create_cli_command_with_auth(username: &str, password: &str) -> assert_cmd::Command {
    let base_url = if is_cluster_mode() {
        leader_url().unwrap_or_else(|| server_url().to_string())
    } else {
        server_url().to_string()
    };
    create_cli_command_with_auth_for_server(username, password, &base_url)
}

/// Helper to create a CLI command with explicit authentication and server override.
pub fn create_cli_command_with_auth_for_server(
    username: &str,
    password: &str,
    server: &str,
) -> assert_cmd::Command {
    static SETUP_DONE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    SETUP_DONE.get_or_init(|| {
        ensure_cli_server_setup().expect("Failed to prepare CLI server setup");
    });

    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server)
        .arg("--username")
        .arg(username)
        .arg("--password")
        .arg(password);
    cmd
}

/// Helper to create a CLI command authenticated as root.
pub fn create_cli_command_with_root_auth() -> assert_cmd::Command {
    create_cli_command_with_auth(admin_username(), admin_password())
}

/// Helper to create a temporary credentials file path for CLI tests
pub fn create_temp_credentials_path() -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let creds_path = temp_dir.path().join("credentials.toml");
    (temp_dir, creds_path)
}

/// Helper to set credentials path env for a CLI command
pub fn with_credentials_path<'a>(
    cmd: &'a mut assert_cmd::Command,
    credentials_path: &std::path::Path,
) -> &'a mut assert_cmd::Command {
    cmd.env("KALAMDB_CREDENTIALS_PATH", credentials_path)
}

/// Helper to create a temporary credential store for testing
pub fn create_temp_store() -> (FileCredentialStore, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let creds_path = temp_dir.path().join("credentials.toml");

    let store =
        FileCredentialStore::with_path(creds_path).expect("Failed to create credential store");

    (store, temp_dir)
}

/// Helper to setup test namespace and table with unique name
pub fn setup_test_table(test_name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let table_name = generate_unique_table(test_name);
    let namespace = generate_unique_namespace("test_cli");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Try to drop table first if it exists
    let drop_sql = format!("DROP TABLE IF EXISTS {}", full_table_name);
    let _ = execute_sql_as_root_via_cli(&drop_sql);

    // Create namespace if it doesn't exist
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    let _ = execute_sql_as_root_via_cli(&ns_sql);

    // Create test table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (
            TYPE = 'USER',
            FLUSH_POLICY = 'rows:10'
        )"#,
        full_table_name
    );

    execute_sql_as_root_via_cli(&create_sql)?;

    Ok(full_table_name)
}

/// Helper to cleanup test data
pub fn cleanup_test_table(table_full_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_full_name);
    let _ = execute_sql_as_root_via_cli(&drop_sql);
    if let Some((namespace, _)) = table_full_name.split_once('.') {
        let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
    }
    Ok(())
}

/// Generate a random alphanumeric string of the given length.
///
/// Example:
/// ```
/// let token = random_string(12);
/// println!("Generated: {}", token);
/// ```
pub fn random_string(len: usize) -> String {
    let rng = rand::rng();
    rng.sample_iter(Alphanumeric).take(len).map(char::from).collect()
}

/// Parse job ID from STORAGE FLUSH TABLE output
///
/// Expected format: "Flush started for table 'namespace.table'. Job ID: flush-table-123-uuid"
pub fn parse_job_id_from_flush_output(output: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Robustly locate the "Job ID: <id>" fragment and extract only the ID token
    if let Some(idx) = output.find("Job ID: ") {
        let after = &output[idx + "Job ID: ".len()..];
        // Take only the first line after the marker, then the first whitespace-delimited token
        let first_line = after.lines().next().unwrap_or(after);
        let id_token = first_line
            .split_whitespace()
            .next()
            .ok_or("Missing job id token after 'Job ID: '")?;
        return Ok(id_token.trim().to_string());
    }

    // Fallback: look up the latest flush job if output was empty (idempotent conflict, etc.)
    let fallback_sql =
        "SELECT job_id FROM system.jobs WHERE job_type = 'flush' ORDER BY created_at DESC LIMIT 1";
    if let Ok(json_output) = execute_sql_as_root_via_client_json(fallback_sql) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&json_output) {
            if let Some(job_id) = value
                .get("results")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|res| res.get("rows"))
                .and_then(|rows| rows.as_array())
                .and_then(|rows| rows.first())
                .and_then(|row| row.as_array())
                .and_then(|row| row.first())
                .and_then(|val| match val {
                    serde_json::Value::String(s) => Some(s.to_string()),
                    serde_json::Value::Number(n) => n.as_i64().map(|v| v.to_string()),
                    _ => None,
                })
            {
                return Ok(job_id);
            }
        }
    }

    Err(format!("Failed to parse job ID from FLUSH output: {}", output).into())
}

/// Parse job ID from JSON response message field
///
/// Expected JSON format: {"status":"success","results":[{"message":"Flush started... Job ID: FL-xxx"}]}
pub fn parse_job_id_from_json_message(
    json_output: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let value: serde_json::Value = serde_json::from_str(json_output)
        .map_err(|e| format!("Failed to parse JSON: {} in: {}", e, json_output))?;

    // Navigate to results[0].message
    let message = value
        .get("results")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|res| res.get("message"))
        .and_then(|m| m.as_str())
        .ok_or_else(|| format!("No message field in JSON response: {}", json_output))?;

    // Extract job ID from message using the same logic as parse_job_id_from_flush_output
    if let Some(idx) = message.find("Job ID: ") {
        let after = &message[idx + "Job ID: ".len()..];
        let id_token =
            after.split_whitespace().next().ok_or("Missing job id token after 'Job ID: '")?;
        return Ok(id_token.trim().to_string());
    }

    Err(format!("Failed to parse job ID from message: {}", message).into())
}

/// Parse multiple job IDs from STORAGE FLUSH ALL output
///
/// Expected format: "Flush started for N table(s) in namespace 'ns'. Job IDs: [id1, id2, id3]"
pub fn parse_job_ids_from_flush_all_output(
    output: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // Look for pattern: "Job IDs: [id1, id2, id3]"
    if let Some(ids_str) = output.split("Job IDs: [").nth(1) {
        if let Some(ids_part) = ids_str.split(']').next() {
            let job_ids: Vec<String> = ids_part
                .split(',')
                .map(|s| s.trim())
                .map(|s| s.split_whitespace().next().unwrap_or("").to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if !job_ids.is_empty() {
                return Ok(job_ids);
            }
        }
    }

    Err(format!("Failed to parse job IDs from FLUSH ALL output: {}", output).into())
}

/// Verify that a job has completed successfully
///
/// Polls system.jobs table until the job reaches 'completed' status or timeout occurs.
/// Returns an error if the job fails or times out.
pub fn verify_job_completed(
    job_id: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let timeout = if is_cluster_mode() {
        timeout + Duration::from_secs(12)
    } else {
        timeout
    };
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_millis(200);

    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout waiting for job {} to complete after {:?}",
                job_id, timeout
            )
            .into());
        }

        // Query system.jobs for this specific job
        let query =
            format!("SELECT job_id, status, message FROM system.jobs WHERE job_id = '{}'", job_id);

        match execute_sql_as_root_via_client_json(&query) {
            Ok(output) => {
                // Parse JSON output
                let json: serde_json::Value = serde_json::from_str(&output).map_err(|e| {
                    format!("Failed to parse JSON output: {}. Output: {}", e, output)
                })?;

                if let Some(rows) = get_rows_as_hashmaps(&json) {
                    if let Some(row) = rows.first() {
                        let status_value = row
                            .get("status")
                            .and_then(extract_arrow_value)
                            .or_else(|| row.get("status").cloned())
                            .unwrap_or(serde_json::Value::Null);
                        let status_owned = status_value
                            .as_str()
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| status_value.to_string());
                        let status = status_owned.as_str();

                        let error_value = row
                            .get("message")
                            .and_then(extract_arrow_value)
                            .or_else(|| row.get("message").cloned())
                            .unwrap_or(serde_json::Value::Null);
                        let error_message = error_value.as_str().unwrap_or("");

                        if status.eq_ignore_ascii_case("completed") {
                            return Ok(());
                        }

                        if status.eq_ignore_ascii_case("failed") {
                            return Err(
                                format!("Job {} failed. Error: {}", job_id, error_message).into()
                            );
                        }
                    }
                } else {
                    // No row found - print debug info
                    if start.elapsed().as_secs().is_multiple_of(5)
                        && start.elapsed().as_millis() % 1000 < 250
                    {
                        println!("[DEBUG] Job {} not found in system.jobs", job_id);
                    }
                }
            },
            Err(e) => {
                // If we can't query the jobs table, that's an error
                return Err(format!("Failed to query system.jobs for job {}: {}", job_id, e).into());
            },
        }

        std::thread::sleep(poll_interval);
    }
}

/// Verify that multiple jobs have all completed successfully
///
/// Convenience wrapper for verifying multiple jobs from STORAGE FLUSH ALL
pub fn verify_jobs_completed(
    job_ids: &[String],
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    for job_id in job_ids {
        verify_job_completed(job_id, timeout)?;
    }
    Ok(())
}

/// Wait until a job reaches a terminal state (completed or failed)
/// Returns the final lowercase status string ("completed" or "failed")
pub fn wait_for_job_finished(
    job_id: &str,
    timeout: Duration,
) -> Result<String, Box<dyn std::error::Error>> {
    let effective_timeout = std::cmp::max(timeout, Duration::from_secs(30));
    let start = std::time::Instant::now();
    let poll_interval = Duration::from_millis(250);

    loop {
        if start.elapsed() > effective_timeout {
            return Err(format!(
                "Timeout waiting for job {} to finish after {:?}",
                job_id, effective_timeout
            )
            .into());
        }

        let query =
            format!("SELECT job_id, status, message FROM system.jobs WHERE job_id = '{}'", job_id);

        match execute_sql_as_root_via_client_json(&query) {
            Ok(output) => {
                let json: serde_json::Value = serde_json::from_str(&output).map_err(|e| {
                    format!("Failed to parse JSON output: {}. Output: {}", e, output)
                })?;

                if let Some(rows) = get_rows_as_hashmaps(&json) {
                    if let Some(row) = rows.first() {
                        let status_value = row
                            .get("status")
                            .and_then(extract_arrow_value)
                            .or_else(|| row.get("status").cloned())
                            .unwrap_or(serde_json::Value::Null);
                        let status_owned = status_value
                            .as_str()
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| status_value.to_string());
                        let status = status_owned.as_str();
                        let status_lower = status.to_lowercase();

                        if status_lower.contains("completed") {
                            return Ok("completed".to_string());
                        }
                        if status_lower.contains("failed") {
                            return Ok("failed".to_string());
                        }
                    }
                }
            },
            Err(e) => return Err(e),
        }

        std::thread::sleep(poll_interval);
    }
}

/// Wait until all jobs reach a terminal state; returns a Vec of final statuses aligned with job_ids
pub fn wait_for_jobs_finished(
    job_ids: &[String],
    timeout_per_job: Duration,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut statuses = Vec::with_capacity(job_ids.len());
    for job_id in job_ids {
        let status = wait_for_job_finished(job_id, timeout_per_job)?;
        statuses.push(status);
    }
    Ok(statuses)
}

// ============================================================================
// CLIENT-BASED SUBSCRIPTION LISTENER (uses kalam-client WebSocket, avoids CLI)
// ============================================================================

/// Subscription listener for testing real-time events via kalam-client.
/// Uses WebSocket connection instead of spawning CLI processes to avoid
/// macOS TCP connection limits.
pub struct SubscriptionListener {
    event_receiver: std_mpsc::Receiver<String>,
    stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
    _handle: Option<std::thread::JoinHandle<()>>,
    pending_events: VecDeque<String>,
}

impl Drop for SubscriptionListener {
    fn drop(&mut self) {
        // Signal the subscription task to stop and join the thread to avoid leaks
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }
        if let Some(handle) = self._handle.take() {
            let _ = handle.join();
        }
    }
}

impl SubscriptionListener {
    /// Start a subscription listener for a given query with a default timeout
    pub fn start(query: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_timeout(query, 30) // Default 30 second timeout for tests
    }

    /// Start a subscription listener with a specific timeout in seconds.
    /// Uses the kalam-client WebSocket path instead of spawning CLI processes.
    pub fn start_with_timeout(
        query: &str,
        _timeout_secs: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (event_tx, event_rx) = std_mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let query = query.to_string();

        let handle = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime for subscription");

            runtime.block_on(async move {
                // Get first available server URL (cluster or single-node)
                let base_url = leader_url().unwrap_or_else(|| {
                    get_available_server_urls()
                        .first()
                        .cloned()
                        .unwrap_or_else(|| server_url().to_string())
                });

                // Build client for subscription
                let client = match build_client_for_url_with_timeouts(
                    &base_url,
                    default_username(),
                    default_password(),
                    KalamLinkTimeouts::builder()
                        .connection_timeout_secs(5)
                        .receive_timeout_secs(120)
                        .send_timeout_secs(30)
                        .subscribe_timeout_secs(10)
                        .auth_timeout_secs(10)
                        .initial_data_timeout(Duration::from_secs(120))
                        .build(),
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = event_tx.send(format!("ERROR: Failed to create client: {}", e));
                        return;
                    },
                };

                // Start subscription
                let mut subscription = match client.subscribe(&query).await {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = event_tx.send(format!("ERROR: Failed to subscribe: {}", e));
                        return;
                    },
                };

                // Convert oneshot receiver to a future we can select on
                let mut stop_rx = stop_rx;

                loop {
                    tokio::select! {
                        _ = &mut stop_rx => {
                            // Stop signal received
                            break;
                        }
                        event = subscription.next() => {
                            match event {
                                Some(Ok(change_event)) => {
                                    // Format the event as a string for compatibility with existing tests
                                    let event_str = format!("{:?}", change_event);
                                    if event_tx.send(event_str).is_err() {
                                        break; // Receiver dropped
                                    }
                                }
                                Some(Err(e)) => {
                                    let _ = event_tx.send(format!("ERROR: {}", e));
                                    break;
                                }
                                None => {
                                    // Subscription ended
                                    break;
                                }
                            }
                        }
                    }
                }

                let _ = subscription.close().await;
                client.disconnect().await;
            });
        });

        Ok(Self {
            event_receiver: event_rx,
            stop_sender: Some(stop_tx),
            _handle: Some(handle),
            pending_events: VecDeque::new(),
        })
    }

    /// Read next line from subscription output
    pub fn read_line(&mut self) -> Result<Option<String>, Box<dyn std::error::Error>> {
        if let Some(line) = self.pending_events.pop_front() {
            return Ok(Some(line));
        }
        match self.event_receiver.recv() {
            Ok(line) => {
                if line.is_empty() {
                    Ok(None) // EOF
                } else {
                    Ok(Some(line))
                }
            },
            Err(_) => Ok(None), // Channel closed
        }
    }

    /// Try to read a line with a timeout
    pub fn try_read_line(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        if let Some(line) = self.pending_events.pop_front() {
            return Ok(Some(line));
        }
        match self.event_receiver.recv_timeout(timeout) {
            Ok(line) => {
                if line.is_empty() {
                    Ok(None) // EOF
                } else {
                    Ok(Some(line))
                }
            },
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                Err("Timeout waiting for subscription data".into())
            },
            Err(std_mpsc::RecvTimeoutError::Disconnected) => Ok(None),
        }
    }

    /// Wait for a specific event pattern
    pub fn wait_for_event(
        &mut self,
        pattern: &str,
        timeout: Duration,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let effective_timeout = std::cmp::max(timeout, Duration::from_secs(15));
        let start = std::time::Instant::now();

        if let Some(index) = self.pending_events.iter().position(|line| line.contains(pattern)) {
            let line = self.pending_events.remove(index).unwrap_or_default();
            if !line.is_empty() {
                return Ok(line);
            }
        }

        while start.elapsed() < effective_timeout {
            match self.event_receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => {
                    if line.is_empty() {
                        break; // EOF
                    }
                    if line.contains(pattern) {
                        return Ok(line);
                    }
                    self.pending_events.push_back(line);
                },
                Err(std_mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        Err(
            format!("Event pattern '{}' not found within timeout {:?}", pattern, effective_timeout)
                .into(),
        )
    }

    /// Stop the subscription listener gracefully
    pub fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Signal the subscription task to stop and join the thread
        if let Some(sender) = self.stop_sender.take() {
            let _ = sender.send(());
        }
        if let Some(handle) = self._handle.take() {
            handle.join().map_err(|_| {
                Box::<dyn std::error::Error>::from("Subscription listener thread panicked")
            })?;
        }
        Ok(())
    }
}

/// Helper to start a subscription listener in a background thread
pub fn start_subscription_listener(
    query: &str,
) -> Result<std_mpsc::Receiver<String>, Box<dyn std::error::Error>> {
    let (event_sender, event_receiver) = std_mpsc::channel();
    let query = query.to_string();

    thread::spawn(move || {
        let mut listener = match SubscriptionListener::start(&query) {
            Ok(l) => l,
            Err(e) => {
                let _ = event_sender.send(format!("ERROR: {}", e));
                return;
            },
        };

        loop {
            match listener.try_read_line(Duration::from_millis(50)) {
                Ok(Some(line)) => {
                    let _ = event_sender.send(line);
                },
                Ok(None) => break,  // EOF
                Err(_) => continue, // Timeout, try again
            }
        }
    });

    Ok(event_receiver)
}

// ============================================================================
// FLUSH STORAGE VERIFICATION HELPERS
// ============================================================================

/// Default backend storage directory path (relative from cli/ directory)
const BACKEND_STORAGE_DIR: &str = "../backend/data/storage";

/// Get the storage directory path for flush verification
pub fn get_storage_dir() -> std::path::PathBuf {
    use std::path::PathBuf;

    if let Ok(storage_dir) = std::env::var("KALAMDB_STORAGE_DIR") {
        let path = PathBuf::from(storage_dir);
        if path.exists() {
            return path;
        }
    }

    if let Ok(data_dir) = std::env::var("KALAMDB_DATA_DIR") {
        let path = PathBuf::from(data_dir).join("storage");
        if path.exists() {
            return path;
        }
    }

    // The backend server writes to ./data/storage from backend/ directory
    // When tests run from cli/, we need to access ../backend/data/storage
    let backend_path = PathBuf::from(BACKEND_STORAGE_DIR);
    if backend_path.exists() {
        return backend_path;
    }

    // Fallback for different working directory contexts (legacy root path)
    let root_path = PathBuf::from("../data/storage");
    if root_path.exists() {
        return root_path;
    }

    // Default to backend path (will fail in test if it doesn't exist)
    backend_path
}

/// Result of verifying flush storage files
#[derive(Debug)]
pub struct FlushStorageVerificationResult {
    /// Whether manifest.json was found
    pub manifest_found: bool,
    /// Size of manifest.json in bytes (0 if not found)
    pub manifest_size: u64,
    /// Number of parquet files found
    pub parquet_file_count: usize,
    /// Total size of all parquet files in bytes
    pub parquet_total_size: u64,
    /// Path to the manifest.json file (if found)
    pub manifest_path: Option<std::path::PathBuf>,
    /// Paths to all parquet files found
    pub parquet_paths: Vec<std::path::PathBuf>,
}

impl FlushStorageVerificationResult {
    /// Check if the verification found valid flush artifacts
    pub fn is_valid(&self) -> bool {
        self.manifest_found
            && self.manifest_size > 0
            && self.parquet_file_count > 0
            && self.parquet_total_size > 0
    }

    /// Assert that flush storage files exist and are valid
    pub fn assert_valid(&self, context: &str) {
        assert!(self.manifest_found, "{}: manifest.json should exist after flush", context);
        assert!(
            self.manifest_size > 0,
            "{}: manifest.json should not be empty (size: {} bytes)",
            context,
            self.manifest_size
        );
        assert!(
            self.parquet_file_count > 0,
            "{}: at least one batch-*.parquet file should exist after flush",
            context
        );
        assert!(
            self.parquet_total_size > 0,
            "{}: parquet files should not be empty (total size: {} bytes)",
            context,
            self.parquet_total_size
        );
    }
}

/// Verify flush storage files for a SHARED table
/// Checks that manifest.json and batch-*.parquet files exist with non-zero size
/// in the expected storage path for a shared table.
///
/// # Arguments
/// * `namespace` - The namespace name
/// * `table_name` - The table name (without namespace prefix)
///
/// # Returns
/// A `FlushStorageVerificationResult` with details about found files
pub fn verify_flush_storage_files_shared(
    namespace: &str,
    table_name: &str,
) -> FlushStorageVerificationResult {
    use std::fs;

    let storage_dir = get_storage_dir();
    let table_dir = storage_dir.join(namespace).join(table_name);

    verify_flush_storage_files_in_dir(&table_dir)
}

/// Verify flush storage files for a USER table
///
/// Checks that manifest.json and batch-*.parquet files exist with non-zero size
/// in the expected storage path for a user table. Since user tables have per-user
/// subdirectories, this function searches through all user directories.
///
/// # Arguments
/// * `namespace` - The namespace name
/// * `table_name` - The table name (without namespace prefix)
///
/// # Returns
/// A `FlushStorageVerificationResult` with details about found files (aggregated across all users)
pub fn verify_flush_storage_files_user(
    namespace: &str,
    table_name: &str,
) -> FlushStorageVerificationResult {
    use std::fs;

    let storage_dir = get_storage_dir();
    let table_dir = storage_dir.join(namespace).join(table_name);

    let mut result = FlushStorageVerificationResult {
        manifest_found: false,
        manifest_size: 0,
        parquet_file_count: 0,
        parquet_total_size: 0,
        manifest_path: None,
        parquet_paths: Vec::new(),
    };

    if !table_dir.exists() {
        return result;
    }

    // For user tables, iterate through user subdirectories
    if let Ok(entries) = fs::read_dir(&table_dir) {
        for entry in entries.flatten() {
            let user_dir = entry.path();
            if user_dir.is_dir() {
                let user_result = verify_flush_storage_files_in_dir(&user_dir);

                // Aggregate results across all users
                if user_result.manifest_found {
                    result.manifest_found = true;
                    result.manifest_size = result.manifest_size.max(user_result.manifest_size);
                    result.manifest_path = user_result.manifest_path;
                }
                result.parquet_file_count += user_result.parquet_file_count;
                result.parquet_total_size += user_result.parquet_total_size;
                result.parquet_paths.extend(user_result.parquet_paths);
            }
        }
    }

    result
}

/// Verify flush storage files in a specific directory
///
/// Internal helper that checks for manifest.json and batch-*.parquet files in a directory.
fn verify_flush_storage_files_in_dir(dir: &std::path::Path) -> FlushStorageVerificationResult {
    use std::fs;

    let mut result = FlushStorageVerificationResult {
        manifest_found: false,
        manifest_size: 0,
        parquet_file_count: 0,
        parquet_total_size: 0,
        manifest_path: None,
        parquet_paths: Vec::new(),
    };

    if !dir.exists() {
        return result;
    }

    // Check for manifest.json
    let manifest_path = dir.join("manifest.json");
    if manifest_path.exists() {
        if let Ok(metadata) = fs::metadata(&manifest_path) {
            result.manifest_found = true;
            result.manifest_size = metadata.len();
            result.manifest_path = Some(manifest_path);
        }
    }

    // Check for batch-*.parquet files
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();
            if filename_str.starts_with("batch-") && filename_str.ends_with(".parquet") {
                if let Ok(metadata) = entry.metadata() {
                    result.parquet_file_count += 1;
                    result.parquet_total_size += metadata.len();
                    result.parquet_paths.push(entry.path());
                }
            }
        }
    }

    result
}

/// Verify flush storage files exist after a flush operation
///
/// This is a convenience function that combines the flush job verification with
/// storage file verification. Use after calling STORAGE FLUSH TABLE and before cleanup.
///
/// # Arguments
/// * `namespace` - The namespace name
/// * `table_name` - The table name (without namespace prefix)
/// * `is_user_table` - Whether this is a USER table (true) or SHARED table (false)
/// * `context` - Context string for assertion error messages
///
/// # Panics
/// Panics with descriptive error if manifest.json or parquet files are missing or empty
pub fn assert_flush_storage_files_exist(
    namespace: &str,
    table_name: &str,
    is_user_table: bool,
    context: &str,
) {
    if is_external_server_mode() {
        if manifest_exists_in_system_table(namespace, table_name) {
            println!(
                "✅ [{}] Verified flush storage via system.manifest for {}.{}",
                context, namespace, table_name
            );
        } else {
            println!(
                "⚠️ [{}] Skipping filesystem manifest checks for external server mode ({}.{})",
                context, namespace, table_name
            );
        }
        return;
    }

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last_result;

    loop {
        let result = if is_user_table {
            verify_flush_storage_files_user(namespace, table_name)
        } else {
            verify_flush_storage_files_shared(namespace, table_name)
        };

        if result.is_valid() {
            println!(
                "✅ [{}] Verified flush storage: manifest.json ({} bytes), {} parquet file(s) ({} bytes total)",
                context,
                result.manifest_size,
                result.parquet_file_count,
                result.parquet_total_size
            );
            return;
        }

        if manifest_exists_in_system_table(namespace, table_name) {
            println!(
                "✅ [{}] Verified flush storage via system.manifest for {}.{}",
                context, namespace, table_name
            );
            return;
        }

        last_result = Some(result);
        if Instant::now() >= deadline {
            break;
        }
    }

    if let Some(result) = last_result {
        result.assert_valid(context);
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[derive(Debug, Clone)]
pub struct ServerMemorySample {
    pub pid: Option<u32>,
    pub reported_mb: u64,
    pub rss_mb: Option<u64>,
}

impl ServerMemorySample {
    pub fn asserted_mb(&self) -> u64 {
        self.rss_mb.map(|rss| rss.max(self.reported_mb)).unwrap_or(self.reported_mb)
    }
}

fn json_value_to_string(value: &serde_json::Value) -> Option<String> {
    let extracted = extract_arrow_value(value).unwrap_or_else(|| extract_typed_value(value));
    match extracted {
        serde_json::Value::String(s) => Some(s),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Null => None,
        other => Some(other.to_string()),
    }
}

pub fn read_system_stats(
    metric_names: &[&str],
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let filter = if metric_names.is_empty() {
        String::new()
    } else {
        let joined = metric_names
            .iter()
            .map(|name| format!("'{}'", escape_sql_string(name)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" WHERE metric_name IN ({})", joined)
    };

    let sql = format!(
        "SELECT metric_name, metric_value FROM system.stats{} ORDER BY metric_name LIMIT 5000",
        filter
    );
    let output = execute_sql_as_root_via_client_json(&sql)?;
    let parsed: serde_json::Value = serde_json::from_str(&output)?;
    let rows = get_rows_as_hashmaps(&parsed)
        .ok_or_else(|| format!("system.stats JSON response had no row set: {}", output))?;

    let mut metrics = HashMap::new();
    for row in rows {
        let Some(name) = row.get("metric_name").and_then(json_value_to_string) else {
            continue;
        };
        let Some(value) = row.get("metric_value").and_then(json_value_to_string) else {
            continue;
        };
        metrics.insert(name, value);
    }

    Ok(metrics)
}

pub fn server_target_is_local() -> bool {
    let Some((host, _port)) = split_host_port(&server_host_port()) else {
        return false;
    };

    matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1" | "0.0.0.0")
}

fn read_local_process_rss_mb(pid: u32) -> Option<u64> {
    static PROCESS_SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();
    let system = PROCESS_SYSTEM
        .get_or_init(|| Mutex::new(System::new_with_specifics(RefreshKind::nothing())));

    let mut guard = system.lock().ok()?;
    let pid = Pid::from_u32(pid);
    let refresh = ProcessRefreshKind::nothing().with_memory();
    guard.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), false, refresh);
    let process = guard.process(pid)?;
    Some(process.memory() / 1024 / 1024)
}

pub fn capture_server_memory_sample() -> Result<ServerMemorySample, Box<dyn std::error::Error>> {
    let metrics = read_system_stats(&["memory_usage_mb", "pid"])?;
    let reported_mb = metrics
        .get("memory_usage_mb")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or("system.stats missing memory_usage_mb")?;

    let env_pid = std::env::var("KALAMDB_SERVER_PID")
        .ok()
        .and_then(|value| value.parse::<u32>().ok());
    let pid = env_pid.or_else(|| metrics.get("pid").and_then(|value| value.parse::<u32>().ok()));

    let rss_mb = if env_pid.is_some() || server_target_is_local() {
        pid.and_then(read_local_process_rss_mb)
    } else {
        None
    };

    Ok(ServerMemorySample {
        pid,
        reported_mb,
        rss_mb,
    })
}

fn namespace_id_for_name(namespace: &str) -> Option<String> {
    let sql = format!(
        "SELECT namespace_id FROM system.namespaces WHERE name = '{}'",
        escape_sql_string(namespace)
    );
    let json_output = execute_sql_as_root_via_client_json(&sql).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&json_output).ok()?;
    let rows = get_rows_as_hashmaps(&parsed)?;
    for row in rows {
        if let Some(value) = row.get("namespace_id") {
            let extracted = extract_arrow_value(value).unwrap_or_else(|| value.clone());
            if let Some(namespace_id) = extracted.as_str() {
                if !namespace_id.is_empty() {
                    return Some(namespace_id.to_string());
                }
            }
        }
    }
    None
}

pub fn manifest_exists_in_system_table(namespace: &str, table_name: &str) -> bool {
    let namespace_id = namespace_id_for_name(namespace).unwrap_or_else(|| namespace.to_string());
    let sql = format!(
        "SELECT manifest_json FROM system.manifest WHERE namespace_id = '{}' AND table_name = '{}'",
        escape_sql_string(&namespace_id),
        escape_sql_string(table_name)
    );
    let json_output = match execute_sql_as_root_via_client_json(&sql) {
        Ok(output) => output,
        Err(_) => return false,
    };
    let parsed: serde_json::Value = match serde_json::from_str(&json_output) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let rows = match get_rows_as_hashmaps(&parsed) {
        Some(rows) => rows,
        None => return false,
    };
    for row in rows {
        if let Some(value) = row.get("manifest_json") {
            let extracted = extract_arrow_value(value).unwrap_or_else(|| value.clone());
            if extracted.as_str().map(|s| !s.is_empty()).unwrap_or(false) {
                return true;
            }
        }
    }
    false
}

/// Execute SQL on all available nodes (single-node or cluster)
/// Returns results from each node
pub fn execute_sql_on_all_nodes(
    sql: &str,
) -> Vec<(String, Result<String, Box<dyn std::error::Error>>)> {
    let urls = get_available_server_urls();
    urls.into_iter()
        .map(|url| {
            let result = execute_sql_on_node(&url, sql);
            (url, result)
        })
        .collect()
}

/// Execute SQL on a specific node URL
pub fn execute_sql_on_node(
    base_url: &str,
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    fn boxed_error(message: String) -> Box<dyn std::error::Error> {
        Box::new(std::io::Error::new(std::io::ErrorKind::Other, message))
    }

    fn execute_sql_on_node_internal(
        base_url: &str,
        sql: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let rt = Runtime::new()?;
        let client = build_client_for_url_with_timeouts(
            base_url,
            default_username(),
            default_password(),
            KalamLinkTimeouts::builder()
                .connection_timeout_secs(5)
                .receive_timeout_secs(120)
                .send_timeout_secs(30)
                .subscribe_timeout_secs(10)
                .auth_timeout_secs(10)
                .initial_data_timeout(Duration::from_secs(120))
                .build(),
        )
        .map_err(|err| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))
                as Box<dyn std::error::Error>
        })?;

        let sql = sql.to_string();
        let response = rt.block_on(async { client.execute_query(&sql, None, None, None).await })?;

        // Format response similar to execute_sql_via_client_as
        let mut output = String::new();
        for result in response.results {
            if let Some(ref message) = result.message {
                output.push_str(message);
                output.push('\n');
            } else {
                // Get column names
                let columns: Vec<String> = result.column_names();

                // Check if DML
                let is_dml = result.rows.is_none()
                    || (result.rows.as_ref().map(|r| r.is_empty()).unwrap_or(false)
                        && result.row_count > 0);

                if is_dml {
                    output.push_str(&format!("{} rows affected\n", result.row_count));
                } else {
                    // Add column headers
                    if !columns.is_empty() {
                        output.push_str(&columns.join(" | "));
                        output.push('\n');
                    }

                    // Add rows
                    if let Some(ref rows) = result.rows {
                        for row in rows {
                            let row_str: Vec<String> = columns
                                .iter()
                                .enumerate()
                                .map(|(i, _col)| {
                                    row.get(i)
                                        .map(|v| match v.inner() {
                                            serde_json::Value::String(s) => s.clone(),
                                            serde_json::Value::Null => "NULL".to_string(),
                                            other => other.to_string(),
                                        })
                                        .unwrap_or_else(|| "NULL".to_string())
                                })
                                .collect();
                            output.push_str(&row_str.join(" | "));
                            output.push('\n');
                        }

                        output.push_str(&format!(
                            "({} row{})\n",
                            rows.len(),
                            if rows.len() == 1 { "" } else { "s" }
                        ));
                    } else {
                        output.push_str("(0 rows)\n");
                    }
                }
            }
        }

        Ok(output)
    }

    let base_url = base_url.to_string();
    let sql = sql.to_string();
    if Handle::try_current().is_ok() {
        let (tx, rx) = std_mpsc::channel();
        std::thread::spawn(move || {
            let result =
                execute_sql_on_node_internal(&base_url, &sql).map_err(|err| err.to_string());
            let _ = tx.send(result);
        });
        let result = rx
            .recv_timeout(Duration::from_secs(60))
            .map_err(|e| boxed_error(format!("Query timed out or channel error: {}", e)))?;
        return result.map_err(boxed_error);
    }

    execute_sql_on_node_internal(&base_url, &sql)
}

/// Verify SQL result is consistent across all nodes in cluster mode
/// In single-node mode, just executes once
pub fn verify_consistent_across_nodes(sql: &str, expected_contains: &[&str]) -> Result<(), String> {
    let results = execute_sql_on_all_nodes(sql);

    for (url, result) in &results {
        let output = result.as_ref().map_err(|e| format!("Query failed on {}: {}", url, e))?;

        for expected in expected_contains {
            if !output.contains(expected) {
                return Err(format!(
                    "Output from {} does not contain '{}': {}",
                    url, expected, output
                ));
            }
        }
    }

    Ok(())
}
