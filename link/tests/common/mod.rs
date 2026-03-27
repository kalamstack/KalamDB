#![allow(dead_code)]

pub mod tcp_proxy;

use std::collections::HashMap;
use kalamdb_configs::ServerConfig;
use kalamdb_server::lifecycle::RunningTestHttpServer;
use reqwest::Client;
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Mutex as TokioMutex;

static SERVER_URL: OnceLock<String> = OnceLock::new();
static ISOLATED_SERVER_URL: OnceLock<String> = OnceLock::new();
static AUTO_TEST_SERVER: OnceLock<Mutex<Option<AutoTestServer>>> = OnceLock::new();
static ISOLATED_AUTO_TEST_SERVER: OnceLock<Mutex<Option<AutoTestServer>>> = OnceLock::new();
static AUTO_TEST_RUNTIME: OnceLock<Runtime> = OnceLock::new();
static ACCESS_TOKENS: OnceLock<TokioMutex<HashMap<String, String>>> = OnceLock::new();

struct AutoTestServer {
    base_url: String,
    _temp_dir: TempDir,
    data_dir: PathBuf,
    _running: RunningTestHttpServer,
}

fn should_auto_start_test_server() -> bool {
    if std::env::var("KALAMDB_SERVER_URL").is_ok() {
        return false;
    }

    std::env::var("KALAMDB_AUTO_START_TEST_SERVER")
        .map(|value| {
            let value = value.trim();
            !(value.eq_ignore_ascii_case("0") || value.eq_ignore_ascii_case("false"))
        })
        .unwrap_or(true)
}

fn url_reachable(url: &str) -> bool {
    let host_port = url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or("127.0.0.1:8080");
    std::net::TcpStream::connect(host_port).map(|_| true).unwrap_or(false)
}

fn ensure_auto_test_server(
    server_slot: &'static OnceLock<Mutex<Option<AutoTestServer>>>,
) -> Option<(String, PathBuf)> {
    let server_mutex = server_slot.get_or_init(|| Mutex::new(None));
    let mut guard = server_mutex.lock().ok()?;

    if guard.is_none() {
        let start_result: Result<AutoTestServer, String> = if Handle::try_current().is_ok() {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = AUTO_TEST_RUNTIME.get_or_init(|| {
                    Runtime::new().expect("Failed to create auto test server runtime")
                });
                let result =
                    runtime.block_on(start_local_test_server()).map_err(|err| err.to_string());
                let _ = tx.send(result);
            });

            match rx.recv_timeout(Duration::from_secs(20)) {
                Ok(result) => result,
                Err(err) => Err(format!("Timed out starting test server: {}", err)),
            }
        } else {
            let runtime = AUTO_TEST_RUNTIME
                .get_or_init(|| Runtime::new().expect("Failed to create auto test server runtime"));
            runtime.block_on(start_local_test_server()).map_err(|err| err.to_string())
        };

        match start_result {
            Ok(server) => {
                *guard = Some(server);
            },
            Err(err) => {
                eprintln!("Failed to auto-start test server: {}", err);
                return None;
            },
        }
    }

    guard.as_ref().map(|server| (server.base_url.clone(), server.data_dir.clone()))
}

async fn start_local_test_server() -> Result<AutoTestServer, Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let data_path = temp_dir.path().to_path_buf();

    let mut config = ServerConfig::default();
    config.server.host = "127.0.0.1".to_string();
    config.server.port = 0;
    config.server.ui_path = None;
    config.storage.data_path = data_path.to_string_lossy().into_owned();
    config.rate_limit.max_queries_per_sec = 100000;
    config.rate_limit.max_messages_per_sec = 10000;

    let (components, app_context) = kalamdb_server::lifecycle::bootstrap_isolated(&config).await?;
    let running =
        kalamdb_server::lifecycle::run_for_tests(&config, components, app_context).await?;

    // Ensure setup completes for the auto-started server
    ensure_server_setup(&running.base_url, "test_password").await?;

    Ok(AutoTestServer {
        base_url: running.base_url.clone(),
        _temp_dir: temp_dir,
        data_dir: data_path,
        _running: running,
    })
}

async fn ensure_server_setup(
    base_url: &str,
    root_password: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let status = client.get(format!("{}/v1/api/auth/status", base_url)).send().await?;

    if !status.status().is_success() {
        return Err(format!("Failed to check setup status: {}", status.status()).into());
    }

    let body: serde_json::Value = status.json().await?;
    let needs_setup = body.get("needs_setup").and_then(|v| v.as_bool()).unwrap_or(false);

    if !needs_setup {
        return Ok(());
    }

    let setup_response = client
        .post(format!("{}/v1/api/auth/setup", base_url))
        .json(&json!({
            "username": "admin",
            "password": root_password,
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

async fn fetch_access_token(
    base_url: &str,
    username: &str,
    password: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = Client::new()
        .post(format!("{}/v1/api/auth/login", base_url))
        .json(&json!({
            "username": username,
            "password": password
        }))
        .send()
        .await?;

    if !response.status().is_success() {
        let text = response.text().await?;
        return Err(format!("Login failed: {}", text).into());
    }

    let body: serde_json::Value = response.json().await?;
    let token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing access_token in login response".to_string())?;

    Ok(token.to_string())
}

async fn root_access_token_for_base_url(
    base_url: &str,
    password: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let token_cache = ACCESS_TOKENS.get_or_init(|| TokioMutex::new(HashMap::new()));
    let mut guard = token_cache.lock().await;
    if let Some(token) = guard.get(base_url) {
        return Ok(token.clone());
    }

    let token = fetch_access_token(base_url, "root", password).await?;
    guard.insert(base_url.to_string(), token.clone());
    Ok(token)
}

pub async fn root_access_token() -> Result<String, Box<dyn std::error::Error>> {
    let password =
        std::env::var("KALAMDB_ROOT_PASSWORD").unwrap_or_else(|_| "test_password".to_string());
    root_access_token_for_base_url(server_url(), &password).await
}

pub fn root_access_token_blocking() -> Result<String, Box<dyn std::error::Error>> {
    let base_url = server_url().to_string();
    let password =
        std::env::var("KALAMDB_ROOT_PASSWORD").unwrap_or_else(|_| "test_password".to_string());
    root_access_token_blocking_for_base_url(base_url, password)
}

pub async fn isolated_root_access_token() -> Result<String, Box<dyn std::error::Error>> {
    root_access_token_for_base_url(isolated_server_url(), "test_password").await
}

pub fn isolated_root_access_token_blocking() -> Result<String, Box<dyn std::error::Error>> {
    root_access_token_blocking_for_base_url(
        isolated_server_url().to_string(),
        "test_password".to_string(),
    )
}

fn root_access_token_blocking_for_base_url(
    base_url: String,
    password: String,
) -> Result<String, Box<dyn std::error::Error>> {
    if Handle::try_current().is_ok() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = Runtime::new()
                .map_err(|e| e.to_string())
                .and_then(|rt| {
                    rt.block_on(root_access_token_for_base_url(&base_url, &password))
                        .map_err(|e| e.to_string())
                });
            let _ = tx.send(result);
        });

        match rx.recv_timeout(Duration::from_secs(20)) {
            Ok(Ok(token)) => return Ok(token),
            Ok(Err(err)) => return Err(err.into()),
            Err(err) => return Err(err.to_string().into()),
        }
    }

    let runtime = AUTO_TEST_RUNTIME
        .get_or_init(|| Runtime::new().expect("Failed to create runtime for access token"));
    runtime.block_on(root_access_token_for_base_url(&base_url, &password))
}

pub fn server_url() -> &'static str {
    SERVER_URL
        .get_or_init(|| {
            if let Ok(url) = std::env::var("KALAMDB_SERVER_URL") {
                return url;
            }

            if should_auto_start_test_server() {
                if let Some((url, storage_dir)) = ensure_auto_test_server(&AUTO_TEST_SERVER) {
                    std::env::set_var("KALAMDB_SERVER_URL", &url);
                    // Use a test password for the auto-started server
                    std::env::set_var("KALAMDB_ROOT_PASSWORD", "test_password");
                    std::env::set_var(
                        "KALAMDB_STORAGE_DIR",
                        storage_dir.to_string_lossy().to_string(),
                    );
                    return url;
                }
            }

            let default_url = "http://localhost:8080".to_string();
            if url_reachable(&default_url) {
                return default_url;
            }

            default_url
        })
        .as_str()
}

pub fn isolated_server_url() -> &'static str {
    ISOLATED_SERVER_URL
        .get_or_init(|| {
            let (url, _) = ensure_auto_test_server(&ISOLATED_AUTO_TEST_SERVER)
                .expect("Failed to auto-start isolated kalam-link test server");
            url
        })
        .as_str()
}

pub async fn is_server_running() -> bool {
    // Use the status endpoint which doesn't require authentication
    match reqwest::Client::new()
        .get(format!("{}/v1/api/auth/status", server_url()))
        .timeout(Duration::from_secs(2))
        .send()
        .await
    {
        Ok(resp) => resp.status().is_success() || resp.status().as_u16() == 428,
        Err(_) => false,
    }
}

pub fn websocket_url() -> String {
    let base = server_url();
    if base.starts_with("https://") {
        base.replacen("https://", "wss://", 1)
    } else {
        base.replacen("http://", "ws://", 1)
    }
}
