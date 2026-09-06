use std::{
    io::{BufRead, BufReader, Read},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command as ProcessCommand, Output, Stdio},
    sync::{mpsc, Arc},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use reqwest::{
    header::{CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE},
    StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;
use testcontainers_modules::{dex::Dex, testcontainers::runners::AsyncRunner};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener as TokioTcpListener, TcpStream},
    sync::oneshot,
};
use url::Url;

use crate::common;

const DEX_CLIENT_ID: &str = "client";
const DEX_FALLBACK_CLIENT_SECRET: &str = "secret";
const DEX_ISSUER: &str = "http://127.0.0.1:5556";
const DEX_USERNAME: &str = "alice@example.org";
const DEX_PASSWORD: &str = "kalamdb123";
const DEX_FALLBACK_USERNAME: &str = "user@example.org";
const DEX_FALLBACK_PASSWORD: &str = "user";
const DEVICE_CLIENT_ID: &str = "kalam-cli-device";
const TEST_RSA_PRIVATE_KEY_DER_BASE64: &str = "MIIEowIBAAKCAQEAwwphfTBE9LBOHdPUXacsmeuDad+rjh81B5MH/74eelEcs3Z+jUxFa7CqqMm7432It9joUO0mULUXfpUBnwFCgGIHEvTWHDOcR+Wgnc07LfYMGxqxlifCEK6RUdfSAVAj97a5DSuIpQ6iAvGp54iBRrf5vgmD/z38fisRa6YrBagWyMOFerPPQP94WhvNRN9Lt7NO+3jgf1N8reh0KMo2KynJDyZ3y/xQWcIrPc/g/FqqRkj8/WrOgpaPzW5Q/Nqcd5GIAEj6cDELk76XL9whbk6ixhnu2mkvIJ/cZenBd2AGM8BbU7XxIi6GzuS2v+PeKjRlGQx8TkGqtjZ4KibY+wIDAQABAoIBAA2qGwVpzdL0zSxCzISZM0M/YFAZFwxYfF8g+nT87Wa1axTZrukYWF7AnFxB8fNwtpTm0fPlgYMzBMfeCaSJso6LD6LQ23VTWlYhLN0RZV2FePinKJz0ASEpEc5RmAl2g2aV+yYEkEi8GzaolrY9do0tU4ZwZTqLLbbrLofDtwzox9K1LXZOdYK2+UZlKXKRJFu06wAd4Pvq3LUP4MmstfaKBklAsGf7hgwt+uREPd3YzLpaWn/5F4gI03sJA1oB+zHS3FAexo8Yxwuy10ATQ4ERdRPc7/86CS3n+XKpoj+IzBjDYDqtM2qcH2YAP3wcU1B8nRGpxJY0pqKPpdFz79kCgYEA++0JY3bFhCAClcwDMlyfzev/LVEV3JqoWNeH5ryQ4qJ2HiXwBcfTqtO4yoTCU7UpSXKI32aBlEhpn4rpZgM8trbivUHkagmoIaSJxGhpdLv35W456kvF7pLWckIziq0g9i+EGGhW0cpCmfFipfjgPUzeZKKovL6QhHiVEVOqnSkCgYEAxjHXSIsHsz30GqbwRmW/e0uQEwABcCActNVB3hj7Q1nePxfFB8sDe+s7FGFXsuhtemkHzA6j5UbzkMlrVSG98geZrlZniXdThS+jRvpEncqfUyO+POqhx6blWyldyo9PgMcvWsB4yuKG3lWKdR3kL8aQX9gcFsOPF4JxyyLFpYMCgYEAqJNu6t25QbZhxHcl1Hdif9rhgCN4K4xaBkkDKYUYtm7b90SPnm6e1vqh9vJrTrQ1Em7P5B2lq+Hgu9+qWpbj86fhhZ8oB0S6+vgtL/5mQrTdJuthWcSmiAQ992sRLkS3f8U/8U0we2WKt5Rs3H7zHlHnpxOpMdOaxOojZdrEmjECgYBKCNozdgPVV+I0hoGgumdRxkM2Zb0jxksS3cqyDUDmws47YUSviY1un8s87LPW1+31WQCZoCpm/h8Dycm3Tlhm7aHhttMMTa+8Q7RJUjmJe+QSKXrpxHfUXaq1Z/lqLihzoXQ2AUnd98qLiQakgxr3IcRSmSa89iYgkRCy4fVUwwKBgHBxSEFAcRyEGvgWVL1Ti8KoiMfTyj8HofGUGON9PmP5yyGabZrd4TdcRozoUYh9jrI3FvwepbAKxnyuGDKAsEIXnAvUgqGZF0AEy0CFrSTHW8WynGzDTEslzWG4Ha1xdGlwv+SpjwThP5Un9k1ei99y/rd0bS1zBKAEUC4gvWOP";
const TEST_RSA_JWK_N: &str = "wwphfTBE9LBOHdPUXacsmeuDad-rjh81B5MH_74eelEcs3Z-jUxFa7CqqMm7432It9joUO0mULUXfpUBnwFCgGIHEvTWHDOcR-Wgnc07LfYMGxqxlifCEK6RUdfSAVAj97a5DSuIpQ6iAvGp54iBRrf5vgmD_z38fisRa6YrBagWyMOFerPPQP94WhvNRN9Lt7NO-3jgf1N8reh0KMo2KynJDyZ3y_xQWcIrPc_g_FqqRkj8_WrOgpaPzW5Q_Nqcd5GIAEj6cDELk76XL9whbk6ixhnu2mkvIJ_cZenBd2AGM8BbU7XxIi6GzuS2v-PeKjRlGQx8TkGqtjZ4KibY-w";
const TEST_RSA_JWK_E: &str = "AQAB";
const TEST_KEY_ID: &str = "cli-device-test-key";

#[derive(Clone)]
struct DexProviderInfo {
    issuer:                 String,
    token_url:              String,
    client_id:              String,
    client_secret:          Option<String>,
    username:               String,
    password:               String,
    subject:                String,
    supports_browser_login: bool,
}

#[derive(Deserialize)]
struct DexTokenResponse {
    access_token: String,
    #[serde(default)]
    id_token:     Option<String>,
}

#[derive(Deserialize)]
struct JwtPayloadSubject {
    sub: String,
}

#[derive(Serialize)]
struct TestJwtClaims {
    sub:   String,
    iss:   String,
    aud:   String,
    exp:   usize,
    iat:   usize,
    nbf:   usize,
    email: String,
}

struct KalamDbTestServer {
    base_url: String,
    child:    Child,
    data_dir: TempDir,
    log_path: PathBuf,
}

#[derive(Deserialize)]
struct CurrentUserResponseView {
    user:            CurrentUserView,
    admin_ui_access: bool,
}

#[derive(Deserialize)]
struct CurrentUserView {
    id:    String,
    role:  String,
    email: Option<String>,
}

impl Drop for KalamDbTestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = self.data_dir.path();
    }
}

struct DeviceOidcProvider {
    issuer: String,
    device_authorization_endpoint: String,
    shutdown_sender: Option<oneshot::Sender<()>>,
    server_handle: tokio::task::JoinHandle<()>,
}

impl Drop for DeviceOidcProvider {
    fn drop(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
        self.server_handle.abort();
    }
}

impl DeviceOidcProvider {
    async fn start(client_id: &str) -> Result<Self> {
        let listener = TokioTcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind CLI OIDC device provider")?;
        let address = listener.local_addr().context("failed to read CLI OIDC provider address")?;
        let issuer = format!("http://{address}");
        let authorization_endpoint = format!("{issuer}/authorize");
        let token_endpoint = format!("{issuer}/token");
        let jwks_uri = format!("{issuer}/jwks");
        let device_authorization_endpoint = format!("{issuer}/device/code");
        let id_token =
            issue_rs256_token(&issuer, client_id, "cli-device-subject", "cli-device@example.org")?;
        let discovery = Arc::new(
            json!({
                "issuer": issuer,
                "authorization_endpoint": authorization_endpoint,
                "token_endpoint": token_endpoint,
                "jwks_uri": jwks_uri,
                "response_types_supported": ["code"],
                "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": ["RS256"],
                "scopes_supported": ["openid", "email", "profile"],
                "grant_types_supported": ["authorization_code", "urn:ietf:params:oauth:grant-type:device_code"],
                "device_authorization_endpoint": device_authorization_endpoint,
            })
            .to_string(),
        );
        let jwks = Arc::new(
            json!({
                "keys": [{
                    "kty": "RSA",
                    "alg": "RS256",
                    "use": "sig",
                    "kid": TEST_KEY_ID,
                    "n": TEST_RSA_JWK_N,
                    "e": TEST_RSA_JWK_E,
                }]
            })
            .to_string(),
        );
        let device_response = Arc::new(
            json!({
                "device_code": "cli-test-device-code",
                "user_code": "ABCD-EFGH",
                "verification_uri": format!("{issuer}/device"),
                "verification_uri_complete": format!("{issuer}/device?user_code=ABCD-EFGH"),
                "expires_in": 600,
                "interval": 1,
            })
            .to_string(),
        );
        let token_response = Arc::new(
            json!({
                "access_token": "provider-access-token",
                "token_type": "Bearer",
                "expires_in": 3600,
                "id_token": id_token,
            })
            .to_string(),
        );
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let server_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_receiver => break,
                    accept = listener.accept() => {
                        let Ok((socket, _)) = accept else {
                            break;
                        };
                        let discovery = Arc::clone(&discovery);
                        let jwks = Arc::clone(&jwks);
                        let device_response = Arc::clone(&device_response);
                        let token_response = Arc::clone(&token_response);
                        tokio::spawn(async move {
                            let _ = handle_oidc_provider_connection(socket, discovery, jwks, device_response, token_response).await;
                        });
                    },
                }
            }
        });

        Ok(Self {
            issuer,
            device_authorization_endpoint,
            shutdown_sender: Some(shutdown_sender),
            server_handle,
        })
    }
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn oidc_cli_dex_bearer_token_works_with_fresh_server() -> Result<()> {
    let Some(()) = with_dex_provider_if_available(|provider| async move {
        let server = start_kalamdb_oidc_server(&OidcServerConfig {
            issuer: provider.issuer.clone(),
            client_id: provider.client_id.clone(),
            client_secret: provider.client_secret.clone(),
            device_authorization_endpoint: None,
            broker_device_flow_enabled: false,
        })
        .await?;
        let options = fetch_login_options(&server.base_url).await?;
        assert_eq!(options["local"]["enabled"], true);
        assert_eq!(options["oidc"]["display_name"], "Dex");
        assert_eq!(options["oidc"]["issuer"], provider.issuer);
        let device_flow = &options["oidc"]["device_flow"];
        let direct_device_endpoint = device_flow["device_authorization_endpoint"].as_str();
        assert_eq!(
            device_flow["direct_supported"].as_bool(),
            Some(direct_device_endpoint.is_some())
        );
        if let Some(endpoint) = direct_device_endpoint {
            assert!(endpoint.starts_with(&provider.issuer));
        }
        assert_eq!(device_flow["broker_supported"].as_bool(), Some(false));

        let token = issue_dex_token(&provider).await?;
        let cli_home = TempDir::new().context("failed to create CLI home")?;
        let credentials_path = cli_home.path().join("credentials.toml");
        let mut command = isolated_cli_command(cli_home.path(), &credentials_path);
        command
            .arg("--url")
            .arg(&server.base_url)
            .arg("--token")
            .arg(token)
            .arg("--command")
            .arg("SELECT 1 AS dex_oidc_cli")
            .timeout(Duration::from_secs(30));

        let output = run_cli_command(command).await?;
        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "Dex OIDC CLI query failed\nstdout:\n{}\nstderr:\n{}\nLast server logs:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
                log_tail(&server.log_path)
            ));
        }
        Ok(())
    })
    .await?
    else {
        return Ok(());
    };

    Ok(())
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn oidc_cli_browser_login_with_dex_works_and_auto_provisions_user() -> Result<()> {
    let Some(provider) = local_dex_provider_if_available().await? else {
        eprintln!(
            "Skipping CLI browser OIDC e2e because shared Dex is not reachable at {DEX_ISSUER}"
        );
        return Ok(());
    };
    if !provider.supports_browser_login {
        eprintln!(
            "Skipping CLI browser OIDC e2e because the Dex provider is not public-browser capable"
        );
        return Ok(());
    }

    let server = start_kalamdb_oidc_server(&OidcServerConfig {
        issuer: provider.issuer.clone(),
        client_id: provider.client_id.clone(),
        client_secret: provider.client_secret.clone(),
        device_authorization_endpoint: None,
        broker_device_flow_enabled: false,
    })
    .await?;

    let cli_home = TempDir::new().context("failed to create CLI home")?;
    let credentials_path = cli_home.path().join("credentials.toml");
    let instance = "oidc-browser";
    let login_output = run_cli_browser_login_command(
        &server,
        &provider,
        cli_home.path(),
        &credentials_path,
        instance,
    )
    .await?;
    if !login_output.status.success() {
        return Err(anyhow::anyhow!(
            "OIDC browser login command failed\nstdout:\n{}\nstderr:\n{}\nLast server logs:\n{}",
            String::from_utf8_lossy(&login_output.stdout),
            String::from_utf8_lossy(&login_output.stderr),
            log_tail(&server.log_path)
        ));
    }
    let login_stdout = String::from_utf8_lossy(&login_output.stdout);
    assert!(
        login_stdout.contains("Logged in with Dex"),
        "unexpected OIDC browser login output: {login_stdout}"
    );
    assert!(
        login_stdout.contains("OIDC BROWSER LOGIN"),
        "OIDC browser login should show the shared CLI prompt: {login_stdout}"
    );

    let token = saved_access_token(&credentials_path, instance)?;
    let refresh_token = saved_refresh_token(&credentials_path, instance)?;
    assert!(!refresh_token.is_empty(), "OIDC browser login should save a refresh token");
    let current_user = fetch_current_user(&server.base_url, &token).await?;
    assert_eq!(current_user.user.id, provider.subject);
    assert_eq!(current_user.user.role, "user");
    assert_eq!(current_user.user.email.as_deref(), Some(provider.username.as_str()));
    assert!(!current_user.admin_ui_access);

    let mut query = isolated_cli_command(cli_home.path(), &credentials_path);
    query
        .arg("--url")
        .arg(&server.base_url)
        .arg("--instance")
        .arg(instance)
        .arg("--command")
        .arg("SELECT 1 AS oidc_browser_cli")
        .timeout(Duration::from_secs(30));
    let query_output = run_cli_command(query).await?;
    if !query_output.status.success() {
        return Err(anyhow::anyhow!(
            "OIDC browser credential check failed\nstdout:\n{}\nstderr:\n{}\nLast server logs:\n{}",
            String::from_utf8_lossy(&query_output.stdout),
            String::from_utf8_lossy(&query_output.stderr),
            log_tail(&server.log_path)
        ));
    }

    Ok(())
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn oidc_cli_headless_direct_device_login_works() -> Result<()> {
    let provider = DeviceOidcProvider::start(DEVICE_CLIENT_ID).await?;
    let server = start_kalamdb_oidc_server(&OidcServerConfig {
        issuer: provider.issuer.clone(),
        client_id: DEVICE_CLIENT_ID.to_string(),
        client_secret: None,
        device_authorization_endpoint: Some(provider.device_authorization_endpoint.clone()),
        broker_device_flow_enabled: false,
    })
    .await?;

    assert_headless_login_saves_usable_credentials(&server, false).await
}

#[tokio::test]
#[ntest::timeout(120000)]
async fn oidc_cli_headless_direct_device_login_with_local_dex_works() -> Result<()> {
    let Some(provider) = local_dex_provider_if_available().await? else {
        eprintln!(
            "Skipping real Dex device-code OIDC e2e because shared Dex is not reachable at \
             {DEX_ISSUER}"
        );
        return Ok(());
    };

    let server = start_kalamdb_oidc_server(&OidcServerConfig {
        issuer: provider.issuer.clone(),
        client_id: provider.client_id.clone(),
        client_secret: provider.client_secret.clone(),
        device_authorization_endpoint: None,
        broker_device_flow_enabled: false,
    })
    .await?;

    let cli_home = TempDir::new().context("failed to create CLI home")?;
    let credentials_path = cli_home.path().join("credentials.toml");
    let instance = "oidc-real-dex-device";
    let login_output = run_cli_device_login_command(
        &server,
        &provider,
        cli_home.path(),
        &credentials_path,
        instance,
    )
    .await?;
    if !login_output.status.success() {
        return Err(anyhow::anyhow!(
            "real Dex device-code OIDC login command failed\nstdout:\n{}\nstderr:\n{}\nLast \
             server logs:\n{}",
            String::from_utf8_lossy(&login_output.stdout),
            String::from_utf8_lossy(&login_output.stderr),
            log_tail(&server.log_path)
        ));
    }

    let login_stdout = String::from_utf8_lossy(&login_output.stdout);
    assert!(
        login_stdout.contains("OIDC DEVICE LOGIN"),
        "real Dex device login should show the shared prompt: {login_stdout}"
    );
    assert!(
        login_stdout.contains("Logged in with Dex"),
        "unexpected real Dex device login output: {login_stdout}"
    );

    let token = saved_access_token(&credentials_path, instance)?;
    let refresh_token = saved_refresh_token(&credentials_path, instance)?;
    assert!(!refresh_token.is_empty(), "real Dex device login should save a refresh token");

    let current_user = fetch_current_user(&server.base_url, &token).await?;
    assert_eq!(current_user.user.id, provider.subject);
    assert_eq!(current_user.user.role, "user");
    assert_eq!(current_user.user.email.as_deref(), Some(provider.username.as_str()));

    let mut query = isolated_cli_command(cli_home.path(), &credentials_path);
    query
        .arg("--url")
        .arg(&server.base_url)
        .arg("--instance")
        .arg(instance)
        .arg("--command")
        .arg("SELECT 1 AS oidc_real_dex_device_cli")
        .timeout(Duration::from_secs(30));
    let query_output = run_cli_command(query).await?;
    if !query_output.status.success() {
        return Err(anyhow::anyhow!(
            "real Dex device credential check failed\nstdout:\n{}\nstderr:\n{}\nLast server \
             logs:\n{}",
            String::from_utf8_lossy(&query_output.stdout),
            String::from_utf8_lossy(&query_output.stderr),
            log_tail(&server.log_path)
        ));
    }

    Ok(())
}

#[tokio::test]
#[ntest::timeout(90000)]
async fn oidc_cli_headless_brokered_device_login_works() -> Result<()> {
    let provider = DeviceOidcProvider::start(DEVICE_CLIENT_ID).await?;
    let server = start_kalamdb_oidc_server(&OidcServerConfig {
        issuer: provider.issuer.clone(),
        client_id: DEVICE_CLIENT_ID.to_string(),
        client_secret: None,
        device_authorization_endpoint: Some(provider.device_authorization_endpoint.clone()),
        broker_device_flow_enabled: true,
    })
    .await?;

    assert_headless_login_saves_usable_credentials(&server, true).await
}

async fn assert_headless_login_saves_usable_credentials(
    server: &KalamDbTestServer,
    brokered: bool,
) -> Result<()> {
    let cli_home = TempDir::new().context("failed to create CLI home")?;
    let credentials_path = cli_home.path().join("credentials.toml");
    let instance = if brokered {
        "oidc-brokered"
    } else {
        "oidc-direct"
    };
    let mut login = isolated_cli_command(cli_home.path(), &credentials_path);
    login
        .arg("--url")
        .arg(&server.base_url)
        .arg("--instance")
        .arg(instance)
        .arg("login")
        .arg("--oidc")
        .arg("--no-browser")
        .timeout(Duration::from_secs(40));
    if brokered {
        login.arg("--brokered");
    }

    let login_output = run_cli_command(login).await?;
    if !login_output.status.success() {
        return Err(anyhow::anyhow!(
            "OIDC login command failed\nstdout:\n{}\nstderr:\n{}\nLast server logs:\n{}",
            String::from_utf8_lossy(&login_output.stdout),
            String::from_utf8_lossy(&login_output.stderr),
            log_tail(&server.log_path)
        ));
    }
    let login_stdout = String::from_utf8_lossy(&login_output.stdout);
    assert!(
        login_stdout.contains("Logged in with Dex"),
        "unexpected OIDC login output: {login_stdout}"
    );
    assert!(
        login_stdout.contains("OIDC DEVICE LOGIN"),
        "OIDC device login should show the shared CLI prompt: {login_stdout}"
    );
    let refresh_token = saved_refresh_token(&credentials_path, instance)?;
    assert!(!refresh_token.is_empty(), "OIDC device login should save a refresh token");

    let mut query = isolated_cli_command(cli_home.path(), &credentials_path);
    query
        .arg("--url")
        .arg(&server.base_url)
        .arg("--instance")
        .arg(instance)
        .arg("--command")
        .arg("SELECT 1 AS oidc_device_cli")
        .timeout(Duration::from_secs(30));

    let query_output = run_cli_command(query).await?;
    if !query_output.status.success() {
        return Err(anyhow::anyhow!(
            "OIDC credential check failed\nstdout:\n{}\nstderr:\n{}\nLast server logs:\n{}",
            String::from_utf8_lossy(&query_output.stdout),
            String::from_utf8_lossy(&query_output.stderr),
            log_tail(&server.log_path)
        ));
    }
    Ok(())
}

struct OidcServerConfig {
    issuer: String,
    client_id: String,
    client_secret: Option<String>,
    device_authorization_endpoint: Option<String>,
    broker_device_flow_enabled: bool,
}

async fn start_kalamdb_oidc_server(config: &OidcServerConfig) -> Result<KalamDbTestServer> {
    let data_dir = TempDir::new().context("failed to create KalamDB test data dir")?;
    let server_port = free_tcp_port().context("failed to reserve HTTP port")?;
    let rpc_port = free_tcp_port().context("failed to reserve RPC port")?;
    let api_port = free_tcp_port().context("failed to reserve cluster API port")?;
    let base_url = format!("http://127.0.0.1:{server_port}");
    let log_path = data_dir.path().join("server.log");
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("failed to open {}", log_path.display()))?;
    let log_file_err = log_file.try_clone().context("failed to clone server log file")?;
    let config_path = common::workspace_root().join("backend").join("server.toml");
    let server_bin = common::kalamdb_server_bin()
        .map_err(|error| anyhow::anyhow!("failed to resolve kalamdb-server binary: {error}"))?;
    let mut command = ProcessCommand::new(server_bin);
    command
        .env("KALAMDB_SERVER_HOST", "127.0.0.1")
        .env("KALAMDB_SERVER_PORT", server_port.to_string())
        .env("KALAMDB_CLUSTER_RPC_ADDR", format!("127.0.0.1:{rpc_port}"))
        .env("KALAMDB_CLUSTER_API_ADDR", format!("127.0.0.1:{api_port}"))
        .env("KALAMDB_DATA_DIR", data_dir.path())
        .env("KALAMDB_ENABLE_PGWIRE", "false")
        .env("KALAMDB_RATE_LIMIT_AUTH_REQUESTS_PER_IP_PER_SEC", "200000")
        .env("KALAMDB_LOG_LEVEL", "warn")
        .env("KALAMDB_JWT_TRUSTED_ISSUERS", format!("kalamdb,{}", config.issuer))
        .env("KALAMDB_AUTH_LOCAL_ENABLED", "true")
        .env("KALAMDB_AUTH_OIDC_ENABLED", "true")
        .env("KALAMDB_AUTH_OIDC_DISPLAY_NAME", "Dex")
        .env("KALAMDB_AUTH_OIDC_ISSUER", &config.issuer)
        .env("KALAMDB_AUTH_OIDC_CLIENT_ID", &config.client_id)
        .env("KALAMDB_AUTH_OIDC_SCOPES", "openid,email,profile")
        .env("KALAMDB_AUTH_OIDC_AUTO_PROVISION", "true")
        .env("KALAMDB_AUTH_OIDC_DEFAULT_ROLE", "user")
        .env(
            "KALAMDB_AUTH_OIDC_BROKER_DEVICE_FLOW_ENABLED",
            if config.broker_device_flow_enabled {
                "true"
            } else {
                "false"
            },
        )
        .arg(config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err));
    command.env("KALAMDB_AUTH_OIDC_CLIENT_SECRET", config.client_secret.as_deref().unwrap_or(""));
    if let Some(endpoint) = &config.device_authorization_endpoint {
        command.env("KALAMDB_AUTH_OIDC_DEVICE_AUTHORIZATION_ENDPOINT", endpoint);
    }

    let child = command.spawn().context("failed to spawn kalamdb-server")?;
    let mut server = KalamDbTestServer {
        base_url,
        child,
        data_dir,
        log_path,
    };
    wait_for_auth_status(&server.base_url, &server.log_path).await?;
    if let Err(error) = common::ensure_test_server_ready_for_url(&server.base_url).await {
        let tail = log_tail(&server.log_path);
        let _ = server.child.kill();
        let _ = server.child.wait();
        return Err(anyhow::anyhow!(
            "failed to prepare KalamDB auth on {}: {error}\nLast server logs:\n{tail}",
            server.base_url
        ));
    }
    Ok(server)
}

async fn wait_for_auth_status(base_url: &str, log_path: &Path) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build readiness HTTP client")?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let url = format!("{base_url}/v1/api/auth/status");
    loop {
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) if response.status() == StatusCode::PRECONDITION_REQUIRED => return Ok(()),
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(100)).await,
            _ => {
                return Err(anyhow::anyhow!(
                    "timed out waiting for KalamDB server at {base_url}\nLast server logs:\n{}",
                    log_tail(log_path)
                ));
            },
        }
    }
}

async fn fetch_login_options(server_url: &str) -> Result<serde_json::Value> {
    let response = reqwest::Client::new()
        .get(format!("{server_url}/v1/api/auth/login-options"))
        .send()
        .await
        .context("failed to request login options")?;
    let status = response.status();
    let body = response.text().await.context("failed to read login options")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!("login options failed with {status}: {body}"));
    }
    serde_json::from_str(&body).context("failed to parse login options")
}

async fn with_dex_provider_if_available<T, F, Fut>(test_fn: F) -> Result<Option<T>>
where
    F: FnOnce(DexProviderInfo) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    if let Some(provider) = local_dex_provider_if_available().await? {
        return test_fn(provider).await.map(Some);
    }

    let container = match Dex::default()
        .with_simple_user()
        .with_simple_client()
        .with_allow_password_grants()
        .start()
        .await
    {
        Ok(container) => container,
        Err(error) => {
            let message = error.to_string();
            if docker_unavailable_message(&message) {
                eprintln!(
                    "Skipping Dex-backed CLI OIDC test because Docker is unavailable: {message}"
                );
                return Ok(None);
            }
            return Err(error).context("failed to start Dex container");
        },
    };

    let host = container.get_host().await.context("failed to resolve Dex host")?.to_string();
    let port = container.get_host_port_ipv4(5556).await.context("failed to resolve Dex port")?;
    let provider = DexProviderInfo {
        issuer:                 format!("http://{host}:{port}"),
        token_url:              format!("http://{host}:{port}/token"),
        client_id:              DEX_CLIENT_ID.to_string(),
        client_secret:          Some(DEX_FALLBACK_CLIENT_SECRET.to_string()),
        username:               DEX_FALLBACK_USERNAME.to_string(),
        password:               DEX_FALLBACK_PASSWORD.to_string(),
        subject:                DEX_FALLBACK_USERNAME.to_string(),
        supports_browser_login: false,
    };
    let result = test_fn(provider).await;
    drop(container);
    result.map(Some)
}

async fn local_dex_provider_if_available() -> Result<Option<DexProviderInfo>> {
    let discovery_url = format!("{DEX_ISSUER}/.well-known/openid-configuration");
    let response = match reqwest::Client::new().get(&discovery_url).send().await {
        Ok(response) => response,
        Err(_) => return Ok(None),
    };

    if !response.status().is_success() {
        return Ok(None);
    }

    let discovery: serde_json::Value =
        response.json().await.context("failed to parse local Dex discovery response")?;
    let token_url = discovery["token_endpoint"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{DEX_ISSUER}/token"));

    let mut provider = DexProviderInfo {
        issuer: DEX_ISSUER.to_string(),
        token_url,
        client_id: DEX_CLIENT_ID.to_string(),
        client_secret: None,
        username: DEX_USERNAME.to_string(),
        password: DEX_PASSWORD.to_string(),
        subject: String::new(),
        supports_browser_login: true,
    };
    let token = issue_dex_token(&provider).await?;
    provider.subject = unverified_jwt_subject(&token)?;
    Ok(Some(provider))
}

async fn issue_dex_token(provider: &DexProviderInfo) -> Result<String> {
    let mut form = vec![
        ("grant_type", "password"),
        ("scope", "openid email profile"),
        ("username", provider.username.as_str()),
        ("password", provider.password.as_str()),
    ];
    if provider.client_secret.is_none() {
        form.push(("client_id", provider.client_id.as_str()));
    }

    let mut request = reqwest::Client::new().post(&provider.token_url);
    if let Some(client_secret) = &provider.client_secret {
        request = request.basic_auth(&provider.client_id, Some(client_secret));
    }
    let response = request.form(&form).send().await.context("failed to request Dex token")?;
    let status = response.status();
    let body = response.text().await.context("failed to read Dex token response")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!("Dex token request failed with {status}: {body}"));
    }
    let token_response: DexTokenResponse =
        serde_json::from_str(&body).context("failed to parse Dex token response")?;
    Ok(token_response.id_token.unwrap_or(token_response.access_token))
}

fn unverified_jwt_subject(token: &str) -> Result<String> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("Dex token was not a JWT"))?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .context("failed to decode Dex token payload")?;
    let claims: JwtPayloadSubject =
        serde_json::from_slice(&decoded).context("failed to parse Dex token payload")?;
    Ok(claims.sub)
}

fn isolated_cli_command(home: &Path, credentials_path: &Path) -> assert_cmd::Command {
    let mut command = assert_cmd::Command::new(common::kalam_bin());
    command
        .env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1")
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("KALAMDB_CREDENTIALS_PATH", credentials_path)
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy");
    command
}

async fn run_cli_command(mut command: assert_cmd::Command) -> Result<Output> {
    tokio::task::spawn_blocking(move || command.output())
        .await
        .context("CLI command task failed")?
        .context("failed to run CLI command")
}

async fn run_cli_browser_login_command(
    server: &KalamDbTestServer,
    provider: &DexProviderInfo,
    home: &Path,
    credentials_path: &Path,
    instance: &str,
) -> Result<Output> {
    let mut command = isolated_cli_process_command(home, credentials_path);
    command
        .arg("--url")
        .arg(&server.base_url)
        .arg("--instance")
        .arg(instance)
        .arg("login")
        .arg("--oidc")
        .env("KALAMDB_CLI_OIDC_OPEN_BROWSER", "false")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().context("failed to spawn CLI browser login")?;
    let stdout = child.stdout.take().context("CLI browser login stdout was not piped")?;
    let stderr = child.stderr.take().context("CLI browser login stderr was not piped")?;
    let (auth_url_sender, auth_url_receiver) = mpsc::channel();
    let stdout_reader = spawn_stdout_reader(stdout, auth_url_sender);
    let stderr_reader = spawn_stderr_reader(stderr);

    let auth_url = tokio::task::spawn_blocking(move || {
        auth_url_receiver.recv_timeout(Duration::from_secs(20))
    })
    .await
    .context("CLI browser login URL waiter failed")?
    .context("CLI browser login did not print an authorization URL")?;

    if let Err(error) = complete_dex_browser_login(provider, &auth_url).await {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }

    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .context("CLI browser login wait task failed")?
        .context("failed waiting for CLI browser login")?;
    let stdout = join_reader(stdout_reader, "stdout")?;
    let stderr = join_reader(stderr_reader, "stderr")?;

    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

async fn run_cli_device_login_command(
    server: &KalamDbTestServer,
    provider: &DexProviderInfo,
    home: &Path,
    credentials_path: &Path,
    instance: &str,
) -> Result<Output> {
    let mut command = isolated_cli_process_command(home, credentials_path);
    command
        .arg("--url")
        .arg(&server.base_url)
        .arg("--instance")
        .arg(instance)
        .arg("login")
        .arg("--oidc")
        .arg("--no-browser")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().context("failed to spawn CLI device login")?;
    let stdout = child.stdout.take().context("CLI device login stdout was not piped")?;
    let stderr = child.stderr.take().context("CLI device login stderr was not piped")?;
    let (verification_url_sender, verification_url_receiver) = mpsc::channel();
    let stdout_reader = spawn_stdout_reader(stdout, verification_url_sender);
    let stderr_reader = spawn_stderr_reader(stderr);

    let verification_url = tokio::task::spawn_blocking(move || {
        verification_url_receiver.recv_timeout(Duration::from_secs(20))
    })
    .await
    .context("CLI device login URL waiter failed")?
    .context("CLI device login did not print a verification URL")?;

    if let Err(error) = complete_dex_device_login(provider, &verification_url).await {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }

    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .context("CLI device login wait task failed")?
        .context("failed waiting for CLI device login")?;
    let stdout = join_reader(stdout_reader, "stdout")?;
    let stderr = join_reader(stderr_reader, "stderr")?;

    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn isolated_cli_process_command(home: &Path, credentials_path: &Path) -> ProcessCommand {
    let mut command = ProcessCommand::new(common::kalam_bin());
    command
        .env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1")
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("KALAMDB_CREDENTIALS_PATH", credentials_path)
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy");
    command
}

fn spawn_stdout_reader<R: Read + Send + 'static>(
    reader: R,
    auth_url_sender: mpsc::Sender<String>,
) -> JoinHandle<Result<Vec<u8>>> {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        let mut output = Vec::new();
        let mut sent_url = false;
        loop {
            let mut line = String::new();
            let read = reader.read_line(&mut line).context("failed to read CLI stdout")?;
            if read == 0 {
                break;
            }
            if !sent_url {
                if let Some(url) = extract_first_url(&line) {
                    let _ = auth_url_sender.send(url);
                    sent_url = true;
                }
            }
            output.extend_from_slice(line.as_bytes());
        }
        Ok(output)
    })
}

fn spawn_stderr_reader<R: Read + Send + 'static>(reader: R) -> JoinHandle<Result<Vec<u8>>> {
    std::thread::spawn(move || {
        let mut reader = reader;
        let mut output = Vec::new();
        reader.read_to_end(&mut output).context("failed to read CLI stderr")?;
        Ok(output)
    })
}

fn join_reader(handle: JoinHandle<Result<Vec<u8>>>, name: &str) -> Result<Vec<u8>> {
    handle
        .join()
        .map_err(|_| anyhow::anyhow!("CLI {name} reader thread panicked"))?
}

fn extract_first_url(line: &str) -> Option<String> {
    let start = line.find("http://").or_else(|| line.find("https://"))?;
    let rest = &line[start..];
    let end = rest
        .find(|character: char| character.is_whitespace() || character == '\u{1b}')
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

async fn complete_dex_browser_login(
    provider: &DexProviderInfo,
    authorization_url: &str,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build Dex browser automation client")?;
    let mut cookies = Vec::new();

    complete_dex_authorization_login(provider, &client, &mut cookies, authorization_url).await
}

async fn complete_dex_device_login(
    provider: &DexProviderInfo,
    verification_url: &str,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build Dex device automation client")?;
    let mut cookies = Vec::new();
    let user_code = Url::parse(verification_url)
        .context("Dex verification URL was invalid")?
        .query_pairs()
        .find_map(|(key, value)| (key == "user_code").then(|| value.to_string()))
        .ok_or_else(|| anyhow::anyhow!("Dex verification URL did not include a user_code"))?;

    let verification_response = client
        .get(verification_url)
        .send()
        .await
        .context("failed to open Dex device verification URL")?;
    collect_set_cookies(verification_response.headers(), &mut cookies);

    let mut verify = client
        .post(format!("{}/device/auth/verify_code", provider.issuer))
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .form(&[("user_code", user_code.as_str())]);
    if !cookies.is_empty() {
        verify = verify.header(COOKIE, cookies.join("; "));
    }
    let verify_response = verify.send().await.context("failed to submit Dex device code")?;
    collect_set_cookies(verify_response.headers(), &mut cookies);
    if !verify_response.status().is_redirection() {
        let status = verify_response.status();
        let body = verify_response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Dex device code submit failed with {status}: {body}"));
    }

    let auth_url =
        absolute_dex_url(provider, response_location(&verify_response, "device code submit")?)?;
    complete_dex_authorization_login(provider, &client, &mut cookies, &auth_url).await
}

async fn complete_dex_authorization_login(
    provider: &DexProviderInfo,
    client: &reqwest::Client,
    cookies: &mut Vec<String>,
    authorization_url: &str,
) -> Result<()> {
    let login_html = load_dex_login_page(provider, client, cookies, authorization_url).await?;
    let action = extract_form_action(&login_html)?;
    let action_url = absolute_dex_url(provider, &action)?;

    let mut post = client
        .post(action_url)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .form(&[
            ("login", provider.username.as_str()),
            ("password", provider.password.as_str()),
        ]);
    if !cookies.is_empty() {
        post = post.header(COOKIE, cookies.join("; "));
    }
    let login_submit = post.send().await.context("failed to submit Dex login form")?;
    collect_set_cookies(login_submit.headers(), cookies);
    if !login_submit.status().is_redirection() {
        let status = login_submit.status();
        let body = login_submit.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Dex login form failed with {status}: {body}"));
    }

    let callback_url =
        absolute_dex_url(provider, response_location(&login_submit, "Dex login submit")?)?;
    follow_oidc_completion_callback(provider, client, cookies, &callback_url).await
}

async fn load_dex_login_page(
    provider: &DexProviderInfo,
    client: &reqwest::Client,
    cookies: &mut Vec<String>,
    authorization_url: &str,
) -> Result<String> {
    let mut next_url = authorization_url.to_string();
    for _ in 0..5 {
        let response = get_with_cookies(client, &next_url, cookies)
            .await
            .context("failed to load Dex login page")?;
        collect_set_cookies(response.headers(), cookies);
        if response.status().is_success() {
            let body = response.text().await.context("failed to read Dex login page")?;
            if extract_form_action(&body).is_ok() {
                return Ok(body);
            }
            if let Some(local_connector_url) = extract_local_connector_href(&body) {
                next_url = absolute_dex_url(provider, &local_connector_url)?;
                continue;
            }
            return Ok(body);
        }
        if response.status().is_redirection() {
            next_url = absolute_dex_url(provider, response_location(&response, "Dex login page")?)?;
            continue;
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Dex login page failed with {status}: {body}"));
    }

    Err(anyhow::anyhow!("Dex login page redirected too many times"))
}

async fn follow_oidc_completion_callback(
    provider: &DexProviderInfo,
    client: &reqwest::Client,
    cookies: &mut Vec<String>,
    callback_url: &str,
) -> Result<()> {
    let mut next_url = callback_url.to_string();
    for _ in 0..5 {
        let response = get_with_cookies(client, &next_url, cookies)
            .await
            .context("failed to call OIDC completion callback")?;
        collect_set_cookies(response.headers(), cookies);
        if response.status().is_success() {
            return Ok(());
        }
        if response.status().is_redirection() {
            next_url =
                absolute_dex_url(provider, response_location(&response, "OIDC completion")?)?;
            continue;
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("OIDC completion callback failed with {status}: {body}"));
    }

    Err(anyhow::anyhow!("OIDC completion callback redirected too many times"))
}

async fn get_with_cookies(
    client: &reqwest::Client,
    url: &str,
    cookies: &[String],
) -> Result<reqwest::Response> {
    let mut request = client.get(url);
    if !cookies.is_empty() {
        request = request.header(COOKIE, cookies.join("; "));
    }
    request.send().await.context("failed to send Dex browser automation request")
}

fn collect_set_cookies(headers: &reqwest::header::HeaderMap, cookies: &mut Vec<String>) {
    for value in headers.get_all(SET_COOKIE).iter().filter_map(|value| value.to_str().ok()) {
        if let Some(cookie) = value.split(';').next().filter(|cookie| !cookie.is_empty()) {
            cookies.push(cookie.to_string());
        }
    }
}

fn response_location<'a>(response: &'a reqwest::Response, context: &str) -> Result<&'a str> {
    response
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("Dex {context} response did not include a Location header"))
}

fn absolute_dex_url(provider: &DexProviderInfo, location: &str) -> Result<String> {
    let decoded = decode_html_url_value(location);
    if decoded.starts_with("http://") || decoded.starts_with("https://") {
        return Ok(decoded);
    }
    if decoded.starts_with('/') {
        return Ok(format!("{}{}", provider.issuer, decoded));
    }
    Ok(format!("{}/{}", provider.issuer.trim_end_matches('/'), decoded))
}

fn extract_form_action(html: &str) -> Result<String> {
    let marker = "action=\"";
    let start = html
        .find(marker)
        .map(|index| index + marker.len())
        .ok_or_else(|| anyhow::anyhow!("Dex login page did not contain a form action"))?;
    let end = html[start..]
        .find('"')
        .map(|index| start + index)
        .ok_or_else(|| anyhow::anyhow!("Dex login form action was not terminated"))?;
    Ok(decode_html_url_value(&html[start..end]))
}

fn extract_local_connector_href(html: &str) -> Option<String> {
    let marker = "href=\"";
    let mut search_start = 0;
    while let Some(relative_start) = html[search_start..].find(marker) {
        let start = search_start + relative_start + marker.len();
        let Some(relative_end) = html[start..].find('"') else {
            return None;
        };
        let end = start + relative_end;
        let href = decode_html_url_value(&html[start..end]);
        if href.starts_with("/auth/local") || href.contains("/auth/local?") {
            return Some(href);
        }
        search_start = end + 1;
    }
    None
}

fn decode_html_url_value(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&#43;", "+")
        .replace("&#x2b;", "+")
        .replace("&#x2B;", "+")
}

fn saved_access_token(credentials_path: &Path, instance: &str) -> Result<String> {
    saved_credential_field(credentials_path, instance, "jwt_token")
}

fn saved_refresh_token(credentials_path: &Path, instance: &str) -> Result<String> {
    saved_credential_field(credentials_path, instance, "refresh_token")
}

fn saved_credential_field(credentials_path: &Path, instance: &str, field: &str) -> Result<String> {
    let contents = std::fs::read_to_string(credentials_path)
        .with_context(|| format!("failed to read {}", credentials_path.display()))?;
    let value: toml::Value =
        toml::from_str(&contents).context("failed to parse credentials TOML")?;
    value
        .get("instances")
        .and_then(|instances| instances.get(instance))
        .and_then(|credentials| credentials.get(field))
        .and_then(toml::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("saved credentials did not include {field} for {instance}"))
}

async fn fetch_current_user(server_url: &str, token: &str) -> Result<CurrentUserResponseView> {
    let response = reqwest::Client::new()
        .get(format!("{server_url}/v1/api/auth/me"))
        .bearer_auth(token)
        .send()
        .await
        .context("failed to request current user")?;
    let status = response.status();
    let body = response.text().await.context("failed to read current user response")?;
    if !status.is_success() {
        return Err(anyhow::anyhow!("current user request failed with {status}: {body}"));
    }
    serde_json::from_str(&body).context("failed to parse current user response")
}

async fn handle_oidc_provider_connection(
    mut socket: TcpStream,
    discovery: Arc<String>,
    jwks: Arc<String>,
    device_response: Arc<String>,
    token_response: Arc<String>,
) -> Result<()> {
    let mut buffer = [0_u8; 8192];
    let read = socket.read(&mut buffer).await.context("failed to read OIDC request")?;
    let request = String::from_utf8_lossy(&buffer[..read]);
    let request_line = request.lines().next().unwrap_or_default();
    if request_line.starts_with("GET /.well-known/openid-configuration ") {
        write_json_response(&mut socket, "200 OK", &discovery).await
    } else if request_line.starts_with("GET /jwks ") {
        write_json_response(&mut socket, "200 OK", &jwks).await
    } else if request_line.starts_with("GET /authorize ") {
        write_text_response(&mut socket, "200 OK", "authorization endpoint").await
    } else if request_line.starts_with("POST /device/code ") {
        write_json_response(&mut socket, "200 OK", &device_response).await
    } else if request_line.starts_with("POST /token ") {
        write_json_response(&mut socket, "200 OK", &token_response).await
    } else {
        write_text_response(&mut socket, "404 Not Found", "not found").await
    }
}

async fn write_json_response(socket: &mut TcpStream, status: &str, body: &str) -> Result<()> {
    write_response(socket, status, "application/json", body).await
}

async fn write_text_response(socket: &mut TcpStream, status: &str, body: &str) -> Result<()> {
    write_response(socket, status, "text/plain", body).await
}

async fn write_response(
    socket: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: \
         close\r\n\r\n{body}",
        body.len()
    );
    socket
        .write_all(response.as_bytes())
        .await
        .context("failed to write OIDC response")
}

fn issue_rs256_token(issuer: &str, client_id: &str, subject: &str, email: &str) -> Result<String> {
    let now = chrono::Utc::now().timestamp();
    let claims = TestJwtClaims {
        sub:   subject.to_string(),
        iss:   issuer.to_string(),
        aud:   client_id.to_string(),
        exp:   (now + 3600) as usize,
        iat:   now.saturating_sub(1) as usize,
        nbf:   now.saturating_sub(1) as usize,
        email: email.to_string(),
    };
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KEY_ID.to_string());
    let der = base64::engine::general_purpose::STANDARD
        .decode(TEST_RSA_PRIVATE_KEY_DER_BASE64)
        .context("invalid RSA private key DER")?;
    let encoding_key = EncodingKey::from_rsa_der(&der);
    jsonwebtoken::encode(&header, &claims, &encoding_key).context("failed to sign RS256 token")
}

fn free_tcp_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("failed to bind ephemeral port")?;
    Ok(listener.local_addr().context("failed to inspect ephemeral port")?.port())
}

fn log_tail(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .rev()
        .take(30)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

fn docker_unavailable_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("socket not found")
        || lower.contains("failed to initialize a docker client")
        || lower.contains("cannot connect to the docker daemon")
        || lower.contains("docker daemon is not running")
        || lower.contains("client error (connect)")
}

#[cfg(test)]
mod tests {
    use super::docker_unavailable_message;

    #[test]
    fn docker_unavailable_message_matches_hyper_connect_errors() {
        assert!(docker_unavailable_message(
            "failed to create a container: Error in the hyper legacy client: client error \
             (Connect)"
        ));
    }
}
