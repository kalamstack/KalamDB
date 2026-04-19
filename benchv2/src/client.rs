use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kalam_client::{
    AuthProvider, ConnectionOptions, HttpVersion, KalamLinkClient, KalamLinkTimeouts,
    QueryResponse, ServerSetupRequest, SubscriptionConfig, SubscriptionManager,
};
use serde::Deserialize;
use serde_json::Value;

const BENCH_HTTP_POOL_MAX_IDLE_PER_HOST: usize = 256;
const BENCH_HTTP_MAX_RETRIES: u32 = 5;

/// Thin wrapper around `KalamLinkClient` that provides convenient helpers
/// for the benchmark suite while delegating all HTTP / WebSocket work to kalam-link.
#[derive(Clone)]
pub struct KalamClient {
    endpoints: Arc<Vec<EndpointClient>>,
    next_endpoint: Arc<AtomicUsize>,
    ws_local_bind_addresses: Arc<Vec<String>>,
}

#[derive(Clone)]
struct EndpointClient {
    base_url: String,
    auth: AuthProvider,
    link: KalamLinkClient,
}

// Keep these simple response types so existing benchmarks compile unchanged.
#[derive(Debug, Deserialize)]
pub struct SqlResponse {
    pub status: String,
    #[serde(default)]
    pub results: Vec<SqlResult>,
    pub took: Option<f64>,
    pub error: Option<SqlError>,
}

#[derive(Debug, Deserialize)]
pub struct SqlResult {
    pub row_count: Option<u64>,
    pub message: Option<String>,
    pub rows: Option<Vec<Vec<Value>>>,
}

#[derive(Debug, Deserialize)]
pub struct SqlError {
    pub code: Option<String>,
    pub message: String,
}

impl KalamClient {
    /// Create a multi-endpoint client by logging into every configured URL.
    /// All URLs must be reachable and authenticatable, otherwise creation fails.
    pub async fn login(urls: &[String], user: &str, password: &str) -> Result<Self, String> {
        Self::login_with_options(urls, user, password, true).await
    }

    async fn login_with_options(
        urls: &[String],
        user: &str,
        password: &str,
        ensure_setup: bool,
    ) -> Result<Self, String> {
        let urls = normalize_http_urls(urls);
        if urls.is_empty() {
            return Err(
                "No valid server URLs provided. Use --urls with one or more endpoints.".to_string()
            );
        }

        let timeouts = default_timeouts();
        let ws_local_bind_addresses = resolve_ws_local_bind_addresses(&urls)?;
        let mut endpoints = Vec::with_capacity(urls.len());
        let mut failures = Vec::new();

        for base_url in urls {
            match Self::build_authenticated_endpoint(
                &base_url,
                user,
                password,
                &timeouts,
                &ws_local_bind_addresses,
                ensure_setup,
            )
            .await
            {
                Ok(endpoint) => endpoints.push(endpoint),
                Err(e) => failures.push(format!("{} -> {}", base_url, e)),
            }
        }

        if !failures.is_empty() {
            let mut msg = format!(
                "Failed to authenticate against {}/{} configured URL(s).",
                failures.len(),
                failures.len() + endpoints.len()
            );
            for failure in failures {
                msg.push_str("\n  - ");
                msg.push_str(&failure);
            }
            return Err(msg);
        }

        Ok(Self {
            endpoints: Arc::new(endpoints),
            next_endpoint: Arc::new(AtomicUsize::new(0)),
            ws_local_bind_addresses: Arc::new(ws_local_bind_addresses),
        })
    }

    /// Convenience helper for a single endpoint.
    pub async fn login_single(
        base_url: &str,
        user: &str,
        password: &str,
    ) -> Result<Self, String> {
        let urls = vec![base_url.to_string()];
        Self::login_with_options(&urls, user, password, true).await
    }

    /// Convenience helper for a single endpoint when setup is already complete.
    pub async fn login_single_steady_state(
        base_url: &str,
        user: &str,
        password: &str,
    ) -> Result<Self, String> {
        let urls = vec![base_url.to_string()];
        Self::login_with_options(&urls, user, password, false).await
    }

    async fn build_authenticated_endpoint(
        base_url: &str,
        user: &str,
        password: &str,
        timeouts: &KalamLinkTimeouts,
        ws_local_bind_addresses: &[String],
        ensure_setup: bool,
    ) -> Result<EndpointClient, String> {
        // Build an unauthenticated client first — needed for setup + login
        let unauthed = KalamLinkClient::builder()
            .base_url(base_url)
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| format!("failed to build kalam-link client: {}", e))?;

        // Complete setup if needed
        if ensure_setup {
            Self::complete_setup_if_needed(&unauthed, user, password).await;
        }

        // Login
        let login_resp = match unauthed.login(user, password).await {
            Ok(r) => r,
            Err(kalam_client::KalamLinkError::SetupRequired(_)) => {
                // Try setup again + retry login
                Self::complete_setup_if_needed(&unauthed, user, password).await;
                unauthed
                    .login(user, password)
                    .await
                    .map_err(|e| format!("Login failed after setup: {}", e))?
            },
            Err(e) => return Err(format!("Login failed: {}", e)),
        };

        let token = login_resp.access_token;
        let auth = AuthProvider::jwt_token(token);

        // Build the authenticated client with sensible timeouts
        let connection_options = benchmark_connection_options(ws_local_bind_addresses);
        let link_builder = KalamLinkClient::builder()
            .base_url(base_url)
            .auth(auth.clone())
            // Preserve enough keep-alive sockets for bursty load tests so
            // repeated iterations do not churn through local ephemeral ports.
            .http_pool_max_idle_per_host(BENCH_HTTP_POOL_MAX_IDLE_PER_HOST)
            .max_retries(BENCH_HTTP_MAX_RETRIES)
            .timeout(Duration::from_secs(60))
            .timeouts(timeouts.clone())
            .connection_options(connection_options);
        let link = link_builder
            .build()
            .map_err(|e| format!("failed to build authenticated kalam-link client: {}", e))?;

        Ok(EndpointClient {
            base_url: base_url.to_string(),
            auth,
            link,
        })
    }

    /// Complete initial server setup if the server hasn't been set up yet.
    async fn complete_setup_if_needed(client: &KalamLinkClient, user: &str, password: &str) {
        let Ok(status) = client.check_setup_status().await else {
            return;
        };
        if !status.needs_setup {
            return;
        }
        eprintln!("  Server needs initial setup, running setup...");
        let req = ServerSetupRequest::new(
            user.to_string(),
            password.to_string(),
            password.to_string(),
            None,
        );
        let _ = client.server_setup(req).await;
    }

    fn pick_endpoint(&self) -> EndpointClient {
        let idx = self.next_endpoint.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
        self.endpoints[idx].clone()
    }

    fn endpoint_for_http_url(&self, base_url: &str) -> Option<EndpointClient> {
        let normalized = normalize_http_url(base_url)?;
        self.endpoints.iter().find(|ep| ep.base_url == normalized).cloned()
    }

    fn endpoint_for_ws_url(&self, ws_url: &str) -> Option<EndpointClient> {
        let ws_norm = normalize_ws_url(ws_url)?;
        self.endpoints
            .iter()
            .find(|ep| {
                normalize_ws_url(&http_to_ws_url(&ep.base_url))
                    .as_ref()
                    .map(|v| v == &ws_norm)
                    .unwrap_or(false)
            })
            .cloned()
    }

    /// Execute a SQL statement returning a `QueryResponse` from kalam-link.
    pub async fn query(&self, sql: &str) -> Result<QueryResponse, String> {
        let endpoint = self.pick_endpoint();
        endpoint
            .link
            .execute_query(sql, None, None, None)
            .await
            .map_err(|e| format!("SQL error: {}", e))
    }

    /// Execute SQL against a specific configured URL.
    pub async fn query_on_url(&self, base_url: &str, sql: &str) -> Result<QueryResponse, String> {
        let endpoint = self
            .endpoint_for_http_url(base_url)
            .ok_or_else(|| format!("URL not configured in client: {}", base_url))?;

        endpoint
            .link
            .execute_query(sql, None, None, None)
            .await
            .map_err(|e| format!("SQL error: {}", e))
    }

    /// Execute a SQL statement and return the parsed response (legacy format).
    pub async fn sql(&self, sql: &str) -> Result<SqlResponse, String> {
        let resp = self.query(sql).await?;
        Ok(query_response_to_sql_response(resp))
    }

    /// Execute SQL against a specific configured URL and return the parsed response.
    pub async fn sql_on_url(&self, base_url: &str, sql: &str) -> Result<SqlResponse, String> {
        let resp = self.query_on_url(base_url, sql).await?;
        Ok(query_response_to_sql_response(resp))
    }

    /// Execute SQL expecting success, return error message on failure.
    pub async fn sql_ok(&self, sql: &str) -> Result<SqlResponse, String> {
        let resp = self.sql(sql).await?;
        if resp.status != "success" {
            let msg = resp.error.map(|e| e.message).unwrap_or_else(|| "unknown error".to_string());
            return Err(format!("SQL error: {}", msg));
        }
        Ok(resp)
    }

    /// Execute SQL expecting success on a specific URL.
    pub async fn sql_ok_on_url(&self, base_url: &str, sql: &str) -> Result<SqlResponse, String> {
        let resp = self.sql_on_url(base_url, sql).await?;
        if resp.status != "success" {
            let msg = resp.error.map(|e| e.message).unwrap_or_else(|| "unknown error".to_string());
            return Err(format!("SQL error: {}", msg));
        }
        Ok(resp)
    }

    /// Validate SQL success across all configured URLs.
    pub async fn validate_sql_on_all_urls(&self, sql: &str) -> Result<(), Vec<String>> {
        let mut failures = Vec::new();
        for url in self.urls() {
            if let Err(e) = self.sql_ok_on_url(&url, sql).await {
                failures.push(format!("{} -> {}", url, e));
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }

    /// Health check — can we reach the server on any endpoint.
    pub async fn health_check(&self) -> bool {
        let endpoint = self.pick_endpoint();
        endpoint.link.health_check().await.is_ok()
    }

    /// Subscribe to a live query using kalam-link's built-in subscription manager.
    pub async fn subscribe(&self, sql: &str) -> Result<SubscriptionManager, String> {
        let endpoint = self.pick_endpoint();
        endpoint
            .link
            .subscribe(sql)
            .await
            .map_err(|e| format!("Subscribe error: {}", e))
    }

    /// Subscribe with a full config (custom subscription ID, options, etc.).
    pub async fn subscribe_with_config(
        &self,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionManager, String> {
        let endpoint = if let Some(ref ws_url) = config.ws_url {
            self.endpoint_for_ws_url(ws_url).unwrap_or_else(|| self.pick_endpoint())
        } else {
            self.pick_endpoint()
        };

        endpoint
            .link
            .subscribe_with_config(config)
            .await
            .map_err(|e| format!("Subscribe error: {}", e))
    }

    /// Create an isolated kalam-link client for a single benchmark connection bucket.
    ///
    /// Unlike `link()`, this does not share the inner WebSocket connection with any
    /// other benchmark task, which lets scale tests deliberately open multiple pooled
    /// WebSocket connections when the backend enforces per-connection subscription caps.
    pub fn new_isolated_link_for_ws_url_with_bind_address(
        &self,
        ws_url: Option<&str>,
        bind_address: Option<&str>,
    ) -> Result<KalamLinkClient, String> {
        let endpoint = if let Some(ws_url) = ws_url {
            self.endpoint_for_ws_url(ws_url)
                .ok_or_else(|| format!("WebSocket URL not configured in client: {}", ws_url))?
        } else {
            self.pick_endpoint()
        };

        let ws_local_bind_addresses = bind_address
            .map(|address| vec![address.to_string()])
            .unwrap_or_else(|| self.ws_local_bind_addresses.as_ref().clone());
        let connection_options = benchmark_connection_options(&ws_local_bind_addresses);

        KalamLinkClient::builder()
            .base_url(endpoint.base_url)
            .auth(endpoint.auth)
            .http_pool_max_idle_per_host(BENCH_HTTP_POOL_MAX_IDLE_PER_HOST)
            .max_retries(BENCH_HTTP_MAX_RETRIES)
            .timeout(Duration::from_secs(60))
            .timeouts(endpoint.link.timeouts().clone())
            .connection_options(connection_options)
            .build()
            .map_err(|e| format!("failed to build isolated kalam-link client: {}", e))
    }

    pub fn new_isolated_link_for_ws_url(
        &self,
        ws_url: Option<&str>,
    ) -> Result<KalamLinkClient, String> {
        self.new_isolated_link_for_ws_url_with_bind_address(ws_url, None)
    }

    pub fn ws_local_bind_addresses(&self) -> &[String] {
        self.ws_local_bind_addresses.as_ref()
    }

    pub fn ws_local_bind_address_for_index(&self, index: usize) -> Option<&str> {
        if self.ws_local_bind_addresses.is_empty() {
            None
        } else {
            Some(self.ws_local_bind_addresses[index % self.ws_local_bind_addresses.len()].as_str())
        }
    }

    /// Get a reference to one inner client for advanced use.
    /// Selection is round-robin across configured URLs.
    pub fn link(&self) -> KalamLinkClient {
        self.pick_endpoint().link
    }

    /// Returns all configured base URLs.
    pub fn urls(&self) -> Vec<String> {
        self.endpoints.iter().map(|ep| ep.base_url.clone()).collect()
    }

    /// Returns a representative base URL (first configured endpoint).
    pub fn base_url(&self) -> &str {
        &self.endpoints[0].base_url
    }

    /// Returns normalized WebSocket URLs derived from configured HTTP URLs.
    pub fn ws_urls(&self) -> Vec<String> {
        self.endpoints.iter().map(|ep| http_to_ws_url(&ep.base_url)).collect()
    }
}

fn resolve_ws_local_bind_addresses(urls: &[String]) -> Result<Vec<String>, String> {
    if let Some(configured) = configured_ws_local_bind_addresses()? {
        return Ok(configured);
    }

    Ok(derive_loopback_bind_addresses(urls))
}

fn configured_ws_local_bind_addresses() -> Result<Option<Vec<String>>, String> {
    let Some(raw) = std::env::var("KALAMDB_BENCH_WS_LOCAL_BIND_ADDRESSES").ok() else {
        return Ok(None);
    };

    let mut bind_addrs = Vec::new();
    for token in raw.split(',') {
        let candidate = token.trim();
        if candidate.is_empty() {
            continue;
        }

        let ip = candidate
            .parse::<IpAddr>()
            .map_err(|e| format!("Invalid KALAMDB_BENCH_WS_LOCAL_BIND_ADDRESSES entry '{}': {}", candidate, e))?;
        let normalized = ip.to_string();
        if !bind_addrs.contains(&normalized) {
            bind_addrs.push(normalized);
        }
    }

    if bind_addrs.is_empty() {
        return Err(
            "KALAMDB_BENCH_WS_LOCAL_BIND_ADDRESSES was set but no valid IP addresses were provided"
                .to_string(),
        );
    }

    Ok(Some(bind_addrs))
}

fn default_timeouts() -> KalamLinkTimeouts {
    KalamLinkTimeouts::builder()
        .connection_timeout(Duration::from_secs(45))
        .receive_timeout(Duration::from_secs(60))
        .send_timeout(Duration::from_secs(30))
        .subscribe_timeout(Duration::from_secs(45))
        .auth_timeout(Duration::from_secs(30))
        .initial_data_timeout(Duration::from_secs(60))
        .keepalive_interval_secs(20)
        .build()
}

fn benchmark_connection_options(ws_local_bind_addresses: &[String]) -> ConnectionOptions {
    let mut options = ConnectionOptions::new()
        .with_ws_local_bind_addresses(ws_local_bind_addresses.to_vec());

    if std::env::var("KALAMDB_BENCH_HTTP2").ok().as_deref() == Some("1") {
        options = options.with_http_version(HttpVersion::Http2);
    }

    options
}

fn normalize_http_urls(urls: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for raw in urls {
        if let Some(url) = normalize_http_url(raw) {
            if !normalized.contains(&url) {
                normalized.push(url);
            }
        }
    }
    normalized
}

fn derive_loopback_bind_addresses(urls: &[String]) -> Vec<String> {
    let mut bind_addrs = Vec::new();
    for url in urls {
        let Some(host) = extract_url_host(url) else {
            continue;
        };
        let Ok(ip) = host.parse::<IpAddr>() else {
            continue;
        };
        if ip.is_loopback() {
            let normalized = ip.to_string();
            if !bind_addrs.contains(&normalized) {
                bind_addrs.push(normalized);
            }
        }
    }

    // Only force a local bind when there is an actual pool to rotate through.
    // Pinning every WebSocket to a single loopback address can reduce the usable
    // socket fanout on macOS even when the benchmark is spreading load across
    // multiple destination ports.
    if bind_addrs.len() > 1 {
        bind_addrs
    } else {
        Vec::new()
    }
}

fn extract_url_host(url: &str) -> Option<String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }

    let after_scheme = trimmed.split("://").nth(1).unwrap_or(trimmed);
    let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
    let host_port = authority.rsplit('@').next().unwrap_or(authority);

    if host_port.starts_with('[') {
        let end = host_port.find(']')?;
        return Some(host_port[1..end].to_string());
    }

    let host = host_port.split(':').next().unwrap_or(host_port);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

fn normalize_http_url(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let with_scheme = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{}", trimmed)
    };

    Some(with_scheme.trim_end_matches('/').to_string())
}

fn normalize_ws_url(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut ws = if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        trimmed.to_string()
    } else if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.replace("http://", "ws://").replace("https://", "wss://")
    } else {
        format!("ws://{}", trimmed)
    };

    ws = ws.trim_end_matches('/').to_string();
    if !ws.contains("/v1/ws") {
        ws.push_str("/v1/ws");
    }

    Some(ws)
}

fn http_to_ws_url(base_url: &str) -> String {
    normalize_ws_url(base_url).unwrap_or_else(|| "ws://localhost:8080/v1/ws".to_string())
}

/// Convert a kalam-link `QueryResponse` into the legacy `SqlResponse` used by benchmarks.
fn query_response_to_sql_response(resp: QueryResponse) -> SqlResponse {
    let status = if resp.success() {
        "success".to_string()
    } else {
        "error".to_string()
    };

    let error = resp.error.as_ref().map(|e| SqlError {
        code: Some(e.code.clone()),
        message: e.message.clone(),
    });

    let results = resp
        .results
        .iter()
        .map(|r| SqlResult {
            row_count: Some(r.row_count as u64),
            message: r.message.clone(),
            rows: r.rows.clone().map(|rows| {
                rows.into_iter()
                    .map(|row| row.into_iter().map(Value::from).collect())
                    .collect()
            }),
        })
        .collect();

    SqlResponse {
        status,
        results,
        took: resp.took,
        error,
    }
}
