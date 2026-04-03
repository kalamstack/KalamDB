//! Main KalamDB client with builder pattern.
//!
//! Provides the primary interface for connecting to KalamDB servers
//! and executing operations.

use crate::{
    auth::{AuthProvider, ResolvedAuth},
    connection::SharedConnection,
    consumer::ConsumerBuilder,
    error::{KalamLinkError, Result},
    event_handlers::EventHandlers,
    models::{
        ConnectionOptions, HealthCheckResponse, HttpVersion, QueryResponse, SubscriptionConfig,
    },
    query::{AuthRefreshCallback, QueryExecutor, UploadProgressCallback},
    subscription::{LiveRowsConfig, LiveRowsSubscription, SubscriptionManager},
    timeouts::KalamLinkTimeouts,
};
use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

/// Main KalamDB client.
///
/// Use [`KalamLinkClientBuilder`] to construct instances with custom configuration.
///
/// # Examples
///
/// ```rust,no_run
/// use kalam_link::KalamLinkClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = KalamLinkClient::builder()
///     .base_url("http://localhost:3000")
///     .timeout(std::time::Duration::from_secs(30))
///     .build()?;
///
/// let response = client.execute_query("SELECT 1", None, None, None).await?;
/// println!("Result: {:?}", response);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct KalamLinkClient {
    base_url: String,
    http_client: reqwest::Client,
    /// Dynamic or static authentication source.
    resolved_auth: ResolvedAuth,
    /// Last successfully resolved static credentials (cached for subscriptions/queries).
    auth: AuthProvider,
    query_executor: QueryExecutor,
    health_cache: Arc<Mutex<HealthCheckCache>>,
    timeouts: KalamLinkTimeouts,
    connection_options: ConnectionOptions,
    event_handlers: EventHandlers,
    /// Shared authentication source readable by the background connection task.
    /// Updated by `set_auth()`.
    shared_resolved_auth: Arc<RwLock<ResolvedAuth>>,
    /// Shared WebSocket connection — `None` until `connect()` is called.
    connection: Arc<Mutex<Option<Arc<SharedConnection>>>>,
}

impl KalamLinkClient {
    /// Create a new builder for configuring the client
    pub fn builder() -> KalamLinkClientBuilder {
        KalamLinkClientBuilder::new()
    }

    /// Execute a SQL query with optional files, parameters, and namespace context
    ///
    /// # Arguments
    /// * `sql` - The SQL query string
    /// * `files` - Optional file uploads for FILE("name") placeholders
    /// * `params` - Optional query parameters for $1, $2, ... placeholders
    /// * `namespace_id` - Optional namespace for unqualified table names
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kalam_link::KalamLinkClient::builder().base_url("http://localhost:3000").build()?;
    /// // Simple query
    /// let result = client.execute_query("SELECT * FROM users", None, None, None).await?;
    ///
    /// // Query with parameters
    /// let params = vec![serde_json::json!(42)];
    /// let result = client.execute_query("SELECT * FROM users WHERE id = $1", None, Some(params), None).await?;
    ///
    /// // Query in specific namespace
    /// let result = client.execute_query("SELECT * FROM messages", None, None, Some("chat")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_query(
        &self,
        sql: &str,
        files: Option<Vec<(&str, &str, Vec<u8>, Option<&str>)>>,
        params: Option<Vec<serde_json::Value>>,
        namespace_id: Option<&str>,
    ) -> Result<QueryResponse> {
        self.execute_query_with_progress(sql, files, params, namespace_id, None).await
    }

    /// Execute a SQL query with optional files and a progress callback for uploads.
    pub async fn execute_query_with_progress(
        &self,
        sql: &str,
        files: Option<Vec<(&str, &str, Vec<u8>, Option<&str>)>>,
        params: Option<Vec<serde_json::Value>>,
        namespace_id: Option<&str>,
        progress: Option<UploadProgressCallback>,
    ) -> Result<QueryResponse> {
        let files_owned = files.map(|items| {
            items
                .into_iter()
                .map(|(placeholder, filename, data, mime)| {
                    (
                        placeholder.to_string(),
                        filename.to_string(),
                        data,
                        mime.map(|m| m.to_string()),
                    )
                })
                .collect()
        });

        self.query_executor
            .execute_with_progress(
                sql,
                files_owned,
                params,
                namespace_id.map(|s| s.to_string()),
                progress,
            )
            .await
    }

    /// Execute a SQL query with file uploads (FILE datatype support).
    ///
    /// This method allows inserting/updating rows that contain FILE columns.
    /// Use FILE("name") placeholders in SQL that reference uploaded files.
    ///
    /// # Arguments
    /// * `sql` - SQL statement containing FILE("name") placeholders
    /// * `files` - Vector of (placeholder_name, filename, data, optional_mime_type)
    /// * `params` - Optional query parameters for $1, $2, ... placeholders  
    /// * `namespace_id` - Optional namespace for unqualified table names
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kalam_link::KalamLinkClient::builder().base_url("http://localhost:3000").build()?;
    /// use std::fs;
    ///
    /// // Read file from disk
    /// let avatar_data = fs::read("avatar.png")?;
    ///
    /// // Insert with file
    /// let result = client.execute_with_files(
    ///     "INSERT INTO users (name, avatar) VALUES ($1, FILE(\"avatar\"))",
    ///     vec![("avatar", "avatar.png", avatar_data, Some("image/png"))],
    ///     Some(vec![serde_json::json!("Alice")]),
    ///     None
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "tokio-runtime")]
    pub async fn execute_with_files(
        &self,
        sql: &str,
        files: Vec<(&str, &str, Vec<u8>, Option<&str>)>,
        params: Option<Vec<serde_json::Value>>,
        namespace_id: Option<&str>,
    ) -> Result<QueryResponse> {
        self.execute_query(sql, Some(files), params, namespace_id).await
    }

    /// Execute a SQL query with file uploads and a progress callback.
    #[cfg(feature = "tokio-runtime")]
    pub async fn execute_with_files_with_progress(
        &self,
        sql: &str,
        files: Vec<(&str, &str, Vec<u8>, Option<&str>)>,
        params: Option<Vec<serde_json::Value>>,
        namespace_id: Option<&str>,
        progress: Option<UploadProgressCallback>,
    ) -> Result<QueryResponse> {
        self.execute_query_with_progress(sql, Some(files), params, namespace_id, progress)
            .await
    }

    /// Subscribe to real-time changes
    ///
    /// Subscriptions are multiplexed over the shared WebSocket connection.
    pub async fn subscribe(&self, query: &str) -> Result<SubscriptionManager> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let subscription_id = format!("sub_{}", nanos);
        self.subscribe_with_config(SubscriptionConfig::new(subscription_id, query))
            .await
    }

    /// Subscribe with advanced configuration (pre-generated ID, options, ws_url override)
    ///
    /// When [`ConnectionOptions::ws_lazy_connect`] is `true` (the default)
    /// and no shared connection exists yet, `connect()` is called
    /// automatically before subscribing.
    pub async fn subscribe_with_config(
        &self,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionManager> {
        // ── Auto-connect: establish the shared connection on first subscribe
        if self.connection_options.ws_lazy_connect {
            let conn_guard = self.connection.lock().await;
            if conn_guard.is_none() {
                drop(conn_guard); // release lock before connect()
                self.connect().await?;
            }
        }

        // ── Shared connection path ───────────────────────────────────────
        {
            let conn_guard = self.connection.lock().await;
            if let Some(ref conn) = *conn_guard {
                let options = config.options.unwrap_or_default();
                let (event_rx, generation, resume_from) =
                    conn.subscribe(config.id.clone(), config.sql, options).await?;
                let unsub_tx = conn.unsubscribe_tx();
                let progress_tx = conn.progress_tx();
                return Ok(SubscriptionManager::from_shared(
                    config.id,
                    event_rx,
                    unsub_tx,
                    progress_tx,
                    generation,
                    resume_from,
                    &self.timeouts,
                ));
            }
        }

        Err(crate::error::KalamLinkError::WebSocketError(
            "Not connected. Call connect() before subscribing.".to_string(),
        ))
    }

    /// Subscribe to a SQL query and receive materialized row snapshots.
    pub async fn live_query_rows(&self, query: &str) -> Result<LiveRowsSubscription> {
        self.live_query_rows_with_config(
            SubscriptionConfig::new(
                format!(
                    "live_rows_{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                ),
                query,
            ),
            LiveRowsConfig::default(),
        )
        .await
    }

    /// Subscribe with advanced low-level and materialization configuration.
    pub async fn live_query_rows_with_config(
        &self,
        config: SubscriptionConfig,
        live_rows_config: LiveRowsConfig,
    ) -> Result<LiveRowsSubscription> {
        let subscription = self.subscribe_with_config(config).await?;
        Ok(LiveRowsSubscription::new(subscription, live_rows_config))
    }

    /// Establish a shared WebSocket connection.
    ///
    /// After calling this, all subsequent [`subscribe()`](Self::subscribe)
    /// calls will multiplex over the single connection.  The connection
    /// handles auto-reconnection and re-subscription automatically.
    ///
    /// Calling `connect()` when already connected is a no-op.
    pub async fn connect(&self) -> Result<()> {
        let mut conn_guard = self.connection.lock().await;
        if conn_guard.is_some() {
            return Ok(());
        }

        let conn = SharedConnection::connect(
            self.base_url.clone(),
            self.shared_resolved_auth.clone(),
            self.timeouts.clone(),
            self.connection_options.clone(),
            self.event_handlers.clone(),
        )
        .await?;

        *conn_guard = Some(Arc::new(conn));
        Ok(())
    }

    /// Disconnect the shared WebSocket connection.
    ///
    /// All active subscriptions will be unsubscribed and the background
    /// task will be shut down.  After this, new `subscribe()` calls will
    /// fall back to per-subscription connections until `connect()` is
    /// called again.
    pub async fn disconnect(&self) {
        let conn = {
            let mut guard = self.connection.lock().await;
            guard.take()
        };
        if let Some(conn) = conn {
            conn.disconnect().await;
        }
    }

    /// Cancel / unsubscribe a subscription by ID on the shared connection.
    ///
    /// This sends an explicit `Unsubscribe` command (generation=None) to
    /// the connection task, which removes the entry from the local map,
    /// sends an unsubscribe message to the server, and **drops** the event
    /// channel sender — causing any in-flight `SubscriptionManager::next()`
    /// call to return `None`.
    ///
    /// Use this when you need to cancel a subscription without holding the
    /// `SubscriptionManager` mutex (e.g., from a Dart `onCancel` callback
    /// while `dartSubscriptionNext` is blocked on the same mutex).
    pub async fn cancel_subscription(&self, id: &str) -> Result<()> {
        let guard = self.connection.lock().await;
        if let Some(ref conn) = *guard {
            conn.unsubscribe(id).await?;
        }
        Ok(())
    }

    /// Whether a shared connection is currently established and connected.
    pub async fn is_connected(&self) -> bool {
        let guard = self.connection.lock().await;
        guard.as_ref().map_or(false, |c| c.is_connected())
    }

    /// List all active subscriptions on the shared connection.
    ///
    /// Returns a snapshot of each subscription's metadata including its ID,
    /// SQL query, last received sequence ID, and last event timestamp.
    ///
    /// Returns an empty vec when no shared connection is active (i.e.
    /// `connect()` has not been called).
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = kalam_link::KalamLinkClient::builder().base_url("http://localhost:3000").build()?;
    /// let subs = client.subscriptions().await;
    /// for s in &subs {
    ///     println!("sub {} — query: {}, last_seq: {:?}", s.id, s.query, s.last_seq_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscriptions(&self) -> Vec<crate::models::SubscriptionInfo> {
        let guard = self.connection.lock().await;
        match guard.as_ref() {
            Some(conn) => conn.list_subscriptions().await,
            None => Vec::new(),
        }
    }

    /// Get the current event handlers
    pub fn event_handlers(&self) -> &EventHandlers {
        &self.event_handlers
    }

    /// Get the configured timeouts
    pub fn timeouts(&self) -> &KalamLinkTimeouts {
        &self.timeouts
    }

    /// Create a topic consumer builder bound to this client
    pub fn consumer(&self) -> ConsumerBuilder {
        ConsumerBuilder::from_client(self.clone())
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    pub(crate) fn auth(&self) -> &AuthProvider {
        &self.auth
    }

    /// Return the resolved auth source (static or dynamic).
    pub fn resolved_auth(&self) -> &ResolvedAuth {
        &self.resolved_auth
    }

    /// Replace the static authentication credentials at runtime.
    ///
    /// Useful when a Dart/FFI caller has obtained fresh credentials via its own
    /// auth provider callback and wants to push them into the Rust client before
    /// the next subscribe call.
    ///
    /// For dynamic auth flows prefer using a [`DynamicAuthProvider`] instead.
    ///
    /// [`DynamicAuthProvider`]: crate::auth::DynamicAuthProvider
    pub fn set_auth(&mut self, auth: AuthProvider) {
        self.auth = auth.clone();
        self.query_executor.set_auth(auth.clone());
        let resolved = ResolvedAuth::Static(auth);
        self.resolved_auth = resolved.clone();
        // Update the shared source so the background connection task picks up
        // fresh credentials on its next reconnect cycle.
        *self.shared_resolved_auth.write().unwrap() = resolved;
    }

    /// Update the shared authentication source without requiring `&mut self`.
    ///
    /// Updates both `shared_resolved_auth` (used by subscribe/connect paths)
    /// and the `QueryExecutor`'s auth snapshot (used by HTTP queries) so that
    /// fresh credentials from Dart's `authProvider` take effect immediately for
    /// all in-flight and future requests.
    ///
    /// Use this from FFI callers (e.g. Dart) where `&mut self` would require
    /// an exclusive write lock on the opaque handle — which deadlocks when the
    /// connection event pump holds a long-lived read lock.
    pub fn update_shared_auth(&self, auth: AuthProvider) {
        // Update the HTTP query executor so new queries use the fresh token.
        self.query_executor.set_auth(auth.clone());
        let resolved = ResolvedAuth::Static(auth);
        *self.shared_resolved_auth.write().unwrap() = resolved;
    }

    /// Resolve fresh credentials from the auth source.
    ///
    /// For static auth this is a cheap clone.  For a [`DynamicAuthProvider`]
    /// this calls `get_auth()`, which may perform network I/O (token refresh,
    /// secret-manager lookup, etc.).
    ///
    /// Callers that open WebSocket connections should call this before each
    /// connect attempt to guarantee up-to-date tokens.
    ///
    /// [`DynamicAuthProvider`]: crate::auth::DynamicAuthProvider
    pub async fn fresh_auth(&self) -> Result<AuthProvider> {
        self.resolved_auth.resolve().await
    }

    /// Check server health and get server information
    pub async fn health_check(&self) -> Result<HealthCheckResponse> {
        {
            let cache = self.health_cache.lock().await;
            if let (Some(last_check), Some(response)) =
                (cache.last_check, cache.last_response.clone())
            {
                if last_check.elapsed() < HEALTH_CHECK_TTL {
                    log::debug!(
                        "[HEALTH_CHECK] Returning cached response (age: {:?})",
                        last_check.elapsed()
                    );
                    return Ok(response);
                }
            }
        }

        let url = format!("{}/v1/api/healthcheck", self.base_url);
        log::debug!("[HEALTH_CHECK] Fetching from url={}", url);
        let start = std::time::Instant::now();
        let response = self.http_client.get(&url).send().await?;
        log::debug!(
            "[HEALTH_CHECK] HTTP response received in {:?}, status={}",
            start.elapsed(),
            response.status()
        );
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let message = if status.as_u16() == 403 {
                format!("Health check endpoint is restricted to localhost connections ({})", body)
            } else {
                format!("HTTP {} — {}", status, body)
            };
            return Err(crate::error::KalamLinkError::ServerError {
                status_code: status.as_u16(),
                message,
            });
        }
        let health_response = response.json::<HealthCheckResponse>().await?;
        log::debug!("[HEALTH_CHECK] JSON parsed in {:?}", start.elapsed());

        let mut cache = self.health_cache.lock().await;
        cache.last_check = Some(Instant::now());
        cache.last_response = Some(health_response.clone());

        Ok(health_response)
    }

    /// Login with username and password to obtain a JWT token
    ///
    /// This method authenticates with the server and returns a JWT access token
    /// that can be used for subsequent API calls via `AuthProvider::jwt_token()`.
    ///
    /// # Arguments
    /// * `username` - The username for authentication
    /// * `password` - The password for authentication
    ///
    /// # Returns
    /// A `LoginResponse` containing the JWT access token and user information
    ///
    /// # Example
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use kalam_link::{KalamLinkClient, AuthProvider};
    ///
    /// // Create a client without authentication to perform login
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .build()?;
    ///
    /// // Login to get JWT token
    /// let login_response = client.login("alice", "secret123").await?;
    ///
    /// // Create a new client with the JWT token for subsequent calls
    /// let authenticated_client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .auth(AuthProvider::jwt_token(login_response.access_token))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn login(
        &self,
        username: &str,
        password: &str,
    ) -> Result<crate::models::LoginResponse> {
        let url = format!("{}/v1/api/auth/login", self.base_url);
        log::debug!("[LOGIN] Authenticating user '{}' at url={}", username, url);

        let login_request = crate::models::LoginRequest {
            username: username.to_string(),
            password: password.to_string(),
        };

        let start = std::time::Instant::now();
        let response = self.http_client.post(&url).json(&login_request).send().await?;

        let status = response.status();
        log::debug!("[LOGIN] HTTP response received in {:?}, status={}", start.elapsed(), status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            log::debug!("[LOGIN] Login failed: {}", error_text);

            // Check for setup_required error (HTTP 428 Precondition Required)
            if status.as_u16() == 428 {
                // Parse the error message for more details
                let message = if let Ok(error_json) =
                    serde_json::from_str::<serde_json::Value>(&error_text)
                {
                    error_json
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Server requires initial setup")
                        .to_string()
                } else {
                    "Server requires initial setup".to_string()
                };
                return Err(KalamLinkError::SetupRequired(message));
            }

            return Err(KalamLinkError::AuthenticationError(format!(
                "Login failed ({}): {}",
                status, error_text
            )));
        }

        let login_response = response.json::<crate::models::LoginResponse>().await?;
        log::debug!(
            "[LOGIN] Successfully authenticated user '{}' in {:?}",
            username,
            start.elapsed()
        );

        Ok(login_response)
    }

    /// Refresh an access token using a refresh token.
    ///
    /// This is useful when the access token has expired but the refresh token
    /// is still valid. Returns a new LoginResponse with fresh tokens.
    ///
    /// # Arguments
    /// * `refresh_token` - The refresh token obtained from a previous login
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::KalamLinkClient;
    ///
    /// # async fn example() -> kalam_link::Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .build()?;
    ///
    /// // Refresh using a stored refresh token
    /// let response = client.refresh_access_token("old_refresh_token").await?;
    /// println!("New access token: {}", response.access_token);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn refresh_access_token(
        &self,
        refresh_token: &str,
    ) -> Result<crate::models::LoginResponse> {
        let url = format!("{}/v1/api/auth/refresh", self.base_url);
        log::debug!("[REFRESH] Refreshing access token at url={}", url);

        let start = std::time::Instant::now();

        // Send refresh token in Authorization header as Bearer token
        let response = self
            .http_client
            .post(&url)
            .header("Authorization", format!("Bearer {}", refresh_token))
            .send()
            .await?;

        let status = response.status();
        log::debug!("[REFRESH] HTTP response received in {:?}, status={}", start.elapsed(), status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            log::debug!("[REFRESH] Token refresh failed: {}", error_text);
            return Err(KalamLinkError::AuthenticationError(format!(
                "Token refresh failed ({}): {}",
                status, error_text
            )));
        }

        let login_response = response.json::<crate::models::LoginResponse>().await?;
        log::debug!("[REFRESH] Successfully refreshed token in {:?}", start.elapsed());

        Ok(login_response)
    }

    /// Check if the server requires initial setup.
    ///
    /// Returns a SetupStatusResponse indicating whether setup is needed.
    /// This endpoint only works from localhost connections.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::KalamLinkClient;
    ///
    /// # async fn example() -> kalam_link::Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:8080")
    ///     .build()?;
    ///
    /// let status = client.check_setup_status().await?;
    /// if status.needs_setup {
    ///     println!("Server requires setup: {}", status.message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_setup_status(&self) -> Result<crate::models::SetupStatusResponse> {
        let url = format!("{}/v1/api/auth/status", self.base_url);
        log::debug!("[SETUP] Checking setup status at url={}", url);

        let start = std::time::Instant::now();
        let response = self.http_client.get(&url).send().await?;

        let status = response.status();
        log::debug!("[SETUP] Status check response in {:?}, status={}", start.elapsed(), status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(KalamLinkError::ServerError {
                status_code: status.as_u16(),
                message: error_text,
            });
        }

        Ok(response.json::<crate::models::SetupStatusResponse>().await?)
    }

    /// Perform initial server setup.
    ///
    /// This sets the root password and creates a DBA user.
    /// Only works from localhost when root has no password configured.
    ///
    /// # Arguments
    /// * `request` - The setup request containing username, password, root_password, and optional email
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::{KalamLinkClient, ServerSetupRequest};
    ///
    /// # async fn example() -> kalam_link::Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:8080")
    ///     .build()?;
    ///
    /// let request = ServerSetupRequest::new(
    ///     "admin",
    ///     "secure_password123",
    ///     "root_password123",
    ///     Some("admin@example.com".to_string()),
    /// );
    ///
    /// let response = client.server_setup(request).await?;
    /// println!("Setup complete: {}", response.message);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn server_setup(
        &self,
        request: crate::models::ServerSetupRequest,
    ) -> Result<crate::models::ServerSetupResponse> {
        let url = format!("{}/v1/api/auth/setup", self.base_url);
        log::debug!("[SETUP] Performing server setup at url={}", url);

        let start = std::time::Instant::now();
        let response = self.http_client.post(&url).json(&request).send().await?;

        let status = response.status();
        log::debug!("[SETUP] Setup response in {:?}, status={}", start.elapsed(), status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());

            // Check for specific error types
            if status.as_u16() == 409 {
                return Err(KalamLinkError::ConfigurationError(
                    "Server is already configured".to_string(),
                ));
            }
            if status.as_u16() == 400 {
                // Parse error for weak password or invalid username
                if let Ok(error_json) = serde_json::from_str::<serde_json::Value>(&error_text) {
                    if let Some(message) = error_json.get("message").and_then(|m| m.as_str()) {
                        return Err(KalamLinkError::ConfigurationError(message.to_string()));
                    }
                }
            }

            return Err(KalamLinkError::ServerError {
                status_code: status.as_u16(),
                message: error_text,
            });
        }

        let setup_response = response.json::<crate::models::ServerSetupResponse>().await?;
        log::info!("[SETUP] Server setup complete: {}", setup_response.message);

        Ok(setup_response)
    }
}

/// Builder for configuring [`KalamLinkClient`] instances.
pub struct KalamLinkClientBuilder {
    base_url: Option<String>,
    timeout: Duration,
    resolved_auth: ResolvedAuth,
    max_retries: u32,
    http_pool_max_idle_per_host: usize,
    timeouts: KalamLinkTimeouts,
    connection_options: ConnectionOptions,
    event_handlers: EventHandlers,
    custom_auth_refresher: Option<AuthRefreshCallback>,
}

impl KalamLinkClientBuilder {
    pub(crate) fn new() -> Self {
        Self {
            base_url: None,
            timeout: Duration::from_secs(30),
            resolved_auth: ResolvedAuth::default(),
            max_retries: 3,
            http_pool_max_idle_per_host: 10,
            timeouts: KalamLinkTimeouts::default(),
            connection_options: ConnectionOptions::default(),
            event_handlers: EventHandlers::default(),
            custom_auth_refresher: None,
        }
    }

    /// Set the base URL for the KalamDB server
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    /// Set request timeout (for HTTP requests)
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set JWT token authentication
    pub fn jwt_token(mut self, token: impl Into<String>) -> Self {
        self.resolved_auth = ResolvedAuth::Static(AuthProvider::jwt_token(token.into()));
        self
    }

    /// Set authentication provider directly
    ///
    /// Note: WebSocket subscriptions require JWT authentication. BasicAuth is for HTTP-only flows.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::{KalamLinkClient, AuthProvider};
    ///
    /// # async fn example() -> kalam_link::Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .auth(AuthProvider::basic_auth("alice".to_string(), "secret".to_string()))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn auth(mut self, auth: AuthProvider) -> Self {
        self.resolved_auth = ResolvedAuth::Static(auth);
        self
    }

    /// Set a dynamic (async) authentication provider.
    ///
    /// The provider's [`get_auth`] method is called on every WebSocket
    /// connect/reconnect, enabling automatic token refresh and rotation.
    ///
    /// This takes precedence over [`auth`] / [`jwt_token`] if both are set.
    ///
    /// [`get_auth`]: crate::auth::DynamicAuthProvider::get_auth
    /// [`auth`]: KalamLinkClientBuilder::auth
    /// [`jwt_token`]: KalamLinkClientBuilder::jwt_token
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::{AuthProvider, DynamicAuthProvider, KalamLinkClient, Result};
    /// use std::sync::Arc;
    /// use std::pin::Pin;
    /// use std::future::Future;
    /// use tokio::sync::Mutex;
    ///
    /// struct TokenStore {
    ///     refresh_token: String,
    ///     base_url: String,
    /// }
    ///
    /// impl DynamicAuthProvider for TokenStore {
    ///     fn get_auth(&self) -> Pin<Box<dyn Future<Output = Result<AuthProvider>> + Send + '_>> {
    ///         Box::pin(async {
    ///             let client = KalamLinkClient::builder()
    ///                 .base_url(&self.base_url)
    ///                 .build()?;
    ///             let resp = client.refresh_access_token(&self.refresh_token).await?;
    ///             Ok(AuthProvider::jwt_token(resp.access_token))
    ///         })
    ///     }
    /// }
    ///
    /// # async fn example() -> Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .auth_provider(Arc::new(TokenStore {
    ///         refresh_token: "rt_xxx".into(),
    ///         base_url: "http://localhost:3000".into(),
    ///     }))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn auth_provider(mut self, provider: crate::auth::ArcDynAuthProvider) -> Self {
        self.resolved_auth = ResolvedAuth::Dynamic(provider);
        self
    }

    /// Set maximum number of retries for failed requests
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the maximum number of idle HTTP keep-alive connections to retain per host.
    pub fn http_pool_max_idle_per_host(mut self, max_idle_per_host: usize) -> Self {
        self.http_pool_max_idle_per_host = max_idle_per_host;
        self
    }

    /// Set a custom auth-refresh callback for TOKEN_EXPIRED recovery.
    ///
    /// When the server returns `TOKEN_EXPIRED`, the executor calls this
    /// callback to obtain fresh credentials and retries the request once.
    /// If not set, the default callback resolves credentials from the
    /// configured auth source.
    pub fn auth_refresher(mut self, refresher: AuthRefreshCallback) -> Self {
        self.custom_auth_refresher = Some(refresher);
        self
    }

    /// Set comprehensive timeout configuration for all operations
    ///
    /// This overrides individual timeout settings like `timeout()`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::{KalamLinkClient, KalamLinkTimeouts};
    ///
    /// # async fn example() -> kalam_link::Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .timeouts(KalamLinkTimeouts::fast())
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn timeouts(mut self, timeouts: KalamLinkTimeouts) -> Self {
        // Also update the HTTP timeout to match
        self.timeout = timeouts.receive_timeout;
        self.timeouts = timeouts;
        self
    }

    /// Set connection options for HTTP and WebSocket behavior
    ///
    /// This allows configuring HTTP version, reconnection behavior, and other
    /// connection-level settings.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::{KalamLinkClient, ConnectionOptions, HttpVersion};
    ///
    /// # async fn example() -> kalam_link::Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .connection_options(
    ///         ConnectionOptions::new()
    ///             .with_http_version(HttpVersion::Http2)
    ///             .with_auto_reconnect(true)
    ///     )
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connection_options(mut self, options: ConnectionOptions) -> Self {
        self.connection_options = options;
        self
    }

    /// Set the HTTP protocol version to use
    ///
    /// Shorthand for setting just the HTTP version without other connection options.
    ///
    /// - `HttpVersion::Http1` - HTTP/1.1 (default, maximum compatibility)
    /// - `HttpVersion::Http2` - HTTP/2 (multiplexing, better for concurrent requests)
    /// - `HttpVersion::Auto` - Let the client negotiate with the server
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::{KalamLinkClient, HttpVersion};
    ///
    /// # async fn example() -> kalam_link::Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .http_version(HttpVersion::Http2)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn http_version(mut self, version: HttpVersion) -> Self {
        self.connection_options.http_version = version;
        self
    }

    /// Set connection lifecycle event handlers.
    ///
    /// Registers callbacks for connect, disconnect, error, and message
    /// debug hooks. All handlers are optional.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kalam_link::{KalamLinkClient, EventHandlers};
    ///
    /// # async fn example() -> kalam_link::Result<()> {
    /// let client = KalamLinkClient::builder()
    ///     .base_url("http://localhost:3000")
    ///     .event_handlers(
    ///         EventHandlers::new()
    ///             .on_connect(|| println!("Connected!"))
    ///             .on_disconnect(|reason| println!("Disconnected: {}", reason))
    ///             .on_error(|err| eprintln!("Error: {}", err))
    ///             .on_receive(|msg| println!("[RECV] {}", msg))
    ///             .on_send(|msg| println!("[SEND] {}", msg)),
    ///     )
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn event_handlers(mut self, handlers: EventHandlers) -> Self {
        self.event_handlers = handlers;
        self
    }

    /// Build the client
    pub fn build(self) -> Result<KalamLinkClient> {
        let base_url = self
            .base_url
            .ok_or_else(|| KalamLinkError::ConfigurationError("base_url is required".into()))?;

        // Snapshot static auth for the QueryExecutor (HTTP requests).
        // Dynamic auth is resolved per-subscribe in subscribe_with_config.
        let static_auth = match &self.resolved_auth {
            ResolvedAuth::Static(p) => p.clone(),
            ResolvedAuth::Dynamic(_) => AuthProvider::none(),
        };

        // Build HTTP client with connection pooling for better throughput
        // Keep-alive connections reduce TCP handshake overhead significantly
        let mut client_builder = reqwest::Client::builder()
            .timeout(self.timeout)
            .connect_timeout(self.timeouts.connection_timeout)
            // Enable connection pooling for high throughput (keep-alive)
            .pool_max_idle_per_host(self.http_pool_max_idle_per_host)
            // Keep idle connections for 90 seconds (slightly longer than server's 75s)
            .pool_idle_timeout(std::time::Duration::from_secs(90));

        // Configure HTTP version based on connection options
        client_builder = match self.connection_options.http_version {
            HttpVersion::Http1 => {
                log::debug!("[CLIENT] Using HTTP/1.1 only");
                client_builder.http1_only()
            },
            #[cfg(feature = "http2")]
            HttpVersion::Http2 => {
                log::debug!("[CLIENT] Using HTTP/2 with prior knowledge");
                // http2_prior_knowledge() assumes the server speaks HTTP/2
                // Use this for known HTTP/2 servers for best performance
                client_builder.http2_prior_knowledge()
            },
            #[cfg(not(feature = "http2"))]
            HttpVersion::Http2 => {
                log::warn!("[CLIENT] HTTP/2 requested but 'http2' feature is not enabled; falling back to HTTP/1.1");
                client_builder.http1_only()
            },
            HttpVersion::Auto => {
                log::debug!("[CLIENT] Using automatic HTTP version negotiation");
                // Default behavior - will negotiate via ALPN for HTTPS
                // For HTTP, will use HTTP/1.1
                client_builder
            },
        };

        let http_client = client_builder
            .build()
            .map_err(|e| KalamLinkError::ConfigurationError(e.to_string()))?;

        let mut query_executor = QueryExecutor::new(
            base_url.clone(),
            http_client.clone(),
            static_auth.clone(),
            self.max_retries,
        );

        // Set up auto-refresh callback so the executor can recover from
        // TOKEN_EXPIRED errors transparently.
        {
            let refresher = if let Some(custom) = self.custom_auth_refresher {
                custom
            } else {
                // Default callback: resolves fresh credentials from the shared
                // auth source (dynamic or static) and, when basic-auth is
                // returned, exchanges it for a JWT via login.
                let shared_auth = Arc::new(RwLock::new(self.resolved_auth.clone()));
                let login_url = format!("{}/v1/api/auth/login", &base_url);
                let login_client = http_client.clone();

                let default_refresher: AuthRefreshCallback = Arc::new(move || {
                    let shared = Arc::clone(&shared_auth);
                    let url = login_url.clone();
                    let client = login_client.clone();
                    Box::pin(async move {
                        let resolved = { shared.read().unwrap().clone() };
                        let auth = resolved.resolve().await?;
                        match auth {
                            AuthProvider::BasicAuth(username, password) => {
                                // Exchange basic credentials for a JWT.
                                let body = serde_json::json!({
                                    "username": username,
                                    "password": password,
                                });
                                let resp = client.post(&url).json(&body).send().await.map_err(
                                    |e: reqwest::Error| KalamLinkError::NetworkError(e.to_string()),
                                )?;
                                if !resp.status().is_success() {
                                    let msg = resp.text().await.unwrap_or_default();
                                    return Err(KalamLinkError::AuthenticationError(format!(
                                        "Login failed during token refresh: {}",
                                        msg
                                    )));
                                }
                                let login_resp = resp
                                    .json::<crate::models::LoginResponse>()
                                    .await
                                    .map_err(|e: reqwest::Error| {
                                        KalamLinkError::NetworkError(e.to_string())
                                    })?;
                                log::debug!("[LINK_HTTP] Reauthenticated via basic login");
                                Ok(AuthProvider::jwt_token(login_resp.access_token))
                            },
                            other => Ok(other),
                        }
                    })
                });
                default_refresher
            };
            query_executor.set_auth_refresher(refresher);

            // Use the same shared_resolved_auth for both the executor callback
            // and the client field so credential updates propagate everywhere.
            Ok(KalamLinkClient {
                base_url,
                http_client,
                resolved_auth: self.resolved_auth.clone(),
                auth: static_auth,
                query_executor,
                health_cache: Arc::new(Mutex::new(HealthCheckCache::default())),
                timeouts: self.timeouts,
                connection_options: self.connection_options,
                event_handlers: self.event_handlers,
                shared_resolved_auth: Arc::new(RwLock::new(self.resolved_auth)),
                connection: Arc::new(Mutex::new(None)),
            })
        }
    }
}

const HEALTH_CHECK_TTL: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
struct HealthCheckCache {
    last_check: Option<Instant>,
    last_response: Option<HealthCheckResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Mutex;

    #[test]
    fn test_builder_pattern() {
        let result = KalamLinkClient::builder()
            .base_url("http://localhost:3000")
            .timeout(Duration::from_secs(10))
            .jwt_token("test_token")
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn test_builder_missing_url() {
        let result = KalamLinkClient::builder().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_with_ws_lazy_connect() {
        let client = KalamLinkClient::builder()
            .base_url("http://localhost:3000")
            .jwt_token("test_token")
            .connection_options(ConnectionOptions::new().with_ws_lazy_connect(true))
            .build()
            .expect("build should succeed");

        assert!(client.connection_options.ws_lazy_connect);
    }

    #[test]
    fn test_builder_default_ws_lazy_connect_is_true() {
        let client = KalamLinkClient::builder()
            .base_url("http://localhost:3000")
            .jwt_token("test_token")
            .build()
            .expect("build should succeed");

        assert!(client.connection_options.ws_lazy_connect);
    }

    #[derive(Debug, Default)]
    struct QueryAuthState {
        headers: Vec<String>,
    }

    #[tokio::test]
    async fn test_set_auth_updates_http_query_executor() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let address = listener.local_addr().expect("read local addr");
        let state = Arc::new(Mutex::new(QueryAuthState::default()));
        let state_clone = Arc::clone(&state);

        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let state = Arc::clone(&state_clone);
                tokio::spawn(async move {
                    let _ = handle_test_request(stream, state).await;
                });
            }
        });

        let mut client = KalamLinkClient::builder()
            .base_url(format!("http://{}", address))
            .jwt_token("expired-token")
            .build()
            .expect("build client");

        let first_error = client
            .execute_query("SELECT 1", None, None, None)
            .await
            .expect_err("expired token should fail");
        assert!(matches!(
            first_error,
            KalamLinkError::ServerError {
                status_code: 401,
                ..
            }
        ));

        client.set_auth(AuthProvider::jwt_token("fresh-token".to_string()));

        let response = client
            .execute_query("SELECT 1", None, None, None)
            .await
            .expect("fresh token should succeed");
        assert!(response.success());

        let recorded_headers = state.lock().await.headers.clone();
        assert_eq!(
            recorded_headers,
            vec![
                "Bearer expired-token".to_string(),
                "Bearer fresh-token".to_string()
            ]
        );

        server.abort();
    }

    async fn handle_test_request(
        mut stream: TcpStream,
        state: Arc<Mutex<QueryAuthState>>,
    ) -> std::io::Result<()> {
        let request = read_test_request(&mut stream).await?;
        let authorization = request.headers.get("authorization").cloned().unwrap_or_default();
        state.lock().await.headers.push(authorization.clone());

        let (status_line, body) = match authorization.as_str() {
            "Bearer fresh-token" => (
                "HTTP/1.1 200 OK",
                json!({
                    "status": "success",
                    "results": [{
                        "schema": [{
                            "name": "value",
                            "data_type": "BigInt",
                            "index": 0
                        }],
                        "rows": [["1"]],
                        "row_count": 1
                    }],
                    "took": 1.0
                })
                .to_string(),
            ),
            _ => ("HTTP/1.1 401 Unauthorized", "Token expired".to_string()),
        };

        let response = format!(
            "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await
    }

    struct TestRequest {
        headers: HashMap<String, String>,
    }

    async fn read_test_request(stream: &mut TcpStream) -> std::io::Result<TestRequest> {
        let mut buffer = Vec::new();
        let mut temp = [0_u8; 1024];
        let mut header_end = None;
        let mut content_length = 0_usize;

        loop {
            let bytes_read = stream.read(&mut temp).await?;
            if bytes_read == 0 {
                break;
            }
            buffer.extend_from_slice(&temp[..bytes_read]);

            if header_end.is_none() {
                if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                    header_end = Some(position + 4);
                    let header_text = String::from_utf8_lossy(&buffer[..position]);
                    for line in header_text.lines().skip(1) {
                        if let Some((name, value)) = line.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length") {
                                content_length = value.trim().parse().unwrap_or(0);
                            }
                        }
                    }
                }
            }

            if let Some(end) = header_end {
                if buffer.len() >= end + content_length {
                    break;
                }
            }
        }

        let header_end = header_end.expect("request should include headers");
        let header_text = String::from_utf8_lossy(&buffer[..header_end - 4]);
        let mut headers = HashMap::new();
        for line in header_text.lines().skip(1) {
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            }
        }

        Ok(TestRequest { headers })
    }
}
