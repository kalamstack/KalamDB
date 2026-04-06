use super::{KalamLinkClient, KalamLinkClientBuilder};
#[cfg(feature = "consumer")]
use crate::consumer::ConsumerBuilder;
use crate::{
    auth::{AuthProvider, ResolvedAuth},
    error::{KalamLinkError, Result},
    event_handlers::EventHandlers,
    models::{QueryResponse, SubscriptionConfig, SubscriptionInfo},
    query::UploadProgressCallback,
    subscription::{LiveRowsConfig, LiveRowsSubscription, SubscriptionManager},
    timeouts::KalamLinkTimeouts,
};
use std::sync::Arc;

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
    /// # let client = kalam_client::KalamLinkClient::builder().base_url("http://localhost:3000").build()?;
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
    #[cfg(feature = "file-uploads")]
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
    #[cfg(feature = "file-uploads")]
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
        if self.connection_options.ws_lazy_connect {
            let conn_guard = self.connection.lock().await;
            if conn_guard.is_none() {
                drop(conn_guard);
                self.connect().await?;
            }
        }

        {
            let conn_guard = self.connection.lock().await;
            if let Some(ref conn) = *conn_guard {
                let (event_rx, generation, resume_from) =
                    conn.subscribe(config.id.clone(), config.sql, config.options).await?;
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

        Err(KalamLinkError::WebSocketError(
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
    /// calls will multiplex over the single connection.
    pub async fn connect(&self) -> Result<()> {
        let mut conn_guard = self.connection.lock().await;
        if conn_guard.is_some() {
            return Ok(());
        }

        let conn = crate::connection::SharedConnection::connect(
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
        guard.as_ref().is_some_and(|conn| conn.is_connected())
    }

    /// List all active subscriptions on the shared connection.
    pub async fn subscriptions(&self) -> Vec<SubscriptionInfo> {
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
    #[cfg(feature = "consumer")]
    pub fn consumer(&self) -> ConsumerBuilder {
        ConsumerBuilder::from_client(self.clone())
    }

    #[cfg(feature = "consumer")]
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    #[cfg(feature = "consumer")]
    pub(crate) fn http_client(&self) -> reqwest::Client {
        self.http_client.clone()
    }

    #[cfg(feature = "consumer")]
    pub(crate) fn auth(&self) -> &AuthProvider {
        &self.auth
    }

    /// Return the resolved auth source (static or dynamic).
    pub fn resolved_auth(&self) -> &ResolvedAuth {
        &self.resolved_auth
    }

    /// Replace the static authentication credentials at runtime.
    pub fn set_auth(&mut self, auth: AuthProvider) {
        self.auth = auth.clone();
        self.query_executor.set_auth(auth.clone());
        let resolved = ResolvedAuth::Static(auth);
        self.resolved_auth = resolved.clone();
        *self.shared_resolved_auth.write().unwrap() = resolved;
    }

    /// Update the shared authentication source without requiring `&mut self`.
    pub fn update_shared_auth(&self, auth: AuthProvider) {
        self.query_executor.set_auth(auth.clone());
        let resolved = ResolvedAuth::Static(auth);
        *self.shared_resolved_auth.write().unwrap() = resolved;
    }

    /// Resolve fresh credentials from the auth source.
    pub async fn fresh_auth(&self) -> Result<AuthProvider> {
        self.resolved_auth.resolve().await
    }
}
