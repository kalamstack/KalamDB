use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

#[cfg(feature = "healthcheck")]
use tokio::sync::Mutex;

#[cfg(feature = "healthcheck")]
use super::HealthCheckCache;
use super::{KalamLinkClient, KalamLinkClientBuilder};
use crate::{
    auth::{AuthProvider, ResolvedAuth},
    error::{KalamLinkError, Result},
    models::{ConnectionOptions, HttpVersion, LoginResponse},
    query::AuthRefreshCallback,
    timeouts::KalamLinkTimeouts,
};

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
            event_handlers: crate::event_handlers::EventHandlers::default(),
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
    pub fn auth(mut self, auth: AuthProvider) -> Self {
        self.resolved_auth = ResolvedAuth::Static(auth);
        self
    }

    /// Set a dynamic (async) authentication provider.
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
    pub fn auth_refresher(mut self, refresher: AuthRefreshCallback) -> Self {
        self.custom_auth_refresher = Some(refresher);
        self
    }

    /// Set comprehensive timeout configuration for all operations
    pub fn timeouts(mut self, timeouts: KalamLinkTimeouts) -> Self {
        self.timeout = timeouts.receive_timeout;
        self.timeouts = timeouts;
        self
    }

    /// Set connection options for HTTP and WebSocket behavior
    pub fn connection_options(mut self, options: ConnectionOptions) -> Self {
        self.connection_options = options;
        self
    }

    /// Set the HTTP protocol version to use
    pub fn http_version(mut self, version: HttpVersion) -> Self {
        self.connection_options.http_version = version;
        self
    }

    /// Set connection lifecycle event handlers.
    pub fn event_handlers(mut self, handlers: crate::event_handlers::EventHandlers) -> Self {
        self.event_handlers = handlers;
        self
    }

    /// Build the client
    pub fn build(self) -> Result<KalamLinkClient> {
        let base_url = self
            .base_url
            .ok_or_else(|| KalamLinkError::ConfigurationError("base_url is required".into()))?;

        let static_auth = match &self.resolved_auth {
            ResolvedAuth::Static(provider) => provider.clone(),
            ResolvedAuth::Dynamic(_) => AuthProvider::none(),
        };

        let mut client_builder = reqwest::Client::builder()
            .timeout(self.timeout)
            .connect_timeout(self.timeouts.connection_timeout)
            .pool_max_idle_per_host(self.http_pool_max_idle_per_host)
            .pool_idle_timeout(Duration::from_secs(90));

        client_builder = match self.connection_options.http_version {
            HttpVersion::Http1 => {
                log::debug!("[CLIENT] Using HTTP/1.1 only");
                client_builder.http1_only()
            },
            #[cfg(feature = "http2")]
            HttpVersion::Http2 => {
                log::debug!("[CLIENT] Using HTTP/2 with prior knowledge");
                client_builder.http2_prior_knowledge()
            },
            #[cfg(not(feature = "http2"))]
            HttpVersion::Http2 => {
                log::warn!(
                    "[CLIENT] HTTP/2 requested but 'http2' feature is not enabled; falling back \
                     to HTTP/1.1"
                );
                client_builder.http1_only()
            },
            HttpVersion::Auto => {
                log::debug!("[CLIENT] Using automatic HTTP version negotiation");
                client_builder
            },
        };

        let http_client = client_builder
            .build()
            .map_err(|error| KalamLinkError::ConfigurationError(error.to_string()))?;

        let mut query_executor = crate::query::QueryExecutor::new(
            base_url.clone(),
            http_client.clone(),
            static_auth.clone(),
            self.max_retries,
        );

        let refresher = if let Some(custom) = self.custom_auth_refresher {
            custom
        } else {
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
                            let body = serde_json::json!({
                                "user": username,
                                "password": password,
                            });
                            let response = client.post(&url).json(&body).send().await.map_err(
                                |error: reqwest::Error| {
                                    KalamLinkError::NetworkError(error.to_string())
                                },
                            )?;
                            if !response.status().is_success() {
                                let message = response.text().await.unwrap_or_default();
                                return Err(KalamLinkError::AuthenticationError(format!(
                                    "Login failed during token refresh: {}",
                                    message
                                )));
                            }
                            let login_response = response.json::<LoginResponse>().await.map_err(
                                |error: reqwest::Error| {
                                    KalamLinkError::NetworkError(error.to_string())
                                },
                            )?;
                            log::debug!("[LINK_HTTP] Reauthenticated via basic login");
                            Ok(AuthProvider::jwt_token(login_response.access_token))
                        },
                        other => Ok(other),
                    }
                })
            });
            default_refresher
        };
        query_executor.set_auth_refresher(refresher);

        Ok(KalamLinkClient {
            base_url,
            http_client,
            resolved_auth: self.resolved_auth.clone(),
            auth: static_auth,
            query_executor,
            #[cfg(feature = "healthcheck")]
            health_cache: Arc::new(Mutex::new(HealthCheckCache::default())),
            timeouts: self.timeouts,
            connection_options: self.connection_options,
            event_handlers: self.event_handlers,
            shared_resolved_auth: Arc::new(RwLock::new(self.resolved_auth)),
            connection: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }
}
