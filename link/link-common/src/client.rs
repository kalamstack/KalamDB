//! Main KalamDB client with builder pattern.
//!
//! Provides the primary interface for connecting to KalamDB servers
//! and executing operations.

#[cfg(feature = "healthcheck")]
use std::time::Instant;
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use tokio::sync::Mutex;

#[cfg(feature = "healthcheck")]
use crate::models::HealthCheckResponse;
use crate::{
    auth::{AuthProvider, ResolvedAuth},
    connection::SharedConnection,
    event_handlers::EventHandlers,
    models::ConnectionOptions,
    query::{AuthRefreshCallback, QueryExecutor},
    timeouts::KalamLinkTimeouts,
};

mod builder;
mod endpoints;
mod runtime;

#[cfg(test)]
mod tests;

/// Main KalamDB client.
///
/// Use [`KalamLinkClientBuilder`] to construct instances with custom configuration.
///
/// # Examples
///
/// ```rust,no_run
/// use kalam_client::KalamLinkClient;
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
    #[cfg(feature = "healthcheck")]
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

#[cfg(feature = "healthcheck")]
const HEALTH_CHECK_TTL: Duration = Duration::from_secs(10);

#[cfg(feature = "healthcheck")]
#[derive(Debug, Default)]
struct HealthCheckCache {
    last_check: Option<Instant>,
    last_response: Option<HealthCheckResponse>,
}
