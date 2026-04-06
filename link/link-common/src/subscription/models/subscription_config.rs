use super::subscription_options::SubscriptionOptions;

/// Configuration for establishing a WebSocket subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// Subscription identifier (client-generated, required)
    pub id: String,
    /// SQL query to register for live updates
    pub sql: String,
    /// Optional subscription options (e.g., last_rows)
    pub options: Option<SubscriptionOptions>,
    /// Override WebSocket URL (falls back to base_url conversion when `None`)
    pub ws_url: Option<String>,
}

impl SubscriptionConfig {
    /// Create a new configuration with required ID and SQL.
    ///
    /// By default, includes empty subscription options (batch streaming configured server-side).
    pub fn new(id: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            sql: sql.into(),
            options: Some(SubscriptionOptions::default()),
            ws_url: None,
        }
    }

    /// Create a configuration without any initial data fetch.
    pub fn without_initial_data(id: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            sql: sql.into(),
            options: None,
            ws_url: None,
        }
    }
}
