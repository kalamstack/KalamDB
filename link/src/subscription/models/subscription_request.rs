use serde::{Deserialize, Serialize};

use super::subscription_options::SubscriptionOptions;

/// Subscription request details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    /// Unique subscription identifier (client-generated)
    pub id: String,
    /// SQL query for live updates (must be a SELECT statement)
    pub sql: String,
    /// Optional subscription options
    #[serde(default)]
    pub options: SubscriptionOptions,
}
