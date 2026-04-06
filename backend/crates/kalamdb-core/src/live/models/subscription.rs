//! Subscription and notification result models

use crate::live::helpers::initial_data::InitialDataResult;
use kalamdb_commons::models::LiveQueryId;
use kalamdb_commons::schemas::SchemaField;
pub use kalamdb_commons::websocket::{ChangeNotification, ChangeType};

/// Result of registering a live query subscription with initial data
#[derive(Debug)]
pub struct SubscriptionResult {
    /// The generated LiveId for the subscription
    pub live_id: LiveQueryId,

    /// Initial data returned with the subscription (if requested)
    pub initial_data: Option<InitialDataResult>,

    /// Schema describing the columns in the subscription result
    /// Contains column name, data type, and index for each field
    pub schema: Vec<SchemaField>,
}
