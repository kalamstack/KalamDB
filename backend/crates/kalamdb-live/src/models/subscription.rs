//! Subscription and notification result models

pub use kalamdb_commons::websocket::{ChangeNotification, ChangeType};
use kalamdb_commons::{models::LiveQueryId, schemas::SchemaField};

use crate::helpers::initial_data::InitialDataResult;

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
