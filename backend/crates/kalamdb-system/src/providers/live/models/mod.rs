//! Live subscription row models.
//!
//! These types define the schema exposed by the in-memory `system.live` view.

mod live_query;
mod live_query_status;

pub use live_query::LiveQuery;
pub use live_query_status::LiveQueryStatus;