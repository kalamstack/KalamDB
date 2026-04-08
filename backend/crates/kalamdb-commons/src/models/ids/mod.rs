//! Type-safe identifier types for KalamDB entities.
//!
//! This module contains newtype wrappers around String to enforce type safety
//! at compile time, preventing accidental mixing of different identifier types.
//!
//! All ID types implement:
//! - `Clone`, `Debug`, `PartialEq`, `Eq`, `Hash` for collections
//! - `Serialize`, `Deserialize` for JSON/binary serialization
//! - `Display` for string formatting
//! - Conversion traits: `AsRef<str>`, `From<String>`, `From<&str>`

mod audit_log_id;
mod connection_id;
mod consumer_group_id;
mod job_id;
mod job_node_id;
mod live_query_id;
mod manifest_id;
mod namespace_id;
mod node_id;
mod row_id;
mod shard_id;
mod storage_id;
mod table_id;
mod transaction_id;
mod table_version_id;
mod topic_id;
mod user_id;
mod user_row_id;

pub use audit_log_id::AuditLogId;
pub use connection_id::ConnectionId;
pub use consumer_group_id::ConsumerGroupId;
pub use job_id::JobId;
pub use job_node_id::JobNodeId;
pub use live_query_id::LiveQueryId;
pub use manifest_id::ManifestId;
pub use namespace_id::NamespaceId;
pub use node_id::NodeId;
pub use row_id::RowId;
pub use shard_id::ShardId;
pub use storage_id::StorageId;
pub use table_id::TableId;
pub use table_version_id::{TableVersionId, LATEST_MARKER, VERSION_MARKER};
pub use transaction_id::TransactionId;
pub use topic_id::TopicId;
pub use user_id::UserId;
pub use user_row_id::UserRowId;
