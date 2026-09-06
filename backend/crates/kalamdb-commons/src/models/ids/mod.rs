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

mod artifact_id;
mod audit_log_id;
mod column_id;
mod connection_id;
mod consumer_group_id;
mod function_module_id;
mod function_revision_id;
mod job_id;
mod job_node_id;
mod live_query_id;
mod manifest_id;
mod migration_id;
mod namespace_id;
mod node_id;
mod routine_grant_id;
mod routine_id;
mod routine_parameter_id;
mod row_id;
mod shard_id;
mod storage_id;
mod table_id;
mod table_version_id;
mod topic_id;
mod transaction_id;
mod trigger_attempt_id;
mod trigger_id;
mod type_field_id;
mod type_id;
mod user_id;
mod user_row_id;

pub use artifact_id::ArtifactId;
pub use audit_log_id::AuditLogId;
pub use column_id::ColumnId;
pub use connection_id::ConnectionId;
pub use consumer_group_id::ConsumerGroupId;
pub use function_module_id::FunctionModuleId;
pub use function_revision_id::FunctionRevisionId;
pub use job_id::JobId;
pub use job_node_id::JobNodeId;
pub use live_query_id::LiveQueryId;
pub use manifest_id::ManifestId;
pub use migration_id::MigrationId;
pub use namespace_id::{NamespaceId, NamespaceIdValidationError};
pub use node_id::NodeId;
pub use routine_grant_id::RoutineGrantId;
pub use routine_id::RoutineId;
pub use routine_parameter_id::RoutineParameterId;
pub use row_id::RowId;
pub use shard_id::ShardId;
pub use storage_id::StorageId;
pub use table_id::TableId;
pub use table_version_id::{TableVersionId, LATEST_MARKER, VERSION_MARKER};
pub use topic_id::TopicId;
pub use transaction_id::TransactionId;
pub use trigger_attempt_id::TriggerAttemptId;
pub use trigger_id::TriggerId;
pub use type_field_id::TypeFieldId;
pub use type_id::TypeId;
pub use user_id::{UserId, UserIdValidationError};
pub use user_row_id::UserRowId;
