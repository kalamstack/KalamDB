use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{NamespaceId, RoutineId, TopicId, TriggerId, UserId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.triggers` row.
#[table(name = "triggers", comment = "Durable topic triggers")]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogTrigger {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Schema-qualified trigger identity"
    )]
    pub trigger_id:        TriggerId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Owning schema"
    )]
    pub namespace_id:      NamespaceId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Unqualified trigger name"
    )]
    pub name:              String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Source topic"
    )]
    pub topic_id:          TopicId,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Target procedure"
    )]
    pub routine_id:        RoutineId,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Trigger principal user"
    )]
    pub principal_user_id: UserId,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "latest or earliest start offset"
    )]
    pub start_from:        String,
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Max delivery attempts before DLQ"
    )]
    pub retries:           i32,
    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Retry backoff in milliseconds"
    )]
    pub retry_backoff_ms:  i64,
    #[column(
        id = 10,
        ordinal = 10,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Per-trigger partition concurrency"
    )]
    pub concurrency:       i32,
    #[column(
        id = 11,
        ordinal = 11,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Whether the trigger is enabled"
    )]
    pub enabled:           bool,
}

impl kalamdb_commons::KSerializable for CatalogTrigger {}
