use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{TopicId, TriggerAttemptId, TriggerId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Persisted `system.trigger_attempts` row.
#[table(
    name = "trigger_attempts",
    comment = "Trigger delivery attempts and DLQ"
)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CatalogTriggerAttempt {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "trigger:partition:offset:attempt"
    )]
    pub attempt_id:       TriggerAttemptId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Parent trigger"
    )]
    pub trigger_id:       TriggerId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Source topic"
    )]
    pub topic_id:         TopicId,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Topic partition"
    )]
    pub partition_id:     i32,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Topic offset"
    )]
    pub offset:           i64,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Stable event identity"
    )]
    pub event_id:         String,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "1-based attempt number"
    )]
    pub attempt:          i32,
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "running | succeeded | retry | dlq"
    )]
    pub status:           String,
    #[column(
        id = 9,
        ordinal = 9,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Lease owner node"
    )]
    #[serde(default)]
    pub lease_owner:      Option<String>,
    #[column(
        id = 10,
        ordinal = 10,
        data_type(KalamDataType::BigInt),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Lease expiry unix millis"
    )]
    #[serde(default)]
    pub lease_expires_at: Option<i64>,
    #[column(
        id = 11,
        ordinal = 11,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Last error"
    )]
    #[serde(default)]
    pub error:            Option<String>,
    #[column(
        id = 12,
        ordinal = 12,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Created unix millis"
    )]
    pub created_at:       i64,
    #[column(
        id = 13,
        ordinal = 13,
        data_type(KalamDataType::BigInt),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Updated unix millis"
    )]
    pub updated_at:       i64,
}

impl kalamdb_commons::KSerializable for CatalogTriggerAttempt {}
