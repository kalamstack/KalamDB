//! Audit log entry for administrative actions.

use kalamdb_commons::{
    datatypes::KalamDataType,
    models::ids::{AuditLogId, UserId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

/// Audit log entry for administrative actions.
///
/// Captures who performed an action, what they targeted, and contextual metadata.
/// For AS USER impersonation: actor_user_id is the person who initiated the action,
/// subject_user_id is the person being impersonated (if applicable).
#[table(
    name = "audit_log",
    comment = "System audit log for security and compliance tracking"
)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AuditLogEntry {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Audit log entry identifier"
    )]
    pub audit_id: AuditLogId,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Event timestamp"
    )]
    pub timestamp: i64,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "User ID who performed the action"
    )]
    pub actor_user_id: UserId,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Action performed (CREATE, UPDATE, DELETE, LOGIN, etc.)"
    )]
    pub action: String,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Target of the action (table name, user ID, etc.)"
    )]
    pub target: String,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Additional details about the action (JSON)"
    )]
    pub details: Option<String>, // JSON blob for additional context
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "IP address of the client"
    )]
    pub ip_address: Option<String>, // Connection source (if available)
    #[column(
        id = 8,
        ordinal = 8,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "User being impersonated (AS USER operations)"
    )]
    pub subject_user_id: Option<UserId>, // User being impersonated (AS USER operations)
}
