use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{NamespaceId, TableId, TableName, UserId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

use crate::models::DBA_NAMESPACE;

#[table(
    name = "notifications",
    namespace = "dba",
    table_type = "shared",
    access_level = "public",
    comment = "Bootstrap-managed shared DBA notifications"
)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NotificationRow {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Notification identifier"
    )]
    pub id: String,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Related user identifier for the notification"
    )]
    pub user_id: UserId,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Notification title"
    )]
    pub title: String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Notification body"
    )]
    pub body: Option<String>,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Boolean),
        nullable = false,
        primary_key = false,
        default = "Literal(false)",
        comment = "Read flag"
    )]
    pub is_read: bool,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Creation timestamp"
    )]
    pub created_at: i64,
    #[column(
        id = 7,
        ordinal = 7,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Last update timestamp"
    )]
    pub updated_at: i64,
}

impl NotificationRow {
    pub fn table_id() -> TableId {
        TableId::new(NamespaceId::new(DBA_NAMESPACE), TableName::new("notifications"))
    }
}
