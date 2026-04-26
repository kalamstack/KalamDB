use kalamdb_commons::{models::UserId, TableId, TableType};

/// Typed FDW delete input before it is converted into a backend request.
#[derive(Debug, Clone)]
pub struct DeleteInput {
    pub table_id: TableId,
    pub table_type: TableType,
    pub pk_value: String,
    pub explicit_user_id: Option<UserId>,
    pub session_user_id: Option<UserId>,
}
