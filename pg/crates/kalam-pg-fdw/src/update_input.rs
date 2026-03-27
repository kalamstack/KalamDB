use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use kalamdb_commons::{TableId, TableType};

/// Typed FDW update input before it is converted into a backend request.
#[derive(Debug, Clone)]
pub struct UpdateInput {
    pub table_id: TableId,
    pub table_type: TableType,
    pub pk_value: String,
    pub updates: Row,
    pub session_user_id: Option<UserId>,
}
