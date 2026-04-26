use kalamdb_commons::{
    models::{rows::Row, UserId},
    TableId, TableType,
};

/// Typed FDW update input before it is converted into a backend request.
#[derive(Debug, Clone)]
pub struct UpdateInput {
    pub table_id: TableId,
    pub table_type: TableType,
    pub pk_value: String,
    pub updates: Row,
    pub session_user_id: Option<UserId>,
}
