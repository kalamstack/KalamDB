use kalamdb_commons::{
    models::{rows::Row, UserId},
    TableId, TableType,
};

/// Typed FDW insert input before it is converted into a backend request.
#[derive(Debug, Clone)]
pub struct InsertInput {
    pub table_id: TableId,
    pub table_type: TableType,
    pub rows: Vec<Row>,
    pub session_user_id: Option<UserId>,
}
