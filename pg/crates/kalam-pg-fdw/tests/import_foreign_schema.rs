use kalam_pg_common::{DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalam_pg_fdw::create_foreign_table_sql;
use kalamdb_commons::{
    models::{
        datatypes::KalamDataType,
        schemas::{ColumnDefinition, SharedTableOptions, TableDefinition, TableOptions},
        NamespaceId, TableName,
    },
    TableAccess, TableType,
};

#[test]
fn import_sql_includes_virtual_columns_for_user_tables() {
    let table = TableDefinition::new(
        NamespaceId::new("app"),
        TableName::new("messages"),
        TableType::User,
        vec![
            ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
            ColumnDefinition::simple(2, "body", 2, KalamDataType::Text),
        ],
        TableOptions::user(),
        None,
    )
    .expect("user table");

    let sql = create_foreign_table_sql("kalam_server", "chat", &table).expect("import sql");

    assert!(sql.contains("\"chat\".\"messages\""));
    assert!(sql.contains("\"id\" BIGINT"));
    assert!(sql.contains("\"body\" TEXT"));
    assert!(sql.contains(&format!("\"{}\" TEXT", USER_ID_COLUMN)));
    assert!(sql.contains(&format!("\"{}\" BIGINT", SEQ_COLUMN)));
    assert!(sql.contains(&format!("\"{}\" BOOLEAN", DELETED_COLUMN)));
    assert!(sql.contains("OPTIONS (namespace 'app', table 'messages', table_type 'user')"));
}

#[test]
fn import_sql_skips_user_id_for_shared_tables() {
    let table = TableDefinition::new(
        NamespaceId::new("app"),
        TableName::new("shared_docs"),
        TableType::Shared,
        vec![ColumnDefinition::simple(1, "body", 1, KalamDataType::Text)],
        TableOptions::Shared(SharedTableOptions {
            access_level: Some(TableAccess::Public),
            ..Default::default()
        }),
        None,
    )
    .expect("shared table");

    let sql = create_foreign_table_sql("kalam_server", "chat", &table).expect("import sql");

    assert!(!sql.contains(&format!("\"{}\" TEXT", USER_ID_COLUMN)));
    assert!(sql.contains(&format!("\"{}\" BIGINT", SEQ_COLUMN)));
    assert!(sql.contains(&format!("\"{}\" BOOLEAN", DELETED_COLUMN)));
}
