//! Unit tests for SQL statement classification.

use kalamdb_commons::{NamespaceId, Role};
use kalamdb_dialect::{SqlStatement, SqlStatementKind};

#[test]
fn test_classify_simple_select() {
    let sql = "SELECT * FROM users";
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Select));
}

#[test]
fn test_classify_simple_cte() {
    let sql = "WITH sales AS (SELECT * FROM orders) SELECT * FROM sales";
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Select));
}

#[test]
fn test_classify_multiple_ctes() {
    let sql = r#"
        WITH
            sales AS (SELECT * FROM orders WHERE type = 'sale'),
            refunds AS (SELECT * FROM orders WHERE type = 'refund')
        SELECT * FROM sales
        UNION ALL
        SELECT * FROM refunds
    "#;
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Select));
}

#[test]
fn test_classify_cte_with_aggregation() {
    let sql = r#"
        WITH user_stats AS (
            SELECT user_id, COUNT(*) as count
            FROM system.users
            GROUP BY user_id
        )
        SELECT * FROM user_stats WHERE count > 5
    "#;
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Select));
}

#[test]
fn test_classify_insert() {
    let sql = "INSERT INTO users (name) VALUES ('Alice')";
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Insert(_)));
}

#[test]
fn test_classify_update() {
    let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Update(_)));
}

#[test]
fn test_classify_delete() {
    let sql = "DELETE FROM users WHERE id = 1";
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Delete(_)));
}

#[test]
fn test_classify_case_insensitive_with() {
    let sql = "with temp as (select 1 as n) select * from temp";
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Select));
}

#[test]
fn test_classify_whitespace_before_with() {
    let sql = "  \n\t WITH sales AS (SELECT 1) SELECT * FROM sales";
    let result = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User);

    assert!(result.is_ok());
    let stmt = result.unwrap();
    assert!(matches!(stmt.kind(), SqlStatementKind::Select));
}

#[test]
fn test_slow_query_trackable_dml_and_select_only() {
    let ns = NamespaceId::new("default");
    let role = Role::Dba;

    let trackable = [
        "SELECT * FROM users",
        "WITH t AS (SELECT 1) SELECT * FROM t",
        "INSERT INTO users (name) VALUES ('Alice')",
        "UPDATE users SET name = 'Bob' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
    ];
    for sql in trackable {
        let stmt = SqlStatement::classify_and_parse(sql, &ns, role).expect(sql);
        assert!(stmt.is_slow_query_trackable(), "expected trackable: {sql}");
    }

    let not_trackable = [
        "DROP TABLE users",
        "ALTER TABLE users ADD COLUMN age INT",
        "CREATE TABLE users (id INT)",
        "SHOW TABLES",
        "BEGIN",
        "COMMIT",
    ];
    for sql in not_trackable {
        let stmt = SqlStatement::classify_and_parse(sql, &ns, role).expect(sql);
        assert!(!stmt.is_slow_query_trackable(), "expected not trackable: {sql}");
    }
}

#[test]
fn test_classify_create_index_on_as_alter_table() {
    let ns = NamespaceId::new("default");
    let stmt = SqlStatement::classify_and_parse(
        "CREATE INDEX idx_conv ON messages (conversation_id)",
        &ns,
        Role::User,
    )
    .expect("classify CREATE INDEX");
    match stmt.kind() {
        SqlStatementKind::AlterTable(alter) => match &alter.operation {
            kalamdb_dialect::ddl::ColumnOperation::CreateScalarIndex { name, columns, .. } => {
                assert_eq!(name, "idx_conv");
                assert_eq!(columns, &["conversation_id".to_string()]);
            },
            other => panic!("expected CreateScalarIndex, got {other:?}"),
        },
        other => panic!("expected AlterTable, got {other:?}"),
    }
}

#[test]
fn test_classify_show_transaction_isolation_level_for_jdbc() {
    let ns = NamespaceId::new("default");
    for role in [Role::User, Role::Service, Role::Dba] {
        let stmt = SqlStatement::classify_and_parse("SHOW TRANSACTION ISOLATION LEVEL", &ns, role)
            .unwrap_or_else(|err| {
                panic!("JDBC SHOW TRANSACTION ISOLATION LEVEL should classify for {role:?}: {err}")
            });
        assert!(
            matches!(stmt.kind(), SqlStatementKind::DataFusionMetaCommand),
            "expected DataFusionMetaCommand for {role:?}, got {:?}",
            stmt.kind()
        );
    }

    let tables = SqlStatement::classify_and_parse("SHOW TABLES", &ns, Role::User)
        .expect("SHOW TABLES must stay a Kalam command");
    assert!(matches!(tables.kind(), SqlStatementKind::ShowTables(_)));
}

#[test]
fn test_classify_call_procedure() {
    let ns = NamespaceId::new("default");
    let stmt = SqlStatement::classify_and_parse("CALL api.echo('hi')", &ns, Role::User)
        .expect("classify CALL");
    match stmt.kind() {
        SqlStatementKind::Call(call) => {
            assert_eq!(call.call.routine_id.as_str(), "api.echo");
            assert_eq!(call.call.arguments.len(), 1);
        },
        other => panic!("expected CALL, got {other:?}"),
    }
    stmt.check_authorization(Role::User).expect("user may CALL");
}
