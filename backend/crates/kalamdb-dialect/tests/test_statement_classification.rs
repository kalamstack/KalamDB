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
