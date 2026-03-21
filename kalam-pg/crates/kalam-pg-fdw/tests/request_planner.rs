use datafusion::logical_expr::Expr;
use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use kalam_pg_common::{DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalam_pg_fdw::{InsertInput, RequestPlanner, ScanInput, VirtualColumn};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{NamespaceId, TableName, UserId};
use kalamdb_commons::{TableId, TableType};

fn table_id() -> TableId {
    TableId::new(NamespaceId::new("pg_fdw"), TableName::new("messages"))
}

#[test]
fn plan_scan_extracts_user_id_filters_and_virtual_columns() {
    let plan = RequestPlanner::plan_scan(ScanInput {
        table_id: table_id(),
        table_type: TableType::User,
        projected_columns: vec![
            "id".to_string(),
            USER_ID_COLUMN.to_string(),
            SEQ_COLUMN.to_string(),
            DELETED_COLUMN.to_string(),
        ],
        filters: vec![
            col(USER_ID_COLUMN).eq(lit("u_fdw")),
            col("body").eq(lit("hello")),
        ],
        limit: Some(5),
        session_user_id: None,
    })
    .expect("plan scan");

    assert_eq!(
        plan.request.projection,
        Some(vec![
            "id".to_string(),
            SEQ_COLUMN.to_string(),
            DELETED_COLUMN.to_string(),
        ])
    );
    assert_eq!(plan.request.filters, vec![Expr::from(col("body").eq(lit("hello"))).unalias()]);
    assert_eq!(plan.request.tenant_context.effective_user_id(), Some(&UserId::new("u_fdw")));
    assert_eq!(
        plan.virtual_columns,
        vec![
            VirtualColumn::UserId,
            VirtualColumn::Seq,
            VirtualColumn::Deleted,
        ]
    );
}

#[test]
fn plan_scan_rejects_conflicting_session_and_filter_user_ids() {
    let err = RequestPlanner::plan_scan(ScanInput {
        table_id: table_id(),
        table_type: TableType::User,
        projected_columns: vec!["id".to_string()],
        filters: vec![col(USER_ID_COLUMN).eq(lit("u_filter"))],
        limit: None,
        session_user_id: Some(UserId::new("u_session")),
    })
    .expect_err("mismatched tenant context should fail");

    assert!(err.to_string().contains("does not match"));
}

#[test]
fn plan_insert_strips_user_id_from_rows() {
    let plan = RequestPlanner::plan_insert(InsertInput {
        table_id: table_id(),
        table_type: TableType::User,
        rows: vec![Row::from_vec(vec![
            (USER_ID_COLUMN.to_string(), ScalarValue::Utf8(Some("u_insert".to_string()))),
            ("id".to_string(), ScalarValue::Int32(Some(1))),
        ])],
        session_user_id: None,
    })
    .expect("plan insert");

    assert_eq!(plan.request.tenant_context.effective_user_id(), Some(&UserId::new("u_insert")));
    assert_eq!(plan.request.rows[0].get(USER_ID_COLUMN), None);
    assert_eq!(plan.request.rows[0].get("id"), Some(&ScalarValue::Int32(Some(1))));
}

#[test]
fn plan_insert_rejects_conflicting_row_user_ids() {
    let err = RequestPlanner::plan_insert(InsertInput {
        table_id: table_id(),
        table_type: TableType::User,
        rows: vec![
            Row::from_vec(vec![(
                USER_ID_COLUMN.to_string(),
                ScalarValue::Utf8(Some("u_1".to_string())),
            )]),
            Row::from_vec(vec![(
                USER_ID_COLUMN.to_string(),
                ScalarValue::Utf8(Some("u_2".to_string())),
            )]),
        ],
        session_user_id: None,
    })
    .expect_err("conflicting row user ids should fail");

    assert!(err.to_string().contains("conflicting _userid"));
}
