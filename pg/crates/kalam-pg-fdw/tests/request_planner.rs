use datafusion_common::ScalarValue;
use kalam_pg_api::ScanFilter;
use kalam_pg_common::{DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalam_pg_fdw::{
    DeleteInput, InsertInput, RequestPlanner, ScanInput, UpdateInput, VirtualColumn,
};
use kalamdb_commons::{
    models::{rows::Row, NamespaceId, TableName, UserId},
    TableId, TableType,
};

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
            ScanFilter::eq(USER_ID_COLUMN, ScalarValue::Utf8(Some("u_fdw".to_string()))),
            ScanFilter::eq("body", ScalarValue::Utf8(Some("hello".to_string()))),
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
    assert_eq!(
        plan.request.filters,
        vec![ScanFilter::eq(
            "body",
            ScalarValue::Utf8(Some("hello".to_string()))
        )]
    );
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
fn plan_scan_allows_filter_user_id_override_of_session() {
    let plan = RequestPlanner::plan_scan(ScanInput {
        table_id: table_id(),
        table_type: TableType::User,
        projected_columns: vec!["id".to_string()],
        filters: vec![ScanFilter::eq(
            USER_ID_COLUMN,
            ScalarValue::Utf8(Some("u_filter".to_string())),
        )],
        limit: None,
        session_user_id: Some(UserId::new("u_session")),
    })
    .expect("filter user override should succeed");

    assert_eq!(
        plan.request.tenant_context.effective_user_id(),
        Some(&UserId::new("u_filter")),
        "filter _userid should take precedence over session"
    );
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

// ── UPDATE planner tests ───────────────────────────────────────────────────

#[test]
fn plan_update_strips_user_id_from_updates() {
    let plan = RequestPlanner::plan_update(UpdateInput {
        table_id: table_id(),
        table_type: TableType::User,
        pk_value: "pk_42".to_string(),
        updates: Row::from_vec(vec![
            (USER_ID_COLUMN.to_string(), ScalarValue::Utf8(Some("u_update".to_string()))),
            ("body".to_string(), ScalarValue::Utf8(Some("new content".to_string()))),
        ]),
        session_user_id: None,
    })
    .expect("plan update");

    assert_eq!(plan.request.tenant_context.effective_user_id(), Some(&UserId::new("u_update")));
    assert_eq!(plan.request.pk_value, "pk_42");
    assert_eq!(plan.request.updates.get(USER_ID_COLUMN), None);
    assert_eq!(
        plan.request.updates.get("body"),
        Some(&ScalarValue::Utf8(Some("new content".to_string())))
    );
}

#[test]
fn plan_update_uses_session_user_id_when_no_explicit() {
    let plan = RequestPlanner::plan_update(UpdateInput {
        table_id: table_id(),
        table_type: TableType::User,
        pk_value: "pk_1".to_string(),
        updates: Row::from_vec(vec![(
            "body".to_string(),
            ScalarValue::Utf8(Some("edited".to_string())),
        )]),
        session_user_id: Some(UserId::new("session_user")),
    })
    .expect("plan update with session user");

    assert_eq!(
        plan.request.tenant_context.effective_user_id(),
        Some(&UserId::new("session_user"))
    );
}

#[test]
fn plan_update_shared_table_allows_no_user_id() {
    let plan = RequestPlanner::plan_update(UpdateInput {
        table_id: table_id(),
        table_type: TableType::Shared,
        pk_value: "pk_shared".to_string(),
        updates: Row::from_vec(vec![(
            "status".to_string(),
            ScalarValue::Utf8(Some("active".to_string())),
        )]),
        session_user_id: None,
    })
    .expect("plan update on shared table");

    assert_eq!(plan.request.tenant_context.effective_user_id(), None);
}

// ── DELETE planner tests ───────────────────────────────────────────────────

#[test]
fn plan_delete_with_explicit_user_id() {
    let plan = RequestPlanner::plan_delete(DeleteInput {
        table_id: table_id(),
        table_type: TableType::User,
        pk_value: "pk_99".to_string(),
        explicit_user_id: Some(UserId::new("u_delete")),
        session_user_id: None,
    })
    .expect("plan delete");

    assert_eq!(plan.request.tenant_context.effective_user_id(), Some(&UserId::new("u_delete")));
    assert_eq!(plan.request.pk_value, "pk_99");
}

#[test]
fn plan_delete_uses_session_user_id() {
    let plan = RequestPlanner::plan_delete(DeleteInput {
        table_id: table_id(),
        table_type: TableType::User,
        pk_value: "pk_7".to_string(),
        explicit_user_id: None,
        session_user_id: Some(UserId::new("session_del")),
    })
    .expect("plan delete with session user");

    assert_eq!(
        plan.request.tenant_context.effective_user_id(),
        Some(&UserId::new("session_del"))
    );
}

#[test]
fn plan_delete_shared_table_allows_no_user_id() {
    let plan = RequestPlanner::plan_delete(DeleteInput {
        table_id: table_id(),
        table_type: TableType::Shared,
        pk_value: "pk_shared_del".to_string(),
        explicit_user_id: None,
        session_user_id: None,
    })
    .expect("plan delete on shared table");

    assert_eq!(plan.request.tenant_context.effective_user_id(), None);
    assert_eq!(plan.request.pk_value, "pk_shared_del");
}

#[test]
fn plan_delete_rejects_user_table_without_user_id() {
    let err = RequestPlanner::plan_delete(DeleteInput {
        table_id: table_id(),
        table_type: TableType::User,
        pk_value: "pk_no_user".to_string(),
        explicit_user_id: None,
        session_user_id: None,
    })
    .expect_err("user table delete without user_id should fail");

    assert!(err.to_string().to_lowercase().contains("user"));
}

#[test]
fn plan_update_rejects_user_table_without_user_id() {
    let err = RequestPlanner::plan_update(UpdateInput {
        table_id: table_id(),
        table_type: TableType::User,
        pk_value: "pk_no_user".to_string(),
        updates: Row::from_vec(vec![(
            "body".to_string(),
            ScalarValue::Utf8(Some("x".to_string())),
        )]),
        session_user_id: None,
    })
    .expect_err("user table update without user_id should fail");

    assert!(err.to_string().to_lowercase().contains("user"));
}
