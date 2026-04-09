mod support;

use datafusion_common::ScalarValue;
use kalamdb_commons::models::UserId;
use kalamdb_commons::Role;
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_core::sql::context::ExecutionResult;
use kalamdb_tables::UserTableProvider;

use support::{
    create_cluster_app_context, create_executor, create_user_table, execute_ok, request_exec_ctx,
    unique_namespace,
};

async fn load_user_rows(
    app_ctx: &std::sync::Arc<kalamdb_core::app_context::AppContext>,
    table_id: &kalamdb_commons::models::TableId,
    user_id: &UserId,
    first_id: i64,
    second_id: i64,
) -> (
    kalamdb_tables::UserTableRow,
    kalamdb_tables::UserTableRow,
) {
    let provider_arc = app_ctx
        .schema_registry()
        .get_provider(table_id)
        .expect("user provider should be registered");
    let provider = provider_arc
        .as_any()
        .downcast_ref::<UserTableProvider>()
        .expect("provider should downcast to UserTableProvider");

    let (_, first_row) = provider
        .find_by_pk(user_id, &ScalarValue::Int64(Some(first_id)))
        .await
        .expect("lookup succeeds")
        .expect("first row exists");
    let (_, second_row) = provider
        .find_by_pk(user_id, &ScalarValue::Int64(Some(second_id)))
        .await
        .expect("lookup succeeds")
        .expect("second row exists");

    (first_row, second_row)
}

#[tokio::test]
#[ntest::timeout(8000)]
async fn multi_row_insert_statement_uses_one_internal_commit() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let user_id = UserId::from("sql-insert-batch-user");
    let table_id = create_user_table(&app_ctx, &unique_namespace("sql_insert_batch"), "items").await;
    let executor = create_executor(app_ctx.clone());
    let exec_ctx = ExecutionContext::new(user_id.clone(), Role::User, app_ctx.base_session_context());

    let sql = format!(
        "INSERT INTO {}.{} (id, name) VALUES (1, 'alpha'), (2, 'beta')",
        table_id.namespace_id(),
        table_id.table_name()
    );
    let result = execute_ok(&executor, &exec_ctx, &sql).await;
    assert!(matches!(result, ExecutionResult::Inserted { rows_affected: 2 }));

    let (first_row, second_row) = load_user_rows(&app_ctx, &table_id, &user_id, 1, 2).await;
    assert_eq!(first_row._commit_seq, second_row._commit_seq);
}

#[tokio::test]
#[ntest::timeout(8000)]
async fn separate_insert_statements_commit_independently() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let user_id = UserId::from("sql-insert-separate-user");
    let table_id =
        create_user_table(&app_ctx, &unique_namespace("sql_insert_separate"), "items").await;
    let executor = create_executor(app_ctx.clone());
    let exec_ctx = ExecutionContext::new(user_id.clone(), Role::User, app_ctx.base_session_context());

    let first_insert = format!(
        "INSERT INTO {}.{} (id, name) VALUES (1, 'alpha')",
        table_id.namespace_id(),
        table_id.table_name()
    );
    let second_insert = format!(
        "INSERT INTO {}.{} (id, name) VALUES (2, 'beta')",
        table_id.namespace_id(),
        table_id.table_name()
    );

    execute_ok(&executor, &exec_ctx, &first_insert).await;
    execute_ok(&executor, &exec_ctx, &second_insert).await;

    let (first_row, second_row) = load_user_rows(&app_ctx, &table_id, &user_id, 1, 2).await;
    assert_ne!(first_row._commit_seq, second_row._commit_seq);
}

#[tokio::test]
#[ntest::timeout(8000)]
async fn explicit_transaction_keeps_multiple_inserts_in_one_commit() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let table_id = create_user_table(&app_ctx, &unique_namespace("sql_insert_tx"), "items").await;
    let executor = create_executor(app_ctx.clone());
    let exec_ctx = request_exec_ctx(&app_ctx, "sql-insert-explicit-tx");
    let user_id = exec_ctx.user_id().clone();

    execute_ok(&executor, &exec_ctx, "BEGIN").await;

    let first_insert = format!(
        "INSERT INTO {}.{} (id, name) VALUES (1, 'alpha')",
        table_id.namespace_id(),
        table_id.table_name()
    );
    let second_insert = format!(
        "INSERT INTO {}.{} (id, name) VALUES (2, 'beta')",
        table_id.namespace_id(),
        table_id.table_name()
    );

    execute_ok(&executor, &exec_ctx, &first_insert).await;
    execute_ok(&executor, &exec_ctx, &second_insert).await;
    execute_ok(&executor, &exec_ctx, "COMMIT").await;

    let (first_row, second_row) = load_user_rows(&app_ctx, &table_id, &user_id, 1, 2).await;
    assert_eq!(first_row._commit_seq, second_row._commit_seq);
}