mod support;

use support::{execute_ok, insert_sql, observer_exec_ctx, select_names, setup_shared_table};

#[tokio::test]
#[ntest::timeout(15000)]
async fn sql_autocommit_still_writes_without_explicit_transaction() {
    let (app_ctx, executor, table_id, _test_db) =
        setup_shared_table("sql_autocommit", "items").await;

    let observer_ctx = observer_exec_ctx(&app_ctx);
    execute_ok(&executor, &observer_ctx, &insert_sql(&table_id, 1, "plain")).await;

    assert_eq!(
        select_names(&executor, &observer_ctx, &table_id).await,
        vec!["plain".to_string()]
    );
}
