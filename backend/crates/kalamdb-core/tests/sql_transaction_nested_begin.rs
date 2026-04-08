mod support;

use support::{execute_err, execute_ok, request_exec_ctx, setup_shared_table};

#[tokio::test]
#[ntest::timeout(15000)]
async fn sql_request_rejects_nested_begin() {
    let (app_ctx, executor, _table_id, _test_db) =
        setup_shared_table("sql_tx_nested", "items").await;
    let request_ctx = request_exec_ctx(&app_ctx, "sql-request-nested");

    execute_ok(&executor, &request_ctx, "BEGIN").await;

    let error = execute_err(&executor, &request_ctx, "BEGIN").await;
    assert!(error.contains("already has an active transaction"));
}