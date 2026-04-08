mod support;

use support::{execute_err, execute_ok, request_exec_ctx, setup_shared_table};

#[tokio::test]
#[ntest::timeout(15000)]
async fn sql_request_rejects_ddl_inside_explicit_transaction() {
    let (app_ctx, executor, table_id, _test_db) =
        setup_shared_table("sql_tx_ddl_reject", "items").await;
    let request_ctx = request_exec_ctx(&app_ctx, "sql-request-ddl-reject");

    execute_ok(&executor, &request_ctx, "BEGIN").await;

    let ddl = format!(
        "CREATE SHARED TABLE {}.ddl_target (id INT PRIMARY KEY, name TEXT)",
        table_id.namespace_id()
    );
    let error = execute_err(&executor, &request_ctx, &ddl).await;
    assert!(error.contains("DDL is not allowed inside explicit transaction"));

    execute_ok(&executor, &request_ctx, "ROLLBACK").await;
}
