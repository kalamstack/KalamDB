mod support;

use kalamdb_core::transactions::TransactionRaftBinding;
use ntest::timeout;
use support::{
    create_cluster_app_context, create_executor, create_shared_table, create_user_table,
    execute_err, execute_ok, insert_sql, request_exec_ctx, request_transaction_state, select_names,
    unique_namespace,
};

#[tokio::test]
#[timeout(10000)]
async fn sql_request_transaction_rejects_cross_group_access_without_aborting_bound_group() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let namespace = unique_namespace("tx_cluster_groups");
    let user_table = create_user_table(&app_ctx, &namespace, "user_items").await;
    let shared_table = create_shared_table(&app_ctx, &namespace, "shared_items").await;
    let executor = create_executor(app_ctx.clone());
    let request_ctx = request_exec_ctx(&app_ctx, "sql-cluster-groups-active");
    let observer_ctx = request_exec_ctx(&app_ctx, "sql-cluster-groups-observer");

    execute_ok(&executor, &request_ctx, "BEGIN").await;
    execute_ok(&executor, &request_ctx, &insert_sql(&user_table, 1, "alpha")).await;

    let mut request_state = request_transaction_state(&request_ctx);
    request_state.sync_from_coordinator(&app_ctx);
    let transaction_id = request_state
        .active_transaction_id()
        .cloned()
        .expect("transaction remains active after BEGIN");

    let handle = app_ctx
        .transaction_coordinator()
        .get_handle(&transaction_id)
        .expect("transaction handle exists");
    let bound_group = match handle.raft_binding {
        TransactionRaftBinding::BoundCluster { group_id, .. } => group_id,
        other => panic!("expected bound cluster transaction, got {:?}", other),
    };

    let error = execute_err(&executor, &request_ctx, &insert_sql(&shared_table, 2, "beta")).await;
    assert!(
        error.contains("already bound to data raft group"),
        "expected cross-group rejection, got: {error}"
    );
    assert!(
        error.contains(&bound_group.to_string()),
        "expected bound group in error: {error}"
    );
    assert!(
        error.contains("data:shared:00"),
        "expected requested shared group in error: {error}"
    );

    assert_eq!(
        select_names(&executor, &request_ctx, &user_table).await,
        vec!["alpha".to_string()]
    );
    assert!(select_names(&executor, &observer_ctx, &user_table).await.is_empty());
    assert!(select_names(&executor, &observer_ctx, &shared_table).await.is_empty());

    execute_ok(&executor, &request_ctx, "ROLLBACK").await;

    assert!(select_names(&executor, &observer_ctx, &user_table).await.is_empty());
    assert!(select_names(&executor, &observer_ctx, &shared_table).await.is_empty());
}
