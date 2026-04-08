mod support;

use kalamdb_commons::models::NodeId;
use kalamdb_commons::TransactionState;
use kalamdb_core::transactions::TransactionRaftBinding;
use ntest::timeout;

use support::{
    create_cluster_app_context, create_executor, create_shared_table, execute_err, execute_ok,
    insert_sql, observer_exec_ctx, request_exec_ctx, request_transaction_state, select_names,
    unique_namespace,
};

#[tokio::test]
#[timeout(10000)]
async fn sql_request_transaction_aborts_when_bound_leader_changes_before_commit() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let namespace = unique_namespace("tx_cluster_failover");
    let table_id = create_shared_table(&app_ctx, &namespace, "items").await;
    let executor = create_executor(app_ctx.clone());
    let request_ctx = request_exec_ctx(&app_ctx, "sql-cluster-failover-active");
    let observer_ctx = observer_exec_ctx(&app_ctx);

    execute_ok(&executor, &request_ctx, "BEGIN").await;
    execute_ok(&executor, &request_ctx, &insert_sql(&table_id, 1, "alpha")).await;

    let mut request_state = request_transaction_state(&request_ctx);
    request_state.sync_from_coordinator(&app_ctx);
    let transaction_id = request_state
        .active_transaction_id()
        .cloned()
        .expect("transaction remains active after staged write");

    let handle = app_ctx
        .transaction_coordinator()
        .get_handle(&transaction_id)
        .expect("transaction handle exists");
    let group_id = match handle.raft_binding {
        TransactionRaftBinding::BoundCluster { group_id, .. } => group_id,
        other => panic!("expected bound cluster transaction, got {:?}", other),
    };

    app_ctx
        .transaction_coordinator()
        .force_raft_binding_for_test(
            &transaction_id,
            TransactionRaftBinding::BoundCluster {
                group_id,
                leader_node_id: NodeId::new(99),
            },
        )
        .expect("force stale leader binding");

    let error = execute_err(&executor, &request_ctx, "COMMIT").await;
    assert!(
        error.contains("was aborted because leader for bound raft group"),
        "expected failover abort, got: {error}"
    );
    assert!(error.contains(&group_id.to_string()), "expected bound group in error: {error}");

    let aborted_handle = app_ctx
        .transaction_coordinator()
        .get_handle(&transaction_id)
        .expect("aborted transaction handle remains until cleanup");
    assert_eq!(aborted_handle.state, TransactionState::Aborted);
    assert!(select_names(&executor, &observer_ctx, &table_id).await.is_empty());

    execute_ok(&executor, &request_ctx, "ROLLBACK").await;

    let mut cleaned_state = request_transaction_state(&request_ctx);
    cleaned_state.sync_from_coordinator(&app_ctx);
    assert!(cleaned_state.active_transaction_id().is_none());
    assert!(app_ctx
        .transaction_coordinator()
        .get_handle(&transaction_id)
        .is_none());
    assert!(select_names(&executor, &observer_ctx, &table_id).await.is_empty());
}