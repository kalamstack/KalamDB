mod support;

use std::collections::BTreeMap;

use datafusion_common::ScalarValue;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::NodeId;
use kalamdb_commons::TransactionState;
use kalamdb_core::transactions::TransactionRaftBinding;
use kalamdb_pg::{InsertRpcRequest, PgService};
use ntest::timeout;

use support::{
    await_shared_leader, begin_transaction, new_cluster_service_with_tables, open_session,
    parse_transaction_id, request, rollback_transaction, scan_shared_rows,
};

#[tokio::test]
#[timeout(10000)]
async fn pg_transaction_aborts_when_bound_leader_changes_before_next_write() {
    let (app_ctx, service, _namespace, table_ids) =
        new_cluster_service_with_tables("pg_tx_cluster_failover", &["items"]).await;
    let table_id = table_ids[0].clone();
    let session_id = "pg-5201-beef";
    let observer_session_id = "pg-5202-beef";

    await_shared_leader(&app_ctx).await;
    open_session(&service, session_id).await;
    open_session(&service, observer_session_id).await;

    let transaction_id = begin_transaction(&service, session_id).await;
    let parsed_transaction_id = parse_transaction_id(&transaction_id);

    support::insert_shared_row(&service, &table_id, session_id, 1, "alpha").await;

    let handle = app_ctx
        .transaction_coordinator()
        .get_handle(&parsed_transaction_id)
        .expect("transaction handle exists");
    let group_id = match handle.raft_binding {
        TransactionRaftBinding::BoundCluster { group_id, .. } => group_id,
        other => panic!("expected bound cluster transaction, got {:?}", other),
    };

    app_ctx
        .transaction_coordinator()
        .force_raft_binding_for_test(
            &parsed_transaction_id,
            TransactionRaftBinding::BoundCluster {
                group_id,
                leader_node_id: NodeId::new(99),
            },
        )
        .expect("force stale leader binding");

    let row_json = serde_json::to_string(&Row::new(BTreeMap::from([
        ("id".to_string(), ScalarValue::Int64(Some(2))),
        ("name".to_string(), ScalarValue::Utf8(Some("beta".to_string()))),
    ])))
    .expect("serialize shared row");

    let status = service
        .insert(request(InsertRpcRequest {
            namespace: table_id.namespace_id().to_string(),
            table_name: table_id.table_name().to_string(),
            table_type: "shared".to_string(),
            session_id: session_id.to_string(),
            user_id: None,
            rows_json: vec![row_json],
        }))
        .await
        .expect_err("post-failover write should abort the transaction");

    let message = status.message().to_string();
    assert!(
        message.contains("was aborted because leader for bound raft group"),
        "expected failover abort, got: {message}"
    );
    assert!(message.contains(&group_id.to_string()), "expected bound group in error: {message}");

    let aborted_handle = app_ctx
        .transaction_coordinator()
        .get_handle(&parsed_transaction_id)
        .expect("aborted transaction handle remains until cleanup");
    assert_eq!(aborted_handle.state, TransactionState::Aborted);
    assert!(scan_shared_rows(&service, &table_id, observer_session_id).await.is_empty());

    let rolled_back_id = rollback_transaction(&service, session_id, &transaction_id).await;
    assert_eq!(rolled_back_id, transaction_id);
    assert!(app_ctx
        .transaction_coordinator()
        .get_handle(&parsed_transaction_id)
        .is_none());
    assert!(scan_shared_rows(&service, &table_id, observer_session_id).await.is_empty());
}