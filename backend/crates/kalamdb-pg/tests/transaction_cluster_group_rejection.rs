mod support;

use std::collections::BTreeMap;

use datafusion_common::ScalarValue;
use kalamdb_commons::models::rows::Row;
use kalamdb_core::{test_helpers::test_app_context, transactions::TransactionRaftBinding};
use kalamdb_pg::{InsertRpcRequest, PgService};
use ntest::timeout;
use support::{
    await_user_leader, begin_transaction, build_service, create_shared_table, create_user_table,
    insert_user_row, open_session, parse_transaction_id, request, rollback_transaction,
    scan_shared_rows, scan_user_rows, unique_namespace,
};

#[tokio::test]
#[timeout(10000)]
async fn pg_transaction_rejects_cross_group_access_without_losing_existing_overlay() {
    let app_ctx = test_app_context();
    let namespace = unique_namespace("pg_tx_cluster_groups");
    let user_table = create_user_table(&app_ctx, &namespace, "user_items").await;
    let shared_table = create_shared_table(&app_ctx, &namespace, "shared_items").await;
    let service = build_service(app_ctx.clone());
    let session_id = "pg-5101-c1a2";
    let observer_session_id = "pg-5102-c1a2";
    let user_id = "pg-user-cluster-groups";

    await_user_leader(&app_ctx, user_id).await;
    open_session(&service, session_id).await;
    open_session(&service, observer_session_id).await;

    let transaction_id = begin_transaction(&service, session_id).await;
    let parsed_transaction_id = parse_transaction_id(&transaction_id);

    insert_user_row(&service, &user_table, session_id, user_id, 1, "alpha").await;

    let handle = app_ctx
        .transaction_coordinator()
        .get_handle(&parsed_transaction_id)
        .expect("transaction handle exists");
    let bound_group = match handle.raft_binding {
        TransactionRaftBinding::BoundCluster { group_id, .. } => group_id,
        other => panic!("expected bound cluster transaction, got {:?}", other),
    };

    let row_json = serde_json::to_string(&Row::new(BTreeMap::from([
        ("id".to_string(), ScalarValue::Int64(Some(2))),
        ("name".to_string(), ScalarValue::Utf8(Some("beta".to_string()))),
    ])))
    .expect("serialize shared row");

    let status = service
        .insert(request(InsertRpcRequest {
            namespace: shared_table.namespace_id().to_string(),
            table_name: shared_table.table_name().to_string(),
            table_type: "shared".to_string(),
            session_id: session_id.to_string(),
            user_id: None,
            rows_json: vec![row_json],
        }))
        .await
        .expect_err("cross-group insert should fail");

    let message = status.message().to_string();
    assert!(
        message.contains("already bound to data raft group"),
        "expected cross-group rejection, got: {message}"
    );
    assert!(
        message.contains(&bound_group.to_string()),
        "expected bound group in error: {message}"
    );
    assert!(message.contains("data:shared:00"), "expected shared group in error: {message}");

    let same_tx_rows = scan_user_rows(&service, &user_table, session_id, user_id).await;
    assert_eq!(same_tx_rows.len(), 1);
    assert_eq!(same_tx_rows[0].get("name").and_then(|value| value.as_str()), Some("alpha"));
    assert!(scan_user_rows(&service, &user_table, observer_session_id, user_id)
        .await
        .is_empty());
    assert!(scan_shared_rows(&service, &shared_table, observer_session_id).await.is_empty());

    let rolled_back_id = rollback_transaction(&service, session_id, &transaction_id).await;
    assert_eq!(rolled_back_id, transaction_id);
    assert!(scan_user_rows(&service, &user_table, observer_session_id, user_id)
        .await
        .is_empty());
    assert!(scan_shared_rows(&service, &shared_table, observer_session_id).await.is_empty());
}
