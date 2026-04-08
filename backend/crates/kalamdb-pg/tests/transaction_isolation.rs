mod support;

use ntest::timeout;

use support::{
    await_shared_leader, begin_transaction, commit_transaction, insert_shared_row,
    new_cluster_service_with_tables, open_session, scan_shared_rows,
};

#[tokio::test]
#[timeout(10000)]
async fn transaction_rows_stay_hidden_from_other_sessions_until_commit() {
    let (app_ctx, service, _namespace, table_ids) =
        new_cluster_service_with_tables("pg_tx_isolation", &["items"]).await;
    let session_a = "pg-4104-1a2b";
    let session_b = "pg-4105-1a2c";

    await_shared_leader(&app_ctx).await;

    open_session(&service, session_a).await;
    open_session(&service, session_b).await;

    let transaction_id = begin_transaction(&service, session_a).await;
    insert_shared_row(&service, &table_ids[0], session_a, 1, "hidden").await;

    let before_commit = scan_shared_rows(&service, &table_ids[0], session_b).await;
    assert!(before_commit.is_empty());

    let committed_transaction_id = commit_transaction(&service, session_a, &transaction_id).await;
    assert_eq!(committed_transaction_id, transaction_id);

    let after_commit = scan_shared_rows(&service, &table_ids[0], session_b).await;
    assert_eq!(after_commit.len(), 1);
    assert_eq!(after_commit[0].get("name").and_then(|value| value.as_str()), Some("hidden"));
}