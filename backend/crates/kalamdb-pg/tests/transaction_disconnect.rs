mod support;

use ntest::timeout;

use support::{
    begin_transaction, insert_shared_row, new_service_with_tables, open_session,
    rollback_transaction, scan_shared_rows,
};

#[tokio::test]
#[timeout(10000)]
async fn begin_transaction_reclaims_stale_session_transaction() {
    let (_app_ctx, service, _namespace, table_ids) =
        new_service_with_tables("pg_tx_disconnect", &["items"]).await;
    let session_id = "pg-4108-1a2b";
    let observer_session = "pg-4109-1a2c";

    open_session(&service, session_id).await;
    open_session(&service, observer_session).await;

    let stale_tx = begin_transaction(&service, session_id).await;
    insert_shared_row(&service, &table_ids[0], session_id, 1, "stale").await;

    // Simulate reconnecting with the same backend/session identity after the previous
    // client path disappeared without sending an explicit rollback.
    open_session(&service, session_id).await;
    let replacement_tx = begin_transaction(&service, session_id).await;

    assert_ne!(stale_tx, replacement_tx);
    assert!(scan_shared_rows(&service, &table_ids[0], observer_session)
        .await
        .is_empty());

    let rolled_back = rollback_transaction(&service, session_id, &replacement_tx).await;
    assert_eq!(rolled_back, replacement_tx);
}