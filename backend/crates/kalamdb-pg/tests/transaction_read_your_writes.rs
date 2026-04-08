mod support;

use ntest::timeout;

use support::{
    begin_transaction, insert_shared_row, new_service_with_tables, open_session,
    rollback_transaction, scan_shared_rows,
};

#[tokio::test]
#[timeout(10000)]
async fn transaction_scan_reads_staged_writes_before_rollback() {
    let (_app_ctx, service, _namespace, table_ids) =
        new_service_with_tables("pg_tx_ryw", &["items"]).await;
    let session_id = "pg-4103-1a2b";
    open_session(&service, session_id).await;

    let transaction_id = begin_transaction(&service, session_id).await;
    insert_shared_row(&service, &table_ids[0], session_id, 1, "alpha").await;

    let visible_rows = scan_shared_rows(&service, &table_ids[0], session_id).await;
    assert_eq!(visible_rows.len(), 1);
    assert_eq!(visible_rows[0].get("name").and_then(|value| value.as_str()), Some("alpha"));

    let rolled_back_transaction_id = rollback_transaction(&service, session_id, &transaction_id).await;
    assert_eq!(rolled_back_transaction_id, transaction_id);

    let post_rollback_rows = scan_shared_rows(&service, &table_ids[0], session_id).await;
    assert!(post_rollback_rows.is_empty());
}