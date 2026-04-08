mod support;

use ntest::timeout;

use support::{
    begin_transaction, insert_shared_row, new_service_with_tables, open_session,
    rollback_transaction, scan_shared_rows,
};

#[tokio::test]
#[timeout(10000)]
async fn transaction_rollback_discards_staged_rows_across_two_tables() {
    let (_app_ctx, service, _namespace, table_ids) =
        new_service_with_tables("pg_tx_rollback", &["items_a", "items_b"]).await;
    let session_id = "pg-4102-1a2b";
    open_session(&service, session_id).await;

    let transaction_id = begin_transaction(&service, session_id).await;
    insert_shared_row(&service, &table_ids[0], session_id, 1, "alpha").await;
    insert_shared_row(&service, &table_ids[1], session_id, 2, "beta").await;

    let rolled_back_transaction_id = rollback_transaction(&service, session_id, &transaction_id).await;
    assert_eq!(rolled_back_transaction_id, transaction_id);

    let first_rows = scan_shared_rows(&service, &table_ids[0], session_id).await;
    let second_rows = scan_shared_rows(&service, &table_ids[1], session_id).await;

    assert!(first_rows.is_empty());
    assert!(second_rows.is_empty());
}