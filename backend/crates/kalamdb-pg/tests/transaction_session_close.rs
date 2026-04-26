mod support;

use ntest::timeout;
use support::{
    begin_transaction, close_session, insert_shared_row, new_service_with_tables, open_session,
    scan_shared_rows,
};

#[tokio::test]
#[timeout(10000)]
async fn close_session_rolls_back_active_transaction_rows() {
    let (_app_ctx, service, _namespace, table_ids) =
        new_service_with_tables("pg_tx_close", &["items"]).await;
    let session_a = "pg-4106-1a2b";
    let session_b = "pg-4107-1a2c";

    open_session(&service, session_a).await;
    open_session(&service, session_b).await;

    let transaction_id = begin_transaction(&service, session_a).await;
    assert!(!transaction_id.is_empty());
    insert_shared_row(&service, &table_ids[0], session_a, 1, "staged").await;

    close_session(&service, session_a).await;

    let visible_rows = scan_shared_rows(&service, &table_ids[0], session_b).await;
    assert!(visible_rows.is_empty());
}
