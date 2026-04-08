mod support;

use ntest::timeout;

use support::{
    await_user_leader, begin_transaction, commit_transaction, insert_user_row,
    new_cluster_user_service_with_tables, open_session, parse_transaction_id, scan_user_rows,
};

#[tokio::test]
#[timeout(10000)]
async fn transaction_commit_persists_rows_across_two_tables_and_keeps_canonical_id() {
    let (app_ctx, service, _namespace, table_ids) =
        new_cluster_user_service_with_tables("pg_tx_commit", &["items_a", "items_b"]).await;
    let session_id = "pg-4101-1a2b";
    let user_id = "pg-user-commit";
    await_user_leader(&app_ctx, user_id).await;
    open_session(&service, session_id).await;

    let transaction_id = begin_transaction(&service, session_id).await;
    let parsed_transaction_id = parse_transaction_id(&transaction_id);

    insert_user_row(&service, &table_ids[0], session_id, user_id, 1, "alpha").await;
    insert_user_row(&service, &table_ids[1], session_id, user_id, 2, "beta").await;

    let overlay = app_ctx
        .transaction_coordinator()
        .get_overlay(&parsed_transaction_id)
        .expect("overlay exists before commit");
    for table_id in &table_ids {
        let table_entries = overlay.table_entries(table_id).expect("table overlay entries");
        assert!(table_entries
            .values()
            .all(|entry| entry.transaction_id == parsed_transaction_id));
    }

    let committed_transaction_id = commit_transaction(&service, session_id, &transaction_id).await;
    assert_eq!(committed_transaction_id, transaction_id);

    let first_rows = scan_user_rows(&service, &table_ids[0], session_id, user_id).await;
    let second_rows = scan_user_rows(&service, &table_ids[1], session_id, user_id).await;

    assert_eq!(first_rows.len(), 1);
    assert_eq!(second_rows.len(), 1);
    assert_eq!(first_rows[0].get("name").and_then(|value| value.as_str()), Some("alpha"));
    assert_eq!(second_rows[0].get("name").and_then(|value| value.as_str()), Some("beta"));
}