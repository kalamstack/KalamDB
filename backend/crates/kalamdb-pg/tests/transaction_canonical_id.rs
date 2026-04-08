mod support;

use ntest::timeout;

use support::{
    await_user_leader, begin_transaction, commit_transaction, insert_user_row,
    new_cluster_user_service_with_tables, open_session, parse_transaction_id,
};

#[tokio::test]
#[timeout(10000)]
async fn canonical_transaction_id_is_preserved_from_begin_through_commit() {
    let (app_ctx, service, _namespace, table_ids) =
        new_cluster_user_service_with_tables("pg_tx_canonical", &["items"]).await;
    let session_id = "pg-4201-c0ffee";
    let user_id = "pg-user-canonical";
    await_user_leader(&app_ctx, user_id).await;
    open_session(&service, session_id).await;

    let transaction_id = begin_transaction(&service, session_id).await;
    let parsed_transaction_id = parse_transaction_id(&transaction_id);

    let handle = app_ctx
        .transaction_coordinator()
        .get_handle(&parsed_transaction_id)
        .expect("transaction handle exists after BEGIN");
    assert_eq!(handle.transaction_id, parsed_transaction_id);
    assert_eq!(handle.owner_id.as_ref(), session_id);

    insert_user_row(&service, &table_ids[0], session_id, user_id, 1, "alpha").await;

    let overlay = app_ctx
        .transaction_coordinator()
        .get_overlay(&parsed_transaction_id)
        .expect("overlay exists before commit");
    let table_entries = overlay
        .table_entries(&table_ids[0])
        .expect("table overlay entries");
    assert!(table_entries
        .values()
        .all(|entry| entry.transaction_id == parsed_transaction_id));

    let committed_transaction_id = commit_transaction(&service, session_id, &transaction_id).await;
    assert_eq!(committed_transaction_id, transaction_id);
    assert!(app_ctx
        .transaction_coordinator()
        .get_handle(&parsed_transaction_id)
        .is_none());
}
