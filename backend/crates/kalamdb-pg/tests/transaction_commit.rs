mod support;

use kalamdb_commons::models::UserId;
use kalamdb_sharding::ShardRouter;
use ntest::timeout;

use support::{
    await_user_leader, begin_transaction, commit_transaction, insert_user_row,
    new_cluster_user_service_with_tables, open_session, parse_transaction_id, scan_user_rows,
};

fn same_user_shard_pair() -> (String, String) {
    let router = ShardRouter::new(32, 1);
    let first = UserId::new("pg-user-scope-a");
    let target_shard = router.user_shard_id(&first);

    for index in 0..1024 {
        let candidate = UserId::new(format!("pg-user-scope-b-{index}"));
        if candidate != first && router.user_shard_id(&candidate) == target_shard {
            return (first.to_string(), candidate.to_string());
        }
    }

    panic!("failed to find two user ids on the same user-data shard")
}

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

#[tokio::test]
#[timeout(10000)]
async fn transaction_commit_preserves_user_scope_for_same_primary_keys() {
    let (app_ctx, service, _namespace, table_ids) =
        new_cluster_user_service_with_tables("pg_tx_commit_scope", &["items"]).await;
    let session_id = "pg-4102-1a2b";
    let (first_user_id, second_user_id) = same_user_shard_pair();

    await_user_leader(&app_ctx, &first_user_id).await;
    await_user_leader(&app_ctx, &second_user_id).await;
    open_session(&service, session_id).await;

    let transaction_id = begin_transaction(&service, session_id).await;
    let parsed_transaction_id = parse_transaction_id(&transaction_id);

    insert_user_row(&service, &table_ids[0], session_id, &first_user_id, 1, "alpha-a").await;
    insert_user_row(&service, &table_ids[0], session_id, &first_user_id, 2, "beta-a").await;
    insert_user_row(&service, &table_ids[0], session_id, &second_user_id, 1, "alpha-b").await;
    insert_user_row(&service, &table_ids[0], session_id, &second_user_id, 2, "beta-b").await;

    let overlay = app_ctx
        .transaction_coordinator()
        .get_overlay(&parsed_transaction_id)
        .expect("overlay exists before commit");
    let table_entries = overlay.table_entries(&table_ids[0]).expect("table overlay entries");
    assert_eq!(table_entries.len(), 4);
    assert_eq!(
        table_entries
            .values()
            .filter(|entry| entry.user_id.as_ref().map(UserId::as_str) == Some(first_user_id.as_str()))
            .count(),
        2
    );
    assert_eq!(
        table_entries
            .values()
            .filter(|entry| entry.user_id.as_ref().map(UserId::as_str) == Some(second_user_id.as_str()))
            .count(),
        2
    );

    let committed_transaction_id = commit_transaction(&service, session_id, &transaction_id).await;
    assert_eq!(committed_transaction_id, transaction_id);

    let first_rows = scan_user_rows(&service, &table_ids[0], session_id, &first_user_id).await;
    let second_rows = scan_user_rows(&service, &table_ids[0], session_id, &second_user_id).await;

    let mut first_names = first_rows
        .iter()
        .filter_map(|row| row.get("name").and_then(|value| value.as_str()).map(str::to_string))
        .collect::<Vec<_>>();
    let mut second_names = second_rows
        .iter()
        .filter_map(|row| row.get("name").and_then(|value| value.as_str()).map(str::to_string))
        .collect::<Vec<_>>();
    first_names.sort();
    second_names.sort();

    assert_eq!(first_rows.len(), 2);
    assert_eq!(second_rows.len(), 2);
    assert_eq!(first_names, vec!["alpha-a".to_string(), "beta-a".to_string()]);
    assert_eq!(second_names, vec!["alpha-b".to_string(), "beta-b".to_string()]);
}
