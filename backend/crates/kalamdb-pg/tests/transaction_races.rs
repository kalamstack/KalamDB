mod support;

use std::sync::Arc;

use ntest::timeout;

use kalamdb_core::transactions::ExecutionOwnerKey;
use kalamdb_pg::{
    CloseSessionRequest, CommitTransactionRequest, PgService, RollbackTransactionRequest,
};
use support::{
    begin_transaction, insert_shared_row, new_service_with_tables, open_session,
    parse_transaction_id, request, scan_shared_rows,
};

fn visible_names(
    rows: Vec<std::collections::HashMap<String, kalamdb_commons::models::KalamCellValue>>,
) -> Vec<String> {
    let mut names = rows
        .into_iter()
        .filter_map(|row| row.get("name").and_then(|value| value.as_str()).map(str::to_string))
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[tokio::test]
#[timeout(15000)]
async fn pg_commit_vs_rollback_repeats_without_leaking_state() {
    let (app_ctx, service, _namespace, table_ids) =
        new_service_with_tables("pg_tx_race_commit_rb", &["items"]).await;
    let service = Arc::new(service);
    let session_id = "pg-8201-deadbeef";
    let observer_session = "pg-8202-deadbeef";
    let owner_key =
        ExecutionOwnerKey::from_pg_session_id(session_id).expect("pg owner key should parse");

    open_session(&service, session_id).await;
    open_session(&service, observer_session).await;

    let mut committed_names = Vec::new();

    for iteration in 0..12 {
        let transaction_id = begin_transaction(&service, session_id).await;
        let row_name = format!("pg-race-{iteration}");
        insert_shared_row(&service, &table_ids[0], session_id, (iteration + 1) as i64, &row_name)
            .await;

        let commit_service = Arc::clone(&service);
        let rollback_service = Arc::clone(&service);
        let commit_tx = transaction_id.clone();
        let rollback_tx = transaction_id.clone();

        let (commit_result, rollback_result) = tokio::join!(
            async move {
                commit_service
                    .commit_transaction(request(CommitTransactionRequest {
                        session_id: session_id.to_string(),
                        transaction_id: commit_tx,
                    }))
                    .await
            },
            async move {
                rollback_service
                    .rollback_transaction(request(RollbackTransactionRequest {
                        session_id: session_id.to_string(),
                        transaction_id: rollback_tx,
                    }))
                    .await
            },
        );

        let commit_ok = commit_result.is_ok();
        let rollback_ok = rollback_result.is_ok();
        assert_ne!(
            commit_ok, rollback_ok,
            "exactly one PG terminal RPC should succeed: commit={commit_result:?} rollback={rollback_result:?}"
        );
        if commit_ok {
            committed_names.push(row_name);
        }

        let parsed_transaction_id = parse_transaction_id(&transaction_id);
        assert!(app_ctx.transaction_coordinator().active_metrics().is_empty());
        assert!(
            app_ctx
                .transaction_coordinator()
                .active_for_owner(&owner_key)
                .is_none(),
            "PG session owner mapping should be cleared after commit/rollback race"
        );
        assert!(
            app_ctx
                .transaction_coordinator()
                .get_handle(&parsed_transaction_id)
                .is_none(),
            "PG commit/rollback race should not leave an active handle behind"
        );
        assert!(
            app_ctx
                .transaction_coordinator()
                .get_overlay(&parsed_transaction_id)
                .is_none(),
            "PG commit/rollback race should not leave an orphaned staged overlay"
        );
    }

    committed_names.sort();
    assert_eq!(
        visible_names(scan_shared_rows(&service, &table_ids[0], observer_session).await),
        committed_names
    );
}

#[tokio::test]
#[timeout(15000)]
async fn pg_commit_vs_close_session_repeats_without_leaking_state() {
    let (app_ctx, service, _namespace, table_ids) =
        new_service_with_tables("pg_tx_race_close", &["items"]).await;
    let service = Arc::new(service);
    let session_id = "pg-8301-feedface";
    let observer_session = "pg-8302-feedface";
    let owner_key =
        ExecutionOwnerKey::from_pg_session_id(session_id).expect("pg owner key should parse");

    open_session(&service, observer_session).await;

    let mut committed_names = Vec::new();

    for iteration in 0..12 {
        open_session(&service, session_id).await;

        let transaction_id = begin_transaction(&service, session_id).await;
        let row_name = format!("pg-close-race-{iteration}");
        insert_shared_row(&service, &table_ids[0], session_id, (iteration + 1) as i64, &row_name)
            .await;

        let commit_service = Arc::clone(&service);
        let close_service = Arc::clone(&service);
        let commit_tx = transaction_id.clone();

        let (commit_result, close_result) = tokio::join!(
            async move {
                commit_service
                    .commit_transaction(request(CommitTransactionRequest {
                        session_id: session_id.to_string(),
                        transaction_id: commit_tx,
                    }))
                    .await
            },
            async move {
                close_service
                    .close_session(request(CloseSessionRequest {
                        session_id: session_id.to_string(),
                    }))
                    .await
            },
        );

        assert!(
            close_result.is_ok(),
            "close_session should remain idempotent under commit races: {close_result:?}"
        );
        if commit_result.is_ok() {
            committed_names.push(row_name);
        }

        let parsed_transaction_id = parse_transaction_id(&transaction_id);
        assert!(app_ctx.transaction_coordinator().active_metrics().is_empty());
        assert!(
            app_ctx
                .transaction_coordinator()
                .active_for_owner(&owner_key)
                .is_none(),
            "PG close_session race should clear owner mapping"
        );
        assert!(
            app_ctx
                .transaction_coordinator()
                .get_handle(&parsed_transaction_id)
                .is_none(),
            "PG close_session race should not leave an active handle behind"
        );
        assert!(
            app_ctx
                .transaction_coordinator()
                .get_overlay(&parsed_transaction_id)
                .is_none(),
            "PG close_session race should not leave an orphaned staged overlay"
        );
    }

    committed_names.sort();
    assert_eq!(
        visible_names(scan_shared_rows(&service, &table_ids[0], observer_session).await),
        committed_names
    );
}