mod support;

use std::{sync::Arc, time::Duration};

use kalamdb_commons::{models::pg_operations::InsertRequest, TableType};
use kalamdb_configs::ServerConfig;
use kalamdb_core::{operations::service::OperationService, transactions::ExecutionOwnerKey};
use kalamdb_pg::OperationExecutor;
use support::{
    create_cluster_app_context, create_cluster_app_context_with_config, create_executor,
    create_shared_table, execute_ok, insert_sql, observer_exec_ctx, request_exec_ctx,
    request_transaction_state, row, select_names, unique_namespace,
};

#[tokio::test]
#[ntest::timeout(6000)]
async fn repeated_commit_vs_rollback_clears_coordinator_state() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let table_id =
        create_shared_table(&app_ctx, &unique_namespace("tx_race_commit_rb"), "items").await;
    let service = Arc::new(OperationService::new(Arc::clone(&app_ctx)));
    let session_id = "pg-7101-deadbeef";
    let owner_key =
        ExecutionOwnerKey::from_pg_session_id(session_id).expect("pg owner key should parse");

    for iteration in 0..24 {
        let transaction_id = service
            .begin_transaction(session_id)
            .await
            .expect("begin transaction succeeds")
            .expect("transaction id returned");
        let row_name = format!("race-{iteration}");

        service
            .execute_insert(InsertRequest {
                table_id: table_id.clone(),
                table_type: TableType::Shared,
                session_id: Some(session_id.to_string()),
                user_id: None,
                rows: vec![row((iteration + 1) as i64, &row_name)],
            })
            .await
            .expect("staged write succeeds");

        let coordinator = app_ctx.transaction_coordinator();
        let commit_coordinator = Arc::clone(&coordinator);
        let rollback_coordinator = Arc::clone(&coordinator);
        let parsed_transaction_id = transaction_id.clone();
        let commit_tx = parsed_transaction_id.clone();
        let rollback_tx = parsed_transaction_id.clone();

        let (commit_result, rollback_result) =
            tokio::join!(async move { commit_coordinator.commit(&commit_tx).await }, async move {
                rollback_coordinator.rollback(&rollback_tx)
            },);

        let commit_ok = commit_result.is_ok();
        let rollback_ok = rollback_result.is_ok();
        assert_ne!(
            commit_ok, rollback_ok,
            "exactly one terminal path should win: commit={commit_result:?} \
             rollback={rollback_result:?}"
        );

        assert!(app_ctx.transaction_coordinator().active_metrics().is_empty());
        assert!(
            app_ctx.transaction_coordinator().active_for_owner(&owner_key).is_none(),
            "owner mapping should be cleared after commit/rollback race"
        );
        assert!(
            app_ctx.transaction_coordinator().get_handle(&parsed_transaction_id).is_none(),
            "terminal race should not leave an active handle behind"
        );
        assert!(
            app_ctx.transaction_coordinator().get_overlay(&parsed_transaction_id).is_none(),
            "terminal race should not leave an orphaned staged overlay"
        );
    }
}

#[tokio::test]
#[ntest::timeout(9000)]
async fn repeated_timeout_cleanup_drops_staged_state() {
    let mut config = ServerConfig::default();
    config.transaction_timeout_secs = 1;

    let (app_ctx, _test_db) = create_cluster_app_context_with_config(config).await;
    let table_id =
        create_shared_table(&app_ctx, &unique_namespace("tx_race_timeout"), "items").await;
    let service = Arc::new(OperationService::new(Arc::clone(&app_ctx)));
    let executor = create_executor(Arc::clone(&app_ctx));
    let observer_ctx = observer_exec_ctx(&app_ctx);
    let session_id = "pg-7102-feedface";
    let owner_key =
        ExecutionOwnerKey::from_pg_session_id(session_id).expect("pg owner key should parse");

    for iteration in 0..2 {
        let transaction_id = service
            .begin_transaction(session_id)
            .await
            .expect("begin transaction succeeds")
            .expect("transaction id returned");

        service
            .execute_insert(InsertRequest {
                table_id: table_id.clone(),
                table_type: TableType::Shared,
                session_id: Some(session_id.to_string()),
                user_id: None,
                rows: vec![row((iteration + 1) as i64, &format!("timeout-{iteration}"))],
            })
            .await
            .expect("staged write succeeds");

        tokio::time::sleep(Duration::from_millis(2200)).await;

        let timeout_error = service
            .execute_insert(InsertRequest {
                table_id: table_id.clone(),
                table_type: TableType::Shared,
                session_id: Some(session_id.to_string()),
                user_id: None,
                rows: vec![row((iteration + 101) as i64, "late")],
            })
            .await
            .expect_err("follow-up write should fail after timeout");
        assert!(timeout_error.message().contains("timed out"), "{timeout_error}");

        let parsed_transaction_id = transaction_id.clone();
        assert!(app_ctx.transaction_coordinator().active_metrics().is_empty());
        assert!(
            app_ctx.transaction_coordinator().get_overlay(&parsed_transaction_id).is_none(),
            "timed out transaction should not retain staged writes"
        );

        service
            .rollback_transaction(session_id, &transaction_id)
            .await
            .expect("timeout cleanup succeeds")
            .expect("rollback returns transaction id");

        assert!(
            app_ctx.transaction_coordinator().active_for_owner(&owner_key).is_none(),
            "timeout cleanup should clear owner mapping"
        );
        assert!(
            app_ctx.transaction_coordinator().get_handle(&parsed_transaction_id).is_none(),
            "timeout cleanup should clear the terminal transaction handle"
        );
    }

    assert!(select_names(&executor, &observer_ctx, &table_id).await.is_empty());
}

#[tokio::test]
#[ntest::timeout(6000)]
async fn repeated_request_cleanup_rolls_back_without_leaking_state() {
    let (app_ctx, executor, table_id, _test_db) =
        support::setup_shared_table("sql_tx_race_cleanup", "items").await;
    let observer_ctx = observer_exec_ctx(&app_ctx);

    for iteration in 0..24 {
        let request_ctx = request_exec_ctx(&app_ctx, &format!("sql-request-race-{iteration}"));
        execute_ok(&executor, &request_ctx, "BEGIN").await;
        execute_ok(
            &executor,
            &request_ctx,
            &insert_sql(&table_id, (iteration + 1) as i64, &format!("staged-{iteration}")),
        )
        .await;

        let mut tx_state = request_transaction_state(&request_ctx);
        tx_state.sync_from_coordinator(&app_ctx);
        let transaction_id = tx_state
            .active_transaction_id()
            .cloned()
            .expect("request transaction should be active before cleanup");

        let rolled_back_id = tx_state
            .rollback_if_active(&app_ctx)
            .expect("request cleanup succeeds")
            .expect("request cleanup should roll back the active transaction");
        assert_eq!(rolled_back_id, transaction_id);

        assert!(app_ctx.transaction_coordinator().active_metrics().is_empty());
        assert!(
            app_ctx
                .transaction_coordinator()
                .active_for_owner(&tx_state.owner_key())
                .is_none(),
            "request cleanup should clear owner mapping"
        );
        assert!(
            app_ctx.transaction_coordinator().get_handle(&transaction_id).is_none(),
            "request cleanup should clear the transaction handle"
        );
        assert!(
            app_ctx.transaction_coordinator().get_overlay(&transaction_id).is_none(),
            "request cleanup should not leave staged overlays behind"
        );
    }

    assert!(select_names(&executor, &observer_ctx, &table_id).await.is_empty());
}
