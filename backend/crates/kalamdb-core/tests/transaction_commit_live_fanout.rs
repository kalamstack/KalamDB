mod support;

use std::sync::Arc;
use std::time::Duration;

use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{
    ConnectionId, LiveQueryId, OperationKind, TransactionOrigin, UserId,
};
use kalamdb_commons::websocket::ChangeType;
use kalamdb_commons::TableType;
use kalamdb_core::transactions::{ExecutionOwnerKey, StagedMutation};
use kalamdb_live::models::{
    NotificationSender, SubscriptionFlowControl, SubscriptionHandle, SubscriptionRuntimeMetadata,
};
use tokio::sync::mpsc;

use support::{create_cluster_app_context, create_shared_table, row, unique_namespace};

fn make_shared_handle(
    subscription_id: &str,
    tx: NotificationSender,
    flow_control: Arc<SubscriptionFlowControl>,
) -> SubscriptionHandle {
    SubscriptionHandle {
        subscription_id: Arc::from(subscription_id),
        filter_expr: None,
        projections: None,
        notification_tx: tx,
        flow_control: Some(flow_control),
        runtime_metadata: Arc::new(SubscriptionRuntimeMetadata::new(
            "SELECT * FROM shared.items",
            None,
            1,
        )),
    }
}

fn insert_mutation(
    transaction_id: &kalamdb_commons::models::TransactionId,
    table_id: &kalamdb_commons::models::TableId,
    pk_value: &str,
    row: Row,
) -> StagedMutation {
    StagedMutation::new(
        transaction_id.clone(),
        table_id.clone(),
        TableType::Shared,
        None,
        OperationKind::Insert,
        pk_value,
        row,
        false,
    )
}

#[tokio::test]
#[ntest::timeout(8000)]
async fn explicit_commit_releases_live_notification_after_commit() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let table_id =
        create_shared_table(&app_ctx, &unique_namespace("tx_live_commit"), "items").await;
    let registry = app_ctx.connection_registry();

    let connection_id = ConnectionId::new("conn-live-commit");
    let subscriber_user = UserId::new("watcher-commit");
    let live_id =
        LiveQueryId::new(subscriber_user, connection_id.clone(), "sub-live-commit".to_string());
    let (tx, mut rx) = mpsc::channel(8);
    let flow_control = Arc::new(SubscriptionFlowControl::new());
    flow_control.mark_initial_complete();
    registry.index_shared_subscription(
        &connection_id,
        live_id,
        table_id.clone(),
        make_shared_handle("sub-live-commit", tx, flow_control),
    );

    let coordinator = app_ctx.transaction_coordinator();
    let transaction_id = coordinator
        .begin(
            ExecutionOwnerKey::internal(1),
            "test-live-commit".to_string().into(),
            TransactionOrigin::Internal,
        )
        .expect("begin transaction succeeds");

    coordinator
        .stage(
            &transaction_id,
            insert_mutation(&transaction_id, &table_id, "1", row(1, "committed")),
        )
        .expect("stage succeeds");

    assert!(
        tokio::time::timeout(Duration::from_millis(150), rx.recv()).await.is_err(),
        "staged transaction should not fan out before commit"
    );

    coordinator.commit(&transaction_id).await.expect("commit succeeds");

    let delivered = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("commit notification arrives")
        .expect("notification payload present");
    let wire = delivered.as_ref();
    assert_eq!(wire.payload.change_type, ChangeType::Insert);

    let json: serde_json::Value = serde_json::from_slice(&wire.to_json()).expect("json payload");
    assert_eq!(json["subscription_id"], "sub-live-commit");
    assert_eq!(json["rows"][0]["id"], "1");
    assert_eq!(json["rows"][0]["name"], "committed");
}

#[tokio::test]
#[ntest::timeout(8000)]
async fn explicit_rollback_emits_no_live_notification() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let table_id =
        create_shared_table(&app_ctx, &unique_namespace("tx_live_rollback"), "items").await;
    let registry = app_ctx.connection_registry();

    let connection_id = ConnectionId::new("conn-live-rollback");
    let subscriber_user = UserId::new("watcher-rollback");
    let live_id =
        LiveQueryId::new(subscriber_user, connection_id.clone(), "sub-live-rollback".to_string());
    let (tx, mut rx) = mpsc::channel(8);
    let flow_control = Arc::new(SubscriptionFlowControl::new());
    flow_control.mark_initial_complete();
    registry.index_shared_subscription(
        &connection_id,
        live_id,
        table_id.clone(),
        make_shared_handle("sub-live-rollback", tx, flow_control),
    );

    let coordinator = app_ctx.transaction_coordinator();
    let transaction_id = coordinator
        .begin(
            ExecutionOwnerKey::internal(2),
            "test-live-rollback".to_string().into(),
            TransactionOrigin::Internal,
        )
        .expect("begin transaction succeeds");

    coordinator
        .stage(
            &transaction_id,
            insert_mutation(&transaction_id, &table_id, "1", row(1, "rolled-back")),
        )
        .expect("stage succeeds");

    coordinator.rollback(&transaction_id).expect("rollback succeeds");

    assert!(
        tokio::time::timeout(Duration::from_millis(250), rx.recv()).await.is_err(),
        "rollback should not fan out any live notification"
    );
}
