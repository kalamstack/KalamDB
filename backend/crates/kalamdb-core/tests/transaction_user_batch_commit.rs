mod support;

use std::sync::Arc;

use datafusion_common::ScalarValue;
use kalamdb_commons::models::pg_operations::InsertRequest;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{OperationKind, TransactionOrigin, UserId};
use kalamdb_commons::TableType;
use kalamdb_core::operations::service::OperationService;
use kalamdb_core::transactions::{ExecutionOwnerKey, StagedMutation};
use kalamdb_pg::OperationExecutor;
use kalamdb_sharding::ShardRouter;
use kalamdb_tables::UserTableProvider;

use support::{create_cluster_app_context, create_user_table, row, unique_namespace};

fn insert_mutation(
    transaction_id: &kalamdb_commons::models::TransactionId,
    table_id: &kalamdb_commons::models::TableId,
    user_id: &UserId,
    pk_value: &str,
    row: Row,
) -> StagedMutation {
    StagedMutation::new(
        transaction_id.clone(),
        table_id.clone(),
        TableType::User,
        Some(user_id.clone()),
        OperationKind::Insert,
        pk_value,
        row,
        false,
    )
}

fn same_user_shard_pair() -> (UserId, UserId) {
    let router = ShardRouter::new(32, 1);
    let first = UserId::new("tx-user-scope-a");
    let target_shard = router.user_shard_id(&first);

    for index in 0..1024 {
        let candidate = UserId::new(format!("tx-user-scope-b-{index}"));
        if candidate != first && router.user_shard_id(&candidate) == target_shard {
            return (first, candidate);
        }
    }

    panic!("failed to find two user ids on the same shard")
}

async fn load_user_row(
    provider: &UserTableProvider,
    user_id: &UserId,
    pk_value: i64,
) -> kalamdb_tables::UserTableRow {
    provider
        .find_by_pk(user_id, &ScalarValue::Int64(Some(pk_value)))
        .await
        .expect("lookup succeeds")
        .expect("row exists after commit")
        .1
}

#[tokio::test]
#[ntest::timeout(8000)]
async fn explicit_commit_persists_same_table_same_user_user_inserts() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let user_id = UserId::new("tx-user-batch");
    let table_id = create_user_table(&app_ctx, &unique_namespace("tx_user_batch"), "items").await;

    let coordinator = app_ctx.transaction_coordinator();
    let transaction_id = coordinator
        .begin(
            ExecutionOwnerKey::internal(11),
            "test-user-batch".to_string().into(),
            TransactionOrigin::Internal,
        )
        .expect("begin transaction succeeds");

    coordinator
        .stage(
            &transaction_id,
            insert_mutation(&transaction_id, &table_id, &user_id, "1", row(1, "alpha")),
        )
        .expect("first stage succeeds");
    coordinator
        .stage(
            &transaction_id,
            insert_mutation(&transaction_id, &table_id, &user_id, "2", row(2, "beta")),
        )
        .expect("second stage succeeds");

    let commit_result = coordinator.commit(&transaction_id).await.expect("commit succeeds");
    let commit_seq = commit_result
        .committed_commit_seq
        .expect("commit should stamp a commit sequence");
    assert_eq!(commit_result.affected_rows, 2);

    let provider_arc = app_ctx
        .schema_registry()
        .get_provider(&table_id)
        .expect("user provider should be registered");
    let provider = provider_arc
        .as_any()
        .downcast_ref::<UserTableProvider>()
        .expect("provider should downcast to UserTableProvider");

    let (first_key, first_row) = provider
        .find_by_pk(&user_id, &ScalarValue::Int64(Some(1)))
        .await
        .expect("lookup succeeds")
        .expect("first row exists after commit");
    let (second_key, second_row) = provider
        .find_by_pk(&user_id, &ScalarValue::Int64(Some(2)))
        .await
        .expect("lookup succeeds")
        .expect("second row exists after commit");

    assert_ne!(first_key.seq, second_key.seq);
    assert_eq!(first_row._commit_seq, commit_seq);
    assert_eq!(second_row._commit_seq, commit_seq);
    assert_eq!(
        first_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("alpha".to_string())))
    );
    assert_eq!(
        second_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("beta".to_string())))
    );
}

#[tokio::test]
#[ntest::timeout(8000)]
async fn explicit_commit_preserves_user_scope_for_same_primary_keys() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let (first_user_id, second_user_id) = same_user_shard_pair();
    let table_id = create_user_table(&app_ctx, &unique_namespace("tx_user_scope"), "items").await;

    let coordinator = app_ctx.transaction_coordinator();
    let transaction_id = coordinator
        .begin(
            ExecutionOwnerKey::internal(12),
            "test-user-scope".to_string().into(),
            TransactionOrigin::Internal,
        )
        .expect("begin transaction succeeds");

    coordinator
        .stage(
            &transaction_id,
            insert_mutation(&transaction_id, &table_id, &first_user_id, "1", row(1, "alpha-a")),
        )
        .expect("first user first stage succeeds");
    coordinator
        .stage(
            &transaction_id,
            insert_mutation(&transaction_id, &table_id, &first_user_id, "2", row(2, "beta-a")),
        )
        .expect("first user second stage succeeds");
    coordinator
        .stage(
            &transaction_id,
            insert_mutation(&transaction_id, &table_id, &second_user_id, "1", row(1, "alpha-b")),
        )
        .expect("second user first stage succeeds");
    coordinator
        .stage(
            &transaction_id,
            insert_mutation(&transaction_id, &table_id, &second_user_id, "2", row(2, "beta-b")),
        )
        .expect("second user second stage succeeds");

    let commit_result = coordinator
        .commit(&transaction_id)
        .await
        .expect("commit succeeds for mixed-user batch");
    assert_eq!(commit_result.affected_rows, 4);

    let provider_arc = app_ctx
        .schema_registry()
        .get_provider(&table_id)
        .expect("user provider should be registered");
    let provider = provider_arc
        .as_any()
        .downcast_ref::<UserTableProvider>()
        .expect("provider should downcast to UserTableProvider");

    let first_user_first_row = load_user_row(provider, &first_user_id, 1).await;
    let first_user_second_row = load_user_row(provider, &first_user_id, 2).await;
    let second_user_first_row = load_user_row(provider, &second_user_id, 1).await;
    let second_user_second_row = load_user_row(provider, &second_user_id, 2).await;

    assert_eq!(
        first_user_first_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("alpha-a".to_string())))
    );
    assert_eq!(
        first_user_second_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("beta-a".to_string())))
    );
    assert_eq!(
        second_user_first_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("alpha-b".to_string())))
    );
    assert_eq!(
        second_user_second_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("beta-b".to_string())))
    );
}

#[tokio::test]
#[ntest::timeout(8000)]
async fn operation_service_preserves_user_scope_for_same_primary_keys() {
    let (app_ctx, _test_db) = create_cluster_app_context().await;
    let (first_user_id, second_user_id) = same_user_shard_pair();
    let table_id =
        create_user_table(&app_ctx, &unique_namespace("tx_user_scope_service"), "items").await;
    let service = Arc::new(OperationService::new(Arc::clone(&app_ctx)));
    let session_id = "pg-7103-deadbeef";

    let transaction_id = service
        .begin_transaction(session_id)
        .await
        .expect("begin transaction succeeds")
        .expect("transaction id returned");

    service
        .execute_insert(InsertRequest {
            table_id: table_id.clone(),
            table_type: TableType::User,
            session_id: Some(session_id.to_string()),
            user_id: Some(first_user_id.clone()),
            rows: vec![row(1, "alpha-a"), row(2, "beta-a")],
        })
        .await
        .expect("first user staged write succeeds");

    service
        .execute_insert(InsertRequest {
            table_id: table_id.clone(),
            table_type: TableType::User,
            session_id: Some(session_id.to_string()),
            user_id: Some(second_user_id.clone()),
            rows: vec![row(1, "alpha-b"), row(2, "beta-b")],
        })
        .await
        .expect("second user staged write succeeds");

    let committed_transaction_id = service
        .commit_transaction(session_id, &transaction_id)
        .await
        .expect("commit succeeds")
        .expect("commit returns transaction id");
    assert_eq!(committed_transaction_id, transaction_id);

    let provider_arc = app_ctx
        .schema_registry()
        .get_provider(&table_id)
        .expect("user provider should be registered");
    let provider = provider_arc
        .as_any()
        .downcast_ref::<UserTableProvider>()
        .expect("provider should downcast to UserTableProvider");

    let first_user_first_row = load_user_row(provider, &first_user_id, 1).await;
    let first_user_second_row = load_user_row(provider, &first_user_id, 2).await;
    let second_user_first_row = load_user_row(provider, &second_user_id, 1).await;
    let second_user_second_row = load_user_row(provider, &second_user_id, 2).await;

    assert_eq!(
        first_user_first_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("alpha-a".to_string())))
    );
    assert_eq!(
        first_user_second_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("beta-a".to_string())))
    );
    assert_eq!(
        second_user_first_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("alpha-b".to_string())))
    );
    assert_eq!(
        second_user_second_row.fields.get("name"),
        Some(&ScalarValue::Utf8(Some("beta-b".to_string())))
    );
}
