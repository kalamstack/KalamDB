mod support;

use datafusion_common::ScalarValue;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{OperationKind, TransactionOrigin, UserId};
use kalamdb_commons::TableType;
use kalamdb_core::transactions::{ExecutionOwnerKey, StagedMutation};
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

    let commit_result = coordinator
        .commit(&transaction_id)
        .await
        .expect("commit succeeds");
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