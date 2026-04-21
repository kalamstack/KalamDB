use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::collect;
use kalamdb_commons::{StorageId, UserId};
use kalamdb_datafusion_sources::exec::DeferredBatchExec;
use kalamdb_store::test_utils::InMemoryBackend;
use kalamdb_store::StorageBackend;
use kalamdb_system::providers::storages::models::StorageMode;
use kalamdb_system::{AuthType, Role, User, UsersTableProvider};

fn total_rows(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[tokio::test]
async fn users_provider_scan_uses_deferred_batch_exec_and_returns_rows() {
    let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    let provider = UsersTableProvider::new(backend);

    provider
        .create_user(User {
            user_id: UserId::new("exec-model-user"),
            password_hash: "hashed_password".to_string(),
            role: Role::User,
            email: Some("exec-model@example.com".to_string()),
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: StorageMode::Table,
            storage_id: Some(StorageId::local()),
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: 1_000,
            updated_at: 1_000,
            last_seen: None,
            deleted_at: None,
        })
        .expect("seed user row");

    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("build users plan");

    assert!(plan.as_any().is::<DeferredBatchExec>());

    let batches = collect(plan, state.task_ctx()).await.expect("collect users plan");
    assert_eq!(total_rows(&batches), 1);
}