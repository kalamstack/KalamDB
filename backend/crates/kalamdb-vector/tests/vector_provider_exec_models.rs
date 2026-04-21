use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::collect;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::models::{ReadContext, Role, UserId};
use kalamdb_datafusion_sources::exec::DeferredBatchExec;
use kalamdb_session_datafusion::SessionUserContext;
use kalamdb_vector::{VectorSearchRuntime, VectorSearchScope, VectorSearchTableFunction};

#[derive(Debug)]
struct EmptyVectorRuntime;

impl VectorSearchRuntime for EmptyVectorRuntime {
    fn resolve_scope(
        &self,
        _table_id: &kalamdb_commons::models::TableId,
        _column_name: &str,
        _session_user: &UserId,
    ) -> datafusion::common::Result<Option<VectorSearchScope>> {
        Ok(None)
    }
}

fn total_rows(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn session_with_user(user_id: &str) -> SessionContext {
    let mut state = SessionContext::new().state().clone();
    state.config_mut().options_mut().extensions.insert(SessionUserContext::new(
        UserId::new(user_id),
        Role::Dba,
        ReadContext::Internal,
    ));
    SessionContext::new_with_state(state)
}

#[tokio::test]
async fn vector_search_scan_uses_deferred_batch_exec_and_keeps_empty_results_stable() {
    let provider = VectorSearchTableFunction::new(Arc::new(EmptyVectorRuntime))
        .call(&[
            Expr::Literal(ScalarValue::Utf8(Some("app.docs".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("embedding".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("[0.1, 0.2, 0.3]".to_string())), None),
            Expr::Literal(ScalarValue::Int64(Some(5)), None),
        ])
        .expect("build vector provider");

    let ctx = session_with_user("vector-exec-model");
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("build vector plan");

    assert!(plan.as_any().is::<DeferredBatchExec>());

    let batches = collect(plan, state.task_ctx()).await.expect("collect vector plan");
    assert_eq!(total_rows(&batches), 0);
}