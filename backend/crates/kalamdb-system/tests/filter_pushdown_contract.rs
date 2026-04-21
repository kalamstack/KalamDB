use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{col, lit, Expr, TableProviderFilterPushDown};
use kalamdb_store::test_utils::InMemoryBackend;
use kalamdb_store::StorageBackend;
use kalamdb_system::UsersTableProvider;

#[test]
fn users_provider_reports_exact_filter_pushdown_for_representative_predicates() {
    let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
    let provider = UsersTableProvider::new(backend);

    let filters = vec![
        col("user_id").eq(lit("filter-user")),
        col("role").eq(lit("user")),
        col("created_at")
            .eq(lit(1_i64))
            .and(col("updated_at").eq(lit(1_i64))),
    ];
    let filter_refs: Vec<&Expr> = filters.iter().collect();

    let results = provider
        .supports_filters_pushdown(&filter_refs)
        .expect("report users pushdown support");

    assert_eq!(
        results,
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Exact,
        ]
    );
}