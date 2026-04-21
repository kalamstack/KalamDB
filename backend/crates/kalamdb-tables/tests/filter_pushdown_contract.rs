use datafusion::logical_expr::{col, lit, Expr, TableProviderFilterPushDown};
use kalamdb_datafusion_sources::provider::{mvcc_filter_capability, pushdown_results_for_filters};
use kalamdb_datafusion_sources::pruning::mvcc_filter_evaluation;

fn classify_mvcc_filters(filters: Vec<Expr>) -> Vec<TableProviderFilterPushDown> {
    let filter_refs: Vec<&Expr> = filters.iter().collect();
    pushdown_results_for_filters(&filter_refs, |filter| {
        mvcc_filter_capability(filter, "id")
    })
}

#[test]
fn mvcc_filters_report_exact_for_resolved_row_evaluation() {
    let results = classify_mvcc_filters(vec![
        col("id").eq(lit(1_i64)),
        col("_seq").gt_eq(lit(10_i64)),
        col("_deleted").eq(lit(false)),
        col("id").eq(lit(2_i64)).and(col("name").eq(lit("beta"))),
    ]);

    assert_eq!(
        results,
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Exact,
        ]
    );
}

#[test]
fn mvcc_filter_evaluation_preserves_inexact_source_pruning_subset() {
    let filters = vec![
        col("id").eq(lit(1_i64)),
        col("_seq").gt_eq(lit(10_i64)),
        col("_deleted").eq(lit(false)),
        col("name").eq(lit("alpha")),
        col("id").eq(lit(2_i64)).and(col("name").eq(lit("beta"))),
    ];
    let evaluation = mvcc_filter_evaluation(&filters, "id");

    assert_eq!(
        evaluation.exact.filters.as_ref(),
        filters.as_slice(),
    );
    assert_eq!(
        evaluation.inexact.filters.as_ref(),
        vec![
            col("id").eq(lit(1_i64)),
            col("_seq").gt_eq(lit(10_i64)),
            col("_deleted").eq(lit(false)),
            col("id").eq(lit(2_i64)).and(col("name").eq(lit("beta"))),
        ]
        .as_slice(),
    );
}