#[allow(dead_code)]
fn uses_current_table_provider_surface() {
    use datafusion::datasource::TableProvider;
    fn _assert_trait<T: TableProvider>() {}
}

#[allow(dead_code)]
fn uses_current_execution_plan_surface() {
    use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
    fn _assert_trait<T: ExecutionPlan>() {}
    fn _accept(_props: PlanProperties) {}
}

#[allow(dead_code)]
fn uses_current_pushdown_surface() {
    use datafusion::logical_expr::TableProviderFilterPushDown;
    fn _accept(_value: TableProviderFilterPushDown) {}
}

#[test]
fn current_api_surface_is_reachable() {
    assert!(true);
}
