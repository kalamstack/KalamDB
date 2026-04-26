#[allow(dead_code)]
fn uses_current_execution_plan_surface() {
    use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
    fn _assert_trait<T: ExecutionPlan>() {}
    fn _accept(_props: PlanProperties) {}
}

#[allow(dead_code)]
fn uses_current_record_batch_stream_surface() {
    use datafusion::{execution::RecordBatchStream, physical_plan::SendableRecordBatchStream};
    fn _assert_trait<T: RecordBatchStream>() {}
    fn _accept(_stream: SendableRecordBatchStream) {}
}

#[allow(dead_code)]
fn uses_shared_exec_surface() {
    use kalamdb_datafusion_sources::exec::{DeferredBatchExec, DeferredBatchSource};
    fn _builder(_source: std::sync::Arc<dyn DeferredBatchSource>) -> DeferredBatchExec {
        DeferredBatchExec::new(_source)
    }
}

#[test]
fn current_api_surface_is_reachable() {
    assert!(true);
}
