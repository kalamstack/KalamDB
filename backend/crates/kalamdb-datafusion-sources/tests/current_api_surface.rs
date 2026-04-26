//! Compile-guard: the shared substrate targets the current DataFusion 53.x
//! `ExecutionPlan`/`TableProvider` API surface and must not depend on any
//! deprecated or removed helper.
//!
//! Failures here mean either DataFusion has been upgraded and the shared
//! crate must be adjusted, or a consumer tried to reintroduce a removed API
//! through the shared surface.

#[allow(dead_code)]
fn uses_current_table_provider_surface() {
    use datafusion::catalog::TableProvider;
    fn _assert_trait<T: TableProvider>() {}
}

#[allow(dead_code)]
fn uses_current_execution_plan_surface() {
    use datafusion::physical_plan::ExecutionPlan;
    fn _assert_trait<T: ExecutionPlan>() {}
}

#[allow(dead_code)]
fn uses_current_plan_properties_surface() {
    use datafusion::physical_plan::PlanProperties;
    fn _expect(_p: PlanProperties) {}
}

#[allow(dead_code)]
fn uses_current_record_batch_stream_surface() {
    use datafusion::{execution::RecordBatchStream, physical_plan::SendableRecordBatchStream};
    fn _assert_trait<T: RecordBatchStream>() {}
    fn _expect(_s: SendableRecordBatchStream) {}
}

#[allow(dead_code)]
fn uses_shared_exec_and_stream_helpers() {
    use std::sync::Arc;

    use arrow_schema::Schema;
    use datafusion::{
        error::{DataFusionError, Result as DataFusionResult},
        physical_plan::SendableRecordBatchStream,
    };
    use kalamdb_datafusion_sources::{exec::projected_schema, stream::one_shot_batch_stream};

    fn _project(schema: Arc<Schema>) -> DataFusionResult<Arc<Schema>> {
        projected_schema(&schema, None)
    }

    fn _stream(schema: Arc<Schema>) -> SendableRecordBatchStream {
        one_shot_batch_stream(schema, async {
            Err(DataFusionError::Execution("compile guard".to_string()))
        })
    }
}

#[test]
fn current_api_surface_is_reachable() {
    // Presence of this test is the guard: if any of the `use` statements
    // above stop compiling, the build fails at the crate's contract-test
    // boundary instead of deep inside a provider implementation.
    assert!(true);
}
