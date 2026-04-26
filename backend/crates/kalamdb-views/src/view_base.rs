//! Base traits and common patterns for virtual views.
//!
//! This module provides the `VirtualView` trait and a shared
//! `TableProvider` implementation so views can compute data dynamically while
//! reusing the deferred execution path.
//!
//! ## Schema Caching
//! Views memoize their Arrow schema using a `static OnceLock<SchemaRef>`.
//! Each view's schema is computed once and shared across all uses.

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    common::DFSchema,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::PhysicalExpr,
    physical_plan::ExecutionPlan,
};
use kalamdb_datafusion_sources::{
    exec::{finalize_deferred_batch, DeferredBatchExec, DeferredBatchSource},
    provider::{combined_filter, pushdown_results_for_filters, FilterCapability},
};
use kalamdb_system::SystemTable;

use crate::error::RegistryError;

/// VirtualView trait defines the core behavior for virtual tables (views)
///
/// Views compute their data dynamically on each query rather than storing data.
/// This trait provides a common interface for all view implementations.
///
/// ## Schema Caching
/// Implementations should memoize schemas (e.g., with `OnceLock`) to ensure
/// zero-cost schema lookups after initialization.
pub trait VirtualView: Send + Sync + std::fmt::Debug {
    /// Get the SystemTable variant for this view
    ///
    /// This links the view to the centralized SystemTable enum for consistent
    /// identification and schema caching.
    fn system_table(&self) -> SystemTable;

    /// Get the Arrow schema for this view
    ///
    /// Implementations should memoize the schema (e.g., with `OnceLock`).
    fn schema(&self) -> SchemaRef;

    /// Compute a RecordBatch with the current view data
    ///
    /// This is called on each query to generate fresh results.
    fn compute_batch(&self) -> Result<RecordBatch, RegistryError>;

    /// Get the view name for logging and debugging
    ///
    /// Default implementation uses the SystemTable's table_name().
    fn view_name(&self) -> &str {
        self.system_table().table_name()
    }

    /// Check if this is a view (always true for VirtualView implementations)
    fn is_view(&self) -> bool {
        true
    }
}

/// ViewTableProvider implements DataFusion's `TableProvider` for any
/// `VirtualView`.
///
/// This generic provider eliminates repeated scan/pushdown boilerplate across
/// views. Each view only needs to implement `VirtualView`.
#[derive(Debug, Clone)]
pub struct ViewTableProvider<V: VirtualView> {
    view: Arc<V>,
}

struct ViewScanSource<V: VirtualView> {
    view: Arc<V>,
    physical_filter: Option<Arc<dyn PhysicalExpr>>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    output_schema: SchemaRef,
}

impl<V: VirtualView> std::fmt::Debug for ViewScanSource<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewScanSource")
            .field("view_name", &self.view.view_name())
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

#[async_trait]
impl<V: VirtualView + 'static> DeferredBatchSource for ViewScanSource<V> {
    fn source_name(&self) -> &'static str {
        "view_scan"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    async fn produce_batch(&self) -> DataFusionResult<RecordBatch> {
        let batch = self.view.compute_batch().map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to compute batch for view {}: {}",
                self.view.view_name(),
                error
            ))
        })?;

        finalize_deferred_batch(
            batch,
            self.physical_filter.as_ref(),
            self.projection.as_deref(),
            self.limit,
            self.source_name(),
        )
    }
}

impl<V: VirtualView> ViewTableProvider<V> {
    /// Create a new view table provider
    pub fn new(view: Arc<V>) -> Self {
        Self { view }
    }

    /// Get reference to the underlying view
    pub fn view(&self) -> &Arc<V> {
        &self.view
    }
}

#[async_trait]
impl<V: VirtualView + 'static> TableProvider for ViewTableProvider<V> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.view.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let base_schema = self.view.schema();
        let output_schema = match projection {
            Some(indices) => base_schema
                .project(indices)
                .map(Arc::new)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?,
            None => Arc::clone(&base_schema),
        };
        let physical_filter = if let Some(filter) = combined_filter(filters) {
            let df_schema = DFSchema::try_from(Arc::clone(&base_schema))?;
            Some(state.create_physical_expr(filter, &df_schema)?)
        } else {
            None
        };

        Ok(Arc::new(DeferredBatchExec::new(Arc::new(ViewScanSource {
            view: Arc::clone(&self.view),
            physical_filter,
            projection: projection.cloned(),
            limit,
            output_schema,
        }))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(pushdown_results_for_filters(filters, |_| FilterCapability::Exact))
    }
}
