//! Shared scan descriptors and provider-facing traits.
//!
//! Consumers implement narrow traits here and compose them with
//! [`exec`][crate::exec] and [`stream`][crate::stream] helpers instead of
//! duplicating `TableProvider` glue across crates.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use crate::pruning::{FilterRequest, LimitRequest, ProjectionRequest, PruningRequest};
use datafusion::logical_expr::{utils::expr_to_columns, Expr, TableProviderFilterPushDown};

/// Capability reporting for a single pushdown filter.
///
/// Mirrors [`TableProviderFilterPushDown`] but is produced by
/// [`SourceProvider::filter_capability`] so each source family can report
/// `Exact`, `Inexact`, or `Unsupported` per filter shape instead of declaring
/// a blanket fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterCapability {
    Exact,
    Inexact,
    Unsupported,
}

impl From<FilterCapability> for TableProviderFilterPushDown {
    fn from(value: FilterCapability) -> Self {
        match value {
            FilterCapability::Exact => TableProviderFilterPushDown::Exact,
            FilterCapability::Inexact => TableProviderFilterPushDown::Inexact,
            FilterCapability::Unsupported => TableProviderFilterPushDown::Unsupported,
        }
    }
}

/// Convert a list of planner-provided filters into DataFusion pushdown
/// capabilities using a per-filter classifier.
pub fn pushdown_results_for_filters<F>(
    filters: &[&Expr],
    mut classify: F,
) -> Vec<TableProviderFilterPushDown>
where
    F: FnMut(&Expr) -> FilterCapability,
{
    filters
        .iter()
        .map(|filter| TableProviderFilterPushDown::from(classify(filter)))
        .collect()
}

/// Capability classification for current MVCC-backed user/shared table scans.
///
/// MVCC sources now enforce the planner's predicates on the fully resolved rows
/// inside their deferred execution source, so they can report `Exact` and avoid
/// an extra external `FilterExec`. A narrower inexact pruning subset still
/// exists in [`crate::pruning::mvcc_filter_evaluation`] for hot/cold scan hints.
pub fn mvcc_filter_capability(_filter: &Expr, _pk_name: &str) -> FilterCapability {
    FilterCapability::Exact
}

/// Descriptor passed from a provider into the shared execution layer.
///
/// The descriptor is cheap to build on the planning hot path: it only captures
/// references (`Arc<Schema>`, borrowed filters, etc.) and the minimal
/// metadata needed for exec construction. Actual I/O is deferred to
/// [`ExecutionPlan::execute`][datafusion::physical_plan::ExecutionPlan::execute].
#[derive(Clone)]
pub struct ScanDescriptor {
    pub schema: SchemaRef,
    pub projection: Option<Arc<[usize]>>,
    pub filters: Arc<[Expr]>,
    pub limit: Option<usize>,
}

impl ScanDescriptor {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            projection: None,
            filters: Arc::from(Vec::<Expr>::new()),
            limit: None,
        }
    }

    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection.map(Arc::from);
        self
    }

    pub fn with_filters(mut self, filters: Vec<Expr>) -> Self {
        self.filters = Arc::from(filters);
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn projection_request(&self) -> ProjectionRequest {
        match self.projection.as_deref() {
            Some(indices) => ProjectionRequest { columns: Some(Arc::from(indices)) },
            None => ProjectionRequest::full(),
        }
    }

    pub fn filter_request(&self) -> FilterRequest {
        FilterRequest {
            filters: Arc::clone(&self.filters),
        }
    }

    pub fn limit_request(&self) -> LimitRequest {
        LimitRequest { limit: self.limit }
    }

    pub fn pruning_request(&self) -> PruningRequest {
        PruningRequest::new(
            self.projection_request(),
            self.filter_request(),
            self.limit_request(),
        )
    }
}

/// Minimal trait every KalamDB source implements on top of DataFusion's
/// [`TableProvider`][datafusion::catalog::TableProvider].
///
/// Storage-specific logic lives in the implementing crate; this trait only
/// carries descriptor production and capability reporting so the shared
/// execution substrate can stay storage-agnostic.
pub trait SourceProvider: Send + Sync {
    /// Report pushdown capability for a single filter expression.
    fn filter_capability(&self, filter: &Expr) -> FilterCapability;

    /// Build a [`ScanDescriptor`] for this source given the planner's
    /// projection, filters, and optional limit.
    fn scan_descriptor(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> ScanDescriptor;
}

/// Combine an ordered filter list into a single `AND` expression.
pub fn combined_filter(filters: &[Expr]) -> Option<Expr> {
    if filters.is_empty() {
        return None;
    }

    let first = filters[0].clone();
    Some(filters[1..].iter().cloned().fold(first, |acc, expr| acc.and(expr)))
}

/// Merge requested projection columns with any columns referenced by filters.
///
/// This is required when a provider evaluates the pushed filters itself and
/// therefore must keep predicate columns available until filtering finishes.
pub fn merged_projection_for_filters(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> Option<Vec<usize>> {
    match projection {
        Some(indices) if filters.is_empty() => Some(indices.clone()),
        Some(indices) => {
            let mut needed: std::collections::HashSet<usize> =
                indices.iter().copied().collect();
            let mut filter_columns = std::collections::HashSet::new();
            for filter in filters {
                let _ = expr_to_columns(filter, &mut filter_columns);
            }
            for column in &filter_columns {
                if let Some((index, _)) = schema.column_with_name(&column.name) {
                    needed.insert(index);
                }
            }

            let mut merged: Vec<usize> = needed.into_iter().collect();
            merged.sort_unstable();
            Some(merged)
        },
        None => None,
    }
}

/// Build a [`ScanDescriptor`] that preserves requested projection columns plus
/// any columns referenced by pushed filters.
pub fn merged_projection_scan_descriptor(
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> ScanDescriptor {
    let merged_projection = merged_projection_for_filters(&schema, projection, filters);
    ScanDescriptor::new(schema)
        .with_projection(merged_projection)
        .with_filters(filters.to_vec())
        .with_limit(limit)
}

/// Remap original schema indices onto a batch schema that was built from a
/// merged projection.
pub fn remap_projection_indices(
    original_schema: &SchemaRef,
    batch_schema: &SchemaRef,
    projection: &[usize],
) -> Vec<usize> {
    projection
        .iter()
        .filter_map(|&original_index| {
            let column_name = original_schema.field(original_index).name();
            batch_schema.column_with_name(column_name).map(|(index, _)| index)
        })
        .collect()
}
