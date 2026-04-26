//! Contract tests for scan descriptor construction and filter capability
//! reporting. These exercise the shared substrate in isolation so provider
//! crates can depend on a stable contract.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::logical_expr::{col, lit};
use kalamdb_datafusion_sources::provider::{FilterCapability, ScanDescriptor, SourceProvider};

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

#[test]
fn scan_descriptor_defaults_are_empty() {
    let schema = test_schema();
    let desc = ScanDescriptor::new(schema.clone());
    assert!(desc.projection.is_none());
    assert!(desc.filters.is_empty());
    assert!(desc.limit.is_none());
    assert!(Arc::ptr_eq(&desc.schema, &schema));
}

#[test]
fn scan_descriptor_with_all_fields() {
    let schema = test_schema();
    let desc = ScanDescriptor::new(schema)
        .with_projection(Some(vec![0]))
        .with_filters(vec![col("id").eq(lit(1i64))])
        .with_limit(Some(10));
    assert_eq!(desc.projection.as_deref(), Some(&[0usize][..]));
    assert_eq!(desc.filters.len(), 1);
    assert_eq!(desc.limit, Some(10));
}

#[test]
fn scan_descriptor_exposes_pruning_requests() {
    let schema = test_schema();
    let desc = ScanDescriptor::new(schema)
        .with_projection(Some(vec![0]))
        .with_filters(vec![col("id").eq(lit(1i64))])
        .with_limit(Some(10));

    let pruning = desc.pruning_request();
    assert_eq!(pruning.projection.columns.as_deref(), Some(&[0usize][..]));
    assert_eq!(pruning.filters.filters.len(), 1);
    assert_eq!(pruning.limit.limit, Some(10));
}

#[test]
fn filter_capability_maps_to_datafusion() {
    use datafusion::logical_expr::TableProviderFilterPushDown as DfPushDown;
    assert!(matches!(DfPushDown::from(FilterCapability::Exact), DfPushDown::Exact));
    assert!(matches!(DfPushDown::from(FilterCapability::Inexact), DfPushDown::Inexact));
    assert!(matches!(
        DfPushDown::from(FilterCapability::Unsupported),
        DfPushDown::Unsupported
    ));
}

struct StubSource {
    schema: Arc<Schema>,
}

impl SourceProvider for StubSource {
    fn filter_capability(&self, filter: &datafusion::logical_expr::Expr) -> FilterCapability {
        use datafusion::logical_expr::Expr;
        match filter {
            Expr::BinaryExpr(_) => FilterCapability::Exact,
            _ => FilterCapability::Unsupported,
        }
    }

    fn scan_descriptor(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> ScanDescriptor {
        ScanDescriptor::new(self.schema.clone())
            .with_projection(projection.cloned())
            .with_filters(filters.to_vec())
            .with_limit(limit)
    }
}

#[test]
fn source_provider_trait_reports_and_builds_descriptor() {
    let src = StubSource {
        schema: test_schema(),
    };
    let filter = col("id").eq(lit(1i64));
    assert_eq!(src.filter_capability(&filter), FilterCapability::Exact);
    let desc = src.scan_descriptor(Some(&vec![0]), &[filter], Some(5));
    assert_eq!(desc.limit, Some(5));
    assert_eq!(desc.filters.len(), 1);
}
