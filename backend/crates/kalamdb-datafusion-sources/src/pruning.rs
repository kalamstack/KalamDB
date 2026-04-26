//! Filter, projection, limit, and pruning descriptors shared across all source
//! families.
//!
//! This module does not implement pruning itself: it defines the descriptor
//! shapes every source family uses to express what should be read, so
//! consumers can hand the descriptors to existing DataFusion, Arrow, and
//! Parquet pruning primitives instead of inventing bespoke machinery.

use std::sync::Arc;

use datafusion::logical_expr::{utils::expr_to_columns, Expr, Operator};

/// Projection descriptor. `None` means "read every column"; otherwise the
/// wrapped `Arc<[usize]>` holds top-level column indices in the source schema.
#[derive(Clone, Debug, Default)]
pub struct ProjectionRequest {
    pub columns: Option<Arc<[usize]>>,
}

impl ProjectionRequest {
    pub fn full() -> Self {
        Self { columns: None }
    }

    pub fn columns(indices: Vec<usize>) -> Self {
        Self {
            columns: Some(Arc::from(indices)),
        }
    }
}

/// Filter descriptor carrying the planner-provided predicates and the
/// capability the source reports for them. Sources should only evaluate
/// `filters` whose capability is [`Exact`][crate::provider::FilterCapability::Exact]
/// or [`Inexact`][crate::provider::FilterCapability::Inexact].
#[derive(Clone)]
pub struct FilterRequest {
    pub filters: Arc<[Expr]>,
}

impl FilterRequest {
    pub fn new(filters: Vec<Expr>) -> Self {
        Self {
            filters: Arc::from(filters),
        }
    }
}

/// Limit descriptor. Consumers may honor the limit where it is safe (cold
/// historical reads, one-shot sources) and MUST ignore it where ordering or
/// MVCC merge semantics would make early termination unsafe.
#[derive(Clone, Copy, Debug, Default)]
pub struct LimitRequest {
    pub limit: Option<usize>,
}

impl LimitRequest {
    pub fn new(limit: Option<usize>) -> Self {
        Self { limit }
    }
}

/// Aggregate pruning request that consumers can pass down to segment-level,
/// row-group-level, and page-level pruning helpers.
#[derive(Clone)]
pub struct PruningRequest {
    pub projection: ProjectionRequest,
    pub filters: FilterRequest,
    pub limit: LimitRequest,
}

impl PruningRequest {
    pub fn new(projection: ProjectionRequest, filters: FilterRequest, limit: LimitRequest) -> Self {
        Self {
            projection,
            filters,
            limit,
        }
    }
}

/// Split MVCC-backed table filters into the exact post-resolution predicates
/// that can be enforced on the final visible rows and the narrower inexact
/// subset that can reduce hot/cold source work before version resolution.
#[derive(Clone)]
pub struct MvccFilterEvaluation {
    pub exact: FilterRequest,
    pub inexact: FilterRequest,
}

impl MvccFilterEvaluation {
    pub fn new(exact: FilterRequest, inexact: FilterRequest) -> Self {
        Self { exact, inexact }
    }
}

pub fn mvcc_filter_evaluation(filters: &[Expr], pk_name: &str) -> MvccFilterEvaluation {
    let inexact_filters = filters
        .iter()
        .filter(|filter| is_mvcc_source_pruning_filter(filter, pk_name))
        .cloned()
        .collect();

    MvccFilterEvaluation::new(
        FilterRequest::new(filters.to_vec()),
        FilterRequest::new(inexact_filters),
    )
}

fn is_mvcc_source_pruning_filter(filter: &Expr, pk_name: &str) -> bool {
    contains_pk_equality(filter, pk_name)
        || contains_seq_bound(filter)
        || references_column(filter, "_deleted")
}

fn contains_pk_equality(filter: &Expr, pk_name: &str) -> bool {
    match filter {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            is_column_literal_comparison(&binary.left, &binary.right, pk_name)
                || is_column_literal_comparison(&binary.right, &binary.left, pk_name)
        },
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            contains_pk_equality(&binary.left, pk_name)
                || contains_pk_equality(&binary.right, pk_name)
        },
        _ => false,
    }
}

fn contains_seq_bound(filter: &Expr) -> bool {
    match filter {
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) =>
        {
            is_column_literal_comparison(&binary.left, &binary.right, "_seq")
                || is_column_literal_comparison(&binary.right, &binary.left, "_seq")
        },
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            contains_seq_bound(&binary.left) || contains_seq_bound(&binary.right)
        },
        _ => false,
    }
}

fn is_column_literal_comparison(left: &Expr, right: &Expr, column_name: &str) -> bool {
    matches!(left, Expr::Column(column) if column.name.eq_ignore_ascii_case(column_name))
        && matches!(right, Expr::Literal(_, _))
}

fn references_column(filter: &Expr, column_name: &str) -> bool {
    let mut columns = std::collections::HashSet::new();
    if expr_to_columns(filter, &mut columns).is_err() {
        return false;
    }

    columns.iter().any(|column| column.name.eq_ignore_ascii_case(column_name))
}
