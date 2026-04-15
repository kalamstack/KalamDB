use crate::error::KalamDbError;
use crate::error_extensions::KalamDbResultExt;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::conversions::arrow_json_conversion::json_rows_to_arrow_batch;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{ReadContext, Role, UserId};
use kalamdb_session_datafusion::{
    extract_full_user_context as extract_full_user_context_session,
    extract_user_context as extract_user_context_session,
};
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::sync::Arc;

static SYSTEM_USER_ID: Lazy<UserId> = Lazy::new(|| UserId::from("_system"));

fn is_seq_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(col) if col.name == SystemColumnNames::SEQ)
}

fn extract_i64_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(val)), _) => Some(*val),
        _ => None,
    }
}

fn invert_comparison_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

fn normalize_seq_comparison(
    binary: &datafusion::logical_expr::BinaryExpr,
) -> Option<(Operator, i64)> {
    if is_seq_column(&binary.left) {
        let value = extract_i64_literal(&binary.right)?;
        return Some((binary.op, value));
    }

    if is_seq_column(&binary.right) {
        let value = extract_i64_literal(&binary.left)?;
        let op = invert_comparison_operator(binary.op)?;
        return Some((op, value));
    }

    None
}

/// Shared core state for all table providers
/// Returns (since_seq, until_seq)
/// since_seq is exclusive (>), until_seq is inclusive (<=)
/// For equality (_seq = X), returns (X-1, X) to match exactly that value
pub fn extract_seq_bounds_from_filter(expr: &Expr) -> (Option<SeqId>, Option<SeqId>) {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And => {
                // Combine bounds from AND expressions
                let (min_l, max_l) = extract_seq_bounds_from_filter(&binary.left);
                let (min_r, max_r) = extract_seq_bounds_from_filter(&binary.right);

                let min = match (min_l, min_r) {
                    (Some(a), Some(b)) => Some(if a > b { a } else { b }), // Max of mins
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };

                let max = match (max_l, max_r) {
                    (Some(a), Some(b)) => Some(if a < b { a } else { b }), // Min of maxes
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };
                (min, max)
            },
            Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                let (op, val) = match normalize_seq_comparison(binary) {
                    Some(result) => result,
                    None => return (None, None),
                };

                match op {
                    Operator::Eq => {
                        let since = val.saturating_sub(1);
                        (Some(SeqId::from(since)), Some(SeqId::from(val)))
                    },
                    Operator::Gt | Operator::GtEq => {
                        let since = if op == Operator::Gt {
                            val
                        } else {
                            val.saturating_sub(1)
                        };
                        (Some(SeqId::from(since)), None)
                    },
                    Operator::Lt | Operator::LtEq => {
                        let until = if op == Operator::Lt {
                            val.saturating_sub(1)
                        } else {
                            val
                        };
                        (None, Some(SeqId::from(until)))
                    },
                    _ => (None, None),
                }
            },
            _ => (None, None),
        },
        _ => (None, None),
    }
}

/// Return a shared `_system` user identifier for scope-agnostic operations
pub fn system_user_id() -> &'static UserId {
    &SYSTEM_USER_ID
}

/// Resolve user scope, defaulting to the shared system identifier for scope-less tables
pub fn resolve_user_scope(scope: Option<&UserId>) -> &UserId {
    scope.unwrap_or_else(|| system_user_id())
}

/// Extract (user_id, role) from DataFusion SessionState extensions.
pub fn extract_user_context(state: &dyn Session) -> Result<(&UserId, Role), KalamDbError> {
    extract_user_context_session(state).map_err(|e| {
        KalamDbError::InvalidOperation(format!("SessionUserContext not found in extensions: {}", e))
    })
}

/// Extract full session context (user_id, role, read_context) from DataFusion SessionState extensions.
///
/// Use this when you need to check read routing (leader-only reads in Raft cluster mode).
pub fn extract_full_user_context(
    state: &dyn Session,
) -> Result<(&UserId, Role, ReadContext), KalamDbError> {
    extract_full_user_context_session(state).map_err(|e| {
        KalamDbError::InvalidOperation(format!("SessionUserContext not found in extensions: {}", e))
    })
}

/// Helper function to inject system columns (_seq, _deleted) into Row values
pub fn inject_system_columns(
    schema: &SchemaRef,
    row: &mut Row,
    seq_value: i64,
    commit_seq_value: u64,
    deleted_value: bool,
) {
    if schema.field_with_name(SystemColumnNames::SEQ).is_ok() {
        row.values
            .insert(SystemColumnNames::SEQ.to_string(), ScalarValue::Int64(Some(seq_value)));
    }
    if schema.field_with_name(SystemColumnNames::COMMIT_SEQ).is_ok() {
        row.values.insert(
            SystemColumnNames::COMMIT_SEQ.to_string(),
            ScalarValue::UInt64(Some(commit_seq_value)),
        );
    }
    if schema.field_with_name(SystemColumnNames::DELETED).is_ok() {
        row.values.insert(
            SystemColumnNames::DELETED.to_string(),
            ScalarValue::Boolean(Some(deleted_value)),
        );
    }
}

/// Trait implemented by provider row types to expose system columns and JSON payload
pub trait ScanRow {
    fn row(&self) -> &Row;
    /// Take ownership of the inner Row, avoiding a clone when consuming.
    fn into_row(self) -> Row;
    fn seq_value(&self) -> i64;
    fn commit_seq_value(&self) -> u64;
    fn deleted_flag(&self) -> bool;
}

impl ScanRow for crate::SharedTableRow {
    fn row(&self) -> &Row {
        &self.fields
    }

    fn into_row(self) -> Row {
        self.fields
    }

    fn seq_value(&self) -> i64 {
        self._seq.as_i64()
    }

    fn commit_seq_value(&self) -> u64 {
        self._commit_seq
    }

    fn deleted_flag(&self) -> bool {
        self._deleted
    }
}

impl ScanRow for crate::UserTableRow {
    fn row(&self) -> &Row {
        &self.fields
    }

    fn into_row(self) -> Row {
        self.fields
    }

    fn seq_value(&self) -> i64 {
        self._seq.as_i64()
    }

    fn commit_seq_value(&self) -> u64 {
        self._commit_seq
    }

    fn deleted_flag(&self) -> bool {
        self._deleted
    }
}

impl ScanRow for crate::StreamTableRow {
    fn row(&self) -> &Row {
        &self.fields
    }

    fn into_row(self) -> Row {
        self.fields
    }

    fn seq_value(&self) -> i64 {
        self._seq.as_i64()
    }

    fn commit_seq_value(&self) -> u64 {
        0
    }

    fn deleted_flag(&self) -> bool {
        false
    }
}

/// Convert resolved key-value rows into an Arrow RecordBatch with system columns injected
pub fn rows_to_arrow_batch<K, R, F>(
    schema: &SchemaRef,
    kvs: Vec<(K, R)>,
    projection: Option<&Vec<usize>>,
    mut enrich_row: F,
) -> Result<RecordBatch, KalamDbError>
where
    R: ScanRow,
    F: FnMut(&mut Row, &R),
{
    let row_count = kvs.len();

    if let Some(proj) = projection {
        if proj.is_empty() {
            let empty_fields: Vec<datafusion::arrow::datatypes::Field> = Vec::new();
            let empty_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(empty_fields));
            if row_count == 0 {
                return Ok(RecordBatch::new_empty(empty_schema));
            }

            let options = RecordBatchOptions::new().with_row_count(Some(row_count));
            return RecordBatch::try_new_with_options(empty_schema, vec![], &options).map_err(
                |e| KalamDbError::InvalidOperation(format!("Failed to build Arrow batch: {}", e)),
            );
        }
    }

    let mut rows: Vec<Row> = Vec::with_capacity(row_count);

    for (_key, row) in kvs.into_iter() {
        let seq = row.seq_value();
        let commit_seq = row.commit_seq_value();
        let deleted = row.deleted_flag();

        // Let the caller inject extra fields (e.g., stream tables add user_id)
        // while we still have a reference to the typed row.
        let mut extra = Row::new(BTreeMap::new());
        enrich_row(&mut extra, &row);

        // Take ownership of the Row to avoid cloning the inner BTreeMap.
        let mut materialized = row.into_row();

        // Merge any caller-injected fields.
        if !extra.values.is_empty() {
            materialized.values.extend(extra.values);
        }

        inject_system_columns(schema, &mut materialized, seq, commit_seq, deleted);
        rows.push(materialized);
    }

    let target_schema = if let Some(proj) = projection {
        let fields: Vec<datafusion::arrow::datatypes::Field> =
            proj.iter().map(|i| schema.field(*i).clone()).collect();
        Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
    } else {
        schema.clone()
    };

    json_rows_to_arrow_batch(&target_schema, rows)
        .into_invalid_operation("Failed to build Arrow batch")
}
