//! Shared AST-level parsing utilities for transactional DML helpers.

use datafusion::scalar::ScalarValue;
use sqlparser::ast::{Expr, UnaryOperator, Value};

// ──────────────────────────────────────────────────────────────────────
// Scalar / Value conversion
// ──────────────────────────────────────────────────────────────────────

/// Convert a sqlparser `Expr` to a DataFusion `ScalarValue`.
///
/// Only handles literal values and simple negation.  Returns `Err` for
/// anything more complex (functions, casts, subqueries, etc.) so the
/// caller can fall back to DataFusion.
pub fn expr_to_scalar(expr: &Expr) -> Result<ScalarValue, &'static str> {
    match expr {
        Expr::Value(val) => sql_value_to_scalar(&val.value),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match strip_nested_expr(expr.as_ref()) {
            Expr::Value(val) => match &val.value {
                Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(ScalarValue::Int64(Some(-i)))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(ScalarValue::Float64(Some(-f)))
                    } else {
                        Err("unsupported numeric literal")
                    }
                },
                _ => Err("unsupported unary literal"),
            },
            _ => Err("unsupported unary expression"),
        },
        _ => Err("unsupported expression"),
    }
}

/// Convert a sqlparser `Value` to a DataFusion `ScalarValue`.
pub fn sql_value_to_scalar(value: &Value) -> Result<ScalarValue, &'static str> {
    match value {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Ok(ScalarValue::Utf8(Some(s.clone())))
        },
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(ScalarValue::Int64(Some(i)))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(ScalarValue::Float64(Some(f)))
            } else {
                Err("unsupported numeric literal")
            }
        },
        Value::Boolean(b) => Ok(ScalarValue::Boolean(Some(*b))),
        Value::Null => Ok(ScalarValue::Null),
        _ => Err("unsupported literal"),
    }
}

/// Recursively unwrap `Expr::Nested(inner)` parentheses.
pub fn strip_nested_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => strip_nested_expr(inner),
        _ => expr,
    }
}
