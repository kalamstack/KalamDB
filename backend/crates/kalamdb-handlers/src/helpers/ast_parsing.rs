//! Shared AST-level parsing utilities for fast-path DML.
//!
//! Consolidates `expr_to_scalar`, `sql_value_to_scalar`, and table-ID
//! extraction logic that was previously duplicated across `fast_insert.rs`
//! and `fast_point_dml.rs`.

use datafusion::scalar::ScalarValue;
use kalamdb_commons::TableId;
use sqlparser::ast::{
    Expr, Insert, ObjectNamePart, TableFactor, TableObject, TableWithJoins, UnaryOperator, Value,
};

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

// ──────────────────────────────────────────────────────────────────────
// Table-ID extraction from AST nodes
// ──────────────────────────────────────────────────────────────────────

/// Resolve a 1- or 2-part identifier list into a `TableId`,
/// applying `default_namespace` when only one part is present.
fn resolve_table_parts(parts: &[String], default_namespace: &str) -> Option<TableId> {
    match parts.len() {
        1 => Some(TableId::from_strings(default_namespace, &parts[0])),
        2 => Some(TableId::from_strings(&parts[0], &parts[1])),
        _ => None,
    }
}

/// Collect identifier parts from an `ObjectName`.
fn object_name_parts(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect()
}

/// Extract `TableId` from an INSERT statement's table reference.
pub fn extract_table_id_from_insert(insert: &Insert, default_namespace: &str) -> Option<TableId> {
    match &insert.table {
        TableObject::TableName(obj_name) => {
            resolve_table_parts(&object_name_parts(obj_name), default_namespace)
        },
        _ => None,
    }
}

/// Extract `TableId` from a `TableWithJoins` node (UPDATE / DELETE).
pub fn extract_table_id_from_table_with_joins(
    table: &TableWithJoins,
    default_namespace: &str,
) -> Option<TableId> {
    if !table.joins.is_empty() {
        return None;
    }
    match &table.relation {
        TableFactor::Table { name, .. } => {
            resolve_table_parts(&object_name_parts(name), default_namespace)
        },
        _ => None,
    }
}

/// Extract `TableId` from a DELETE statement (handles both
/// `FROM <table>` and bare `<table>` syntax).
pub fn extract_table_id_from_delete(
    delete: &sqlparser::ast::Delete,
    default_namespace: &str,
) -> Option<TableId> {
    let table = match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables)
        | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
            if tables.len() != 1 {
                return None;
            }
            tables.first()?
        },
    };
    extract_table_id_from_table_with_joins(table, default_namespace)
}
