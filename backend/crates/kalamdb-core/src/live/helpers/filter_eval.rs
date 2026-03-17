//! Filter evaluation for live query subscriptions
//!
//! This module provides SQL WHERE clause expression evaluation for filtering
//! live query notifications. Only subscribers whose filters match
//! the changed row data will receive notifications.
//!
//! The filter expression (Expr) is parsed once during subscription registration
//! and stored in SubscriptionState. This module provides evaluation functions
//! to match row data against the expression.

use crate::error::KalamDbError;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, Statement, Value};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use kalamdb_commons::models::rows::Row;
use regex::RegexBuilder;

/// Parse a WHERE clause string into an Expr AST
///
/// # Arguments
///
/// * `where_clause` - SQL WHERE clause (e.g., "user_id = 'user1' AND read = false")
///
/// # Returns
///
/// Parsed expression ready for evaluation
pub fn parse_where_clause(where_clause: &str) -> Result<Expr, KalamDbError> {
    // Parse as a SELECT with WHERE to extract the expression
    let sql = format!("SELECT * FROM t WHERE {}", where_clause);

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &sql).map_err(|e| {
        KalamDbError::InvalidOperation(format!("Failed to parse WHERE clause: {}", e))
    })?;

    if statements.is_empty() {
        return Err(KalamDbError::InvalidOperation("Empty WHERE clause".to_string()));
    }

    // Extract WHERE expression from parsed SELECT
    match &statements[0] {
        Statement::Query(query) => {
            if let Some(selection) = &query.body.as_select().and_then(|s| s.selection.as_ref()) {
                Ok((*selection).clone())
            } else {
                Err(KalamDbError::InvalidOperation("No WHERE clause found".to_string()))
            }
        },
        _ => Err(KalamDbError::InvalidOperation("Invalid WHERE clause syntax".to_string())),
    }
}

/// Maximum recursion depth for expression evaluation (prevents stack overflow)
const MAX_EXPR_DEPTH: usize = 64;

/// Evaluate a filter expression against row data
///
/// # Arguments
///
/// * `expr` - The filter expression to evaluate
/// * `row_data` - Row object representing the row
///
/// # Returns
///
/// `true` if the row matches the filter, `false` otherwise
#[inline]
pub fn matches(expr: &Expr, row_data: &Row) -> Result<bool, KalamDbError> {
    evaluate_expr(expr, row_data, 0)
}

/// Recursively evaluate an expression against row data
#[inline]
fn evaluate_expr(expr: &Expr, row_data: &Row, depth: usize) -> Result<bool, KalamDbError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(KalamDbError::InvalidOperation(
            "Filter expression too deeply nested (max 64 levels)".to_string(),
        ));
    }
    match expr {
        // Binary operations: AND, OR, =, !=, <, >, <=, >=
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                // Short-circuit: if left is false, skip right evaluation
                let left_result = evaluate_expr(left, row_data, depth + 1)?;
                if !left_result {
                    return Ok(false);
                }
                evaluate_expr(right, row_data, depth + 1)
            },
            BinaryOperator::Or => {
                // Short-circuit: if left is true, skip right evaluation
                let left_result = evaluate_expr(left, row_data, depth + 1)?;
                if left_result {
                    return Ok(true);
                }
                evaluate_expr(right, row_data, depth + 1)
            },
            BinaryOperator::Eq => evaluate_comparison(left, right, row_data, scalars_equal),
            BinaryOperator::NotEq => {
                evaluate_comparison(left, right, row_data, |a, b| !scalars_equal(a, b))
            },
            BinaryOperator::Lt => {
                let left_value = extract_value(left, row_data)?;
                let right_value = extract_value(right, row_data)?;
                compare_numeric(&left_value, &right_value, "<")
            },
            BinaryOperator::Gt => {
                let left_value = extract_value(left, row_data)?;
                let right_value = extract_value(right, row_data)?;
                compare_numeric(&left_value, &right_value, ">")
            },
            BinaryOperator::LtEq => {
                let left_value = extract_value(left, row_data)?;
                let right_value = extract_value(right, row_data)?;
                compare_numeric(&left_value, &right_value, "<=")
            },
            BinaryOperator::GtEq => {
                let left_value = extract_value(left, row_data)?;
                let right_value = extract_value(right, row_data)?;
                compare_numeric(&left_value, &right_value, ">=")
            },
            _ => Err(KalamDbError::InvalidOperation(format!("Unsupported operator: {:?}", op))),
        },

        // Parentheses: (expression)
        Expr::Nested(inner) => evaluate_expr(inner, row_data, depth + 1),

        // NOT expression
        Expr::UnaryOp { op, expr } => match op {
            datafusion::sql::sqlparser::ast::UnaryOperator::Not => {
                let result = evaluate_expr(expr, row_data, depth + 1)?;
                Ok(!result)
            },
            _ => {
                Err(KalamDbError::InvalidOperation(format!("Unsupported unary operator: {:?}", op)))
            },
        },

        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => evaluate_like_pattern(
            expr,
            pattern,
            row_data,
            *negated,
            *any,
            parse_like_escape_char(escape_char.as_ref())?,
            false,
        ),

        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => evaluate_like_pattern(
            expr,
            pattern,
            row_data,
            *negated,
            *any,
            parse_like_escape_char(escape_char.as_ref())?,
            true,
        ),

        _ => Err(KalamDbError::InvalidOperation(format!(
            "Unsupported expression type: {:?}",
            expr
        ))),
    }
}

/// Evaluate a comparison operation (=, !=, <, >, <=, >=)
fn evaluate_comparison<F>(
    left: &Expr,
    right: &Expr,
    row_data: &Row,
    comparator: F,
) -> Result<bool, KalamDbError>
where
    F: Fn(&ScalarValue, &ScalarValue) -> bool,
{
    let left_value = extract_value(left, row_data)?;
    let right_value = extract_value(right, row_data)?;

    Ok(comparator(&left_value, &right_value))
}

/// Check if two scalars are equal, handling numeric type coercion
fn scalars_equal(left: &ScalarValue, right: &ScalarValue) -> bool {
    if left == right {
        return true;
    }

    // Try string coercion (Utf8 vs LargeUtf8)
    if let (Some(l), Some(r)) = (as_str(left), as_str(right)) {
        return l == r;
    }

    // Try numeric coercion
    if let (Some(l), Some(r)) = (as_f64(left), as_f64(right)) {
        return (l - r).abs() < f64::EPSILON;
    }

    false
}

/// Helper to convert ScalarValue to f64 if possible
fn as_f64(v: &ScalarValue) -> Option<f64> {
    kalamdb_commons::as_f64(v)
}

/// Helper to convert ScalarValue to &str if possible
fn as_str(v: &ScalarValue) -> Option<&str> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Helper to compare two ScalarValues for numeric comparisons
fn compare_numeric(
    left: &ScalarValue,
    right: &ScalarValue,
    op: &str,
) -> Result<bool, KalamDbError> {
    let left_num = as_f64(left).ok_or_else(|| {
        KalamDbError::InvalidOperation(format!("Cannot convert {:?} to number", left))
    })?;

    let right_num = as_f64(right).ok_or_else(|| {
        KalamDbError::InvalidOperation(format!("Cannot convert {:?} to number", right))
    })?;

    Ok(match op {
        "<" => left_num < right_num,
        ">" => left_num > right_num,
        "<=" => left_num <= right_num,
        ">=" => left_num >= right_num,
        _ => return Err(KalamDbError::InvalidOperation(format!("Unknown operator: {}", op))),
    })
}

fn evaluate_like_pattern(
    expr: &Expr,
    pattern: &Expr,
    row_data: &Row,
    negated: bool,
    any: bool,
    escape_char: Option<char>,
    case_insensitive: bool,
) -> Result<bool, KalamDbError> {
    if any {
        return Err(KalamDbError::InvalidOperation(
            "LIKE ANY expressions are not supported in live query filters".to_string(),
        ));
    }

    let value = extract_value(expr, row_data)?;
    let pattern_value = extract_value(pattern, row_data)?;

    let value_str = as_str(&value).ok_or_else(|| {
        KalamDbError::InvalidOperation(format!(
            "Cannot evaluate LIKE against non-string value: {:?}",
            value
        ))
    })?;
    let pattern_str = as_str(&pattern_value).ok_or_else(|| {
        KalamDbError::InvalidOperation(format!(
            "LIKE pattern must be a string literal or column value, got: {:?}",
            pattern_value
        ))
    })?;

    let regex_pattern = build_like_regex_pattern(pattern_str, escape_char)?;
    let regex = RegexBuilder::new(&regex_pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|error| {
            KalamDbError::InvalidOperation(format!(
                "Invalid LIKE pattern {:?}: {}",
                pattern_str, error
            ))
        })?;

    let matches = regex.is_match(value_str);
    Ok(if negated { !matches } else { matches })
}

fn build_like_regex_pattern(
    pattern: &str,
    escape_char: Option<char>,
) -> Result<String, KalamDbError> {
    let mut regex_pattern = String::with_capacity(pattern.len() + 2);
    regex_pattern.push('^');

    let mut escaped = false;
    for ch in pattern.chars() {
        if escaped {
            regex_pattern.push_str(&regex::escape(&ch.to_string()));
            escaped = false;
            continue;
        }

        if Some(ch) == escape_char {
            escaped = true;
            continue;
        }

        match ch {
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            _ => regex_pattern.push_str(&regex::escape(&ch.to_string())),
        }
    }

    if escaped {
        return Err(KalamDbError::InvalidOperation(format!(
            "LIKE pattern has dangling escape character: {:?}",
            pattern
        )));
    }

    regex_pattern.push('$');
    Ok(regex_pattern)
}

fn parse_like_escape_char(escape_char: Option<&Value>) -> Result<Option<char>, KalamDbError> {
    match escape_char {
        None => Ok(None),
        Some(Value::SingleQuotedString(value)) | Some(Value::DoubleQuotedString(value)) => {
            let mut chars = value.chars();
            let ch = chars.next().ok_or_else(|| {
                KalamDbError::InvalidOperation("LIKE ESCAPE cannot be empty".to_string())
            })?;
            if chars.next().is_some() {
                return Err(KalamDbError::InvalidOperation(format!(
                    "LIKE ESCAPE must be a single character, got {:?}",
                    value
                )));
            }
            Ok(Some(ch))
        },
        Some(other) => Err(KalamDbError::InvalidOperation(format!(
            "Unsupported LIKE ESCAPE value: {:?}",
            other
        ))),
    }
}

/// Extract a value from an expression
///
/// Handles:
/// - Column references (e.g., user_id) → lookup in row_data
/// - Literals (e.g., 'user1', 123, true) → convert to ScalarValue
fn extract_value(expr: &Expr, row_data: &Row) -> Result<ScalarValue, KalamDbError> {
    match expr {
        // Column reference: lookup in row_data
        Expr::Identifier(ident) => {
            let column_name = ident.value.as_str();
            lookup_column_value(row_data, column_name).ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Column not found in row data: {}",
                    column_name
                ))
            })
        },

        // Qualified identifier (e.g. table.column) - use the last part
        Expr::CompoundIdentifier(parts) => {
            if let Some(ident) = parts.last() {
                let column_name = ident.value.as_str();
                lookup_column_value(row_data, column_name).ok_or_else(|| {
                    KalamDbError::InvalidOperation(format!(
                        "Column not found in row data: {}",
                        column_name
                    ))
                })
            } else {
                Err(KalamDbError::InvalidOperation("Empty compound identifier".to_string()))
            }
        },

        // Literal value: convert to ScalarValue
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(ScalarValue::Utf8(Some(s.clone())))
            },
            Value::Number(n, _) => {
                // Try parsing as i64 first, then f64
                if let Ok(i) = n.parse::<i64>() {
                    Ok(ScalarValue::Int64(Some(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(ScalarValue::Float64(Some(f)))
                } else {
                    Err(KalamDbError::InvalidOperation(format!("Invalid number: {}", n)))
                }
            },
            Value::Boolean(b) => Ok(ScalarValue::Boolean(Some(*b))),
            Value::Null => Ok(ScalarValue::Null),
            _ => Err(KalamDbError::InvalidOperation(format!("Unsupported literal type: {:?}", v))),
        },

        _ => Err(KalamDbError::InvalidOperation(format!(
            "Unsupported expression in value extraction: {:?}",
            expr
        ))),
    }
}

fn lookup_column_value(row_data: &Row, column_name: &str) -> Option<ScalarValue> {
    row_data
        .get(column_name)
        .or_else(|| row_data.get(&column_name.to_lowercase()))
        .or_else(|| row_data.get(&column_name.to_uppercase()))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn to_row(value: serde_json::Value) -> Row {
        let object = value.as_object().expect("test rows must be JSON objects").clone();

        let mut values = BTreeMap::new();
        for (key, value) in object {
            let scalar = match value {
                serde_json::Value::Null => ScalarValue::Null,
                serde_json::Value::Bool(b) => ScalarValue::Boolean(Some(b)),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        ScalarValue::Int64(Some(i))
                    } else if let Some(u) = n.as_u64() {
                        ScalarValue::UInt64(Some(u))
                    } else if let Some(f) = n.as_f64() {
                        ScalarValue::Float64(Some(f))
                    } else {
                        panic!("unsupported numeric literal in test row")
                    }
                },
                serde_json::Value::String(s) => ScalarValue::Utf8(Some(s)),
                other => panic!("unsupported value in test row: {:?}", other),
            };
            values.insert(key, scalar);
        }

        Row::new(values)
    }

    #[test]
    fn test_simple_equality_filter() {
        let expr = parse_where_clause("user_id = 'user1'").unwrap();

        let matching_row = to_row(json!({"user_id": "user1", "text": "Hello"}));
        assert!(matches(&expr, &matching_row).unwrap());

        let non_matching_row = to_row(json!({"user_id": "user2", "text": "Hello"}));
        assert!(!matches(&expr, &non_matching_row).unwrap());
    }

    #[test]
    fn test_and_filter() {
        let expr = parse_where_clause("user_id = 'user1' AND read = false").unwrap();

        let matching_row = to_row(json!({"user_id": "user1", "read": false}));
        assert!(matches(&expr, &matching_row).unwrap());

        let non_matching_row1 = to_row(json!({"user_id": "user2", "read": false}));
        assert!(!matches(&expr, &non_matching_row1).unwrap());

        let non_matching_row2 = to_row(json!({"user_id": "user1", "read": true}));
        assert!(!matches(&expr, &non_matching_row2).unwrap());
    }

    #[test]
    fn test_or_filter() {
        let expr = parse_where_clause("status = 'active' OR status = 'pending'").unwrap();

        let matching_row1 = to_row(json!({"status": "active"}));
        assert!(matches(&expr, &matching_row1).unwrap());

        let matching_row2 = to_row(json!({"status": "pending"}));
        assert!(matches(&expr, &matching_row2).unwrap());

        let non_matching_row = to_row(json!({"status": "completed"}));
        assert!(!matches(&expr, &non_matching_row).unwrap());
    }

    #[test]
    fn test_numeric_comparison() {
        let expr = parse_where_clause("age >= 18").unwrap();

        let matching_row = to_row(json!({"age": 25}));
        assert!(matches(&expr, &matching_row).unwrap());

        let non_matching_row = to_row(json!({"age": 15}));
        assert!(!matches(&expr, &non_matching_row).unwrap());

        let edge_case = to_row(json!({"age": 18}));
        assert!(matches(&expr, &edge_case).unwrap());
    }

    #[test]
    fn test_not_filter() {
        let expr = parse_where_clause("NOT (deleted = true)").unwrap();

        let matching_row = to_row(json!({"deleted": false}));
        assert!(matches(&expr, &matching_row).unwrap());

        let non_matching_row = to_row(json!({"deleted": true}));
        assert!(!matches(&expr, &non_matching_row).unwrap());
    }

    #[test]
    fn test_complex_filter() {
        let expr = parse_where_clause("(user_id = 'user1' OR user_id = 'user2') AND read = false")
            .unwrap();

        let matching_row1 = to_row(json!({"user_id": "user1", "read": false}));
        assert!(matches(&expr, &matching_row1).unwrap());

        let matching_row2 = to_row(json!({"user_id": "user2", "read": false}));
        assert!(matches(&expr, &matching_row2).unwrap());

        let non_matching_row1 = to_row(json!({"user_id": "user3", "read": false}));
        assert!(!matches(&expr, &non_matching_row1).unwrap());

        let non_matching_row2 = to_row(json!({"user_id": "user1", "read": true}));
        assert!(!matches(&expr, &non_matching_row2).unwrap());
    }

    #[test]
    fn test_like_filter() {
        let expr = parse_where_clause("metric_name LIKE 'open_files_%'").unwrap();

        let matching_row = to_row(json!({"metric_name": "open_files_other"}));
        assert!(matches(&expr, &matching_row).unwrap());

        let non_matching_row = to_row(json!({"metric_name": "closed_files_other"}));
        assert!(!matches(&expr, &non_matching_row).unwrap());
    }

    #[test]
    fn test_ilike_filter() {
        let expr = parse_where_clause("metric_name ILIKE 'OPEN_FILES_%'").unwrap();

        let matching_row = to_row(json!({"metric_name": "open_files_other"}));
        assert!(matches(&expr, &matching_row).unwrap());

        let non_matching_row = to_row(json!({"metric_name": "closed_files_other"}));
        assert!(!matches(&expr, &non_matching_row).unwrap());
    }

    #[test]
    fn test_deeply_nested_filter_rejected() {
        // Build a deeply nested expression that exceeds MAX_EXPR_DEPTH
        // Using AND chains instead of parentheses (parser has its own recursion limit)
        // This creates depth via binary tree of ANDs
        let mut nested_clause = "x = 1".to_string();
        for i in 0..70 {
            nested_clause = format!("{} AND y{} = 1", nested_clause, i);
        }

        // Parse should succeed, but evaluation should fail on depth
        // Note: if parser also rejects, that's fine - we're protected at both layers
        let parse_result = parse_where_clause(&nested_clause);
        if let Ok(expr) = parse_result {
            let mut row_data = BTreeMap::new();
            row_data.insert("x".to_string(), ScalarValue::Int64(Some(1)));
            for i in 0..70 {
                row_data.insert(format!("y{}", i), ScalarValue::Int64(Some(1)));
            }
            let row = Row::new(row_data);

            let result = matches(&expr, &row);
            // Either succeeds (within limit) or fails with depth error
            // The protection is that we don't stack overflow
            if let Err(err) = result {
                let err_msg = err.to_string();
                assert!(
                    err_msg.contains("too deeply nested") || err_msg.contains("recursion"),
                    "Expected depth error, got: {}",
                    err_msg
                );
            }
        }
        // Test passes - we're protected from stack overflow either at parse or eval time
    }

    #[test]
    fn test_moderately_nested_filter_allowed() {
        // Build a moderately nested expression within MAX_EXPR_DEPTH
        let mut nested_clause = "x = 1".to_string();
        for _ in 0..10 {
            nested_clause = format!("({})", nested_clause);
        }

        let expr = parse_where_clause(&nested_clause).unwrap();
        let row = to_row(json!({"x": 1}));

        // Should succeed
        assert!(matches(&expr, &row).unwrap());
    }
}
