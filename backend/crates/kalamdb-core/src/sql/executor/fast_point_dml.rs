use crate::app_context::AppContext;
use crate::error::KalamDbError;
use crate::schema_registry::SchemaRegistry;
use crate::sql::{ExecutionContext, ExecutionResult};
use kalamdb_commons::models::rows::row::Row;
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::TableAccess;
use kalamdb_commons::TableId;
use kalamdb_session::{
    check_shared_table_write_access_level, check_user_table_write_access_level,
    shared_table_access_level,
};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Expr, ObjectNamePart, Statement,
};
use std::collections::BTreeMap;
use std::sync::Arc;

use super::helpers::ast_parsing::{self, expr_to_scalar, strip_nested_expr};

fn log_fast_path_skip(op: &str, reason: &str) {
    tracing::debug!(operation = op, reason, "sql.fast_path_skip");
}

pub async fn try_fast_update(
    statement: &Statement,
    app_context: &AppContext,
    exec_ctx: &ExecutionContext,
    schema_registry: &Arc<SchemaRegistry>,
    prepared_table_id: Option<&TableId>,
    prepared_table_type: Option<TableType>,
) -> Result<Option<ExecutionResult>, KalamDbError> {
    let update = match statement {
        Statement::Update(update) => update,
        _ => return Ok(None),
    };

    if update.from.is_some()
        || update.returning.is_some()
        || update.or.is_some()
        || update.limit.is_some()
    {
        log_fast_path_skip("update", "unsupported_update_clauses");
        return Ok(None);
    }

    let table_id = match prepared_table_id {
        Some(id) => id.clone(),
        None => match ast_parsing::extract_table_id_from_table_with_joins(
            &update.table,
            exec_ctx.default_namespace().as_str(),
        ) {
            Some(id) => id,
            None => {
                log_fast_path_skip("update", "unsupported_table_reference");
                return Ok(None);
            },
        },
    };

    let table_def = match schema_registry.get_table_if_exists(&table_id)? {
        Some(def) => def,
        None => return Ok(None),
    };

    let table_type = prepared_table_type.unwrap_or(table_def.table_type);
    if !matches!(table_type, TableType::User | TableType::Shared)
        || table_id.namespace_id().is_system_namespace()
    {
        log_fast_path_skip("update", "unsupported_table_type");
        return Ok(None);
    }

    check_fast_path_write_access(
        exec_ctx,
        &table_id,
        table_type,
        shared_table_access_level(&table_def),
    )?;

    let pk_columns = table_def.get_primary_key_columns();
    if pk_columns.len() != 1 {
        log_fast_path_skip("update", "non_single_primary_key");
        return Ok(None);
    }
    let pk_column = pk_columns[0];
    let pk_value = match extract_pk_literal(update.selection.as_ref(), pk_column) {
        Some(value) => value,
        None => {
            log_fast_path_skip("update", "where_not_point_pk_equality");
            return Ok(None);
        },
    };
    let updates = match assignments_to_row(&update.assignments, pk_column) {
        Ok(row) => row,
        Err(_) => {
            log_fast_path_skip("update", "unsupported_assignments");
            return Ok(None);
        },
    };

    tracing::debug!(
        table_id = %table_id,
        pk = %pk_value,
        update_columns = updates.values.len(),
        table_type = %table_type,
        "sql.fast_update"
    );

    tracing::debug!(table_id = %table_id, pk = %pk_value, table_type = %table_type, "sql.fast_update_applier");

    let rows_affected = match table_type {
        TableType::User | TableType::Stream => app_context
            .applier()
            .update_user_data(
                table_id.clone(),
                exec_ctx.user_id().clone(),
                vec![updates],
                Some(pk_value.clone()),
            )
            .await?
            .rows_affected(),
        TableType::Shared => app_context
            .applier()
            .update_shared_data(table_id.clone(), vec![updates], Some(pk_value.clone()))
            .await?
            .rows_affected(),
        TableType::System => return Ok(None),
    };

    Ok(Some(ExecutionResult::Updated {
        rows_affected,
    }))
}

pub async fn try_fast_delete(
    statement: &Statement,
    app_context: &AppContext,
    exec_ctx: &ExecutionContext,
    schema_registry: &Arc<SchemaRegistry>,
    prepared_table_id: Option<&TableId>,
    prepared_table_type: Option<TableType>,
) -> Result<Option<ExecutionResult>, KalamDbError> {
    let delete = match statement {
        Statement::Delete(delete) => delete,
        _ => return Ok(None),
    };

    if delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
        || !delete.tables.is_empty()
    {
        log_fast_path_skip("delete", "unsupported_delete_clauses");
        return Ok(None);
    }

    let table_id = match prepared_table_id {
        Some(id) => id.clone(),
        None => {
            match ast_parsing::extract_table_id_from_delete(
                delete,
                exec_ctx.default_namespace().as_str(),
            ) {
                Some(id) => id,
                None => {
                    log_fast_path_skip("delete", "unsupported_table_reference");
                    return Ok(None);
                },
            }
        },
    };

    let table_def = match schema_registry.get_table_if_exists(&table_id)? {
        Some(def) => def,
        None => return Ok(None),
    };

    let table_type = prepared_table_type.unwrap_or(table_def.table_type);
    if !matches!(table_type, TableType::User | TableType::Shared | TableType::Stream)
        || table_id.namespace_id().is_system_namespace()
    {
        log_fast_path_skip("delete", "unsupported_table_type");
        return Ok(None);
    }

    check_fast_path_write_access(
        exec_ctx,
        &table_id,
        table_type,
        shared_table_access_level(&table_def),
    )?;

    let pk_columns = table_def.get_primary_key_columns();
    if pk_columns.len() != 1 {
        log_fast_path_skip("delete", "non_single_primary_key");
        return Ok(None);
    }
    let pk_column = pk_columns[0];
    let pk_value = match extract_pk_literal(delete.selection.as_ref(), pk_column) {
        Some(value) => value,
        None => {
            log_fast_path_skip("delete", "where_not_point_pk_equality");
            return Ok(None);
        },
    };

    tracing::debug!(table_id = %table_id, pk = %pk_value, table_type = %table_type, "sql.fast_delete");

    tracing::debug!(table_id = %table_id, pk = %pk_value, table_type = %table_type, "sql.fast_delete_applier");

    let rows_affected = match table_type {
        TableType::User | TableType::Stream => app_context
            .applier()
            .delete_user_data(
                table_id.clone(),
                exec_ctx.user_id().clone(),
                Some(vec![pk_value.clone()]),
            )
            .await?
            .rows_affected(),
        TableType::Shared => app_context
            .applier()
            .delete_shared_data(table_id.clone(), Some(vec![pk_value.clone()]))
            .await?
            .rows_affected(),
        TableType::System => return Ok(None),
    };

    Ok(Some(ExecutionResult::Deleted {
        rows_affected,
    }))
}

fn check_fast_path_write_access(
    exec_ctx: &ExecutionContext,
    table_id: &TableId,
    table_type: TableType,
    shared_access_level: TableAccess,
) -> Result<(), KalamDbError> {
    match table_type {
        TableType::User => check_user_table_write_access_level(
            exec_ctx.user_role(),
            table_id.namespace_id(),
            table_id.table_name(),
        )
        .map_err(|e| KalamDbError::PermissionDenied(e.to_string())),
        TableType::Shared => check_shared_table_write_access_level(
            exec_ctx.user_role(),
            shared_access_level,
            table_id.namespace_id(),
            table_id.table_name(),
        )
        .map_err(|e| KalamDbError::PermissionDenied(e.to_string())),
        _ => Ok(()),
    }
}

fn extract_pk_literal(selection: Option<&Expr>, pk_column: &str) -> Option<String> {
    let expr = match selection {
        Some(expr) => strip_nested_expr(expr),
        None => return None,
    };

    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    if !matches!(op, BinaryOperator::Eq) {
        return None;
    }

    if expr_column_name(strip_nested_expr(left.as_ref())).as_deref() == Some(pk_column) {
        return Some(expr_to_scalar(strip_nested_expr(right.as_ref())).ok()?.to_string());
    }
    if expr_column_name(strip_nested_expr(right.as_ref())).as_deref() == Some(pk_column) {
        return Some(expr_to_scalar(strip_nested_expr(left.as_ref())).ok()?.to_string());
    }

    None
}

fn expr_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn assignments_to_row(assignments: &[Assignment], pk_column: &str) -> Result<Row, &'static str> {
    if assignments.is_empty() {
        return Err("empty assignment list");
    }

    let mut values = BTreeMap::new();
    for assignment in assignments {
        let column = match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name_last_ident(name)?,
            AssignmentTarget::Tuple(_) => return Err("tuple assignment unsupported"),
        };
        if column == pk_column {
            return Err("primary key updates unsupported in fast path");
        }

        values.insert(column, expr_to_scalar(strip_nested_expr(&assignment.value))?);
    }

    Ok(Row::new(values))
}

fn object_name_last_ident(name: &sqlparser::ast::ObjectName) -> Result<String, &'static str> {
    name.0
        .iter()
        .rev()
        .find_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .ok_or("unsupported identifier")
}

#[cfg(test)]
mod tests {
    use super::{assignments_to_row, extract_pk_literal};
    use sqlparser::ast::Statement;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn extracts_simple_update_pk_literal() {
        let statement = parse("UPDATE users SET name = 'jamal' WHERE id = 7");
        let Statement::Update(update) = statement else {
            panic!("expected update statement");
        };

        assert_eq!(extract_pk_literal(update.selection.as_ref(), "id"), Some("7".to_string()));
        let row = assignments_to_row(&update.assignments, "id").unwrap();
        assert_eq!(row.values.len(), 1);
        assert!(row.values.contains_key("name"));
    }

    #[test]
    fn extracts_simple_delete_pk_literal() {
        let statement = parse("DELETE FROM users WHERE id = 'abc'");
        let Statement::Delete(delete) = statement else {
            panic!("expected delete statement");
        };

        assert_eq!(extract_pk_literal(delete.selection.as_ref(), "id"), Some("abc".to_string()));
    }
}
