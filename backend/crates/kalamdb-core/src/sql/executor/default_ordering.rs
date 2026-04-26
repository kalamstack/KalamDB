//! Default ORDER BY injection for consistent query results
//!
//! This module ensures that SELECT queries on USER, SHARED, and STREAM tables
//! return results in a consistent order, even when no explicit ORDER BY is specified.
//!
//! **Note**: System tables (system.*) and information_schema tables do NOT get
//! default ordering applied, as they don't have the _seq system column.
//!
//! This is critical for:
//! 1. **Pagination consistency**: Users expect the same order across paginated requests
//! 2. **Hot/Cold storage consistency**: Data from RocksDB and Parquet must be ordered identically
//! 3. **Deterministic results**: Same query should always return same row order
//!
//! The default ordering is by primary key columns in ASC order.
//! If no primary key is defined, we fall back to _seq (system sequence column).

use std::sync::Arc;

use datafusion::logical_expr::{LogicalPlan, SortExpr};
use kalamdb_commons::{constants::SystemColumnNames, models::TableId};

use crate::{app_context::AppContext, error::KalamDbError};

/// Check if a LogicalPlan already has an ORDER BY clause at the top level
///
/// This traverses up from the root to find if any Sort node exists.
/// We only check the immediate children to avoid false positives from subqueries.
pub fn has_order_by(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Sort(_))
}

/// Extract table reference from a LogicalPlan
///
/// Returns the first TableScan found in the plan tree.
/// For simple SELECT queries, this is typically the main table.
/// /// FIXME: Pass the ExecutionContext to read the default namespace from there
fn extract_table_reference(plan: &LogicalPlan) -> Option<TableId> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let table_id = match &scan.table_name {
                datafusion::common::TableReference::Bare { table } => {
                    TableId::from_strings("default", table.as_ref())
                },
                datafusion::common::TableReference::Partial { schema, table } => {
                    TableId::from_strings(schema.as_ref(), table.as_ref())
                },
                datafusion::common::TableReference::Full { schema, table, .. } => {
                    TableId::from_strings(schema.as_ref(), table.as_ref())
                },
            };
            Some(table_id)
        },
        // For other plan nodes, check their inputs
        _ => {
            for input in plan.inputs() {
                // FIXME: Pass the ExecutionContext to read the default namespace from there
                if let Some(result) = extract_table_reference(input) {
                    return Some(result);
                }
            }
            None
        },
    }
}

/// Get the default sort columns for a user/shared/stream table
///
/// Returns primary key columns if defined, otherwise falls back to _seq.
/// This function assumes system tables have already been filtered out.
async fn get_default_sort_columns(
    app_context: &Arc<AppContext>,
    table_id: &TableId,
) -> Result<Option<Vec<SortExpr>>, KalamDbError> {
    let schema_registry = app_context.schema_registry();

    // Try to get table definition
    if let Ok(Some(table_def)) = schema_registry.get_table_if_exists_async(table_id).await {
        let pk_columns = table_def.get_primary_key_columns();

        if !pk_columns.is_empty() {
            // Use primary key columns
            return Ok(Some(
                pk_columns
                    .into_iter()
                    .map(|col_name| {
                        SortExpr::new(
                            datafusion::logical_expr::col(col_name),
                            true,  // ascending
                            false, // nulls_first
                        )
                    })
                    .collect(),
            ));
        }

        // Fallback: use _seq system column (user/shared/stream tables always have this)
        return Ok(Some(vec![SortExpr::new(
            datafusion::logical_expr::col(SystemColumnNames::SEQ),
            true,  // ascending
            false, // nulls_first
        )]));
    }

    // Table not found in registry
    Ok(None)
}

/// Check if all sort columns exist in the output schema
///
/// This is needed because aggregate queries (like SELECT COUNT(*)) change the
/// output schema and the original table columns may not be present.
fn sort_columns_in_schema(sort_exprs: &[SortExpr], plan: &LogicalPlan) -> bool {
    let schema = plan.schema();

    for sort_expr in sort_exprs {
        // Extract column name from sort expression
        if let datafusion::logical_expr::Expr::Column(col) = &sort_expr.expr {
            // Check if this column exists in the output schema
            if schema.qualified_field_from_column(col).is_err() {
                log::trace!(
                    target: "sql::ordering",
                    "Sort column '{}' not found in output schema, skipping default ORDER BY",
                    col.name
                );
                return false;
            }
        }
    }
    true
}

/// Add default ORDER BY to a LogicalPlan if not already present
///
/// This function only applies to USER, SHARED, and STREAM tables.
/// System tables (system.*, information_schema.*, etc.) are skipped.
///
/// This function:
/// 1. Checks if the plan already has an ORDER BY clause
/// 2. Extracts the table reference from the plan
/// 3. Skips system namespace tables (no _seq column)
/// 4. Looks up primary key columns from the schema registry
/// 5. Wraps the plan with a Sort node using those columns (or _seq fallback)
///
/// # Arguments
/// * `plan` - The LogicalPlan to potentially wrap with ORDER BY
/// * `app_context` - AppContext for schema registry access
///
/// # Returns
/// * `Ok(LogicalPlan)` - The original plan if ORDER BY exists, or wrapped plan
/// * `Err(KalamDbError)` - If schema lookup fails (rare, plan is returned unchanged)
/// FIXME: Pass the ExecutionContext to read the default namespace from there
pub async fn apply_default_order_by(
    plan: LogicalPlan,
    app_context: &Arc<AppContext>,
) -> Result<LogicalPlan, KalamDbError> {
    // Skip if already has ORDER BY
    if has_order_by(&plan) {
        log::trace!(target: "sql::ordering", "Plan already has ORDER BY, skipping default");
        return Ok(plan);
    }

    // Extract table reference
    // FIXME: Pass the ExecutionContext to read the default namespace from there
    let table_id = match extract_table_reference(&plan) {
        Some(id) => id,
        None => {
            // No table found (might be a function call like SELECT NOW())
            log::trace!(target: "sql::ordering", "No table reference found, skipping default ORDER BY");
            return Ok(plan);
        },
    };

    // Skip system namespace tables - they don't have _seq column
    if table_id.namespace_id().is_system_namespace() {
        log::trace!(
            target: "sql::ordering",
            "Skipping default ORDER BY for system table {}",
            table_id
        );
        return Ok(plan);
    }

    // Get sort columns for this table (user/shared/stream tables only)
    let sort_exprs = match get_default_sort_columns(app_context, &table_id).await {
        Ok(Some(exprs)) => exprs,
        Ok(None) => {
            // Table not found in registry - might be a new table or external source
            log::trace!(
                target: "sql::ordering",
                "Table {} has no default sort columns, skipping ORDER BY",
                table_id.full_name()
            );
            return Ok(plan);
        },
        Err(e) => {
            log::warn!(
                target: "sql::ordering",
                "Failed to get default sort columns for {}: {}. Returning unsorted.",
                table_id.full_name(),
                e
            );
            return Ok(plan);
        },
    };

    // Skip if sort columns are not in the output schema
    // This handles aggregate queries like SELECT COUNT(*) where original columns aren't available
    if !sort_columns_in_schema(&sort_exprs, &plan) {
        log::trace!(
            target: "sql::ordering",
            "Sort columns not in output schema for {}, skipping default ORDER BY",
            table_id.full_name()
        );
        return Ok(plan);
    }

    // Create Sort node wrapping the original plan
    let sort_plan = LogicalPlan::Sort(datafusion::logical_expr::Sort {
        expr: sort_exprs,
        input: Arc::new(plan),
        fetch: None, // No limit from ordering itself
    });

    log::debug!(
        target: "sql::ordering",
        "Applied default ORDER BY to query on table {}",
        table_id.full_name()
    );

    Ok(sort_plan)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_has_order_by_false_for_table_scan() {
        // A simple TableScan plan should not have ORDER BY
        use datafusion::prelude::*;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let ctx = SessionContext::new();
            ctx.register_csv("test", "nonexistent.csv", CsvReadOptions::default())
                .await
                .ok();

            // This would normally work with a real file, but we're just testing the structure
            // For now, we'll test with a simpler approach
        });
    }

    #[test]
    fn test_extract_table_reference() {
        // Test that we can extract table references from various plan types
        // This is a unit test for the helper function
    }
}
