use crate::delete_input::DeleteInput;
use crate::delete_plan::DeletePlan;
use crate::insert_input::InsertInput;
use crate::insert_plan::InsertPlan;
use crate::scan_input::ScanInput;
use crate::scan_plan::ScanPlan;
use crate::update_input::UpdateInput;
use crate::update_plan::UpdatePlan;
use crate::virtual_column::VirtualColumn;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use kalam_pg_api::{DeleteRequest, InsertRequest, ScanRequest, TenantContext, UpdateRequest};
use kalam_pg_common::{KalamPgError, DELETED_COLUMN, SEQ_COLUMN, USER_ID_COLUMN};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;

/// Backend-agnostic request planner used by the FDW layer.
pub struct RequestPlanner;

impl RequestPlanner {
    /// Build a scan request and record which projected columns are FDW virtual columns.
    pub fn plan_scan(input: ScanInput) -> Result<ScanPlan, KalamPgError> {
        let (explicit_user_id, physical_filters) = extract_user_id_from_filters(input.filters)?;
        let tenant_context = TenantContext::new(explicit_user_id, input.session_user_id);
        let virtual_columns = collect_virtual_columns(&input.projected_columns);
        let physical_projection = collect_physical_projection(&input.projected_columns);
        let request = ScanRequest {
            table_id: input.table_id,
            table_type: input.table_type,
            tenant_context,
            remote_session: None,
            projection: physical_projection,
            filters: physical_filters,
            limit: input.limit,
        };

        request.validate()?;

        Ok(ScanPlan {
            request,
            virtual_columns,
        })
    }

    /// Build an insert request and strip `_userid` from each row payload.
    pub fn plan_insert(input: InsertInput) -> Result<InsertPlan, KalamPgError> {
        let (explicit_user_id, rows) = extract_user_id_from_rows(input.rows)?;
        let request = InsertRequest::new(
            input.table_id,
            input.table_type,
            TenantContext::new(explicit_user_id, input.session_user_id),
            rows,
        );

        request.validate()?;

        Ok(InsertPlan { request })
    }

    /// Build an update request and strip `_userid` from the update payload.
    pub fn plan_update(input: UpdateInput) -> Result<UpdatePlan, KalamPgError> {
        let (explicit_user_id, updates) = extract_user_id_from_row(input.updates)?;
        let request = UpdateRequest::new(
            input.table_id,
            input.table_type,
            TenantContext::new(explicit_user_id, input.session_user_id),
            input.pk_value,
            updates,
        );

        request.validate()?;

        Ok(UpdatePlan { request })
    }

    /// Build a delete request from typed FDW input.
    pub fn plan_delete(input: DeleteInput) -> Result<DeletePlan, KalamPgError> {
        let request = DeleteRequest::new(
            input.table_id,
            input.table_type,
            TenantContext::new(input.explicit_user_id, input.session_user_id),
            input.pk_value,
        );

        request.validate()?;

        Ok(DeletePlan { request })
    }
}

fn collect_virtual_columns(projected_columns: &[String]) -> Vec<VirtualColumn> {
    let mut virtual_columns = Vec::new();

    for column_name in projected_columns {
        match column_name.as_str() {
            USER_ID_COLUMN => virtual_columns.push(VirtualColumn::UserId),
            SEQ_COLUMN => virtual_columns.push(VirtualColumn::Seq),
            DELETED_COLUMN => virtual_columns.push(VirtualColumn::Deleted),
            _ => {},
        }
    }

    virtual_columns
}

fn collect_physical_projection(projected_columns: &[String]) -> Option<Vec<String>> {
    let columns: Vec<String> = projected_columns
        .iter()
        .filter(|column_name| column_name.as_str() != USER_ID_COLUMN)
        .cloned()
        .collect();

    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

fn extract_user_id_from_filters(
    filters: Vec<Expr>,
) -> Result<(Option<UserId>, Vec<Expr>), KalamPgError> {
    let mut explicit_user_id = None;
    let mut physical_filters = Vec::new();

    for filter in filters {
        if let Some(user_id) = try_extract_user_id_filter(&filter)? {
            merge_user_id(&mut explicit_user_id, user_id, "scan filters")?;
        } else {
            physical_filters.push(filter);
        }
    }

    Ok((explicit_user_id, physical_filters))
}

fn extract_user_id_from_rows(rows: Vec<Row>) -> Result<(Option<UserId>, Vec<Row>), KalamPgError> {
    let mut explicit_user_id = None;
    let mut stripped_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let (row_user_id, stripped_row) = extract_user_id_from_row(row)?;
        if let Some(user_id) = row_user_id {
            merge_user_id(&mut explicit_user_id, user_id, "insert rows")?;
        }
        stripped_rows.push(stripped_row);
    }

    Ok((explicit_user_id, stripped_rows))
}

fn extract_user_id_from_row(row: Row) -> Result<(Option<UserId>, Row), KalamPgError> {
    let mut values = row.values;
    let user_id = values.remove(USER_ID_COLUMN).map(extract_user_id_from_scalar).transpose()?;

    Ok((user_id, Row::new(values)))
}

fn try_extract_user_id_filter(filter: &Expr) -> Result<Option<UserId>, KalamPgError> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else {
        return Ok(None);
    };

    if *op != Operator::Eq {
        return Ok(None);
    }

    if let (Some(column_name), Some(user_id)) =
        (extract_column_name(left), extract_user_id_literal(right)?)
    {
        if column_name == USER_ID_COLUMN {
            return Ok(Some(user_id));
        }
    }

    if let (Some(column_name), Some(user_id)) =
        (extract_column_name(right), extract_user_id_literal(left)?)
    {
        if column_name == USER_ID_COLUMN {
            return Ok(Some(user_id));
        }
    }

    Ok(None)
}

fn extract_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        _ => None,
    }
}

fn extract_user_id_literal(expr: &Expr) -> Result<Option<UserId>, KalamPgError> {
    let Expr::Literal(value, _) = expr else {
        return Ok(None);
    };

    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Ok(Some(UserId::new(value.clone())))
        },
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => Ok(None),
        _ => Err(KalamPgError::Validation(format!(
            "_userid filters must compare against string literals, got {}",
            value
        ))),
    }
}

fn extract_user_id_from_scalar(value: ScalarValue) -> Result<UserId, KalamPgError> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            Ok(UserId::new(value))
        },
        other => Err(KalamPgError::Validation(format!(
            "_userid values must be strings, got {}",
            other
        ))),
    }
}

fn merge_user_id(
    current: &mut Option<UserId>,
    next: UserId,
    source: &str,
) -> Result<(), KalamPgError> {
    match current {
        Some(existing) if existing != &next => Err(KalamPgError::Validation(format!(
            "conflicting _userid values detected in {}: '{}' vs '{}'",
            source,
            existing.as_str(),
            next.as_str()
        ))),
        Some(_) => Ok(()),
        None => {
            *current = Some(next);
            Ok(())
        },
    }
}
