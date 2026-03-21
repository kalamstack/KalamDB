use crate::EmbeddedExtensionState;
use datafusion::scalar::ScalarValue;
use kalam_pg_api::MutationResponse;
use kalam_pg_common::{KalamPgError, USER_ID_COLUMN};
use kalam_pg_fdw::{
    DeleteInput, InsertInput, RequestPlanner, ScanInput, UpdateInput, VirtualColumn,
};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use kalamdb_core::app_context::AppContext;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Embedded FDW service that turns typed FDW inputs into backend calls.
pub struct EmbeddedFdwService {
    extension_state: EmbeddedExtensionState,
}

impl EmbeddedFdwService {
    /// Create the service from a shared embedded [`AppContext`].
    pub fn new(app_context: Arc<AppContext>) -> Result<Self, KalamPgError> {
        Ok(Self {
            extension_state: EmbeddedExtensionState::new(app_context)?,
        })
    }

    /// Execute a scan and materialize Arrow batches into Kalam rows for the extension layer.
    pub async fn scan(&self, input: ScanInput) -> Result<Vec<Row>, KalamPgError> {
        let scan_plan = RequestPlanner::plan_scan(input)?;
        let tenant_user_id = scan_plan.request.tenant_context.effective_user_id().cloned();
        let scan_response = self.extension_state.executor()?.scan(scan_plan.request).await?;
        let mut rows = materialize_rows(scan_response.batches)?;

        if scan_plan.virtual_columns.contains(&VirtualColumn::UserId) {
            inject_user_id_column(&mut rows, tenant_user_id);
        }

        Ok(rows)
    }

    /// Execute an insert using FDW row input.
    pub async fn insert(&self, input: InsertInput) -> Result<MutationResponse, KalamPgError> {
        let insert_plan = RequestPlanner::plan_insert(input)?;
        self.extension_state.executor()?.insert(insert_plan.request).await
    }

    /// Execute an update using FDW row input.
    pub async fn update(&self, input: UpdateInput) -> Result<MutationResponse, KalamPgError> {
        let update_plan = RequestPlanner::plan_update(input)?;
        self.extension_state.executor()?.update(update_plan.request).await
    }

    /// Execute a delete using FDW row input.
    pub async fn delete(&self, input: DeleteInput) -> Result<MutationResponse, KalamPgError> {
        let delete_plan = RequestPlanner::plan_delete(input)?;
        self.extension_state.executor()?.delete(delete_plan.request).await
    }
}

fn materialize_rows(
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
) -> Result<Vec<Row>, KalamPgError> {
    let total_rows = batches.iter().map(|batch| batch.num_rows()).sum();
    let mut rows = Vec::with_capacity(total_rows);

    for batch in batches {
        let schema = batch.schema();

        for row_index in 0..batch.num_rows() {
            let mut values = BTreeMap::new();

            for (column_index, field) in schema.fields().iter().enumerate() {
                let value =
                    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
                        .map_err(|err| KalamPgError::Execution(err.to_string()))?;
                values.insert(field.name().clone(), value);
            }

            rows.push(Row::new(values));
        }
    }

    Ok(rows)
}

fn inject_user_id_column(rows: &mut [Row], user_id: Option<UserId>) {
    let user_id_value = ScalarValue::Utf8(user_id.map(|user_id| user_id.to_string()));

    for row in rows {
        row.values.insert(USER_ID_COLUMN.to_string(), user_id_value.clone());
    }
}
