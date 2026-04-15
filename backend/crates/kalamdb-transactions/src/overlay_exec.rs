use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion::scalar::ScalarValue;
use datafusion::{common::Result as DataFusionResult, error::DataFusionError};
use futures_util::{stream, TryStreamExt};

use kalamdb_commons::conversions::arrow_json_conversion::json_rows_to_arrow_batch;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use kalamdb_commons::TableId;

use crate::overlay::TransactionOverlay;

/// Physical execution node that merges transaction-local overlay rows with committed scan output.
#[derive(Debug, Clone)]
pub struct TransactionOverlayExec {
    input: Arc<dyn ExecutionPlan>,
    table_id: TableId,
    primary_key_column: Arc<str>,
    overlay: TransactionOverlay,
    user_scope: Option<UserId>,
    final_projection: Option<Vec<usize>>,
    fetch: Option<usize>,
    cache: Arc<PlanProperties>,
}

impl TransactionOverlayExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        table_id: TableId,
        primary_key_column: impl Into<Arc<str>>,
        overlay: TransactionOverlay,
        user_scope: Option<UserId>,
        final_projection: Option<Vec<usize>>,
        fetch: Option<usize>,
    ) -> DataFusionResult<Self> {
        let output_schema = projected_schema(&input.schema(), final_projection.as_ref())?;
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));

        Ok(Self {
            input,
            table_id,
            primary_key_column: primary_key_column.into(),
            overlay,
            user_scope,
            final_projection,
            fetch,
            cache,
        })
    }
}

impl DisplayAs for TransactionOverlayExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "TransactionOverlayExec: table={}, pk={}, fetch={:?}",
                    self.table_id, self.primary_key_column, self.fetch
                )
            },
            DisplayFormatType::TreeRender => {
                write!(f, "table={}, pk={}", self.table_id, self.primary_key_column)
            },
        }
    }
}

impl ExecutionPlan for TransactionOverlayExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let input = children.swap_remove(0);
        Ok(Arc::new(Self::try_new(
            input,
            self.table_id.clone(),
            Arc::clone(&self.primary_key_column),
            self.overlay.clone(),
            self.user_scope.clone(),
            self.final_projection.clone(),
            self.fetch,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input);
        let overlay = self.overlay.clone();
        let table_id = self.table_id.clone();
        let primary_key_column = Arc::clone(&self.primary_key_column);
        let final_projection = self.final_projection.clone();
        let input_schema = input.schema();
        let output_schema = self.schema();
        let user_scope = self.user_scope.clone();
        let fetch = self.fetch;

        let stream = stream::once(async move {
            let input_stream = input.execute(partition, context)?;
            let batches = input_stream.try_collect::<Vec<RecordBatch>>().await?;
            merge_batches_with_overlay(
                &input_schema,
                &table_id,
                primary_key_column.as_ref(),
                &overlay,
                user_scope.as_ref(),
                &batches,
                final_projection.as_ref(),
                fetch,
            )
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, stream)))
    }
}

fn projected_schema(
    input_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    match projection {
        Some(indices) => input_schema
            .project(indices)
            .map(Arc::new)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
        None => Ok(Arc::clone(input_schema)),
    }
}

fn merge_batches_with_overlay(
    input_schema: &SchemaRef,
    table_id: &TableId,
    primary_key_column: &str,
    overlay: &TransactionOverlay,
    overlay_user_scope: Option<&UserId>,
    batches: &[RecordBatch],
    final_projection: Option<&Vec<usize>>,
    fetch: Option<usize>,
) -> DataFusionResult<RecordBatch> {
    let mut rows: Vec<Option<Row>> = Vec::new();
    let mut row_index_by_pk: HashMap<String, usize> = HashMap::new();

    for batch in batches {
        for row in record_batch_to_rows(batch)? {
            let primary_key = extract_primary_key(&row, primary_key_column)?;
            if let Some(existing_index) = row_index_by_pk.get(&primary_key).copied() {
                rows[existing_index] = Some(row);
            } else {
                row_index_by_pk.insert(primary_key, rows.len());
                rows.push(Some(row));
            }
        }
    }

    let mut overlay_entries = overlay
        .table_entries(table_id)
        .map(|entries| {
            entries
                .values()
                .filter(|entry| {
                    overlay_user_scope
                        .map(|user_id| entry.user_id.as_ref() == Some(user_id))
                        .unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    overlay_entries.sort_by_key(|entry| entry.mutation_order);

    for entry in overlay_entries {
        if entry.is_deleted() {
            if let Some(existing_index) = row_index_by_pk.remove(entry.primary_key.as_str()) {
                rows[existing_index] = None;
            }
            continue;
        }

        if let Some(existing_index) = row_index_by_pk.get(entry.primary_key.as_str()).copied() {
            if let Some(existing_row) = rows[existing_index].as_mut() {
                merge_row(existing_row, &entry.payload);
            }
        } else {
            row_index_by_pk.insert(entry.primary_key.clone(), rows.len());
            rows.push(Some(entry.payload.clone()));
        }
    }

    let mut merged_rows: Vec<Row> = rows.into_iter().flatten().collect();
    if let Some(fetch) = fetch {
        merged_rows.truncate(fetch);
    }

    let full_batch = json_rows_to_arrow_batch(input_schema, merged_rows)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;

    match final_projection {
        Some(indices) => full_batch
            .project(indices)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
        None => Ok(full_batch),
    }
}

fn record_batch_to_rows(batch: &RecordBatch) -> DataFusionResult<Vec<Row>> {
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_index in 0..batch.num_rows() {
        let mut values = BTreeMap::new();
        for (column_index, field) in schema.fields().iter().enumerate() {
            let value =
                ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)?;
            values.insert(field.name().to_string(), value);
        }
        rows.push(Row::new(values));
    }

    Ok(rows)
}

fn extract_primary_key(row: &Row, primary_key_column: &str) -> DataFusionResult<String> {
    row.values
        .get(primary_key_column)
        .map(|value| value.to_string())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "transaction overlay row is missing primary key column '{}'",
                primary_key_column
            ))
        })
}

fn merge_row(base: &mut Row, overlay: &Row) {
    for (column_name, value) in &overlay.values {
        base.values.insert(column_name.clone(), value.clone());
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::overlay::TransactionOverlayEntry;
    use kalamdb_commons::models::{NamespaceId, OperationKind, TableName, TransactionId};
    use kalamdb_commons::TableType;

    fn row(values: &[(&str, ScalarValue)]) -> Row {
        let mut fields = BTreeMap::new();
        for (name, value) in values {
            fields.insert((*name).to_string(), value.clone());
        }
        Row::new(fields)
    }

    #[test]
    fn overlay_merge_replaces_updates_filters_deletes_and_appends_inserts() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let base_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("before"), Some("remove")])),
            ],
        )
        .expect("base batch");

        let transaction_id = TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
        let table_id = TableId::new(NamespaceId::new("app"), TableName::new("items"));
        let mut overlay = TransactionOverlay::new(transaction_id.clone());
        overlay.apply_entry(TransactionOverlayEntry {
            transaction_id: transaction_id.clone(),
            mutation_order: 0,
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            user_id: None,
            operation_kind: OperationKind::Update,
            primary_key: "1".to_string(),
            payload: row(&[("name", ScalarValue::Utf8(Some("after".to_string())))]),
            tombstone: false,
        });
        overlay.apply_entry(TransactionOverlayEntry {
            transaction_id: transaction_id.clone(),
            mutation_order: 1,
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            user_id: None,
            operation_kind: OperationKind::Delete,
            primary_key: "2".to_string(),
            payload: Row::new(BTreeMap::new()),
            tombstone: true,
        });
        overlay.apply_entry(TransactionOverlayEntry {
            transaction_id,
            mutation_order: 2,
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            user_id: None,
            operation_kind: OperationKind::Insert,
            primary_key: "3".to_string(),
            payload: row(&[
                ("id", ScalarValue::Int64(Some(3))),
                ("name", ScalarValue::Utf8(Some("inserted".to_string()))),
            ]),
            tombstone: false,
        });

        let merged = merge_batches_with_overlay(
            &schema,
            &table_id,
            "id",
            &overlay,
            None,
            &[base_batch],
            None,
            None,
        )
        .expect("merged batch");

        assert_eq!(merged.num_rows(), 2);
        let rows = record_batch_to_rows(&merged).expect("rows");
        assert_eq!(rows[0].values.get("id"), Some(&ScalarValue::Int64(Some(1))));
        assert_eq!(rows[0].values.get("name"), Some(&ScalarValue::Utf8(Some("after".to_string()))));
        assert_eq!(rows[1].values.get("id"), Some(&ScalarValue::Int64(Some(3))));
        assert_eq!(
            rows[1].values.get("name"),
            Some(&ScalarValue::Utf8(Some("inserted".to_string())))
        );
    }

    #[test]
    fn overlay_merge_honors_user_scope_for_same_primary_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )
        .expect("empty batch");

        let transaction_id = TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
        let table_id = TableId::new(NamespaceId::new("app"), TableName::new("items"));
        let first_user = UserId::new("user-a");
        let second_user = UserId::new("user-b");
        let mut overlay = TransactionOverlay::new(transaction_id.clone());

        overlay.apply_entry(TransactionOverlayEntry {
            transaction_id: transaction_id.clone(),
            mutation_order: 0,
            table_id: table_id.clone(),
            table_type: TableType::User,
            user_id: Some(first_user.clone()),
            operation_kind: OperationKind::Insert,
            primary_key: "1".to_string(),
            payload: row(&[
                ("id", ScalarValue::Int64(Some(1))),
                ("name", ScalarValue::Utf8(Some("alice".to_string()))),
            ]),
            tombstone: false,
        });
        overlay.apply_entry(TransactionOverlayEntry {
            transaction_id,
            mutation_order: 1,
            table_id: table_id.clone(),
            table_type: TableType::User,
            user_id: Some(second_user.clone()),
            operation_kind: OperationKind::Insert,
            primary_key: "1".to_string(),
            payload: row(&[
                ("id", ScalarValue::Int64(Some(1))),
                ("name", ScalarValue::Utf8(Some("bob".to_string()))),
            ]),
            tombstone: false,
        });

        let first_merged = merge_batches_with_overlay(
            &schema,
            &table_id,
            "id",
            &overlay,
            Some(&first_user),
            &[empty_batch.clone()],
            None,
            None,
        )
        .expect("first user merged batch");
        let second_merged = merge_batches_with_overlay(
            &schema,
            &table_id,
            "id",
            &overlay,
            Some(&second_user),
            &[empty_batch],
            None,
            None,
        )
        .expect("second user merged batch");

        let first_rows = record_batch_to_rows(&first_merged).expect("first rows");
        let second_rows = record_batch_to_rows(&second_merged).expect("second rows");
        assert_eq!(first_rows.len(), 1);
        assert_eq!(second_rows.len(), 1);
        assert_eq!(
            first_rows[0].values.get("name"),
            Some(&ScalarValue::Utf8(Some("alice".to_string())))
        );
        assert_eq!(
            second_rows[0].values.get("name"),
            Some(&ScalarValue::Utf8(Some("bob".to_string())))
        );
    }
}
