use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    common::Result as DataFusionResult,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
        PlanProperties,
    },
    scalar::ScalarValue,
};
use futures_util::TryStreamExt;
use kalamdb_commons::{
    conversions::arrow_json_conversion::json_rows_to_arrow_batch,
    models::{rows::Row, UserId},
    TableId,
};
use kalamdb_datafusion_sources::{exec::projected_schema, stream::one_shot_batch_stream};

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
        let output_schema = projected_schema(&input.schema(), final_projection.as_deref())?;
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            // Overlay rows apply to the whole relation, so this node must collapse
            // child partitions and emit a single merged output partition.
            Partitioning::UnknownPartitioning(1),
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
        vec![false]
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
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "TransactionOverlayExec only supports partition 0, got {partition}",
            )));
        }

        let input = Arc::clone(&self.input);
        let overlay = self.overlay.clone();
        let table_id = self.table_id.clone();
        let primary_key_column = Arc::clone(&self.primary_key_column);
        let final_projection = self.final_projection.clone();
        let output_schema = self.schema();
        let user_scope = self.user_scope.clone();
        let fetch = self.fetch;

        Ok(one_shot_batch_stream(output_schema, async move {
            merge_partitions_with_overlay(
                input,
                context,
                &table_id,
                primary_key_column.as_ref(),
                &overlay,
                user_scope.as_ref(),
                final_projection.as_ref(),
                fetch,
            )
            .await
        }))
    }
}

async fn merge_partitions_with_overlay(
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    table_id: &TableId,
    primary_key_column: &str,
    overlay: &TransactionOverlay,
    overlay_user_scope: Option<&UserId>,
    final_projection: Option<&Vec<usize>>,
    fetch: Option<usize>,
) -> DataFusionResult<RecordBatch> {
    let input_schema = input.schema();
    let mut rows: Vec<Option<Row>> = Vec::new();
    let mut row_index_by_pk: HashMap<String, usize> = HashMap::new();

    for partition in 0..input.output_partitioning().partition_count() {
        let input_stream = input.execute(partition, Arc::clone(&context))?;
        futures_util::pin_mut!(input_stream);
        while let Some(batch) = input_stream.try_next().await? {
            merge_batch_into_rows(&mut rows, &mut row_index_by_pk, &batch, primary_key_column)?;
        }
    }

    finalize_overlay_rows(
        &input_schema,
        table_id,
        primary_key_column,
        overlay,
        overlay_user_scope,
        rows,
        row_index_by_pk,
        final_projection,
        fetch,
    )
}

#[cfg(test)]
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
        merge_batch_into_rows(&mut rows, &mut row_index_by_pk, batch, primary_key_column)?;
    }

    finalize_overlay_rows(
        input_schema,
        table_id,
        primary_key_column,
        overlay,
        overlay_user_scope,
        rows,
        row_index_by_pk,
        final_projection,
        fetch,
    )
}

fn merge_batch_into_rows(
    rows: &mut Vec<Option<Row>>,
    row_index_by_pk: &mut HashMap<String, usize>,
    batch: &RecordBatch,
    primary_key_column: &str,
) -> DataFusionResult<()> {
    for row in record_batch_to_rows(batch)? {
        let primary_key = extract_primary_key(&row, primary_key_column)?;
        if let Some(existing_index) = row_index_by_pk.get(&primary_key).copied() {
            rows[existing_index] = Some(row);
        } else {
            row_index_by_pk.insert(primary_key, rows.len());
            rows.push(Some(row));
        }
    }

    Ok(())
}

fn finalize_overlay_rows(
    input_schema: &SchemaRef,
    table_id: &TableId,
    _primary_key_column: &str,
    overlay: &TransactionOverlay,
    overlay_user_scope: Option<&UserId>,
    mut rows: Vec<Option<Row>>,
    mut row_index_by_pk: HashMap<String, usize>,
    final_projection: Option<&Vec<usize>>,
    fetch: Option<usize>,
) -> DataFusionResult<RecordBatch> {
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
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use kalamdb_commons::{
        models::{NamespaceId, OperationKind, TableName, TransactionId},
        TableType,
    };

    use super::*;
    use crate::overlay::TransactionOverlayEntry;

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

    #[tokio::test]
    async fn overlay_exec_applies_overlay_once_across_multiple_input_partitions() {
        use datafusion::{execution::context::SessionContext, physical_plan::collect};
        use kalamdb_datafusion_sources::stream::one_shot_batch_stream;

        #[derive(Debug)]
        struct EmptyTwoPartitionExec {
            schema: SchemaRef,
            properties: Arc<PlanProperties>,
        }

        impl EmptyTwoPartitionExec {
            fn new(schema: SchemaRef) -> Self {
                Self {
                    schema: Arc::clone(&schema),
                    properties: Arc::new(PlanProperties::new(
                        EquivalenceProperties::new(schema),
                        Partitioning::UnknownPartitioning(2),
                        datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                        datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                    )),
                }
            }
        }

        impl DisplayAs for EmptyTwoPartitionExec {
            fn fmt_as(
                &self,
                _t: DisplayFormatType,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                write!(f, "EmptyTwoPartitionExec")
            }
        }

        impl ExecutionPlan for EmptyTwoPartitionExec {
            fn name(&self) -> &str {
                Self::static_name()
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn properties(&self) -> &Arc<PlanProperties> {
                &self.properties
            }

            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                Vec::new()
            }

            fn with_new_children(
                self: Arc<Self>,
                children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
                if !children.is_empty() {
                    return Err(DataFusionError::Execution(
                        "EmptyTwoPartitionExec does not accept children".to_string(),
                    ));
                }
                Ok(self)
            }

            fn execute(
                &self,
                partition: usize,
                _context: Arc<TaskContext>,
            ) -> DataFusionResult<SendableRecordBatchStream> {
                if partition >= 2 {
                    return Err(DataFusionError::Execution(format!(
                        "unexpected partition {partition}",
                    )));
                }

                let batch = RecordBatch::new_empty(Arc::clone(&self.schema));
                Ok(one_shot_batch_stream(Arc::clone(&self.schema), async move { Ok(batch) }))
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let transaction_id = TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
        let table_id = TableId::new(NamespaceId::new("app"), TableName::new("items"));
        let mut overlay = TransactionOverlay::new(transaction_id.clone());
        overlay.apply_entry(TransactionOverlayEntry {
            transaction_id,
            mutation_order: 0,
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            user_id: None,
            operation_kind: OperationKind::Insert,
            primary_key: "1".to_string(),
            payload: row(&[
                ("id", ScalarValue::Int64(Some(1))),
                ("name", ScalarValue::Utf8(Some("alpha".to_string()))),
            ]),
            tombstone: false,
        });

        let exec = Arc::new(
            TransactionOverlayExec::try_new(
                Arc::new(EmptyTwoPartitionExec::new(Arc::clone(&schema))),
                table_id,
                "id",
                overlay,
                None,
                None,
                None,
            )
            .expect("overlay exec"),
        );

        assert_eq!(exec.properties().output_partitioning().partition_count(), 1);

        let batches = collect(exec, SessionContext::new().task_ctx())
            .await
            .expect("collect overlay rows");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name array")
                .value(0),
            "alpha"
        );
    }
}
