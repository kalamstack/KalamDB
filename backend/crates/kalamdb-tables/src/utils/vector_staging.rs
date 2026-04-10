use std::collections::HashMap;
use std::sync::Arc;

use crate::error::KalamDbError;
use crate::utils::{base, unified_dml};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::StorageKey;
use kalamdb_commons::TableId;
use kalamdb_store::IndexedEntityStore;
use kalamdb_system::VectorMetric;
use kalamdb_vector::{VectorHotOp, VectorHotOpType};

pub(crate) fn build_vector_upsert_batch_ops<T, I, K, FRow, FKey>(
    table_id: &TableId,
    primary_key_field_name: &str,
    vector_columns: &[(String, u32)],
    entries: I,
    mut row_for: FRow,
    mut key_for: FKey,
) -> Result<HashMap<String, Vec<(K, VectorHotOp)>>, KalamDbError>
where
    I: IntoIterator<Item = T>,
    FRow: FnMut(&T) -> &Row,
    FKey: FnMut(&T, &str) -> K,
{
    let mut ops_by_column: HashMap<String, Vec<(K, VectorHotOp)>> = HashMap::new();

    for entry in entries {
        let row = row_for(&entry);
        let pk = unified_dml::extract_user_pk_value(row, primary_key_field_name)?;

        for (column_name, dimensions) in vector_columns {
            let Some(value) = row.get(column_name.as_str()) else {
                continue;
            };
            let Some(vector) = base::extract_embedding_vector(value, *dimensions) else {
                continue;
            };

            ops_by_column.entry(column_name.clone()).or_default().push((
                key_for(&entry, pk.as_str()),
                VectorHotOp::new(
                    table_id.clone(),
                    column_name.clone(),
                    pk.clone(),
                    VectorHotOpType::Upsert,
                    Some(vector),
                    None,
                    *dimensions,
                    VectorMetric::Cosine,
                ),
            ));
        }
    }

    Ok(ops_by_column)
}

pub(crate) fn build_vector_delete_ops<K, FKey>(
    table_id: &TableId,
    vector_columns: &[(String, u32)],
    pk: &str,
    mut key_for: FKey,
) -> HashMap<String, Vec<(K, VectorHotOp)>>
where
    FKey: FnMut(&str) -> K,
{
    let mut ops_by_column: HashMap<String, Vec<(K, VectorHotOp)>> = HashMap::new();

    for (column_name, dimensions) in vector_columns {
        ops_by_column.entry(column_name.clone()).or_default().push((
            key_for(pk),
            VectorHotOp::new(
                table_id.clone(),
                column_name.clone(),
                pk.to_string(),
                VectorHotOpType::Delete,
                None,
                None,
                *dimensions,
                VectorMetric::Cosine,
            ),
        ));
    }

    ops_by_column
}

pub(crate) async fn stage_vector_ops_by_column<K>(
    vector_stores: &HashMap<String, Arc<IndexedEntityStore<K, VectorHotOp>>>,
    ops_by_column: HashMap<String, Vec<(K, VectorHotOp)>>,
    action: &str,
) -> Result<(), KalamDbError>
where
    K: StorageKey + Clone + Send + Sync + 'static,
{
    for (column_name, ops) in ops_by_column {
        let store = vector_stores.get(&column_name).ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "Missing cached vector store for column '{}'",
                column_name
            ))
        })?;
        store.insert_batch_async(ops).await.map_err(|error| {
            KalamDbError::InvalidOperation(format!(
                "Failed to {} ops for column '{}': {}",
                action, column_name, error
            ))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{build_vector_delete_ops, build_vector_upsert_batch_ops};
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::ids::SeqId;
    use kalamdb_commons::models::rows::Row;
    use kalamdb_commons::models::{NamespaceId, TableName};
    use kalamdb_commons::TableId;

    fn table_id() -> TableId {
        TableId::new(NamespaceId::new("app"), TableName::new("items"))
    }

    #[test]
    fn build_vector_upsert_batch_ops_groups_columns() {
        let rows = vec![
            (
                SeqId::from_i64(1),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("row-1".to_string()))),
                    (
                        "embedding".to_string(),
                        ScalarValue::Utf8(Some("[1.0,2.0]".to_string())),
                    ),
                ]),
            ),
            (
                SeqId::from_i64(2),
                Row::from_vec(vec![
                    ("id".to_string(), ScalarValue::Utf8(Some("row-2".to_string()))),
                    (
                        "embedding".to_string(),
                        ScalarValue::Utf8(Some("[3.0,4.0]".to_string())),
                    ),
                ]),
            ),
        ];

        let ops = build_vector_upsert_batch_ops(
            &table_id(),
            "id",
            &[("embedding".to_string(), 2)],
            rows.iter(),
            |(_, row)| row,
            |(seq, _), pk| (*seq, pk.to_string()),
        )
        .unwrap();

        let embedding_ops = ops.get("embedding").expect("embedding ops");
        assert_eq!(embedding_ops.len(), 2);
        assert_eq!(embedding_ops[0].1.pk, "row-1");
        assert_eq!(embedding_ops[1].1.pk, "row-2");
    }

    #[test]
    fn build_vector_delete_ops_builds_one_delete_per_column() {
        let ops = build_vector_delete_ops(
            &table_id(),
            &[("embedding".to_string(), 2), ("alt_embedding".to_string(), 3)],
            "row-1",
            |pk| pk.to_string(),
        );

        assert_eq!(ops.len(), 2);
        assert_eq!(ops.get("embedding").expect("embedding")[0].1.pk, "row-1");
        assert_eq!(
            ops.get("alt_embedding").expect("alt embedding")[0].1.dimensions,
            3
        );
    }
}