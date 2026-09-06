use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    logical_expr::Expr,
};
use kalamdb_commons::{schemas::TableDefinition, KSerializable, StorageKey};
use kalamdb_store::{entity_store::EntityStore, IndexedEntityStore};
use serde::Serialize;

use crate::{
    error::SystemError,
    providers::base::{extract_filter_value, system_rows_to_batch},
    system_row_mapper::model_to_system_row,
};

pub fn scan_all_rows<K, V>(
    store: &IndexedEntityStore<K, V>,
    schema: &SchemaRef,
    definition: &TableDefinition,
) -> Result<RecordBatch, SystemError>
where
    K: StorageKey,
    V: KSerializable + Serialize,
{
    let rows = store
        .scan_all_typed(None, None, None)?
        .into_iter()
        .map(|(_, model)| model_to_system_row(&model, definition))
        .collect::<Result<Vec<_>, _>>()?;
    system_rows_to_batch(schema, rows)
}

pub fn scan_filtered_rows<K, V>(
    store: &IndexedEntityStore<K, V>,
    schema: &SchemaRef,
    definition: &TableDefinition,
    pk_column: &str,
    parse_key: fn(&str) -> Option<K>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<RecordBatch, SystemError>
where
    K: StorageKey,
    V: KSerializable + Serialize,
{
    if let Some(pk) = extract_filter_value(filters, pk_column) {
        if let Some(key) = parse_key(&pk) {
            if let Some(model) = store.get(&key)? {
                return system_rows_to_batch(
                    schema,
                    vec![model_to_system_row(&model, definition)?],
                );
            }
        }
        return system_rows_to_batch(schema, vec![]);
    }

    let iter = store.scan_iterator(None, None)?;
    let effective_limit = limit.unwrap_or(100_000);
    let mut rows = Vec::with_capacity(effective_limit.min(1000));
    for item in iter {
        let (_, model) = item?;
        rows.push(model_to_system_row(&model, definition)?);
        if rows.len() >= effective_limit {
            break;
        }
    }
    system_rows_to_batch(schema, rows)
}
