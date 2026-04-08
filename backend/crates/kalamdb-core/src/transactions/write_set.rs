use std::collections::HashMap;

use kalamdb_commons::models::{TableId, TransactionId};

use super::{StagedMutation, TransactionOverlay};

/// Cold staged-write state allocated lazily on the first transaction write.
#[derive(Debug, Clone)]
pub struct TransactionWriteSet {
    pub transaction_id: TransactionId,
    pub next_mutation_order: u64,
    pub ordered_mutations: Vec<StagedMutation>,
    pub latest_by_table_key: HashMap<TableId, HashMap<String, usize>>,
    pub overlay_cache: HashMap<TableId, TransactionOverlay>,
    pub buffer_bytes: usize,
    pub savepoint_marks: Vec<u64>,
}

impl TransactionWriteSet {
    pub fn new(transaction_id: TransactionId) -> Self {
        Self {
            transaction_id,
            next_mutation_order: 0,
            ordered_mutations: Vec::new(),
            latest_by_table_key: HashMap::new(),
            overlay_cache: HashMap::new(),
            buffer_bytes: 0,
            savepoint_marks: Vec::new(),
        }
    }

    pub fn stage(&mut self, mut mutation: StagedMutation) -> u64 {
        let mutation_order = self.next_mutation_order;
        self.next_mutation_order += 1;
        mutation.mutation_order = mutation_order;

        let table_id = mutation.table_id.clone();
        let primary_key = mutation.primary_key.clone();
        self.buffer_bytes += mutation.approximate_size_bytes();

        let mutation_index = self.ordered_mutations.len();
        self.ordered_mutations.push(mutation);
        self.latest_by_table_key
            .entry(table_id.clone())
            .or_default()
            .insert(primary_key, mutation_index);
        self.overlay_cache
            .insert(table_id.clone(), self.build_table_overlay(&table_id));

        mutation_order
    }

    #[inline]
    pub fn affected_rows(&self) -> usize {
        self.ordered_mutations.len()
    }

    #[inline]
    pub fn ordered_mutations(&self) -> &[StagedMutation] {
        &self.ordered_mutations
    }

    pub fn latest_mutation(&self, table_id: &TableId, primary_key: &str) -> Option<&StagedMutation> {
        let mutation_index = *self.latest_by_table_key.get(table_id)?.get(primary_key)?;
        self.ordered_mutations.get(mutation_index)
    }

    pub fn overlay_for_table(&self, table_id: &TableId) -> Option<TransactionOverlay> {
        self.overlay_cache.get(table_id).cloned()
    }

    pub fn merged_overlay(&self) -> TransactionOverlay {
        let mut overlay = TransactionOverlay::new(self.transaction_id.clone());
        for table_overlay in self.overlay_cache.values() {
            overlay.merge_from(table_overlay);
        }
        overlay
    }

    fn build_table_overlay(&self, table_id: &TableId) -> TransactionOverlay {
        let mut overlay = TransactionOverlay::new(self.transaction_id.clone());

        if let Some(latest_by_key) = self.latest_by_table_key.get(table_id) {
            for mutation_index in latest_by_key.values() {
                if let Some(mutation) = self.ordered_mutations.get(*mutation_index) {
                    overlay.apply_entry(mutation.overlay_entry());
                }
            }
        }

        overlay
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use std::collections::BTreeMap;

    use super::*;
    use kalamdb_commons::models::rows::Row;
    use kalamdb_commons::models::{OperationKind, TableName};
    use kalamdb_commons::models::{NamespaceId, TableId};
    use kalamdb_commons::TableType;

    #[test]
    fn merged_overlay_keeps_latest_state_per_primary_key() {
        let transaction_id = TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
        let table_id = TableId::new(NamespaceId::new("app"), TableName::new("items"));
        let mut write_set = TransactionWriteSet::new(transaction_id.clone());

        let mut first_values = BTreeMap::new();
        first_values.insert("id".to_string(), ScalarValue::Int64(Some(1)));
        first_values.insert("name".to_string(), ScalarValue::Utf8(Some("first".to_string())));
        write_set.stage(StagedMutation::new(
            transaction_id.clone(),
            table_id.clone(),
            TableType::Shared,
            None,
            OperationKind::Insert,
            "1",
            Row::new(first_values),
            false,
        ));

        let mut second_values = BTreeMap::new();
        second_values.insert("id".to_string(), ScalarValue::Int64(Some(1)));
        second_values.insert("name".to_string(), ScalarValue::Utf8(Some("second".to_string())));
        write_set.stage(StagedMutation::new(
            transaction_id,
            table_id.clone(),
            TableType::Shared,
            None,
            OperationKind::Update,
            "1",
            Row::new(second_values),
            false,
        ));

        let overlay = write_set.merged_overlay();
        let entry = overlay.latest_visible_entry(&table_id, "1").unwrap();
        assert_eq!(entry.mutation_order, 1);
        assert_eq!(entry.operation_kind, OperationKind::Update);
    }
}