use std::collections::HashMap;

use kalamdb_commons::models::{TableId, TransactionId, UserId};

use super::{StagedMutation, TransactionOverlay};

fn scoped_table_key(user_id: Option<&UserId>, primary_key: &str) -> String {
    match user_id {
        Some(user_id) => {
            format!("u{}:{}:{}", user_id.as_str().len(), user_id.as_str(), primary_key)
        },
        None => format!("s:{}", primary_key),
    }
}

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

    pub fn stage(&mut self, mutation: StagedMutation) -> u64 {
        let estimated_bytes = mutation.approximate_size_bytes();
        self.stage_with_estimated_size(mutation, estimated_bytes)
    }

    pub fn stage_with_estimated_size(
        &mut self,
        mut mutation: StagedMutation,
        estimated_bytes: usize,
    ) -> u64 {
        let mutation_order = self.next_mutation_order;
        self.next_mutation_order += 1;
        mutation.mutation_order = mutation_order;

        let overlay_entry = mutation.overlay_entry();
        let table_id = mutation.table_id.clone();
        let table_key = scoped_table_key(mutation.user_id.as_ref(), mutation.primary_key.as_str());
        self.buffer_bytes += estimated_bytes;

        let mutation_index = self.ordered_mutations.len();
        self.ordered_mutations.push(mutation);
        self.latest_by_table_key
            .entry(table_id.clone())
            .or_default()
            .insert(table_key, mutation_index);
        self.overlay_cache
            .entry(table_id.clone())
            .or_insert_with(|| TransactionOverlay::new(self.transaction_id.clone()))
            .apply_entry(overlay_entry);

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

    pub fn latest_mutation(
        &self,
        table_id: &TableId,
        primary_key: &str,
    ) -> Option<&StagedMutation> {
        self.latest_mutation_for_scope(table_id, None, primary_key)
    }

    pub fn latest_mutation_for_scope(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        primary_key: &str,
    ) -> Option<&StagedMutation> {
        let table_key = scoped_table_key(user_id, primary_key);
        let mutation_index = *self.latest_by_table_key.get(table_id)?.get(table_key.as_str())?;
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
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion_common::ScalarValue;
    use kalamdb_commons::{
        models::{rows::Row, NamespaceId, OperationKind, TableId, TableName},
        TableType,
    };

    use super::*;

    fn row(values: &[(&'static str, ScalarValue)]) -> Row {
        let mut fields = BTreeMap::new();
        for (name, value) in values {
            fields.insert((*name).to_string(), value.clone());
        }
        Row::new(fields)
    }

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
        assert_eq!(entry.operation_kind, OperationKind::Insert);
        assert_eq!(
            entry.payload.values.get("name"),
            Some(&ScalarValue::Utf8(Some("second".to_string())))
        );
    }

    #[test]
    fn stage_updates_overlay_incrementally() {
        let transaction_id = TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
        let table_id = TableId::new(NamespaceId::new("app"), TableName::new("items"));
        let mut write_set = TransactionWriteSet::new(transaction_id.clone());

        let mut inserted_values = BTreeMap::new();
        inserted_values.insert("id".to_string(), ScalarValue::Int64(Some(1)));
        inserted_values.insert("name".to_string(), ScalarValue::Utf8(Some("before".to_string())));
        inserted_values.insert("color".to_string(), ScalarValue::Utf8(Some("red".to_string())));
        write_set.stage(StagedMutation::new(
            transaction_id.clone(),
            table_id.clone(),
            TableType::Shared,
            None,
            OperationKind::Insert,
            "1",
            Row::new(inserted_values),
            false,
        ));

        let mut updated_values = BTreeMap::new();
        updated_values.insert("name".to_string(), ScalarValue::Utf8(Some("after".to_string())));
        write_set.stage(StagedMutation::new(
            transaction_id,
            table_id.clone(),
            TableType::Shared,
            None,
            OperationKind::Update,
            "1",
            Row::new(updated_values),
            false,
        ));

        let overlay = write_set.overlay_for_table(&table_id).expect("table overlay");
        let entry = overlay.latest_visible_entry(&table_id, "1").expect("overlay entry");
        assert_eq!(entry.operation_kind, OperationKind::Insert);
        assert_eq!(entry.payload.values.get("id"), Some(&ScalarValue::Int64(Some(1))));
        assert_eq!(
            entry.payload.values.get("name"),
            Some(&ScalarValue::Utf8(Some("after".to_string())))
        );
        assert_eq!(
            entry.payload.values.get("color"),
            Some(&ScalarValue::Utf8(Some("red".to_string())))
        );
    }

    #[test]
    fn merged_overlay_preserves_distinct_user_scope_for_same_primary_key() {
        let transaction_id = TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
        let table_id = TableId::new(NamespaceId::new("app"), TableName::new("items"));
        let first_user = UserId::new("user-a");
        let second_user = UserId::new("user-b");
        let mut write_set = TransactionWriteSet::new(transaction_id.clone());

        write_set.stage(StagedMutation::new(
            transaction_id.clone(),
            table_id.clone(),
            TableType::User,
            Some(first_user.clone()),
            OperationKind::Insert,
            "1",
            row(&[
                ("id", ScalarValue::Int64(Some(1))),
                ("name", ScalarValue::Utf8(Some("alice".to_string()))),
            ]),
            false,
        ));
        write_set.stage(StagedMutation::new(
            transaction_id,
            table_id.clone(),
            TableType::User,
            Some(second_user.clone()),
            OperationKind::Insert,
            "1",
            row(&[
                ("id", ScalarValue::Int64(Some(1))),
                ("name", ScalarValue::Utf8(Some("bob".to_string()))),
            ]),
            false,
        ));

        let overlay = write_set.merged_overlay();
        assert_eq!(overlay.table_entries(&table_id).expect("table entries").len(), 2);
        assert_eq!(
            overlay
                .latest_visible_entry_for_scope(&table_id, Some(&first_user), "1")
                .expect("first user entry")
                .payload
                .values
                .get("name"),
            Some(&ScalarValue::Utf8(Some("alice".to_string())))
        );
        assert_eq!(
            overlay
                .latest_visible_entry_for_scope(&table_id, Some(&second_user), "1")
                .expect("second user entry")
                .payload
                .values
                .get("name"),
            Some(&ScalarValue::Utf8(Some("bob".to_string())))
        );
        assert!(write_set.latest_mutation_for_scope(&table_id, Some(&first_user), "1").is_some());
        assert!(write_set
            .latest_mutation_for_scope(&table_id, Some(&second_user), "1")
            .is_some());
    }
}
