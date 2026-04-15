use std::collections::{BTreeMap, HashMap, HashSet};

use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{OperationKind, TableId, TransactionId, UserId};
use kalamdb_commons::TableType;

use crate::query_context::TransactionOverlayView;

fn scoped_entry_key(user_id: Option<&UserId>, primary_key: &str) -> String {
    match user_id {
        Some(user_id) => {
            format!("u{}:{}:{}", user_id.as_str().len(), user_id.as_str(), primary_key)
        },
        None => format!("s:{}", primary_key),
    }
}

/// Shared overlay entry exposed across crate boundaries for transaction-local reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionOverlayEntry {
    pub transaction_id: TransactionId,
    pub mutation_order: u64,
    pub table_id: TableId,
    pub table_type: TableType,
    pub user_id: Option<UserId>,
    pub operation_kind: OperationKind,
    pub primary_key: String,
    pub payload: Row,
    pub tombstone: bool,
}

impl TransactionOverlayEntry {
    #[inline]
    pub fn is_deleted(&self) -> bool {
        self.tombstone || matches!(self.operation_kind, OperationKind::Delete)
    }
}

/// Query-time overlay for transaction-local read visibility.
#[derive(Debug, Clone)]
pub struct TransactionOverlay {
    pub transaction_id: TransactionId,
    pub entries_by_table: HashMap<TableId, BTreeMap<String, TransactionOverlayEntry>>,
    pub inserted_keys: HashMap<TableId, HashSet<String>>,
    pub deleted_keys: HashMap<TableId, HashSet<String>>,
    pub updated_keys: HashMap<TableId, HashSet<String>>,
}

impl TransactionOverlay {
    #[inline]
    pub fn new(transaction_id: TransactionId) -> Self {
        Self {
            transaction_id,
            entries_by_table: HashMap::new(),
            inserted_keys: HashMap::new(),
            deleted_keys: HashMap::new(),
            updated_keys: HashMap::new(),
        }
    }

    pub fn apply_entry(&mut self, entry: TransactionOverlayEntry) {
        let table_id = entry.table_id.clone();
        let primary_key = entry.primary_key.clone();
        let user_id = entry.user_id.clone();
        let entry_key = scoped_entry_key(user_id.as_ref(), primary_key.as_str());
        let effective_entry =
            self.merge_visible_entry(&table_id, user_id.as_ref(), primary_key.as_str(), entry);

        self.entries_by_table
            .entry(table_id.clone())
            .or_default()
            .insert(entry_key.clone(), effective_entry.clone());

        self.clear_key_membership(&table_id, entry_key.as_str());

        let target_map = if effective_entry.is_deleted() {
            &mut self.deleted_keys
        } else {
            match effective_entry.operation_kind {
                OperationKind::Insert => &mut self.inserted_keys,
                OperationKind::Update => &mut self.updated_keys,
                OperationKind::Delete => &mut self.deleted_keys,
            }
        };

        target_map.entry(table_id).or_default().insert(entry_key);
    }

    pub fn merge_from(&mut self, other: &TransactionOverlay) {
        for table_entries in other.entries_by_table.values() {
            for entry in table_entries.values() {
                self.apply_entry(entry.clone());
            }
        }
    }

    #[inline]
    pub fn latest_visible_entry(
        &self,
        table_id: &TableId,
        primary_key: &str,
    ) -> Option<&TransactionOverlayEntry> {
        self.latest_visible_entry_for_scope(table_id, None, primary_key)
    }

    #[inline]
    pub fn latest_visible_entry_for_scope(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        primary_key: &str,
    ) -> Option<&TransactionOverlayEntry> {
        let entry_key = scoped_entry_key(user_id, primary_key);
        self.entries_by_table.get(table_id)?.get(entry_key.as_str())
    }

    #[inline]
    pub fn table_entries(
        &self,
        table_id: &TableId,
    ) -> Option<&BTreeMap<String, TransactionOverlayEntry>> {
        self.entries_by_table.get(table_id)
    }

    pub fn table_overlay(&self, table_id: &TableId) -> Option<TransactionOverlay> {
        let entries = self.entries_by_table.get(table_id)?.clone();

        let mut overlay = TransactionOverlay::new(self.transaction_id.clone());
        overlay.entries_by_table.insert(table_id.clone(), entries);

        if let Some(keys) = self.inserted_keys.get(table_id) {
            overlay.inserted_keys.insert(table_id.clone(), keys.clone());
        }
        if let Some(keys) = self.deleted_keys.get(table_id) {
            overlay.deleted_keys.insert(table_id.clone(), keys.clone());
        }
        if let Some(keys) = self.updated_keys.get(table_id) {
            overlay.updated_keys.insert(table_id.clone(), keys.clone());
        }

        Some(overlay)
    }

    fn clear_key_membership(&mut self, table_id: &TableId, entry_key: &str) {
        for key_set in [
            &mut self.inserted_keys,
            &mut self.deleted_keys,
            &mut self.updated_keys,
        ] {
            if let Some(keys) = key_set.get_mut(table_id) {
                keys.remove(entry_key);
                if keys.is_empty() {
                    key_set.remove(table_id);
                }
            }
        }
    }

    fn merge_visible_entry(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        primary_key: &str,
        mut next: TransactionOverlayEntry,
    ) -> TransactionOverlayEntry {
        if next.is_deleted() {
            return next;
        }

        let entry_key = scoped_entry_key(user_id, primary_key);

        let Some(current) = self
            .entries_by_table
            .get(table_id)
            .and_then(|entries| entries.get(entry_key.as_str()))
        else {
            return next;
        };

        if current.is_deleted() {
            return next;
        }

        if matches!(next.operation_kind, OperationKind::Update) {
            let mut merged_values = current.payload.values.clone();
            for (column_name, value) in &next.payload.values {
                merged_values.insert(column_name.clone(), value.clone());
            }
            next.payload = Row::new(merged_values);
            if matches!(current.operation_kind, OperationKind::Insert) {
                next.operation_kind = OperationKind::Insert;
            }
        }

        next
    }
}

impl TransactionOverlayView for TransactionOverlay {
    fn overlay(&self) -> TransactionOverlay {
        self.clone()
    }

    fn overlay_for_table(&self, table_id: &TableId) -> Option<TransactionOverlay> {
        self.table_overlay(table_id)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use std::collections::BTreeMap;

    use super::*;
    use kalamdb_commons::models::{NamespaceId, TableName};

    fn row(values: &[(&str, ScalarValue)]) -> Row {
        let mut fields = BTreeMap::new();
        for (name, value) in values {
            fields.insert((*name).to_string(), value.clone());
        }
        Row::new(fields)
    }

    #[test]
    fn update_after_insert_preserves_inserted_columns() {
        let transaction_id = TransactionId::new("01960f7b-3d15-7d6d-b26c-7e4db6f25f8d");
        let table_id = TableId::new(NamespaceId::new("app"), TableName::new("items"));
        let mut overlay = TransactionOverlay::new(transaction_id.clone());

        overlay.apply_entry(TransactionOverlayEntry {
            transaction_id: transaction_id.clone(),
            mutation_order: 0,
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            user_id: None,
            operation_kind: OperationKind::Insert,
            primary_key: "1".to_string(),
            payload: row(&[
                ("id", ScalarValue::Int64(Some(1))),
                ("name", ScalarValue::Utf8(Some("before".to_string()))),
                ("color", ScalarValue::Utf8(Some("red".to_string()))),
            ]),
            tombstone: false,
        });

        overlay.apply_entry(TransactionOverlayEntry {
            transaction_id,
            mutation_order: 1,
            table_id: table_id.clone(),
            table_type: TableType::Shared,
            user_id: None,
            operation_kind: OperationKind::Update,
            primary_key: "1".to_string(),
            payload: row(&[("name", ScalarValue::Utf8(Some("after".to_string())))]),
            tombstone: false,
        });

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
    fn preserves_distinct_user_scopes_for_same_primary_key() {
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
            payload: row(&[("name", ScalarValue::Utf8(Some("alice".to_string())))]),
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
            payload: row(&[("name", ScalarValue::Utf8(Some("bob".to_string())))]),
            tombstone: false,
        });

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
    }
}
