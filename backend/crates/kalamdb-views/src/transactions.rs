//! system.transactions virtual view
//!
//! **Type**: Virtual View (not backed by persistent storage)
//!
//! Provides the current set of active explicit transactions tracked by the
//! in-memory transaction coordinator.

use std::sync::{Arc, OnceLock};

use datafusion::arrow::{
    array::{ArrayRef, Int64Builder, StringBuilder},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use kalamdb_commons::{
    datatypes::KalamDataType,
    schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
    NamespaceId, SystemTable, TableName,
};
use parking_lot::RwLock;

use crate::view_base::VirtualView;

/// Serializable snapshot of an active explicit transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionSnapshot {
    pub transaction_id: String,
    pub owner_id: String,
    pub origin: String,
    pub state: String,
    pub age_ms: i64,
    pub idle_ms: i64,
    pub write_count: i64,
    pub write_bytes: i64,
    pub touched_tables_count: i64,
    pub snapshot_commit_seq: i64,
}

/// Active-transaction snapshot callback type.
pub type TransactionsSnapshotCallback = Arc<dyn Fn() -> Vec<TransactionSnapshot> + Send + Sync>;

fn transactions_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            TransactionsView::definition()
                .to_arrow_schema()
                .expect("Failed to convert system.transactions TableDefinition to Arrow schema")
        })
        .clone()
}

/// Virtual view that snapshots active explicit transactions from memory.
pub struct TransactionsView {
    snapshot_callback: Arc<RwLock<Option<TransactionsSnapshotCallback>>>,
}

impl std::fmt::Debug for TransactionsView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionsView")
            .field("has_callback", &self.snapshot_callback.read().is_some())
            .finish()
    }
}

impl TransactionsView {
    pub fn new() -> Self {
        Self {
            snapshot_callback: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_snapshot_callback(&self, callback: TransactionsSnapshotCallback) {
        *self.snapshot_callback.write() = Some(callback);
    }

    pub fn definition() -> TableDefinition {
        let columns = vec![
            ColumnDefinition::new(
                1,
                "transaction_id",
                1,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Canonical explicit transaction identifier".to_string()),
            ),
            ColumnDefinition::new(
                2,
                "owner_id",
                2,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Human-readable execution owner identifier".to_string()),
            ),
            ColumnDefinition::new(
                3,
                "origin",
                3,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Transaction origin surface".to_string()),
            ),
            ColumnDefinition::new(
                4,
                "state",
                4,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Current transaction lifecycle state".to_string()),
            ),
            ColumnDefinition::new(
                5,
                "age_ms",
                5,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Transaction age in milliseconds".to_string()),
            ),
            ColumnDefinition::new(
                6,
                "idle_ms",
                6,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Milliseconds since the transaction last performed work".to_string()),
            ),
            ColumnDefinition::new(
                7,
                "write_count",
                7,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Number of staged mutations currently buffered".to_string()),
            ),
            ColumnDefinition::new(
                8,
                "write_bytes",
                8,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Approximate in-memory size of the staged write set".to_string()),
            ),
            ColumnDefinition::new(
                9,
                "touched_tables_count",
                9,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Number of tables referenced by the transaction".to_string()),
            ),
            ColumnDefinition::new(
                10,
                "snapshot_commit_seq",
                10,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Committed snapshot boundary captured at BEGIN".to_string()),
            ),
        ];

        TableDefinition::new(
            NamespaceId::system(),
            TableName::new(SystemTable::Transactions.table_name()),
            TableType::System,
            columns,
            TableOptions::system(),
            Some(
                "Active explicit transactions across pg RPC, SQL batch, and internal origins"
                    .to_string(),
            ),
        )
        .expect("Failed to create system.transactions view definition")
    }
}

impl VirtualView for TransactionsView {
    fn system_table(&self) -> SystemTable {
        SystemTable::Transactions
    }

    fn schema(&self) -> SchemaRef {
        transactions_schema()
    }

    fn compute_batch(&self) -> Result<RecordBatch, crate::error::RegistryError> {
        let mut snapshot = self
            .snapshot_callback
            .read()
            .as_ref()
            .map(|callback| callback())
            .unwrap_or_default();

        snapshot.sort_by(|left, right| {
            left.origin
                .cmp(&right.origin)
                .then_with(|| left.transaction_id.cmp(&right.transaction_id))
        });

        let mut transaction_id_builder = StringBuilder::new();
        let mut owner_id_builder = StringBuilder::new();
        let mut origin_builder = StringBuilder::new();
        let mut state_builder = StringBuilder::new();
        let mut age_ms_builder = Int64Builder::new();
        let mut idle_ms_builder = Int64Builder::new();
        let mut write_count_builder = Int64Builder::new();
        let mut write_bytes_builder = Int64Builder::new();
        let mut touched_tables_count_builder = Int64Builder::new();
        let mut snapshot_commit_seq_builder = Int64Builder::new();

        for transaction in snapshot {
            transaction_id_builder.append_value(&transaction.transaction_id);
            owner_id_builder.append_value(&transaction.owner_id);
            origin_builder.append_value(&transaction.origin);
            state_builder.append_value(&transaction.state);
            age_ms_builder.append_value(transaction.age_ms);
            idle_ms_builder.append_value(transaction.idle_ms);
            write_count_builder.append_value(transaction.write_count);
            write_bytes_builder.append_value(transaction.write_bytes);
            touched_tables_count_builder.append_value(transaction.touched_tables_count);
            snapshot_commit_seq_builder.append_value(transaction.snapshot_commit_seq);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(transaction_id_builder.finish()),
            Arc::new(owner_id_builder.finish()),
            Arc::new(origin_builder.finish()),
            Arc::new(state_builder.finish()),
            Arc::new(age_ms_builder.finish()),
            Arc::new(idle_ms_builder.finish()),
            Arc::new(write_count_builder.finish()),
            Arc::new(write_bytes_builder.finish()),
            Arc::new(touched_tables_count_builder.finish()),
            Arc::new(snapshot_commit_seq_builder.finish()),
        ];

        RecordBatch::try_new(transactions_schema(), columns)
            .map_err(|error| crate::error::RegistryError::Other(error.to_string()))
    }
}

pub type TransactionsTableProvider = crate::view_base::ViewTableProvider<TransactionsView>;
