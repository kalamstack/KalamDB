//! system.sessions virtual view
//!
//! **Type**: Virtual View (not backed by persistent storage)
//!
//! Provides the current set of active PostgreSQL gRPC sessions tracked by the
//! server-side pg session registry.

use std::sync::{Arc, OnceLock};

use datafusion::arrow::{
    array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder},
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

/// Serializable snapshot of a live PostgreSQL gRPC session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgSessionSnapshot {
    pub session_id: String,
    pub current_schema: Option<String>,
    pub transaction_id: Option<String>,
    pub transaction_state: Option<String>,
    pub transaction_has_writes: bool,
    pub client_addr: Option<String>,
    pub opened_at_ms: i64,
    pub last_seen_at_ms: i64,
    pub last_method: Option<String>,
}

/// Active-session snapshot callback type.
pub type SessionsSnapshotCallback = Arc<dyn Fn() -> Vec<PgSessionSnapshot> + Send + Sync>;

fn sessions_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            SessionsView::definition()
                .to_arrow_schema()
                .expect("Failed to convert system.sessions TableDefinition to Arrow schema")
        })
        .clone()
}

fn parse_backend_pid(session_id: &str) -> Option<i64> {
    session_id
        .strip_prefix("pg-")
        .and_then(|value| value.split('-').next())
        .and_then(|value| value.parse::<i64>().ok())
}

fn derive_state(snapshot: &PgSessionSnapshot) -> &'static str {
    match snapshot.transaction_state.as_deref() {
        Some("active") => "idle in transaction",
        _ => "idle",
    }
}

/// Virtual view that snapshots active PostgreSQL gRPC sessions from memory.
pub struct SessionsView {
    snapshot_callback: Arc<RwLock<Option<SessionsSnapshotCallback>>>,
}

impl std::fmt::Debug for SessionsView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionsView")
            .field("has_callback", &self.snapshot_callback.read().is_some())
            .finish()
    }
}

impl SessionsView {
    pub fn new() -> Self {
        Self {
            snapshot_callback: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_snapshot_callback(&self, callback: SessionsSnapshotCallback) {
        *self.snapshot_callback.write() = Some(callback);
    }

    pub fn definition() -> TableDefinition {
        let columns = vec![
            ColumnDefinition::new(
                1,
                "session_id",
                1,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Server-side PostgreSQL gRPC session identifier".to_string()),
            ),
            ColumnDefinition::new(
                2,
                "backend_pid",
                2,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some(
                    "Parsed PostgreSQL backend PID when session_id follows pg-<pid> or \
                     pg-<pid>-<config-hash>"
                        .to_string(),
                ),
            ),
            ColumnDefinition::new(
                3,
                "current_schema",
                3,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Current schema reported by the pg extension session".to_string()),
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
                Some("Session state similar to pg_stat_activity semantics".to_string()),
            ),
            ColumnDefinition::new(
                5,
                "transaction_id",
                5,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Active remote transaction identifier, if any".to_string()),
            ),
            ColumnDefinition::new(
                6,
                "transaction_state",
                6,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Current remote transaction lifecycle state".to_string()),
            ),
            ColumnDefinition::new(
                7,
                "transaction_has_writes",
                7,
                KalamDataType::Boolean,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Whether the active transaction has performed writes".to_string()),
            ),
            ColumnDefinition::new(
                8,
                "client_addr",
                8,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Observed client socket address for the gRPC session".to_string()),
            ),
            ColumnDefinition::new(
                9,
                "transport",
                9,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Transport used by the session".to_string()),
            ),
            ColumnDefinition::new(
                10,
                "opened_at",
                10,
                KalamDataType::Timestamp,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("When the server first observed this session".to_string()),
            ),
            ColumnDefinition::new(
                11,
                "last_seen_at",
                11,
                KalamDataType::Timestamp,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Most recent RPC activity timestamp for this session".to_string()),
            ),
            ColumnDefinition::new(
                12,
                "last_method",
                12,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Most recent gRPC method observed for this session".to_string()),
            ),
        ];

        TableDefinition::new(
            NamespaceId::system(),
            TableName::new(SystemTable::Sessions.table_name()),
            TableType::System,
            columns,
            TableOptions::system(),
            Some("Active PostgreSQL gRPC sessions tracked by the pg extension bridge".to_string()),
        )
        .expect("Failed to create system.sessions view definition")
    }
}

impl VirtualView for SessionsView {
    fn system_table(&self) -> SystemTable {
        SystemTable::Sessions
    }

    fn schema(&self) -> SchemaRef {
        sessions_schema()
    }

    fn compute_batch(&self) -> Result<RecordBatch, crate::error::RegistryError> {
        let mut snapshot = self
            .snapshot_callback
            .read()
            .as_ref()
            .map(|callback| callback())
            .unwrap_or_default();

        snapshot.sort_by(|left, right| {
            right
                .last_seen_at_ms
                .cmp(&left.last_seen_at_ms)
                .then_with(|| left.session_id.cmp(&right.session_id))
        });

        let mut session_ids = StringBuilder::new();
        let mut backend_pids = Int64Builder::new();
        let mut current_schemas = StringBuilder::new();
        let mut states = StringBuilder::new();
        let mut transaction_ids = StringBuilder::new();
        let mut transaction_states = StringBuilder::new();
        let mut transaction_has_writes = BooleanBuilder::new();
        let mut client_addrs = StringBuilder::new();
        let mut transports = StringBuilder::new();
        let mut opened_ats = TimestampMicrosecondBuilder::new();
        let mut last_seen_ats = TimestampMicrosecondBuilder::new();
        let mut last_methods = StringBuilder::new();

        for session in snapshot {
            session_ids.append_value(&session.session_id);

            if let Some(backend_pid) = parse_backend_pid(&session.session_id) {
                backend_pids.append_value(backend_pid);
            } else {
                backend_pids.append_null();
            }

            if let Some(current_schema) = session.current_schema.as_deref() {
                current_schemas.append_value(current_schema);
            } else {
                current_schemas.append_null();
            }

            states.append_value(derive_state(&session));

            if let Some(transaction_id) = session.transaction_id.as_deref() {
                transaction_ids.append_value(transaction_id);
            } else {
                transaction_ids.append_null();
            }

            if let Some(transaction_state) = session.transaction_state.as_deref() {
                transaction_states.append_value(transaction_state);
            } else {
                transaction_states.append_null();
            }

            transaction_has_writes.append_value(session.transaction_has_writes);

            if let Some(client_addr) = session.client_addr.as_deref() {
                client_addrs.append_value(client_addr);
            } else {
                client_addrs.append_null();
            }

            transports.append_value("grpc");
            opened_ats.append_value(session.opened_at_ms * 1000);
            last_seen_ats.append_value(session.last_seen_at_ms * 1000);

            if let Some(last_method) = session.last_method.as_deref() {
                last_methods.append_value(last_method);
            } else {
                last_methods.append_null();
            }
        }

        RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(session_ids.finish()) as ArrayRef,
                Arc::new(backend_pids.finish()) as ArrayRef,
                Arc::new(current_schemas.finish()) as ArrayRef,
                Arc::new(states.finish()) as ArrayRef,
                Arc::new(transaction_ids.finish()) as ArrayRef,
                Arc::new(transaction_states.finish()) as ArrayRef,
                Arc::new(transaction_has_writes.finish()) as ArrayRef,
                Arc::new(client_addrs.finish()) as ArrayRef,
                Arc::new(transports.finish()) as ArrayRef,
                Arc::new(opened_ats.finish()) as ArrayRef,
                Arc::new(last_seen_ats.finish()) as ArrayRef,
                Arc::new(last_methods.finish()) as ArrayRef,
            ],
        )
        .map_err(|error| {
            crate::error::RegistryError::Other(format!(
                "Failed to build system.sessions batch: {}",
                error
            ))
        })
    }
}

pub type SessionsTableProvider = crate::view_base::ViewTableProvider<SessionsView>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_view_definition_has_expected_name() {
        let definition = SessionsView::definition();
        assert_eq!(definition.table_name.as_str(), "sessions");
        assert_eq!(definition.columns.len(), 12);
    }

    #[test]
    fn sessions_view_compute_batch_uses_snapshot_callback() {
        let view = SessionsView::new();
        let callback: SessionsSnapshotCallback = Arc::new(|| {
            vec![PgSessionSnapshot {
                session_id: "pg-321-deadbeef".to_string(),
                current_schema: Some("app".to_string()),
                transaction_id: Some("tx-pg-321-0".to_string()),
                transaction_state: Some("active".to_string()),
                transaction_has_writes: true,
                client_addr: Some("127.0.0.1:40000".to_string()),
                opened_at_ms: 1,
                last_seen_at_ms: 2,
                last_method: Some("ExecuteQuery".to_string()),
            }]
        });
        view.set_snapshot_callback(callback);

        let batch = view.compute_batch().expect("compute batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 12);
    }

    #[test]
    fn parse_backend_pid_accepts_config_scoped_session_ids() {
        assert_eq!(parse_backend_pid("pg-321-deadbeef"), Some(321));
        assert_eq!(parse_backend_pid("pg-321"), Some(321));
    }
}
