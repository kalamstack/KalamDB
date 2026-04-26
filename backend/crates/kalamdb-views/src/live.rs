//! system.live virtual view
//!
//! **Type**: Virtual View (not backed by persistent storage)
//!
//! Provides the current set of active subscriptions from the in-memory
//! connection registry.

use std::sync::{Arc, OnceLock};

use datafusion::arrow::{
    array::{ArrayRef, Int64Builder, StringBuilder, TimestampMicrosecondBuilder},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use kalamdb_commons::{schemas::TableDefinition, SystemTable};
use kalamdb_system::LiveQuery;
use parking_lot::RwLock;

use crate::view_base::VirtualView;

/// Live-query snapshot callback type.
pub type LiveSnapshotCallback = Arc<dyn Fn() -> Vec<LiveQuery> + Send + Sync>;

fn live_schema(system_table: SystemTable) -> SchemaRef {
    match system_table {
        SystemTable::Live => {
            static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
            SCHEMA
                .get_or_init(|| {
                    LiveView::definition_for(SystemTable::Live)
                        .to_arrow_schema()
                        .expect("Failed to convert system.live TableDefinition to Arrow schema")
                })
                .clone()
        },
        _ => unreachable!("LiveView only supports system.live"),
    }
}

/// Virtual view that snapshots active subscriptions from memory.
pub struct LiveView {
    system_table: SystemTable,
    snapshot_callback: Arc<RwLock<Option<LiveSnapshotCallback>>>,
}

impl std::fmt::Debug for LiveView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveView")
            .field("system_table", &self.system_table)
            .field("has_callback", &self.snapshot_callback.read().is_some())
            .finish()
    }
}

impl LiveView {
    pub fn new(system_table: SystemTable) -> Self {
        Self {
            system_table,
            snapshot_callback: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_snapshot_callback(&self, callback: LiveSnapshotCallback) {
        *self.snapshot_callback.write() = Some(callback);
    }

    pub fn definition_for(system_table: SystemTable) -> TableDefinition {
        match system_table {
            SystemTable::Live => {
                let mut definition = LiveQuery::definition();
                definition.table_comment = Some(
                    "Active in-memory live subscriptions (computed on each query)".to_string(),
                );
                definition
            },
            _ => unreachable!("LiveView only supports system.live"),
        }
    }
}

impl VirtualView for LiveView {
    fn system_table(&self) -> SystemTable {
        self.system_table
    }

    fn schema(&self) -> SchemaRef {
        live_schema(self.system_table)
    }

    fn compute_batch(&self) -> Result<RecordBatch, crate::error::RegistryError> {
        let snapshot = self
            .snapshot_callback
            .read()
            .as_ref()
            .map(|callback| callback())
            .unwrap_or_default();

        let mut live_ids = StringBuilder::new();
        let mut connection_ids = StringBuilder::new();
        let mut subscription_ids = StringBuilder::new();
        let mut namespace_ids = StringBuilder::new();
        let mut table_names = StringBuilder::new();
        let mut user_ids = StringBuilder::new();
        let mut queries = StringBuilder::new();
        let mut options = StringBuilder::new();
        let mut statuses = StringBuilder::new();
        let mut created_ats = TimestampMicrosecondBuilder::new();
        let mut last_updates = TimestampMicrosecondBuilder::new();
        let mut last_ping_ats = TimestampMicrosecondBuilder::new();
        let mut changes = Int64Builder::new();
        let mut node_ids = Int64Builder::new();

        for live_query in snapshot {
            live_ids.append_value(live_query.live_id.to_string());
            connection_ids.append_value(&live_query.connection_id);
            subscription_ids.append_value(&live_query.subscription_id);
            namespace_ids.append_value(live_query.namespace_id.as_str());
            table_names.append_value(live_query.table_name.as_str());
            user_ids.append_value(live_query.user_id.as_str());
            queries.append_value(&live_query.query);

            if let Some(options_json) = &live_query.options {
                options.append_value(serde_json::to_string(options_json).unwrap_or_default());
            } else {
                options.append_null();
            }

            statuses.append_value(live_query.status.as_str());
            created_ats.append_value(live_query.created_at * 1000);
            last_updates.append_value(live_query.last_update * 1000);
            last_ping_ats.append_value(live_query.last_ping_at * 1000);
            changes.append_value(live_query.changes);
            node_ids.append_value(live_query.node_id.as_u64() as i64);
        }

        RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(live_ids.finish()) as ArrayRef,
                Arc::new(connection_ids.finish()) as ArrayRef,
                Arc::new(subscription_ids.finish()) as ArrayRef,
                Arc::new(namespace_ids.finish()) as ArrayRef,
                Arc::new(table_names.finish()) as ArrayRef,
                Arc::new(user_ids.finish()) as ArrayRef,
                Arc::new(queries.finish()) as ArrayRef,
                Arc::new(options.finish()) as ArrayRef,
                Arc::new(statuses.finish()) as ArrayRef,
                Arc::new(created_ats.finish()) as ArrayRef,
                Arc::new(last_updates.finish()) as ArrayRef,
                Arc::new(changes.finish()) as ArrayRef,
                Arc::new(node_ids.finish()) as ArrayRef,
                Arc::new(last_ping_ats.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            crate::error::RegistryError::Other(format!("Failed to build live view batch: {}", e))
        })
    }
}

pub type LiveTableProvider = crate::view_base::ViewTableProvider<LiveView>;

#[cfg(test)]
mod tests {
    use kalamdb_commons::{
        models::{ConnectionId, LiveQueryId, NamespaceId, UserId},
        NodeId, TableName,
    };
    use kalamdb_system::LiveQueryStatus;

    use super::*;

    fn sample_live_query() -> LiveQuery {
        let user_id = UserId::new("u_live");
        let connection_id = ConnectionId::new("conn_live");
        LiveQuery {
            live_id: LiveQueryId::new(user_id.clone(), connection_id.clone(), "sub_live"),
            connection_id: connection_id.as_str().to_string(),
            subscription_id: "sub_live".to_string(),
            namespace_id: NamespaceId::from("default"),
            table_name: TableName::from("events"),
            user_id,
            query: "SELECT * FROM default.events".to_string(),
            options: Some(serde_json::json!({"batch_size":100})),
            status: LiveQueryStatus::Active,
            created_at: 1,
            last_update: 2,
            last_ping_at: 3,
            changes: 4,
            node_id: NodeId::new(5),
        }
    }

    #[test]
    fn test_live_view_definition_names() {
        assert_eq!(LiveView::definition_for(SystemTable::Live).table_name.as_str(), "live");
    }

    #[test]
    fn test_live_view_compute_with_callback() {
        let view = LiveView::new(SystemTable::Live);
        let callback: LiveSnapshotCallback = Arc::new(|| vec![sample_live_query()]);
        view.set_snapshot_callback(callback);

        let batch = view.compute_batch().expect("compute batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 14);
    }
}
