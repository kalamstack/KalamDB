//! system.cluster virtual view
//!
//! **Type**: Virtual View (not backed by persistent storage)
//!
//! Provides live OpenRaft cluster status and metrics:
//! - Node ID, role (leader/follower/learner/candidate), status
//! - RPC and API addresses
//! - Raft group leadership counts
//! - OpenRaft metrics: term, log index, replication lag, snapshot index
//!
//! **DataFusion Pattern**: Implements VirtualView trait for consistent view behavior
//! - Fetches live data from OpenRaft metrics on each query
//! - Zero memory overhead when idle (no cached state)
//! - Only used in cluster mode
//!
//! **Performance Optimizations**:
//! - Pre-allocated vectors with known capacity
//! - Schema cached via `OnceLock` (zero-cost after first call)
//! - Direct Arrow array construction without intermediate allocations
//! - Minimal copies using references where possible
//!
//! **Schema**: TableDefinition provides consistent metadata for views

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanArray, Float32Array, Int16Array, Int32Array, Int64Array, StringArray,
        },
        datatypes::SchemaRef,
        record_batch::RecordBatch,
    },
    common::DFSchema,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::PhysicalExpr,
};
use kalamdb_commons::{
    datatypes::KalamDataType,
    schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
    NamespaceId, TableName,
};
use kalamdb_datafusion_sources::{
    exec::{finalize_deferred_batch, DeferredBatchExec, DeferredBatchSource},
    provider::{combined_filter, pushdown_results_for_filters, FilterCapability},
};
use kalamdb_raft::{ClusterInfo, CommandExecutor, RaftExecutor, ServerStateExt};
use kalamdb_system::SystemTable;

use crate::{error::RegistryError, view_base::VirtualView};

/// Get the cluster schema (memoized)
fn cluster_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            ClusterView::definition()
                .to_arrow_schema()
                .expect("Failed to convert cluster TableDefinition to Arrow schema")
        })
        .clone()
}

/// ClusterView - Displays live Raft cluster status
///
/// **Memory Efficiency**:
/// - No cached state (data fetched on-demand from OpenRaft)
/// - Only stores Arc to CommandExecutor (8 bytes pointer)
/// - Schema is memoized via `OnceLock`
///
/// **Query Performance**:
/// - O(N) where N = number of cluster nodes (typically 3-7)
/// - Single cluster info fetch per query
/// - Pre-allocated vectors avoid reallocation overhead
/// - Direct Arrow array construction
#[derive(Debug)]
pub struct ClusterView {
    /// Reference to the command executor (provides cluster info)
    /// Only 8 bytes - the actual executor is shared across the application
    executor: Arc<dyn CommandExecutor>,
}

impl ClusterView {
    /// Get the TableDefinition for system.cluster view
    ///
    /// Schema (25 columns):
    /// - cluster_id TEXT NOT NULL
    /// - node_id BIGINT NOT NULL
    /// - role TEXT NOT NULL
    /// - status TEXT NOT NULL
    /// - rpc_addr TEXT NOT NULL
    /// - api_addr TEXT NOT NULL
    /// - is_self BOOLEAN NOT NULL
    /// - is_leader BOOLEAN NOT NULL
    /// - groups_leading INT NOT NULL
    /// - total_groups INT NOT NULL
    /// - current_term BIGINT (nullable - OpenRaft metrics)
    /// - last_applied_log BIGINT (nullable)
    /// - leader_last_log_index BIGINT (nullable)
    /// - snapshot_index BIGINT (nullable)
    /// - catchup_progress_pct TINYINT (nullable)
    /// - replication_lag BIGINT (nullable)
    /// - hostname TEXT (nullable - node metadata)
    /// - version TEXT (nullable)
    /// - memory_mb BIGINT (nullable)
    /// - memory_usage_mb BIGINT (nullable - physical footprint on macOS, RSS elsewhere)
    /// - cpu_usage_percent FLOAT (nullable)
    /// - uptime_seconds BIGINT (nullable)
    /// - uptime_human TEXT (nullable)
    /// - os TEXT (nullable)
    /// - arch TEXT (nullable)
    pub fn definition() -> TableDefinition {
        let columns = vec![
            ColumnDefinition::new(
                1,
                "cluster_id",
                1,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Cluster identifier".to_string()),
            ),
            ColumnDefinition::new(
                2,
                "node_id",
                2,
                KalamDataType::BigInt,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Node ID within the cluster".to_string()),
            ),
            ColumnDefinition::new(
                3,
                "role",
                3,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Node role (leader, follower, learner, candidate)".to_string()),
            ),
            ColumnDefinition::new(
                4,
                "status",
                4,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Node status".to_string()),
            ),
            ColumnDefinition::new(
                5,
                "rpc_addr",
                5,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("RPC address for Raft communication".to_string()),
            ),
            ColumnDefinition::new(
                6,
                "api_addr",
                6,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("API address for client requests".to_string()),
            ),
            ColumnDefinition::new(
                7,
                "is_self",
                7,
                KalamDataType::Boolean,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Whether this is the current node".to_string()),
            ),
            ColumnDefinition::new(
                8,
                "is_leader",
                8,
                KalamDataType::Boolean,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Whether this node is the leader".to_string()),
            ),
            ColumnDefinition::new(
                9,
                "groups_leading",
                9,
                KalamDataType::Int,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Number of Raft groups this node leads".to_string()),
            ),
            ColumnDefinition::new(
                10,
                "total_groups",
                10,
                KalamDataType::Int,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Total number of Raft groups".to_string()),
            ),
            // OpenRaft metrics (nullable - may not be available for all nodes)
            ColumnDefinition::new(
                11,
                "current_term",
                11,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Current Raft term".to_string()),
            ),
            ColumnDefinition::new(
                12,
                "last_applied_log",
                12,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Last applied log index".to_string()),
            ),
            ColumnDefinition::new(
                13,
                "leader_last_log_index",
                13,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Leader's last log index".to_string()),
            ),
            ColumnDefinition::new(
                14,
                "snapshot_index",
                14,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Last snapshot index".to_string()),
            ),
            ColumnDefinition::new(
                15,
                "catchup_progress_pct",
                15,
                KalamDataType::SmallInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Catchup progress percentage (0-100)".to_string()),
            ),
            ColumnDefinition::new(
                16,
                "replication_lag",
                16,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Replication lag (entries behind leader)".to_string()),
            ),
            // Node metadata (replicated via OpenRaft membership)
            ColumnDefinition::new(
                17,
                "hostname",
                17,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Machine hostname".to_string()),
            ),
            ColumnDefinition::new(
                18,
                "version",
                18,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("KalamDB version".to_string()),
            ),
            ColumnDefinition::new(
                19,
                "memory_mb",
                19,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Total system memory in MB".to_string()),
            ),
            ColumnDefinition::new(
                20,
                "memory_usage_mb",
                20,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Current KalamDB process memory usage in MB".to_string()),
            ),
            ColumnDefinition::new(
                21,
                "cpu_usage_percent",
                21,
                KalamDataType::Float,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Current KalamDB process CPU usage percentage".to_string()),
            ),
            ColumnDefinition::new(
                22,
                "uptime_seconds",
                22,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("KalamDB server uptime in seconds".to_string()),
            ),
            ColumnDefinition::new(
                23,
                "uptime_human",
                23,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("KalamDB server uptime in compact human-readable form".to_string()),
            ),
            ColumnDefinition::new(
                24,
                "os",
                24,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Operating system".to_string()),
            ),
            ColumnDefinition::new(
                25,
                "arch",
                25,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("CPU architecture".to_string()),
            ),
        ];

        TableDefinition::new(
            NamespaceId::system(),
            TableName::new(SystemTable::Cluster.table_name()),
            TableType::System,
            columns,
            TableOptions::system(),
            Some("Live OpenRaft cluster status and metrics (read-only view)".to_string()),
        )
        .expect("Failed to create system.cluster view definition")
    }

    /// Create a new cluster view
    ///
    /// **Cost**: O(1) - just stores an Arc pointer
    pub fn new(executor: Arc<dyn CommandExecutor>) -> Self {
        Self { executor }
    }

    /// Get live cluster information from OpenRaft
    ///
    /// **Cost**: O(1) - CommandExecutor already has this data in memory
    fn get_cluster_info(&self) -> ClusterInfo {
        self.executor.get_cluster_info()
    }
}

impl VirtualView for ClusterView {
    fn system_table(&self) -> SystemTable {
        SystemTable::Cluster
    }

    fn schema(&self) -> SchemaRef {
        cluster_schema()
    }

    /// Compute a RecordBatch with current cluster status
    ///
    /// **Performance**: O(N) where N = number of nodes (typically 3-7)
    /// - Single cluster info fetch
    /// - Pre-allocated vectors with exact capacity
    /// - Zero intermediate string allocations (uses &str)
    /// - Direct Arrow array construction
    fn compute_batch(&self) -> Result<RecordBatch, RegistryError> {
        let info = self.get_cluster_info();
        let num_nodes = info.nodes.len();

        // Pre-allocate vectors with exact capacity to avoid reallocation
        let mut cluster_ids = Vec::with_capacity(num_nodes);
        let mut node_ids = Vec::with_capacity(num_nodes);
        let mut roles = Vec::with_capacity(num_nodes);
        let mut statuses = Vec::with_capacity(num_nodes);
        let mut rpc_addrs = Vec::with_capacity(num_nodes);
        let mut api_addrs = Vec::with_capacity(num_nodes);
        let mut is_selfs = Vec::with_capacity(num_nodes);
        let mut is_leaders = Vec::with_capacity(num_nodes);
        let mut groups_leading = Vec::with_capacity(num_nodes);
        let mut total_groups = Vec::with_capacity(num_nodes);
        let mut current_terms = Vec::with_capacity(num_nodes);
        let mut last_applied_logs = Vec::with_capacity(num_nodes);
        let mut leader_last_log_indexes = Vec::with_capacity(num_nodes);
        let mut snapshot_indexes = Vec::with_capacity(num_nodes);
        let mut catchup_progress_pcts = Vec::with_capacity(num_nodes);
        let mut replication_lags = Vec::with_capacity(num_nodes);
        // Node metadata columns
        let mut hostnames: Vec<Option<&str>> = Vec::with_capacity(num_nodes);
        let mut versions: Vec<Option<&str>> = Vec::with_capacity(num_nodes);
        let mut memory_mbs: Vec<Option<i64>> = Vec::with_capacity(num_nodes);
        let mut memory_usage_mbs: Vec<Option<i64>> = Vec::with_capacity(num_nodes);
        let mut cpu_usage_percents: Vec<Option<f32>> = Vec::with_capacity(num_nodes);
        let mut uptime_seconds: Vec<Option<i64>> = Vec::with_capacity(num_nodes);
        let mut uptime_humans: Vec<Option<&str>> = Vec::with_capacity(num_nodes);
        let mut oses: Vec<Option<&str>> = Vec::with_capacity(num_nodes);
        let mut archs: Vec<Option<&str>> = Vec::with_capacity(num_nodes);

        // Single pass through nodes - collect all data
        for node in &info.nodes {
            cluster_ids.push(info.cluster_id.as_str());
            node_ids.push(node.node_id.as_u64() as i64);
            roles.push(node.role.as_str());
            statuses.push(node.status.as_str());
            rpc_addrs.push(node.rpc_addr.as_str());
            api_addrs.push(node.api_addr.as_str());
            is_selfs.push(node.is_self);
            is_leaders.push(node.is_leader);
            groups_leading.push(node.groups_leading as i32);
            total_groups.push(node.total_groups as i32);
            current_terms.push(node.current_term.map(|v| v as i64));
            last_applied_logs.push(node.last_applied_log.map(|v| v as i64));
            leader_last_log_indexes.push(node.leader_last_log_index.map(|v| v as i64));
            snapshot_indexes.push(node.snapshot_index.map(|v| v as i64));
            catchup_progress_pcts.push(node.catchup_progress_pct.map(|v| v as i16));
            replication_lags.push(node.replication_lag.map(|v| v as i64));
            // Node metadata
            hostnames.push(node.hostname.as_deref());
            versions.push(node.version.as_deref());
            memory_mbs.push(node.memory_mb.map(|v| v as i64));
            memory_usage_mbs.push(node.memory_usage_mb.map(|v| v as i64));
            cpu_usage_percents.push(node.cpu_usage_percent);
            uptime_seconds.push(node.uptime_seconds.map(|v| v as i64));
            uptime_humans.push(node.uptime_human.as_deref());
            oses.push(node.os.as_deref());
            archs.push(node.arch.as_deref());
        }

        // Build RecordBatch with direct Arrow array construction
        RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(StringArray::from(cluster_ids)) as ArrayRef,
                Arc::new(Int64Array::from(node_ids)) as ArrayRef,
                Arc::new(StringArray::from(roles)) as ArrayRef,
                Arc::new(StringArray::from(statuses)) as ArrayRef,
                Arc::new(StringArray::from(rpc_addrs)) as ArrayRef,
                Arc::new(StringArray::from(api_addrs)) as ArrayRef,
                Arc::new(BooleanArray::from(is_selfs)) as ArrayRef,
                Arc::new(BooleanArray::from(is_leaders)) as ArrayRef,
                Arc::new(Int32Array::from(groups_leading)) as ArrayRef,
                Arc::new(Int32Array::from(total_groups)) as ArrayRef,
                Arc::new(Int64Array::from(current_terms)) as ArrayRef,
                Arc::new(Int64Array::from(last_applied_logs)) as ArrayRef,
                Arc::new(Int64Array::from(leader_last_log_indexes)) as ArrayRef,
                Arc::new(Int64Array::from(snapshot_indexes)) as ArrayRef,
                Arc::new(Int16Array::from(catchup_progress_pcts)) as ArrayRef,
                Arc::new(Int64Array::from(replication_lags)) as ArrayRef,
                // Node metadata columns
                Arc::new(StringArray::from(hostnames)) as ArrayRef,
                Arc::new(StringArray::from(versions)) as ArrayRef,
                Arc::new(Int64Array::from(memory_mbs)) as ArrayRef,
                Arc::new(Int64Array::from(memory_usage_mbs)) as ArrayRef,
                Arc::new(Float32Array::from(cpu_usage_percents)) as ArrayRef,
                Arc::new(Int64Array::from(uptime_seconds)) as ArrayRef,
                Arc::new(StringArray::from(uptime_humans)) as ArrayRef,
                Arc::new(StringArray::from(oses)) as ArrayRef,
                Arc::new(StringArray::from(archs)) as ArrayRef,
            ],
        )
        .map_err(|e| RegistryError::Other(format!("Failed to build cluster batch: {}", e)))
    }
}

/// Type for the cluster table provider (custom to enable async peer refresh in scan)
pub struct ClusterTableProvider {
    view: Arc<ClusterView>,
}

struct ClusterScanSource {
    view: Arc<ClusterView>,
    physical_filter: Option<Arc<dyn PhysicalExpr>>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    output_schema: SchemaRef,
}

impl std::fmt::Debug for ClusterScanSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterScanSource")
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

#[async_trait]
impl DeferredBatchSource for ClusterScanSource {
    fn source_name(&self) -> &'static str {
        "cluster_view_scan"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    async fn produce_batch(&self) -> datafusion::error::Result<RecordBatch> {
        if let Some(raft_executor) = self.view.executor.as_any().downcast_ref::<RaftExecutor>() {
            raft_executor.refresh_peer_stats().await;
        }

        let batch = self.view.compute_batch().map_err(|error| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to compute cluster batch: {}",
                error
            ))
        })?;

        finalize_deferred_batch(
            batch,
            self.physical_filter.as_ref(),
            self.projection.as_deref(),
            self.limit,
            self.source_name(),
        )
    }
}

impl std::fmt::Debug for ClusterTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterTableProvider").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::datasource::TableProvider for ClusterTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.view.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::View
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let base_schema = self.view.schema();
        let output_schema = match projection {
            Some(indices) => base_schema.project(indices).map(Arc::new).map_err(|error| {
                datafusion::error::DataFusionError::ArrowError(Box::new(error), None)
            })?,
            None => Arc::clone(&base_schema),
        };
        let physical_filter = if let Some(filter) = combined_filter(filters) {
            let df_schema = DFSchema::try_from(Arc::clone(&base_schema))?;
            Some(state.create_physical_expr(filter, &df_schema)?)
        } else {
            None
        };

        Ok(Arc::new(DeferredBatchExec::new(Arc::new(ClusterScanSource {
            view: Arc::clone(&self.view),
            physical_filter,
            projection: projection.cloned(),
            limit,
            output_schema,
        }))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(pushdown_results_for_filters(filters, |_| FilterCapability::Exact))
    }
}

/// Helper function to create a cluster table provider
///
/// **Usage**: Only called in cluster mode during AppContext initialization
pub fn create_cluster_provider(executor: Arc<dyn CommandExecutor>) -> ClusterTableProvider {
    ClusterTableProvider {
        view: Arc::new(ClusterView::new(executor)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema() {
        let schema = cluster_schema();
        assert_eq!(schema.fields().len(), 25);
        assert_eq!(schema.field(0).name(), "cluster_id");
        assert_eq!(schema.field(1).name(), "node_id");
        assert_eq!(schema.field(2).name(), "role");
        assert_eq!(schema.field(7).name(), "is_leader");
        // Node metadata columns
        assert_eq!(schema.field(16).name(), "hostname");
        assert_eq!(schema.field(17).name(), "version");
        assert_eq!(schema.field(18).name(), "memory_mb");
        assert_eq!(schema.field(19).name(), "memory_usage_mb");
        assert_eq!(schema.field(20).name(), "cpu_usage_percent");
        assert_eq!(schema.field(21).name(), "uptime_seconds");
        assert_eq!(schema.field(22).name(), "uptime_human");
        assert_eq!(schema.field(23).name(), "os");
        assert_eq!(schema.field(24).name(), "arch");
    }

    #[test]
    fn test_schema_singleton() {
        // Verify schema is only created once
        let schema1 = cluster_schema();
        let schema2 = cluster_schema();
        assert!(Arc::ptr_eq(&schema1, &schema2));
    }
}
