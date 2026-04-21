//! Base traits and utilities for system table providers.
//!
//! This module centralizes the deferred scan plumbing shared by system table
//! providers so planning stays lightweight and provider families do not keep
//! reimplementing the same DataFusion boilerplate.
//!
//! ## Key Components
//!
//! - [`SystemTableScan`]: Trait for indexed system tables with unified scan logic
//! - [`SimpleSystemTableScan`]: Trait for non-indexed/simple system tables
//! - Shared deferred execution sources for lightweight planning

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, DFSchema};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use kalamdb_datafusion_sources::exec::{
    finalize_deferred_batch, DeferredBatchExec, DeferredBatchSource,
};
use kalamdb_datafusion_sources::pruning::{
    FilterRequest, LimitRequest, ProjectionRequest, PruningRequest,
};
use kalamdb_datafusion_sources::provider::{combined_filter, pushdown_results_for_filters, FilterCapability};
use kalamdb_commons::conversions::json_rows_to_arrow_batch;
use kalamdb_commons::models::rows::{Row, SystemTableRow};
use kalamdb_commons::{KSerializable, StorageKey};
use kalamdb_store::{EntityStore, IndexedEntityStore};

use crate::error::SystemError;

/// Result type for DataFusion operations
pub type DataFusionResult<T> = Result<T, DataFusionError>;

/// Shared static metadata for indexed system table providers.
///
/// Providers build this once and reuse it across:
/// - `SystemTableScan` implementation (table_name, schema, key parsing)
/// - `TableProvider` implementation (schema)
#[derive(Clone, Copy)]
pub struct IndexedProviderDefinition<K> {
    pub table_name: &'static str,
    pub primary_key_column: &'static str,
    pub schema: fn() -> arrow::datatypes::SchemaRef,
    pub parse_key: fn(&str) -> Option<K>,
}

/// Shared static metadata for non-indexed/simple system table providers.
#[derive(Clone, Copy)]
pub struct SimpleProviderDefinition {
    pub table_name: &'static str,
    pub schema: fn() -> arrow::datatypes::SchemaRef,
}

/// Exact pushdown strategy for providers that evaluate filters inside their
/// deferred execution source before returning batches.
pub fn exact_filter_pushdown(
    filters: &[&Expr],
) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
    Ok(pushdown_results_for_filters(filters, |_| FilterCapability::Exact))
}

/// Shared conversion path for system table scan serialization.
///
/// Converts `SystemTableRow` values into Arrow using the same ScalarValue->Arrow
/// coercion path used by shared/user tables.
pub fn system_rows_to_batch(
    schema: &arrow::datatypes::SchemaRef,
    rows: Vec<SystemTableRow>,
) -> Result<RecordBatch, SystemError> {
    let rows: Vec<Row> = rows.into_iter().map(|row| row.fields).collect();
    json_rows_to_arrow_batch(schema, rows)
        .map_err(|e| SystemError::SerializationError(format!("system rows to batch failed: {e}")))
}

fn scan_limit_for_filters(filters: &FilterRequest, limit: LimitRequest) -> Option<usize> {
    if filters.filters.is_empty() {
        limit.limit
    } else {
        None
    }
}

fn build_indexed_batch<P, K, V>(
    provider: P,
    filters: Vec<Expr>,
    limit: Option<usize>,
) -> DataFusionResult<RecordBatch>
where
    P: SystemTableScan<K, V> + Clone + Send + Sync + 'static,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    use datafusion::logical_expr::Operator;
    use datafusion::scalar::ScalarValue;

    let mut start_key: Option<K> = None;
    let mut prefix: Option<K> = None;
    let pk_column = provider.primary_key_column();
    for expr in &filters {
        if let Expr::BinaryExpr(binary) = expr {
            if let Expr::Column(col) = binary.left.as_ref() {
                if let Expr::Literal(val, _) = binary.right.as_ref() {
                    if col.name == pk_column {
                        if let ScalarValue::Utf8(Some(value)) = val {
                            match binary.op {
                                Operator::Eq => prefix = provider.parse_key(value),
                                Operator::Gt | Operator::GtEq => {
                                    start_key = provider.parse_key(value)
                                },
                                _ => {},
                            }
                        }
                    }
                }
            }
        }
    }

    // Provider-side filters run after this store iteration. Pushing LIMIT down
    // under those filters can underfetch matching rows, so only use it when the
    // scan has no provider-evaluated filter work left to do.
    let filter_request = FilterRequest::new(filters.clone());
    let limit_request = LimitRequest::new(limit);
    let scan_limit = scan_limit_for_filters(&filter_request, limit_request);
    let store = provider.store();
    let mut pairs: Vec<(K, V)> = Vec::new();
    if let Some((index_idx, index_prefix)) = store.find_best_index_for_filters(&filters) {
        let iter = store
            .scan_by_index_iter(index_idx, Some(&index_prefix), scan_limit)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "Failed to scan {} by index: {}",
                    provider.table_name(),
                    error
                ))
            })?;

        let effective_limit = scan_limit.unwrap_or(100_000);
        for result in iter {
            let (key, value) = result.map_err(|error| {
                DataFusionError::Execution(format!(
                    "Failed to scan {} by index: {}",
                    provider.table_name(),
                    error
                ))
            })?;
            pairs.push((key, value));
            if pairs.len() >= effective_limit {
                break;
            }
        }
    } else {
        let iter = store
            .scan_iterator(prefix.as_ref(), start_key.as_ref())
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "Failed to create iterator for {}: {}",
                    provider.table_name(),
                    error
                ))
            })?;

        let effective_limit = scan_limit.unwrap_or(100_000);
        for result in iter {
            match result {
                Ok((key, value)) => {
                    pairs.push((key, value));
                    if pairs.len() >= effective_limit {
                        break;
                    }
                },
                Err(error) => {
                    log::warn!(
                        "Error during scan of {}: {}",
                        provider.table_name(),
                        error
                    );
                },
            }
        }
    }

    provider.create_batch_from_pairs(pairs).map_err(|error| {
        DataFusionError::Execution(format!(
            "Failed to build {} batch: {}",
            provider.table_name(),
            error
        ))
    })
}

/// Trait for indexed system table providers with unified scan logic.
///
/// This trait provides a common implementation for the `scan()` method
/// used by all system table providers, reducing code duplication.
///
/// ## Features
/// - Automatic filter extraction for prefix/start_key scans
/// - Secondary index lookup via `find_best_index_for_filters`
/// - Deferred execution so store iteration runs at execute time, not planning time
/// - Consistent error handling and logging
///
/// ## Example Implementation
/// ```rust,ignore
/// impl SystemTableScan<UserId, User> for UsersTableProvider {
///     fn store(&self) -> &IndexedEntityStore<UserId, User> { &self.store }
///     fn table_name(&self) -> &str { "system.users" }
///     fn primary_key_column(&self) -> &str { "user_id" }
///     fn arrow_schema(&self) -> SchemaRef { Self::schema() }
///     fn create_batch_from_pairs(&self, pairs: Vec<(UserId, User)>) -> Result<RecordBatch, SystemError> { ... }
/// }
/// ```
struct IndexedSystemScanSource<P, K, V>
where
    P: SystemTableScan<K, V> + Clone + Send + Sync + 'static,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    provider: P,
    pruning: PruningRequest,
    physical_filter: Option<Arc<dyn PhysicalExpr>>,
    output_schema: arrow::datatypes::SchemaRef,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<P, K, V> std::fmt::Debug for IndexedSystemScanSource<P, K, V>
where
    P: SystemTableScan<K, V> + Clone + Send + Sync + 'static,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedSystemScanSource")
            .field("table_name", &self.provider.table_name())
            .field("projection", &self.pruning.projection.columns)
            .field("limit", &self.pruning.limit.limit)
            .field("filter_count", &self.pruning.filters.filters.len())
            .finish()
    }
}

#[async_trait]
impl<P, K, V> DeferredBatchSource for IndexedSystemScanSource<P, K, V>
where
    P: SystemTableScan<K, V> + Clone + Send + Sync + 'static,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    fn source_name(&self) -> &'static str {
        "indexed_system_scan"
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.output_schema)
    }

    async fn produce_batch(&self) -> DataFusionResult<RecordBatch> {
        let provider = self.provider.clone();
        let filters = self.pruning.filters.filters.as_ref().to_vec();
        let limit = self.pruning.limit.limit;
        let table_name = self.provider.table_name().to_string();
        let batch = tokio::task::spawn_blocking(move || build_indexed_batch(provider, filters, limit))
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "{} scan task failed: {}",
                    table_name, error
                ))
            })??;

        finalize_deferred_batch(
            batch,
            self.physical_filter.as_ref(),
            self.pruning.projection.columns.as_deref(),
            self.pruning.limit.limit,
            self.source_name(),
        )
    }
}

struct SimpleSystemScanSource<P, K, V>
where
    P: SimpleSystemTableScan<K, V> + Clone + Send + Sync + 'static,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    provider: P,
    pruning: PruningRequest,
    physical_filter: Option<Arc<dyn PhysicalExpr>>,
    output_schema: arrow::datatypes::SchemaRef,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<P, K, V> std::fmt::Debug for SimpleSystemScanSource<P, K, V>
where
    P: SimpleSystemTableScan<K, V> + Clone + Send + Sync + 'static,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleSystemScanSource")
            .field("table_name", &self.provider.table_name())
            .field("projection", &self.pruning.projection.columns)
            .field("limit", &self.pruning.limit.limit)
            .field("filter_count", &self.pruning.filters.filters.len())
            .finish()
    }
}

#[async_trait]
impl<P, K, V> DeferredBatchSource for SimpleSystemScanSource<P, K, V>
where
    P: SimpleSystemTableScan<K, V> + Clone + Send + Sync + 'static,
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    fn source_name(&self) -> &'static str {
        "simple_system_scan"
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.output_schema)
    }

    async fn produce_batch(&self) -> DataFusionResult<RecordBatch> {
        let provider = self.provider.clone();
        let filters = self.pruning.filters.filters.as_ref().to_vec();
        let limit = scan_limit_for_filters(&self.pruning.filters, self.pruning.limit);
        let table_name = self.provider.table_name().to_string();
        let batch = tokio::task::spawn_blocking(move || {
            provider.scan_to_batch(&filters, limit).map_err(|error| {
                DataFusionError::Execution(format!(
                    "Failed to build {} batch: {}",
                    provider.table_name(),
                    error
                ))
            })
        })
        .await
        .map_err(|error| {
            DataFusionError::Execution(format!("{} scan task failed: {}", table_name, error))
        })??;

        finalize_deferred_batch(
            batch,
            self.physical_filter.as_ref(),
            self.pruning.projection.columns.as_deref(),
            self.pruning.limit.limit,
            self.source_name(),
        )
    }
}

#[async_trait]
pub trait SystemTableScan<K, V>: Send + Sync + Clone
where
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    /// Returns a reference to the indexed entity store
    fn store(&self) -> &IndexedEntityStore<K, V>;

    /// Returns the table name for logging (e.g., "system.users")
    fn table_name(&self) -> &str;

    /// Returns the primary key column name for filter extraction (e.g., "user_id")
    fn primary_key_column(&self) -> &str;

    /// Returns the Arrow schema for this table
    fn arrow_schema(&self) -> arrow::datatypes::SchemaRef;

    /// Parse a string value into the key type (for filter extraction)
    fn parse_key(&self, value: &str) -> Option<K>;

    /// Create a RecordBatch from key-value pairs
    ///
    /// Implementations should convert their entity type to Arrow arrays.
    fn create_batch_from_pairs(&self, pairs: Vec<(K, V)>) -> Result<RecordBatch, SystemError>;

    /// Default scan implementation with deferred execution.
    ///
    /// Planning stays lightweight: `scan()` captures the filter, projection,
    /// and optional limit, while the actual store iteration and batch building
    /// happen inside the shared deferred execution node.
    async fn base_system_scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>>
    where
        Self: Sized + 'static,
    {
        let schema = self.arrow_schema();
        let pruning = PruningRequest::new(
            match projection {
                Some(indices) => ProjectionRequest::columns(indices.clone()),
                None => ProjectionRequest::full(),
            },
            FilterRequest::new(filters.to_vec()),
            LimitRequest::new(limit),
        );
        let output_schema = match projection {
            Some(indices) => schema
                .project(indices)
                .map(Arc::new)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?,
            None => Arc::clone(&schema),
        };
        let physical_filter = if let Some(filter) = combined_filter(filters) {
            let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
            Some(state.create_physical_expr(filter, &df_schema)?)
        } else {
            None
        };

        Ok(Arc::new(DeferredBatchExec::new(Arc::new(
            IndexedSystemScanSource::<Self, K, V> {
                provider: self.clone(),
                pruning,
                physical_filter,
                output_schema,
                _marker: std::marker::PhantomData,
            },
        ))))
    }
}

/// Trait for simple system tables without secondary indexes
///
/// For tables like namespaces, storages that don't have IndexedEntityStore.
///
/// ## Performance
///
/// Override `scan_to_batch()` to enable filter/limit-aware scanning.
/// The default falls back to `scan_all_to_batch()` (full table scan).
///
/// When `scan_to_batch()` is overridden, providers can:
/// - Use primary key equality filters for O(1) point lookups
/// - Respect `LIMIT` at the iterator level (early termination)
/// - Avoid materializing the entire table for simple queries
#[async_trait]
pub trait SimpleSystemTableScan<K, V>: Send + Sync + Clone
where
    K: StorageKey + Clone + Send + Sync + 'static,
    V: KSerializable + Clone + Send + Sync + 'static,
{
    /// Returns the table name for logging
    fn table_name(&self) -> &str;

    /// Returns the Arrow schema for this table
    fn arrow_schema(&self) -> arrow::datatypes::SchemaRef;

    /// Scan all entries and return as RecordBatch (full table scan).
    ///
    /// Used as fallback when `scan_to_batch` is not overridden.
    fn scan_all_to_batch(&self) -> Result<RecordBatch, SystemError>;

    /// Scan entries with optional filter and limit support.
    ///
    /// Override this method to enable optimized scanning:
    /// - Primary key equality filter → point lookup via `EntityStore::get()`
    /// - Limit → iterator with early termination
    ///
    /// Default falls back to `scan_all_to_batch()` (full table scan).
    fn scan_to_batch(
        &self,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<RecordBatch, SystemError> {
        self.scan_all_to_batch()
    }

    /// Default scan implementation for simple tables.
    ///
    /// Planning only captures the requested work. The concrete table read and
    /// `RecordBatch` construction happen in the shared deferred execution node.
    async fn base_simple_scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>>
    where
        Self: Sized + 'static,
    {
        let schema = self.arrow_schema();
        let pruning = PruningRequest::new(
            match projection {
                Some(indices) => ProjectionRequest::columns(indices.clone()),
                None => ProjectionRequest::full(),
            },
            FilterRequest::new(filters.to_vec()),
            LimitRequest::new(limit),
        );
        let output_schema = match projection {
            Some(indices) => schema
                .project(indices)
                .map(Arc::new)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?,
            None => Arc::clone(&schema),
        };
        let physical_filter = if let Some(filter) = combined_filter(filters) {
            let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
            Some(state.create_physical_expr(filter, &df_schema)?)
        } else {
            None
        };

        Ok(Arc::new(DeferredBatchExec::new(Arc::new(
            SimpleSystemScanSource::<Self, K, V> {
                provider: self.clone(),
                pruning,
                physical_filter,
                output_schema,
                _marker: std::marker::PhantomData,
            },
        ))))
    }
}

/// Helper function to extract string equality filter value for a column
pub fn extract_filter_value(filters: &[Expr], column_name: &str) -> Option<String> {
    use datafusion::logical_expr::Operator;
    use datafusion::scalar::ScalarValue;

    for expr in filters {
        if let Expr::BinaryExpr(binary) = expr {
            if let Expr::Column(col) = binary.left.as_ref() {
                if col.name == column_name && binary.op == Operator::Eq {
                    if let Expr::Literal(ScalarValue::Utf8(Some(s)), _) = binary.right.as_ref() {
                        return Some(s.clone());
                    }
                }
            }
        }
    }
    None
}

/// Helper function to extract range filter values for a column
pub fn extract_range_filters(
    filters: &[Expr],
    column_name: &str,
) -> (Option<String>, Option<String>) {
    use datafusion::logical_expr::Operator;
    use datafusion::scalar::ScalarValue;

    let mut start = None;
    let mut end = None;

    for expr in filters {
        if let Expr::BinaryExpr(binary) = expr {
            if let Expr::Column(col) = binary.left.as_ref() {
                if col.name == column_name {
                    if let Expr::Literal(ScalarValue::Utf8(Some(s)), _) = binary.right.as_ref() {
                        match binary.op {
                            Operator::Gt | Operator::GtEq => start = Some(s.clone()),
                            Operator::Lt | Operator::LtEq => end = Some(s.clone()),
                            _ => {},
                        }
                    }
                }
            }
        }
    }
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::collect;
    use kalamdb_commons::{KSerializable, StorageKey};
    use kalamdb_store::test_utils::InMemoryBackend;
    use kalamdb_store::{IndexedEntityStore, StorageBackend};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct DummyValue {
        value: String,
    }

    impl KSerializable for DummyValue {}

    struct RecordingBackend {
        inner: InMemoryBackend,
        last_scan: Mutex<Option<(Option<Vec<u8>>, Option<Vec<u8>>, Option<usize>)>>,
        scan_calls: AtomicUsize,
    }

    impl RecordingBackend {
        fn new() -> Self {
            Self {
                inner: InMemoryBackend::new(),
                last_scan: Mutex::new(None),
                scan_calls: AtomicUsize::new(0),
            }
        }

        fn last_scan(&self) -> Option<(Option<Vec<u8>>, Option<Vec<u8>>, Option<usize>)> {
            self.last_scan.lock().unwrap().clone()
        }

        fn scan_calls(&self) -> usize {
            self.scan_calls.load(Ordering::SeqCst)
        }
    }

    impl StorageBackend for RecordingBackend {
        fn get(
            &self,
            partition: &kalamdb_commons::storage::Partition,
            key: &[u8],
        ) -> kalamdb_store::storage_trait::Result<Option<Vec<u8>>> {
            self.inner.get(partition, key)
        }

        fn put(
            &self,
            partition: &kalamdb_commons::storage::Partition,
            key: &[u8],
            value: &[u8],
        ) -> kalamdb_store::storage_trait::Result<()> {
            self.inner.put(partition, key, value)
        }

        fn delete(
            &self,
            partition: &kalamdb_commons::storage::Partition,
            key: &[u8],
        ) -> kalamdb_store::storage_trait::Result<()> {
            self.inner.delete(partition, key)
        }

        fn batch(
            &self,
            operations: Vec<kalamdb_store::storage_trait::Operation>,
        ) -> kalamdb_store::storage_trait::Result<()> {
            self.inner.batch(operations)
        }

        fn scan(
            &self,
            partition: &kalamdb_commons::storage::Partition,
            prefix: Option<&[u8]>,
            start_key: Option<&[u8]>,
            limit: Option<usize>,
        ) -> kalamdb_store::storage_trait::Result<kalamdb_commons::storage::KvIterator<'_>>
        {
            *self.last_scan.lock().unwrap() =
                Some((prefix.map(|p| p.to_vec()), start_key.map(|k| k.to_vec()), limit));
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.scan(partition, prefix, start_key, limit)
        }

        fn partition_exists(&self, partition: &kalamdb_commons::storage::Partition) -> bool {
            self.inner.partition_exists(partition)
        }

        fn create_partition(
            &self,
            partition: &kalamdb_commons::storage::Partition,
        ) -> kalamdb_store::storage_trait::Result<()> {
            self.inner.create_partition(partition)
        }

        fn list_partitions(
            &self,
        ) -> kalamdb_store::storage_trait::Result<Vec<kalamdb_commons::storage::Partition>>
        {
            self.inner.list_partitions()
        }

        fn drop_partition(
            &self,
            partition: &kalamdb_commons::storage::Partition,
        ) -> kalamdb_store::storage_trait::Result<()> {
            self.inner.drop_partition(partition)
        }

        fn compact_partition(
            &self,
            partition: &kalamdb_commons::storage::Partition,
        ) -> kalamdb_store::storage_trait::Result<()> {
            self.inner.compact_partition(partition)
        }

        fn stats(&self) -> kalamdb_store::storage_trait::StorageStats {
            self.inner.stats()
        }
    }

    #[derive(Clone)]
    struct DummyProvider {
        store: IndexedEntityStore<kalamdb_commons::models::UserId, DummyValue>,
    }

    impl DummyProvider {
        fn new(backend: Arc<dyn StorageBackend>) -> Self {
            let store = IndexedEntityStore::new(backend, "system_dummy", Vec::new());
            Self { store }
        }
    }

    #[async_trait::async_trait]
    impl SystemTableScan<kalamdb_commons::models::UserId, DummyValue> for DummyProvider {
        fn store(&self) -> &IndexedEntityStore<kalamdb_commons::models::UserId, DummyValue> {
            &self.store
        }

        fn table_name(&self) -> &str {
            "system.dummy"
        }

        fn primary_key_column(&self) -> &str {
            "user_id"
        }

        fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]))
        }

        fn parse_key(&self, value: &str) -> Option<kalamdb_commons::models::UserId> {
            Some(kalamdb_commons::models::UserId::new(value))
        }

        fn create_batch_from_pairs(
            &self,
            pairs: Vec<(kalamdb_commons::models::UserId, DummyValue)>,
        ) -> Result<RecordBatch, SystemError> {
            let mut ids = Vec::with_capacity(pairs.len());
            let mut values = Vec::with_capacity(pairs.len());
            for (id, value) in pairs {
                ids.push(Some(id.as_str().to_string()));
                values.push(Some(value.value));
            }
            RecordBatch::try_new(
                self.arrow_schema(),
                vec![Arc::new(StringArray::from(ids)), Arc::new(StringArray::from(values))],
            )
                .map_err(|e| SystemError::Other(e.to_string()))
        }
    }

    #[derive(Clone)]
    struct DummySimpleProvider {
        limits: Arc<Mutex<Vec<Option<usize>>>>,
    }

    impl DummySimpleProvider {
        fn new() -> Self {
            Self {
                limits: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn arrow_schema_impl() -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]))
        }

        fn build_batch() -> Result<RecordBatch, SystemError> {
            RecordBatch::try_new(
                Self::arrow_schema_impl(),
                vec![
                    Arc::new(StringArray::from(vec![Some("u1"), Some("u2")])),
                    Arc::new(StringArray::from(vec![Some("miss"), Some("match")])),
                ],
            )
            .map_err(|error| SystemError::Other(error.to_string()))
        }
    }

    #[async_trait::async_trait]
    impl SimpleSystemTableScan<kalamdb_commons::models::UserId, DummyValue> for DummySimpleProvider {
        fn table_name(&self) -> &str {
            "system.dummy_simple"
        }

        fn arrow_schema(&self) -> arrow::datatypes::SchemaRef {
            Self::arrow_schema_impl()
        }

        fn scan_all_to_batch(&self) -> Result<RecordBatch, SystemError> {
            Self::build_batch()
        }

        fn scan_to_batch(
            &self,
            _filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<RecordBatch, SystemError> {
            self.limits.lock().unwrap().push(limit);
            Self::build_batch()
        }
    }

    #[tokio::test]
    async fn test_base_system_scan_uses_prefix_for_pk_filter() {
        let backend = Arc::new(RecordingBackend::new());
        let provider = DummyProvider::new(backend.clone());

        let user_id = kalamdb_commons::models::UserId::new("u1");
        provider
            .store
            .insert(
                &user_id,
                &DummyValue {
                    value: "v".to_string(),
                },
            )
            .unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let filter = col("user_id").eq(lit("u1"));

        let plan = provider.base_system_scan(&state, None, &[filter], None).await.unwrap();

        // Deferred scans should not touch storage during planning.
        assert_eq!(backend.scan_calls(), 0);

        let batches = collect(plan, state.task_ctx())
            .await
            .expect("collect deferred system scan");
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);

        assert_eq!(backend.scan_calls(), 1);
        let last = backend.last_scan().expect("missing scan");
        assert_eq!(last.0, Some(user_id.storage_key()));
        assert_eq!(last.1, None);
    }

    #[tokio::test]
    async fn test_base_system_scan_does_not_underfetch_filtered_limit() {
        let backend = Arc::new(RecordingBackend::new());
        let provider = DummyProvider::new(backend);

        provider
            .store
            .insert(
                &kalamdb_commons::models::UserId::new("u1"),
                &DummyValue {
                    value: "miss".to_string(),
                },
            )
            .unwrap();
        provider
            .store
            .insert(
                &kalamdb_commons::models::UserId::new("u2"),
                &DummyValue {
                    value: "match".to_string(),
                },
            )
            .unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let filter = col("value").eq(lit("match"));

        let plan = provider
            .base_system_scan(&state, None, &[filter], Some(1))
            .await
            .unwrap();

        let batches = collect(plan, state.task_ctx())
            .await
            .expect("collect filtered system scan");
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);

        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec!["u2".to_string()]);
    }

    #[tokio::test]
    async fn test_base_simple_scan_skips_limit_pushdown_when_filters_exist() {
        let provider = DummySimpleProvider::new();
        let limits = Arc::clone(&provider.limits);
        let ctx = SessionContext::new();
        let state = ctx.state();
        let filter = col("value").eq(lit("match"));

        let plan = provider
            .base_simple_scan(&state, None, &[filter], Some(1))
            .await
            .unwrap();

        let batches = collect(plan, state.task_ctx())
            .await
            .expect("collect filtered simple scan");
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        assert_eq!(limits.lock().unwrap().as_slice(), &[None]);
    }
}
