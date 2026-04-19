//! Stream Eviction Job Executor
//!
//! **Phase 9 (T149)**: JobExecutor implementation for stream table eviction
//!
//! Handles TTL-based eviction for stream tables.
//!
//! ## Responsibilities
//! - Enforce ttl_seconds policy for stream tables
//! - Delete expired records based on created_at timestamp
//! - Track eviction metrics (rows evicted, bytes freed)
//! - Support partial eviction with continuation
//!
//! ## Parameters Format
//! ```json
//! {
//!   "namespace_id": "default",
//!   "table_name": "events",
//!   "table_type": "Stream",
//!   "ttl_seconds": 86400,
//!   "batch_size": 10000
//! }
//! ```

use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use async_trait::async_trait;
use kalamdb_commons::ids::{SeqId, SnowflakeGenerator};
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::TableId;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::KalamDbResultExt;
use kalamdb_core::providers::StreamTableProvider;
#[cfg(test)]
use kalamdb_core::schema_registry::TablesSchemaRegistryAdapter;
#[cfg(test)]
use kalamdb_sharding::ShardRouter;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn default_batch_size() -> u64 {
    10000
}

/// Typed parameters for stream eviction operations (T193)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvictionParams {
    /// Table identifier (required)
    pub table_id: TableId,
    /// Table type (must be Stream - validated in validate())
    pub table_type: TableType,
    /// TTL in seconds (required, must be > 0)
    pub ttl_seconds: u64,
    /// Batch size for eviction (optional, defaults to 10000)
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
}

impl JobParams for StreamEvictionParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        if self.table_type != TableType::Stream {
            return Err(KalamDbError::InvalidOperation(format!(
                "table_type must be Stream, got: {:?}",
                self.table_type
            )));
        }
        if self.ttl_seconds == 0 {
            return Err(KalamDbError::InvalidOperation(
                "ttl_seconds must be greater than 0".to_string(),
            ));
        }
        if self.batch_size == 0 {
            return Err(KalamDbError::InvalidOperation(
                "batch_size must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Stream Eviction Job Executor
///
/// Executes TTL-based eviction operations for stream tables.
pub struct StreamEvictionExecutor;

impl StreamEvictionExecutor {
    /// Create a new StreamEvictionExecutor
    pub fn new() -> Self {
        Self
    }
}

fn compute_cutoff_window(ttl_seconds: u64) -> Result<(u64, SeqId), KalamDbError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .into_invalid_operation("System time error")?
        .as_millis() as u64;
    let ttl_ms = ttl_seconds.saturating_mul(1000);
    let cutoff_ms = now.saturating_sub(ttl_ms);
    let normalized_cutoff = cutoff_ms.max(SnowflakeGenerator::DEFAULT_EPOCH);
    let cutoff_seq = SeqId::max_id_for_timestamp(normalized_cutoff).map_err(|e| {
        KalamDbError::InvalidOperation(format!("Failed to compute cutoff snowflake id: {}", e))
    })?;
    Ok((cutoff_ms, cutoff_seq))
}

#[async_trait]
impl JobExecutor for StreamEvictionExecutor {
    type Params = StreamEvictionParams;

    fn job_type(&self) -> JobType {
        JobType::StreamEviction
    }

    fn name(&self) -> &'static str {
        "StreamEvictionExecutor"
    }

    async fn pre_validate(
        &self,
        app_ctx: &Arc<AppContext>,
        params: &Self::Params,
    ) -> Result<bool, KalamDbError> {
        params.validate()?;

        let schema_registry = app_ctx.schema_registry();
        let provider_arc = match schema_registry.get_provider(&params.table_id) {
            Some(p) => p,
            None => return Ok(false),
        };
        let stream_provider =
            provider_arc.as_any().downcast_ref::<StreamTableProvider>().ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Cached provider for {} is not a StreamTableProvider",
                    params.table_id
                ))
            })?;
        let store = stream_provider.store_arc();
        let (cutoff_ms, _cutoff_seq) = compute_cutoff_window(params.ttl_seconds)?;

        store.has_logs_before(cutoff_ms).map_err(|e| {
            KalamDbError::InvalidOperation(format!(
                "Failed to scan stream logs during pre-validation: {}",
                e
            ))
        })
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        ctx.log_info("Starting stream eviction operation");

        // Parameters already validated in JobContext - type-safe access
        let params = ctx.params();
        let table_id = params.table_id.clone();
        let ttl_seconds = params.ttl_seconds;
        let batch_size = params.batch_size;

        ctx.log_info(&format!(
            "Evicting expired records from stream {} (ttl: {}s, batch: {})",
            table_id, ttl_seconds, batch_size
        ));

        // Calculate cutoff time for eviction (records created before this time are expired)
        let (cutoff_ms, _cutoff_seq) = compute_cutoff_window(ttl_seconds)?;

        ctx.log_info(&format!(
            "Cutoff time: {}ms (records with timestamp < {} are expired)",
            cutoff_ms, cutoff_ms
        ));

        // Get table's StreamTableStore
        // Use the registered StreamTableProvider so we access the live in-memory store
        let schema_registry = ctx.app_ctx.schema_registry();
        let provider_arc = match schema_registry.get_provider(&table_id) {
            Some(p) => p,
            None => {
                ctx.log_warn(&format!(
                    "Stream provider not registered for {}; skipping eviction",
                    table_id
                ));
                return Ok(JobDecision::Completed {
                    message: Some(format!(
                        "Stream table {} not registered; nothing to evict",
                        table_id
                    )),
                });
            },
        };
        let stream_provider =
            provider_arc.as_any().downcast_ref::<StreamTableProvider>().ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "Cached provider for {} is not a StreamTableProvider",
                    table_id
                ))
            })?;
        let store = stream_provider.store_arc();
        let deleted_count = store.delete_old_logs(cutoff_ms).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to delete old stream logs: {}", e))
        })?;

        if deleted_count == 0 {
            ctx.log_info("No expired rows found; nothing to evict");
            return Ok(JobDecision::Completed {
                message: Some(format!("No expired rows found in {}", table_id)),
            });
        }

        ctx.log_info(&format!(
            "Stream eviction completed - {} log files removed from {}",
            deleted_count, table_id
        ));

        Ok(JobDecision::Completed {
            message: Some(format!(
                "Evicted {} expired log files from {} (ttl: {}s)",
                deleted_count, table_id, ttl_seconds
            )),
        })
    }

    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("Stream eviction job cancellation requested");
        // Allow cancellation since partial eviction is acceptable
        Ok(())
    }
}

impl Default for StreamEvictionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use datafusion::datasource::TableProvider;
    use kalamdb_commons::models::datatypes::KalamDataType;
    use kalamdb_commons::models::schemas::{
        ColumnDefinition, TableDefinition, TableOptions, TableType,
    };
    use kalamdb_commons::models::{TableId, TableName, UserId};
    use kalamdb_commons::{ChangeNotification, JobId, NamespaceId, NodeId};
    use kalamdb_core::app_context::AppContext;
    use kalamdb_core::providers::arrow_json_conversion::json_to_row;
    use kalamdb_core::providers::base::{BaseTableProvider, TableProviderCore};
    use kalamdb_core::providers::StreamTableProvider;
    use kalamdb_core::test_helpers::test_app_context_simple;
    use kalamdb_system::providers::jobs::models::Job;
    use kalamdb_system::NotificationService;
    use kalamdb_system::SchemaRegistry;
    use kalamdb_tables::utils::TableServices;
    use kalamdb_tables::StreamTableStoreConfig;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    fn make_job(id: &str, job_type: JobType, _ns: &str) -> Job {
        let now = chrono::Utc::now().timestamp_millis();
        Job {
            job_id: JobId::new(id),
            job_type,
            status: kalamdb_system::JobStatus::Running,
            leader_status: None,
            parameters: None,
            message: None,
            exception_trace: None,
            idempotency_key: None,
            retry_count: 0,
            max_retries: 3,
            memory_used: None,
            cpu_used: None,
            created_at: now,
            updated_at: now,
            started_at: Some(now),
            finished_at: None,
            node_id: NodeId::from(1u64),
            leader_node_id: None,
            queue: None,
            priority: None,
        }
    }

    struct StreamTestHarness {
        table_id: TableId,
        namespace: NamespaceId,
        table_name_value: String,
        provider: Arc<StreamTableProvider>,
        _stream_temp_dir: tempfile::TempDir,
    }

    fn setup_stream_table(app_ctx: &Arc<AppContext>) -> StreamTestHarness {
        let namespace = NamespaceId::new("chat_stream_jobs");
        let table_name_value =
            format!("typing_events_{}", Utc::now().timestamp_nanos_opt().unwrap_or(0));
        let tbl = TableName::new(&table_name_value);
        let table_id = TableId::new(namespace.clone(), tbl.clone());

        let table_def = TableDefinition::new(
            namespace.clone(),
            tbl.clone(),
            TableType::Stream,
            vec![
                ColumnDefinition::primary_key(1, "event_id", 1, KalamDataType::Text),
                ColumnDefinition::simple(2, "payload", 2, KalamDataType::Text),
            ],
            TableOptions::stream(1),
            None,
        )
        .expect("table definition");

        app_ctx.schema_registry().put(table_def).expect("Failed to put table def");

        let stream_temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let stream_store = Arc::new(kalamdb_tables::new_stream_table_store(
            &table_id,
            StreamTableStoreConfig {
                base_dir: stream_temp_dir
                    .path()
                    .join("streams")
                    .join(namespace.as_str())
                    .join(table_name_value.as_str()),
                max_rows_per_user: 256, // Default per-user retention limit
                shard_router: ShardRouter::default_config(),
                ttl_seconds: Some(1),
                storage_mode: kalamdb_tables::StreamTableStorageMode::Memory,
            },
        ));
        let tables_schema_registry =
            Arc::new(TablesSchemaRegistryAdapter::new(app_ctx.schema_registry()));
        let services = Arc::new(TableServices::new(
            tables_schema_registry.clone(),
            app_ctx.system_columns_service(),
            Some(app_ctx.storage_registry()),
            app_ctx.manifest_service(),
            Arc::clone(app_ctx.notification_service())
                as Arc<dyn NotificationService<Notification = ChangeNotification>>,
            app_ctx.clone(),
            app_ctx.commit_sequence_tracker(),
            Some(app_ctx.topic_publisher() as Arc<dyn kalamdb_system::TopicPublisher>),
        ));
        let arrow_schema =
            tables_schema_registry.get_arrow_schema(&table_id).expect("get arrow schema");
        let cached_data = app_ctx.schema_registry().get(&table_id).expect("table def");
        let core = Arc::new(TableProviderCore::new(
            Arc::clone(&cached_data.table),
            services,
            "event_id".to_string(),
            arrow_schema,
            HashMap::new(),
        ));
        let provider = Arc::new(StreamTableProvider::new(core, stream_store, Some(1)));
        let provider_trait: Arc<dyn TableProvider> = provider.clone();
        app_ctx
            .schema_registry()
            .insert_provider(table_id.clone(), provider_trait)
            .expect("register provider");

        StreamTestHarness {
            table_id,
            namespace,
            table_name_value,
            provider,
            _stream_temp_dir: stream_temp_dir,
        }
    }

    #[test]
    fn test_params_validation_success() {
        let params = StreamEvictionParams {
            table_id: TableId::new(
                NamespaceId::default(),
                kalamdb_commons::TableName::new("events"),
            ),
            table_type: TableType::Stream,
            ttl_seconds: 86400,
            batch_size: 10000,
        };
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_params_validation_invalid_table_type() {
        let params = StreamEvictionParams {
            table_id: TableId::new(
                NamespaceId::default(),
                kalamdb_commons::TableName::new("events"),
            ),
            table_type: TableType::User, // Wrong type
            ttl_seconds: 86400,
            batch_size: 10000,
        };
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_params_validation_zero_ttl() {
        let params = StreamEvictionParams {
            table_id: TableId::new(
                NamespaceId::default(),
                kalamdb_commons::TableName::new("events"),
            ),
            table_type: TableType::Stream,
            ttl_seconds: 0, // Invalid
            batch_size: 10000,
        };
        assert!(params.validate().is_err());
    }

    #[test]
    fn test_executor_properties() {
        let executor = StreamEvictionExecutor::new();
        assert_eq!(executor.job_type(), JobType::StreamEviction);
        assert_eq!(executor.name(), "StreamEvictionExecutor");
    }

    #[tokio::test]
    async fn test_execute_evicts_expired_rows_from_provider_store() {
        let app_ctx = test_app_context_simple();
        let harness = setup_stream_table(&app_ctx);
        let provider = harness.provider.clone();

        // Insert a couple of rows
        let user = UserId::new("user-ttl");
        provider
            .insert(&user, json_to_row(&json!({"event_id": "evt1", "payload": "hello"})).unwrap())
            .await
            .expect("insert evt1");
        provider
            .insert(&user, json_to_row(&json!({"event_id": "evt2", "payload": "world"})).unwrap())
            .await
            .expect("insert evt2");

        // Wait for TTL to make them eligible for eviction
        // Need to wait longer than TTL to ensure Snowflake timestamp is old enough
        sleep(Duration::from_millis(1500)).await;

        let mut job = make_job("SE-evict", JobType::StreamEviction, harness.namespace.as_str());
        job.parameters = Some(
            serde_json::json!({
                "namespace_id": harness.namespace.as_str(),
                "table_name": harness.table_name_value.clone(),
                "table_type": "Stream",
                "ttl_seconds": 1,
                "batch_size": 100
            }),
        );

        let params = StreamEvictionParams {
            table_id: harness.table_id.clone(),
            table_type: TableType::Stream,
            ttl_seconds: 1,
            batch_size: 100,
        };

        let ctx = JobContext::new(app_ctx.clone(), job.job_id.as_str().to_string(), params);
        let executor = StreamEvictionExecutor::new();
        let decision = executor.execute(&ctx).await.expect("execute eviction");

        match decision {
            JobDecision::Completed { message } => {
                assert!(message.unwrap().contains("Evicted"));
            },
            other => panic!("Expected Completed decision, got {:?}", other),
        }

        let remaining_rows =
            harness.provider.store_arc().scan_all(None).expect("scan store after eviction");

        assert_eq!(remaining_rows.len(), 0, "All expired rows should be removed");
    }

    #[tokio::test]
    async fn test_pre_validate_skips_when_no_expired_rows() {
        let app_ctx = test_app_context_simple();
        let harness = setup_stream_table(&app_ctx);

        let user = UserId::new("user-prevalidate-no-expired");
        harness
            .provider
            .insert(&user, json_to_row(&json!({"event_id": "evt1", "payload": "fresh"})).unwrap())
            .await
            .expect("insert fresh row");

        let params = StreamEvictionParams {
            table_id: harness.table_id.clone(),
            table_type: TableType::Stream,
            ttl_seconds: 60,
            batch_size: 100,
        };

        let executor = StreamEvictionExecutor::new();
        let should_run =
            executor.pre_validate(&app_ctx, &params).await.expect("pre_validate execution");

        assert!(!should_run, "No expired rows should skip job creation");
    }

    #[tokio::test]
    async fn test_pre_validate_detects_expired_rows() {
        let app_ctx = test_app_context_simple();
        let harness = setup_stream_table(&app_ctx);

        let user = UserId::new("user-prevalidate-expired");
        harness
            .provider
            .insert(&user, json_to_row(&json!({"event_id": "evt1", "payload": "expired"})).unwrap())
            .await
            .expect("insert expired row");

        sleep(Duration::from_millis(1200)).await;

        let params = StreamEvictionParams {
            table_id: harness.table_id.clone(),
            table_type: TableType::Stream,
            ttl_seconds: 1,
            batch_size: 10,
        };

        let executor = StreamEvictionExecutor::new();
        let should_run =
            executor.pre_validate(&app_ctx, &params).await.expect("pre_validate execution");

        assert!(should_run, "Expired rows should trigger job creation");
    }
}
