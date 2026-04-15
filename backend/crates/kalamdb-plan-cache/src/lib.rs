use datafusion::logical_expr::LogicalPlan;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::schemas::TableType;
use kalamdb_commons::{NamespaceId, Role, TableId};
use moka::sync::Cache;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_PLAN_MAX_ENTRIES: u64 = 1000;
const DEFAULT_IDLE_TTL_SECS: u64 = 900;
const DEFAULT_INSERT_METADATA_MAX_ENTRIES: u64 = 2048;

#[derive(Debug, Clone)]
pub struct SqlCacheRegistryConfig {
    pub plan_max_entries: u64,
    pub plan_idle_ttl: Duration,
    pub insert_metadata_max_entries: u64,
    pub insert_metadata_idle_ttl: Duration,
}

impl SqlCacheRegistryConfig {
    pub fn new(plan_max_entries: u64, plan_idle_ttl: Duration) -> Self {
        Self {
            plan_max_entries,
            plan_idle_ttl,
            insert_metadata_max_entries: DEFAULT_INSERT_METADATA_MAX_ENTRIES,
            insert_metadata_idle_ttl: Duration::from_secs(DEFAULT_IDLE_TTL_SECS),
        }
    }
}

impl Default for SqlCacheRegistryConfig {
    fn default() -> Self {
        Self {
            plan_max_entries: DEFAULT_PLAN_MAX_ENTRIES,
            plan_idle_ttl: Duration::from_secs(DEFAULT_IDLE_TTL_SECS),
            insert_metadata_max_entries: DEFAULT_INSERT_METADATA_MAX_ENTRIES,
            insert_metadata_idle_ttl: Duration::from_secs(DEFAULT_IDLE_TTL_SECS),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanCacheKey {
    pub namespace: NamespaceId,
    pub role: Role,
    pub sql: String,
}

impl PlanCacheKey {
    pub fn new(namespace: NamespaceId, role: Role, sql: impl Into<String>) -> Self {
        Self {
            namespace,
            role,
            sql: sql.into(),
        }
    }
}

pub struct PlanCache {
    cache: Cache<PlanCacheKey, Arc<LogicalPlan>>,
}

impl PlanCache {
    pub fn new() -> Self {
        Self::with_config(DEFAULT_PLAN_MAX_ENTRIES, Duration::from_secs(DEFAULT_IDLE_TTL_SECS))
    }

    pub fn with_config(max_entries: u64, idle_ttl: Duration) -> Self {
        let cache = Cache::builder().max_capacity(max_entries).time_to_idle(idle_ttl).build();
        Self { cache }
    }

    pub fn get(&self, cache_key: &PlanCacheKey) -> Option<Arc<LogicalPlan>> {
        self.cache.get(cache_key)
    }

    pub fn insert(&self, cache_key: PlanCacheKey, plan: LogicalPlan) {
        self.cache.insert(cache_key, Arc::new(plan));
    }

    pub fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks();
    }

    pub fn len(&self) -> usize {
        self.cache.run_pending_tasks();
        self.cache.entry_count() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.cache.run_pending_tasks();
        self.cache.entry_count() == 0
    }
}

impl Default for PlanCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InsertMetadataCacheKey {
    pub table_id: TableId,
    pub requested_columns: Vec<String>,
}

impl InsertMetadataCacheKey {
    pub fn new(table_id: TableId, requested_columns: Vec<String>) -> Self {
        Self {
            table_id,
            requested_columns,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FastInsertMetadata {
    pub table_type: TableType,
    pub column_names: Vec<String>,
    pub missing_defaults: Vec<FastInsertDefaultEntry>,
    pub primary_key_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FastInsertDefaultEntry {
    pub column_name: String,
    pub template: FastInsertDefaultTemplate,
}

impl FastInsertDefaultEntry {
    pub fn new(column_name: String, template: FastInsertDefaultTemplate) -> Self {
        Self {
            column_name,
            template,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FastInsertDefaultTemplate {
    Literal(ScalarValue),
    CurrentTimestamp,
    CurrentUser,
    SnowflakeId,
    UuidV7,
    Ulid,
}

pub struct InsertMetadataCache {
    cache: Cache<InsertMetadataCacheKey, Arc<FastInsertMetadata>>,
}

impl InsertMetadataCache {
    pub fn with_config(max_entries: u64, idle_ttl: Duration) -> Self {
        let cache = Cache::builder().max_capacity(max_entries).time_to_idle(idle_ttl).build();
        Self { cache }
    }

    pub fn get(&self, cache_key: &InsertMetadataCacheKey) -> Option<Arc<FastInsertMetadata>> {
        self.cache.get(cache_key)
    }

    pub fn insert_arc(&self, cache_key: InsertMetadataCacheKey, metadata: Arc<FastInsertMetadata>) {
        self.cache.insert(cache_key, metadata);
    }

    pub fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks();
    }
}

pub struct SqlCacheRegistry {
    plan_cache: PlanCache,
    insert_metadata_cache: InsertMetadataCache,
}

impl SqlCacheRegistry {
    pub fn new(config: SqlCacheRegistryConfig) -> Self {
        Self {
            plan_cache: PlanCache::with_config(config.plan_max_entries, config.plan_idle_ttl),
            insert_metadata_cache: InsertMetadataCache::with_config(
                config.insert_metadata_max_entries,
                config.insert_metadata_idle_ttl,
            ),
        }
    }

    pub fn plan_cache(&self) -> &PlanCache {
        &self.plan_cache
    }

    pub fn insert_metadata_cache(&self) -> &InsertMetadataCache {
        &self.insert_metadata_cache
    }

    pub fn clear(&self) {
        self.plan_cache.clear();
        self.insert_metadata_cache.clear();
    }
}

impl Default for SqlCacheRegistry {
    fn default() -> Self {
        Self::new(SqlCacheRegistryConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::EmptyRelation;
    use std::sync::Arc;

    #[test]
    fn clearing_registry_invalidates_all_caches() {
        let registry = SqlCacheRegistry::default();
        let insert_key =
            InsertMetadataCacheKey::new(TableId::from_strings("default", "events"), vec![]);

        registry.plan_cache().insert(
            PlanCacheKey::new(NamespaceId::new("default"), Role::User, "SELECT 1"),
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(DFSchema::empty()),
            }),
        );
        registry.insert_metadata_cache().insert_arc(
            insert_key.clone(),
            Arc::new(FastInsertMetadata {
                table_type: TableType::User,
                column_names: vec!["id".to_string()],
                missing_defaults: vec![],
                primary_key_column: Some("id".to_string()),
            }),
        );

        registry.clear();

        assert!(registry.plan_cache().is_empty());
        assert!(registry.insert_metadata_cache().get(&insert_key).is_none());
    }
}
