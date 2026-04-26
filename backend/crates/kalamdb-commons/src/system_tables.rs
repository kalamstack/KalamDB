//! System table and view enumeration
//!
//! Defines all system tables and views available in KalamDB.
//! This is the authoritative list of all `system.*` objects.

/// System table and view enumeration
///
/// All system tables and views in KalamDB. This enum ensures type-safe registration
/// and prevents typos in table/view names.
///
/// ## Tables vs Views
/// - **Tables**: Persisted in RocksDB, have a column family
/// - **Views**: Computed on-demand, no storage backing
use crate::constants::ColumnFamilyNames;
use crate::models::TableId;

/// Memory/performance profile applied to a storage partition or physical RocksDB CF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnFamilyProfile {
    /// System metadata tables and compatibility partitions.
    SystemMeta,
    /// System-maintained secondary indexes.
    SystemIndex,
    /// User/shared/stream data partitions and append-heavy message stores.
    HotData,
    /// PK and vector indexes that are latency-sensitive but smaller than data CFs.
    HotIndex,
    /// Raft log/state CF.
    Raft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SystemTable {
    // ==================== PERSISTED TABLES ====================
    /// system.users - User accounts (persisted)
    Users,
    /// system.namespaces - Database namespaces (persisted)
    Namespaces,
    /// system.schemas - All table schemas and their histories (persisted)
    Schemas,
    /// system.table_schemas - Table schema versions (persisted)
    TableSchemas,
    /// system.storages - Storage configurations (persisted)
    Storages,
    /// system.jobs - Background job tracking (persisted)
    Jobs,
    /// system.job_nodes - Per-node job execution state (persisted)
    JobNodes,
    /// system.audit_log - Administrative audit trail (persisted)
    AuditLog,
    /// system.manifest - Manifest cache entries for query optimization (persisted)
    Manifest,
    /// system.topics - Durable pub/sub topics (persisted)
    Topics,
    /// system.topic_offsets - Consumer group offset tracking (persisted)
    TopicOffsets,

    // ==================== VIRTUAL VIEWS ====================
    /// system.stats - Runtime metrics (computed on-demand)
    Stats,
    /// system.live - Active in-memory live subscriptions (computed on-demand)
    Live,
    /// system.sessions - Active PostgreSQL gRPC sessions (computed on-demand)
    Sessions,
    /// system.transactions - Active explicit transactions across all origins (computed on-demand)
    Transactions,
    /// system.settings - Server configuration settings (computed on-demand)
    Settings,
    /// system.server_logs - Server log entries (computed from log files)
    ServerLogs,
    /// system.cluster - Raft cluster status and metrics (computed on-demand)
    Cluster,
    /// system.cluster_groups - Per-Raft-group membership and replication status (computed
    /// on-demand)
    ClusterGroups,
    /// system.datatypes - Supported data type mappings (computed on-demand)
    Datatypes,
    /// system.describe - DESCRIBE TABLE functionality (computed on-demand)
    Describe,
    /// system.tables - Table metadata view (computed from system.schemas)
    Tables,
    /// system.columns - Column metadata view (computed from system.schemas)
    Columns,
}

impl SystemTable {
    /// Get the table name as used in SQL (e.g., "users", "tables")
    pub fn table_name(&self) -> &'static str {
        match self {
            SystemTable::Users => "users",
            SystemTable::Namespaces => "namespaces",
            SystemTable::Schemas => "schemas",
            SystemTable::TableSchemas => "table_schemas",
            SystemTable::Storages => "storages",
            SystemTable::Jobs => "jobs",
            SystemTable::JobNodes => "job_nodes",
            SystemTable::AuditLog => "audit_log",
            SystemTable::Manifest => "manifest",
            SystemTable::Topics => "topics",
            SystemTable::TopicOffsets => "topic_offsets",
            // Views
            SystemTable::Stats => "stats",
            SystemTable::Live => "live",
            SystemTable::Sessions => "sessions",
            SystemTable::Transactions => "transactions",
            SystemTable::Settings => "settings",
            SystemTable::ServerLogs => "server_logs",
            SystemTable::Cluster => "cluster",
            SystemTable::ClusterGroups => "cluster_groups",
            SystemTable::Datatypes => "datatypes",
            SystemTable::Describe => "describe",
            SystemTable::Tables => "tables",
            SystemTable::Columns => "columns",
        }
    }

    /// Get the fully-qualified TableId for this system table/view
    pub fn table_id(&self) -> TableId {
        TableId::from_strings(crate::constants::SYSTEM_NAMESPACE, self.table_name())
    }

    /// Returns true if this is a virtual view (computed on-demand, not persisted)
    pub fn is_view(&self) -> bool {
        matches!(
            self,
            SystemTable::Stats
                | SystemTable::Live
                | SystemTable::Sessions
                | SystemTable::Transactions
                | SystemTable::Settings
                | SystemTable::ServerLogs
                | SystemTable::Cluster
                | SystemTable::ClusterGroups
                | SystemTable::Datatypes
                | SystemTable::Describe
                | SystemTable::Tables
                | SystemTable::Columns
        )
    }

    /// Get the logical storage partition name (e.g., "system_users").
    /// Returns None for views because they have no storage backing.
    pub fn column_family_name(&self) -> Option<&'static str> {
        match self {
            SystemTable::Users => Some("system_users"),
            SystemTable::Namespaces => Some("system_namespaces"),
            SystemTable::Schemas => Some("system_schemas"),
            SystemTable::TableSchemas => Some("system_table_schemas"),
            SystemTable::Storages => Some("system_storages"),
            SystemTable::Jobs => Some("system_jobs"),
            SystemTable::JobNodes => Some("system_job_nodes"),
            SystemTable::AuditLog => Some("system_audit_log"),
            SystemTable::Manifest => Some("manifest_cache"),
            SystemTable::Topics => Some("system_topics"),
            SystemTable::TopicOffsets => Some("system_topic_offsets"),
            // Views have no column family
            SystemTable::Stats
            | SystemTable::Live
            | SystemTable::Sessions
            | SystemTable::Transactions
            | SystemTable::Settings
            | SystemTable::ServerLogs
            | SystemTable::Cluster
            | SystemTable::ClusterGroups
            | SystemTable::Datatypes
            | SystemTable::Describe
            | SystemTable::Tables
            | SystemTable::Columns => None,
        }
    }

    /// Returns the RocksDB tuning profile for this persisted system table.
    pub fn column_family_profile(&self) -> ColumnFamilyProfile {
        ColumnFamilyProfile::SystemMeta
    }

    /// Parse from table name (with or without "system." prefix)
    pub fn from_name(name: &str) -> Result<Self, String> {
        // Remove "system." prefix if present
        let name = name.strip_prefix("system.").unwrap_or(name);

        match name {
            // Tables
            "users" | "system_users" => Ok(SystemTable::Users),
            "namespaces" | "system_namespaces" => Ok(SystemTable::Namespaces),
            "schemas" | "system_schemas" => Ok(SystemTable::Schemas),
            "table_schemas" | "system_table_schemas" => Ok(SystemTable::TableSchemas),
            "storages" | "system_storages" => Ok(SystemTable::Storages),
            "live" => Ok(SystemTable::Live),
            "jobs" | "system_jobs" => Ok(SystemTable::Jobs),
            "job_nodes" | "system_job_nodes" => Ok(SystemTable::JobNodes),
            "audit_log" | "system_audit_log" => Ok(SystemTable::AuditLog),
            "manifest" | "manifest_cache" => Ok(SystemTable::Manifest),
            "topics" | "system_topics" => Ok(SystemTable::Topics),
            "topic_offsets" | "system_topic_offsets" => Ok(SystemTable::TopicOffsets),
            // Views
            "stats" => Ok(SystemTable::Stats),
            "sessions" => Ok(SystemTable::Sessions),
            "transactions" => Ok(SystemTable::Transactions),
            "settings" => Ok(SystemTable::Settings),
            "server_logs" => Ok(SystemTable::ServerLogs),
            "cluster" => Ok(SystemTable::Cluster),
            "cluster_groups" => Ok(SystemTable::ClusterGroups),
            "datatypes" => Ok(SystemTable::Datatypes),
            "describe" => Ok(SystemTable::Describe),
            "tables" | "system_tables" => Ok(SystemTable::Tables),
            "columns" | "system_columns" => Ok(SystemTable::Columns),
            _ => Err(format!("Unknown system table or view: {}", name)),
        }
    }

    /// Get all system tables (persisted only, excludes views)
    pub fn all_tables() -> &'static [SystemTable] {
        &[
            SystemTable::Users,
            SystemTable::Namespaces,
            SystemTable::Schemas,
            SystemTable::TableSchemas,
            SystemTable::Storages,
            SystemTable::Jobs,
            SystemTable::JobNodes,
            SystemTable::AuditLog,
            SystemTable::Manifest,
            SystemTable::Topics,
            SystemTable::TopicOffsets,
        ]
    }

    /// Get all system views (computed on-demand)
    pub fn all_views() -> &'static [SystemTable] {
        &[
            SystemTable::Stats,
            SystemTable::Live,
            SystemTable::Sessions,
            SystemTable::Transactions,
            SystemTable::Settings,
            SystemTable::ServerLogs,
            SystemTable::Cluster,
            SystemTable::ClusterGroups,
            SystemTable::Datatypes,
            SystemTable::Describe,
            SystemTable::Tables,
            SystemTable::Columns,
        ]
    }

    /// Get all system tables and views
    pub fn all() -> &'static [SystemTable] {
        &[
            // Tables
            SystemTable::Users,
            SystemTable::Namespaces,
            SystemTable::Schemas,
            SystemTable::TableSchemas,
            SystemTable::Storages,
            SystemTable::Jobs,
            SystemTable::JobNodes,
            SystemTable::AuditLog,
            SystemTable::Manifest,
            SystemTable::Topics,
            SystemTable::TopicOffsets,
            // Views
            SystemTable::Stats,
            SystemTable::Live,
            SystemTable::Sessions,
            SystemTable::Transactions,
            SystemTable::Settings,
            SystemTable::ServerLogs,
            SystemTable::Cluster,
            SystemTable::ClusterGroups,
            SystemTable::Datatypes,
            SystemTable::Describe,
            SystemTable::Tables,
            SystemTable::Columns,
        ]
    }

    /// Check if a table name is a system table or view
    pub fn is_system_table(name: &str) -> bool {
        Self::from_name(name).is_ok()
    }

    /// Returns a shared Partition for this system table's column family.
    /// Returns None for views (they have no storage backing).
    ///
    /// Allocates each Partition once and returns a reference,
    /// avoiding repeated String allocations across the codebase.
    pub fn partition(&self) -> Option<&'static crate::storage::Partition> {
        use once_cell::sync::Lazy;

        use crate::storage::Partition;

        static USERS: Lazy<Partition> = Lazy::new(|| Partition::new("system_users"));
        static NAMESPACES: Lazy<Partition> = Lazy::new(|| Partition::new("system_namespaces"));
        static SCHEMAS: Lazy<Partition> = Lazy::new(|| Partition::new("system_schemas"));
        static TABLE_SCHEMAS: Lazy<Partition> =
            Lazy::new(|| Partition::new("system_table_schemas"));
        static STORAGES: Lazy<Partition> = Lazy::new(|| Partition::new("system_storages"));
        static JOBS: Lazy<Partition> = Lazy::new(|| Partition::new("system_jobs"));
        static JOB_NODES: Lazy<Partition> = Lazy::new(|| Partition::new("system_job_nodes"));
        static AUDIT_LOG: Lazy<Partition> = Lazy::new(|| Partition::new("system_audit_log"));
        static MANIFEST: Lazy<Partition> = Lazy::new(|| Partition::new("manifest_cache"));
        static TOPICS: Lazy<Partition> = Lazy::new(|| Partition::new("system_topics"));
        static TOPIC_OFFSETS: Lazy<Partition> =
            Lazy::new(|| Partition::new("system_topic_offsets"));

        match self {
            SystemTable::Users => Some(&USERS),
            SystemTable::Namespaces => Some(&NAMESPACES),
            SystemTable::Schemas => Some(&SCHEMAS),
            SystemTable::TableSchemas => Some(&TABLE_SCHEMAS),
            SystemTable::Storages => Some(&STORAGES),
            SystemTable::Jobs => Some(&JOBS),
            SystemTable::JobNodes => Some(&JOB_NODES),
            SystemTable::AuditLog => Some(&AUDIT_LOG),
            SystemTable::Manifest => Some(&MANIFEST),
            SystemTable::Topics => Some(&TOPICS),
            SystemTable::TopicOffsets => Some(&TOPIC_OFFSETS),
            // Views have no partition
            SystemTable::Stats
            | SystemTable::Live
            | SystemTable::Sessions
            | SystemTable::Transactions
            | SystemTable::Settings
            | SystemTable::ServerLogs
            | SystemTable::Cluster
            | SystemTable::ClusterGroups
            | SystemTable::Datatypes
            | SystemTable::Describe
            | SystemTable::Tables
            | SystemTable::Columns => None,
        }
    }
}

/// Additional named partitions that are not SystemTable rows
/// but still stored as dedicated column families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoragePartition {
    /// Unified information_schema.tables storage
    InformationSchemaTables,
    /// Username index for system.users (unique index)
    SystemUsersUsernameIdx,
    /// Role index for system.users (non-unique index)
    SystemUsersRoleIdx,
    /// Deleted_at index for system.users (non-unique index)
    SystemUsersDeletedAtIdx,
    /// Manifest cache for query optimization (Phase 4 - US6)
    ManifestCache,
    /// PendingWrite index for system.manifest (non-unique index)
    ManifestPendingWriteIdx,
    /// Status + CreatedAt index for system.jobs (non-unique index)
    SystemJobsStatusIdx,
    /// Idempotency key index for system.jobs (unique-ish)
    SystemJobsIdempotencyIdx,
}

impl StoragePartition {
    /// Returns the partition (column family) name
    pub fn name(&self) -> &'static str {
        match self {
            StoragePartition::InformationSchemaTables => "information_schema_tables",
            StoragePartition::SystemUsersUsernameIdx => "system_users_username_idx",
            StoragePartition::SystemUsersRoleIdx => "system_users_role_idx",
            StoragePartition::SystemUsersDeletedAtIdx => "system_users_deleted_at_idx",
            StoragePartition::ManifestCache => "manifest_cache",
            StoragePartition::ManifestPendingWriteIdx => "manifest_pending_write_idx",
            StoragePartition::SystemJobsStatusIdx => "system_jobs_status_idx",
            StoragePartition::SystemJobsIdempotencyIdx => "system_jobs_idempotency_idx",
        }
    }

    /// Returns the RocksDB tuning profile for this named partition.
    pub fn column_family_profile(&self) -> ColumnFamilyProfile {
        match self {
            StoragePartition::SystemUsersUsernameIdx
            | StoragePartition::SystemUsersRoleIdx
            | StoragePartition::SystemUsersDeletedAtIdx
            | StoragePartition::ManifestPendingWriteIdx
            | StoragePartition::SystemJobsStatusIdx
            | StoragePartition::SystemJobsIdempotencyIdx => ColumnFamilyProfile::SystemIndex,
            StoragePartition::InformationSchemaTables | StoragePartition::ManifestCache => {
                ColumnFamilyProfile::SystemMeta
            },
        }
    }

    /// Parse a persisted partition name into a known static partition.
    pub fn from_partition_name(name: &str) -> Option<Self> {
        match name {
            "information_schema_tables" => Some(StoragePartition::InformationSchemaTables),
            "system_users_username_idx" => Some(StoragePartition::SystemUsersUsernameIdx),
            "system_users_role_idx" => Some(StoragePartition::SystemUsersRoleIdx),
            "system_users_deleted_at_idx" => Some(StoragePartition::SystemUsersDeletedAtIdx),
            "manifest_cache" => Some(StoragePartition::ManifestCache),
            "manifest_pending_write_idx" => Some(StoragePartition::ManifestPendingWriteIdx),
            "system_jobs_status_idx" => Some(StoragePartition::SystemJobsStatusIdx),
            "system_jobs_idempotency_idx" => Some(StoragePartition::SystemJobsIdempotencyIdx),
            _ => None,
        }
    }

    /// Returns a shared Partition reference for this named partition.
    pub fn partition(&self) -> &'static crate::storage::Partition {
        use once_cell::sync::Lazy;

        use crate::storage::Partition;

        static INFO: Lazy<Partition> =
            Lazy::new(|| Partition::new(StoragePartition::InformationSchemaTables.name()));
        static USERNAME_IDX: Lazy<Partition> =
            Lazy::new(|| Partition::new(StoragePartition::SystemUsersUsernameIdx.name()));
        static ROLE_IDX: Lazy<Partition> =
            Lazy::new(|| Partition::new(StoragePartition::SystemUsersRoleIdx.name()));
        static DELETED_AT_IDX: Lazy<Partition> =
            Lazy::new(|| Partition::new(StoragePartition::SystemUsersDeletedAtIdx.name()));
        static MANIFEST_CACHE: Lazy<Partition> =
            Lazy::new(|| Partition::new(StoragePartition::ManifestCache.name()));
        static JOBS_STATUS_IDX: Lazy<Partition> =
            Lazy::new(|| Partition::new(StoragePartition::SystemJobsStatusIdx.name()));
        static JOBS_IDEMPOTENCY_IDX: Lazy<Partition> =
            Lazy::new(|| Partition::new(StoragePartition::SystemJobsIdempotencyIdx.name()));
        static MANIFEST_PENDING_WRITE_IDX: Lazy<Partition> =
            Lazy::new(|| Partition::new(StoragePartition::ManifestPendingWriteIdx.name()));

        match self {
            StoragePartition::InformationSchemaTables => &INFO,
            StoragePartition::SystemUsersUsernameIdx => &USERNAME_IDX,
            StoragePartition::SystemUsersRoleIdx => &ROLE_IDX,
            StoragePartition::SystemUsersDeletedAtIdx => &DELETED_AT_IDX,
            StoragePartition::ManifestCache => &MANIFEST_CACHE,
            StoragePartition::SystemJobsStatusIdx => &JOBS_STATUS_IDX,
            StoragePartition::SystemJobsIdempotencyIdx => &JOBS_IDEMPOTENCY_IDX,
            StoragePartition::ManifestPendingWriteIdx => &MANIFEST_PENDING_WRITE_IDX,
        }
    }
}

/// Classify a storage partition or physical RocksDB CF name into a typed tuning profile.
#[must_use]
pub fn classify_column_family_name(name: &str) -> ColumnFamilyProfile {
    match name {
        "system_meta" => return ColumnFamilyProfile::SystemMeta,
        "system_index" => return ColumnFamilyProfile::SystemIndex,
        "hot_data" => return ColumnFamilyProfile::HotData,
        "hot_index" => return ColumnFamilyProfile::HotIndex,
        "raft_data" => return ColumnFamilyProfile::Raft,
        _ => {},
    }

    if let Ok(table) = SystemTable::from_name(name) {
        if table.column_family_name().is_some() {
            return table.column_family_profile();
        }
    }

    if let Some(partition) = StoragePartition::from_partition_name(name) {
        return partition.column_family_profile();
    }

    if name == "topic_messages" {
        return ColumnFamilyProfile::HotData;
    }

    if name == "topic_offsets" {
        return ColumnFamilyProfile::HotIndex;
    }

    if name.starts_with(ColumnFamilyNames::USER_TABLE_PREFIX)
        || name.starts_with(ColumnFamilyNames::SHARED_TABLE_PREFIX)
        || name.starts_with(ColumnFamilyNames::STREAM_TABLE_PREFIX)
    {
        return if name.ends_with("_pk_idx") {
            ColumnFamilyProfile::HotIndex
        } else {
            ColumnFamilyProfile::HotData
        };
    }

    if name.starts_with("vix_") {
        return if name.ends_with("_pk_idx") {
            ColumnFamilyProfile::HotIndex
        } else {
            ColumnFamilyProfile::HotData
        };
    }

    ColumnFamilyProfile::SystemMeta
}

impl std::fmt::Display for SystemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "system.{}", self.table_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name() {
        assert_eq!(SystemTable::Users.table_name(), "users");
        assert_eq!(SystemTable::Namespaces.table_name(), "namespaces");
        assert_eq!(SystemTable::Jobs.table_name(), "jobs");
        assert_eq!(SystemTable::AuditLog.table_name(), "audit_log");
        // Views
        assert_eq!(SystemTable::Stats.table_name(), "stats");
        assert_eq!(SystemTable::Sessions.table_name(), "sessions");
        assert_eq!(SystemTable::Cluster.table_name(), "cluster");
        assert_eq!(SystemTable::ClusterGroups.table_name(), "cluster_groups");
    }

    #[test]
    fn test_is_view() {
        // Tables are not views
        assert!(!SystemTable::Users.is_view());
        assert!(!SystemTable::Jobs.is_view());
        assert!(!SystemTable::Manifest.is_view());
        // Views are views
        assert!(SystemTable::Stats.is_view());
        assert!(SystemTable::Sessions.is_view());
        assert!(SystemTable::Settings.is_view());
        assert!(SystemTable::ServerLogs.is_view());
        assert!(SystemTable::Cluster.is_view());
        assert!(SystemTable::ClusterGroups.is_view());
        assert!(SystemTable::Datatypes.is_view());
    }

    #[test]
    fn test_column_family_name() {
        // Tables have column families
        assert_eq!(SystemTable::Users.column_family_name(), Some("system_users"));
        assert_eq!(SystemTable::Namespaces.column_family_name(), Some("system_namespaces"));
        assert_eq!(SystemTable::Jobs.column_family_name(), Some("system_jobs"));
        assert_eq!(SystemTable::AuditLog.column_family_name(), Some("system_audit_log"));
        // Views have no column family
        assert_eq!(SystemTable::Stats.column_family_name(), None);
        assert_eq!(SystemTable::Sessions.column_family_name(), None);
        assert_eq!(SystemTable::Cluster.column_family_name(), None);
    }

    #[test]
    fn test_from_name() {
        // Tables
        assert_eq!(SystemTable::from_name("users").unwrap(), SystemTable::Users);
        assert_eq!(SystemTable::from_name("system.users").unwrap(), SystemTable::Users);
        assert_eq!(SystemTable::from_name("system_users").unwrap(), SystemTable::Users);
        assert_eq!(SystemTable::from_name("storages").unwrap(), SystemTable::Storages);
        // Views
        assert_eq!(SystemTable::from_name("stats").unwrap(), SystemTable::Stats);
        assert_eq!(SystemTable::from_name("sessions").unwrap(), SystemTable::Sessions);
        assert_eq!(SystemTable::from_name("system.cluster").unwrap(), SystemTable::Cluster);
        assert_eq!(
            SystemTable::from_name("system.cluster_groups").unwrap(),
            SystemTable::ClusterGroups
        );
        // Invalid
        assert!(SystemTable::from_name("invalid_table").is_err());
    }

    #[test]
    fn test_static_column_family_profiles() {
        assert_eq!(SystemTable::Users.column_family_profile(), ColumnFamilyProfile::SystemMeta);
        assert_eq!(
            StoragePartition::SystemUsersUsernameIdx.column_family_profile(),
            ColumnFamilyProfile::SystemIndex
        );
        assert_eq!(
            StoragePartition::ManifestCache.column_family_profile(),
            ColumnFamilyProfile::SystemMeta
        );
    }

    #[test]
    fn test_dynamic_column_family_profiles() {
        assert_eq!(
            classify_column_family_name("shared_default:profiles"),
            ColumnFamilyProfile::HotData
        );
        assert_eq!(
            classify_column_family_name("shared_default:profiles_pk_idx"),
            ColumnFamilyProfile::HotIndex
        );
        assert_eq!(
            classify_column_family_name("vix_default:profiles_embedding_shared_ops"),
            ColumnFamilyProfile::HotData
        );
        assert_eq!(
            classify_column_family_name("vix_default:profiles_embedding_shared_pk_idx"),
            ColumnFamilyProfile::HotIndex
        );
        assert_eq!(classify_column_family_name("topic_messages"), ColumnFamilyProfile::HotData);
        assert_eq!(classify_column_family_name("raft_data"), ColumnFamilyProfile::Raft);
    }

    #[test]
    fn test_is_system_table() {
        assert!(SystemTable::is_system_table("users"));
        assert!(SystemTable::is_system_table("system.users"));
        assert!(SystemTable::is_system_table("storages"));
        assert!(SystemTable::is_system_table("stats"));
        assert!(SystemTable::is_system_table("sessions"));
        assert!(SystemTable::is_system_table("system.cluster"));
        assert!(SystemTable::is_system_table("system.cluster_groups"));
        assert!(!SystemTable::is_system_table("my_custom_table"));
    }

    #[test]
    fn test_display() {
        assert_eq!(SystemTable::Users.to_string(), "system.users");
        assert_eq!(SystemTable::Jobs.to_string(), "system.jobs");
        assert_eq!(SystemTable::Stats.to_string(), "system.stats");
        assert_eq!(SystemTable::Sessions.to_string(), "system.sessions");
    }

    #[test]
    fn test_all() {
        let all = SystemTable::all();
        assert_eq!(all.len(), 23); // 11 tables + 12 views
        assert!(all.contains(&SystemTable::Users));
        assert!(all.contains(&SystemTable::Storages));
        assert!(all.contains(&SystemTable::AuditLog));
        assert!(all.contains(&SystemTable::TopicOffsets));
        assert!(all.contains(&SystemTable::Stats));
        assert!(all.contains(&SystemTable::Live));
        assert!(all.contains(&SystemTable::Sessions));
        assert!(all.contains(&SystemTable::Cluster));
        assert!(all.contains(&SystemTable::ClusterGroups));
        assert!(all.contains(&SystemTable::Tables));
        assert!(all.contains(&SystemTable::Columns));
    }

    #[test]
    fn test_all_tables() {
        let tables = SystemTable::all_tables();
        assert_eq!(tables.len(), 11);
        assert!(tables.iter().all(|t| !t.is_view()));
    }

    #[test]
    fn test_all_views() {
        let views = SystemTable::all_views();
        assert_eq!(views.len(), 12);
        assert!(views.iter().all(|v| v.is_view()));
    }

    #[test]
    fn test_partition() {
        // Tables have partitions
        assert!(SystemTable::Users.partition().is_some());
        assert!(SystemTable::Jobs.partition().is_some());
        // Views have no partitions
        assert!(SystemTable::Stats.partition().is_none());
        assert!(SystemTable::Sessions.partition().is_none());
        assert!(SystemTable::Cluster.partition().is_none());
        assert!(SystemTable::ClusterGroups.partition().is_none());
    }
}
