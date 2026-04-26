//! Manifest cache models for query optimization (Phase 4 - US6).
//!
//! This module defines ManifestFile and ManifestCacheEntry structures for
//! tracking Parquet batch file metadata and caching manifests in RocksDB.
//!
//! ## Serialization Strategy
//!
//! - **RocksDB**: FlatBuffers envelope + FlexBuffers payload
//! - **manifest.json files**: Use JSON serialization via `serde_json::to_string_pretty()`
//!
//! `ColumnStats.min/max` use `StoredScalarValue` for typed scalar stats and
//! proper JSON output for manifest.json files.

use std::collections::HashMap;

use kalamdb_commons::{
    ids::SeqId,
    models::{rows::StoredScalarValue, TableId},
    UserId,
};
use serde::{Deserialize, Serialize};

use super::FileSubfolderState;

/// Synchronization state of a cached manifest entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncState {
    /// Cache is in sync with storage (manifest.json on disk matches cache)
    #[default]
    InSync,
    /// Cache has local changes that need to be written to storage (pending flush)
    PendingWrite,
    /// Flush in progress: Parquet being written to temp location (atomic write pattern)
    /// If server crashes in this state, temp files can be cleaned up on restart.
    Syncing,
    /// Cache may be stale and needs refresh from storage
    Stale,
    /// Error occurred during last sync attempt
    Error,
}

impl std::fmt::Display for SyncState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncState::InSync => write!(f, "in_sync"),
            SyncState::PendingWrite => write!(f, "pending_write"),
            SyncState::Syncing => write!(f, "syncing"),
            SyncState::Stale => write!(f, "stale"),
            SyncState::Error => write!(f, "error"),
        }
    }
}

/// Status of a segment in the manifest lifecycle.
///
/// Tracks the write state of each segment to enable crash-safe flush operations.
/// If a flush fails mid-way, `InProgress` segments can be cleaned up on restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentStatus {
    /// Segment write in progress (temp file exists, not yet renamed to final location)
    /// If server crashes in this state, temp files should be cleaned up on restart.
    InProgress,
    /// Segment successfully written and committed (final file exists)
    #[default]
    Committed,
    /// Segment marked for deletion (compaction/cleanup)
    /// Will be removed by the next compaction job.
    Tombstone,
}

impl std::fmt::Display for SegmentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmentStatus::InProgress => write!(f, "in_progress"),
            SegmentStatus::Committed => write!(f, "committed"),
            SegmentStatus::Tombstone => write!(f, "tombstone"),
        }
    }
}

impl SegmentStatus {
    /// Returns true if the segment is fully written and available for reads
    pub fn is_readable(&self) -> bool {
        matches!(self, SegmentStatus::Committed)
    }

    /// Returns true if the segment needs cleanup (incomplete or marked for deletion)
    pub fn needs_cleanup(&self) -> bool {
        matches!(self, SegmentStatus::InProgress | SegmentStatus::Tombstone)
    }
}

/// Manifest cache entry stored in RocksDB (Phase 4 - US6).
///
/// Fields:
/// - `manifest`: The Manifest object (stored via binary codec)
/// - `etag`: Storage ETag or version identifier for freshness validation
/// - `last_refreshed`: Unix timestamp (milliseconds) of last successful refresh
/// - `sync_state`: Current synchronization state (InSync | Stale | Error)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestCacheEntry {
    /// The Manifest object (stored directly)
    pub manifest: Manifest,

    /// ETag or version identifier from storage backend
    pub etag: Option<String>,

    /// Last refresh timestamp (Unix milliseconds)
    pub last_refreshed: i64,

    /// Synchronization state
    pub sync_state: SyncState,
}

impl ManifestCacheEntry {
    /// Create a new cache entry
    pub fn new(
        manifest: Manifest,
        etag: Option<String>,
        last_refreshed: i64,
        sync_state: SyncState,
    ) -> Self {
        Self {
            manifest,
            etag,
            last_refreshed,
            sync_state,
        }
    }

    /// Serialize manifest to JSON string (for display in system.manifest table)
    pub fn manifest_json(&self) -> String {
        serde_json::to_string(&self.manifest).unwrap_or_else(|_| "{}".to_string())
    }

    /// Normalize last_refreshed to milliseconds (handles legacy seconds data)
    pub fn last_refreshed_millis(&self) -> i64 {
        if self.last_refreshed < 1_000_000_000_000 {
            self.last_refreshed * 1000
        } else {
            self.last_refreshed
        }
    }

    /// Check if entry is stale based on TTL (milliseconds)
    pub fn is_stale(&self, ttl_millis: i64, now_millis: i64) -> bool {
        now_millis - self.last_refreshed_millis() > ttl_millis
    }

    /// Mark entry as stale
    pub fn mark_stale(&mut self) {
        self.sync_state = SyncState::Stale;
    }

    /// Mark entry as pending write (local changes need to be synced to storage)
    pub fn mark_pending_write(&mut self) {
        self.sync_state = SyncState::PendingWrite;
    }

    /// Mark entry as syncing (Parquet write in progress to temp location)
    ///
    /// This state indicates the flush is in progress:
    /// - Parquet file is being written to a temp location
    /// - If crash occurs, temp files can be cleaned up on restart
    /// - Transitions to InSync after successful rename to final location
    pub fn mark_syncing(&mut self) {
        self.sync_state = SyncState::Syncing;
    }

    /// Mark entry as in sync
    pub fn mark_in_sync(&mut self, etag: Option<String>, timestamp_millis: i64) {
        self.sync_state = SyncState::InSync;
        self.etag = etag;
        self.last_refreshed = timestamp_millis;
    }

    /// Mark entry as error
    pub fn mark_error(&mut self) {
        self.sync_state = SyncState::Error;
    }
}

/// Statistics for a single column in a segment.
///
/// Min/max values use `StoredScalarValue` for typed scalar stats.
/// This enables proper JSON output for manifest.json files.
/// - Type-safe comparisons without string parsing
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Minimum value in the column
    pub min: Option<StoredScalarValue>,

    /// Maximum value in the column
    pub max: Option<StoredScalarValue>,

    /// Number of null values
    pub null_count: Option<i64>,
}

impl ColumnStats {
    /// Create new column stats
    pub fn new(
        min: Option<StoredScalarValue>,
        max: Option<StoredScalarValue>,
        null_count: Option<i64>,
    ) -> Self {
        Self {
            min,
            max,
            null_count,
        }
    }

    /// Parse min as i64 (for numeric columns)
    pub fn min_as_i64(&self) -> Option<i64> {
        self.min.as_ref().and_then(|v| match v {
            StoredScalarValue::Int64(Some(s)) => s.parse().ok(),
            StoredScalarValue::Int32(Some(i)) => Some(*i as i64),
            StoredScalarValue::Int16(Some(i)) => Some(*i as i64),
            StoredScalarValue::Int8(Some(i)) => Some(*i as i64),
            StoredScalarValue::UInt64(Some(s)) => s.parse().ok(),
            StoredScalarValue::UInt32(Some(i)) => Some(*i as i64),
            StoredScalarValue::UInt16(Some(i)) => Some(*i as i64),
            StoredScalarValue::UInt8(Some(i)) => Some(*i as i64),
            _ => None,
        })
    }

    /// Parse max as i64 (for numeric columns)
    pub fn max_as_i64(&self) -> Option<i64> {
        self.max.as_ref().and_then(|v| match v {
            StoredScalarValue::Int64(Some(s)) => s.parse().ok(),
            StoredScalarValue::Int32(Some(i)) => Some(*i as i64),
            StoredScalarValue::Int16(Some(i)) => Some(*i as i64),
            StoredScalarValue::Int8(Some(i)) => Some(*i as i64),
            StoredScalarValue::UInt64(Some(s)) => s.parse().ok(),
            StoredScalarValue::UInt32(Some(i)) => Some(*i as i64),
            StoredScalarValue::UInt16(Some(i)) => Some(*i as i64),
            StoredScalarValue::UInt8(Some(i)) => Some(*i as i64),
            _ => None,
        })
    }

    /// Get min as string value (for string columns)
    pub fn min_as_str(&self) -> Option<String> {
        self.min.as_ref().and_then(|v| match v {
            StoredScalarValue::Utf8(Some(s)) => Some(s.clone()),
            StoredScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
            _ => None,
        })
    }

    /// Get max as string value (for string columns)
    pub fn max_as_str(&self) -> Option<String> {
        self.max.as_ref().and_then(|v| match v {
            StoredScalarValue::Utf8(Some(s)) => Some(s.clone()),
            StoredScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
            _ => None,
        })
    }
}

/// Segment metadata tracking a data file (Parquet) or hot storage segment.
/// Fields ordered for optimal memory alignment (8-byte types first).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    // 8-byte aligned fields first
    /// Minimum sequence number in this segment (for MVCC pruning)
    pub min_seq: SeqId,

    /// Maximum sequence number in this segment (for MVCC pruning)
    pub max_seq: SeqId,

    /// Number of rows in this segment
    pub row_count: u64,

    /// Size in bytes
    pub size_bytes: u64,

    /// Creation timestamp (Unix milliseconds)
    pub created_at: i64,

    /// Unique segment identifier (UUID)
    pub id: String,

    /// Path to the segment file (relative to table root)
    pub path: String,

    /// Column statistics (min/max/nulls) keyed by column_id
    /// Uses stable column_id for correct mapping after column renames
    #[serde(default)]
    pub column_stats: HashMap<u64, ColumnStats>,

    /// Schema version when this segment was written (Phase 16)
    ///
    /// Links this Parquet file to a specific table schema version.
    /// Use `TablesStore::get_version(table_id, schema_version)` to
    /// retrieve the exact TableDefinition for reading this segment.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,

    // /// If true, this segment is marked for deletion (compaction/cleanup)
    // /// @deprecated Use `status == SegmentStatus::Tombstone` instead
    // #[serde(default)]
    // pub tombstone: bool,
    /// Status of this segment in the flush lifecycle
    /// InProgress → Committed → Tombstone
    #[serde(default)]
    pub status: SegmentStatus,
}

/// Default schema version for backward compatibility with existing manifests
fn default_schema_version() -> u32 {
    1
}

impl SegmentMetadata {
    pub fn new(
        id: String,
        path: String,
        column_stats: HashMap<u64, ColumnStats>,
        min_seq: SeqId,
        max_seq: SeqId,
        row_count: u64,
        size_bytes: u64,
    ) -> Self {
        Self {
            id,
            path,
            column_stats,
            min_seq,
            max_seq,
            row_count,
            size_bytes,
            created_at: chrono::Utc::now().timestamp_millis(),
            // tombstone: false,
            schema_version: 1, // Default to version 1
            status: SegmentStatus::Committed,
        }
    }

    /// Create a new segment with a specific schema version
    pub fn with_schema_version(
        id: String,
        path: String,
        column_stats: HashMap<u64, ColumnStats>,
        min_seq: SeqId,
        max_seq: SeqId,
        row_count: u64,
        size_bytes: u64,
        schema_version: u32,
    ) -> Self {
        Self {
            id,
            path,
            column_stats,
            min_seq,
            max_seq,
            row_count,
            size_bytes,
            created_at: chrono::Utc::now().timestamp_millis(),
            // tombstone: false,
            schema_version,
            status: SegmentStatus::Committed,
        }
    }

    /// Create an in-progress segment (before Parquet write is complete)
    ///
    /// Used during flush to track segments that are being written.
    /// If server crashes, these segments should be cleaned up on restart.
    pub fn in_progress(
        id: String,
        path: String,
        min_seq: SeqId,
        max_seq: SeqId,
        schema_version: u32,
    ) -> Self {
        Self {
            id,
            path,
            column_stats: HashMap::new(),
            min_seq,
            max_seq,
            row_count: 0,
            size_bytes: 0,
            created_at: chrono::Utc::now().timestamp_millis(),
            schema_version,
            status: SegmentStatus::InProgress,
        }
    }

    /// Mark this segment as committed (write complete)
    pub fn mark_committed(
        &mut self,
        row_count: u64,
        size_bytes: u64,
        column_stats: HashMap<u64, ColumnStats>,
    ) {
        self.status = SegmentStatus::Committed;
        self.row_count = row_count;
        self.size_bytes = size_bytes;
        self.column_stats = column_stats;
    }

    /// Mark this segment for deletion
    pub fn mark_tombstone(&mut self) {
        self.status = SegmentStatus::Tombstone;
    }

    /// Check if this segment is readable (committed and not tombstoned)
    pub fn is_readable(&self) -> bool {
        self.status.is_readable()
    }

    /// Check if this segment needs cleanup
    pub fn needs_cleanup(&self) -> bool {
        self.status.needs_cleanup()
    }
}

/// Similarity metric used by vector indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorMetric {
    #[default]
    Cosine,
    L2,
    Dot,
}

/// Vector index engine used for a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorEngine {
    #[default]
    USearch,
}

/// Manifest-tracked state of a vector index column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorIndexState {
    #[default]
    Active,
    Syncing,
    Error,
}

/// Vector index metadata embedded in manifest.json per indexed column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexMetadata {
    /// Column name this metadata belongs to.
    pub column_name: String,
    /// Whether indexing is enabled for this column.
    pub enabled: bool,
    /// Similarity metric used for ANN search.
    pub metric: VectorMetric,
    /// Index engine implementation.
    pub engine: VectorEngine,
    /// Embedding dimensions for this column.
    pub dimensions: u32,
    /// Relative snapshot path under table storage root.
    pub snapshot_path: Option<String>,
    /// Monotonic snapshot version.
    pub snapshot_version: u64,
    /// Last staged sequence applied into snapshot.
    pub last_applied_seq: SeqId,
    /// Last update timestamp.
    pub updated_at: i64,
    /// Current index sync state.
    #[serde(default)]
    pub state: VectorIndexState,
}

impl VectorIndexMetadata {
    pub fn new(
        column_name: impl Into<String>,
        dimensions: u32,
        metric: VectorMetric,
        engine: VectorEngine,
    ) -> Self {
        Self {
            column_name: column_name.into(),
            enabled: true,
            metric,
            engine,
            dimensions,
            snapshot_path: None,
            snapshot_version: 0,
            last_applied_seq: SeqId::from(0i64),
            updated_at: chrono::Utc::now().timestamp_millis(),
            state: VectorIndexState::Active,
        }
    }

    pub fn mark_syncing(&mut self) {
        self.state = VectorIndexState::Syncing;
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    pub fn mark_error(&mut self) {
        self.state = VectorIndexState::Error;
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    pub fn record_snapshot(&mut self, snapshot_path: String, last_applied_seq: SeqId) {
        self.snapshot_version = self.snapshot_version.saturating_add(1);
        self.snapshot_path = Some(snapshot_path);
        self.last_applied_seq = last_applied_seq;
        self.state = VectorIndexState::Active;
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }
}

/// Manifest file tracking segments for a table.
///
/// The manifest is the source of truth for data location (Hot vs Cold).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Table identifier
    pub table_id: TableId,

    /// Optional user_id for User tables
    pub user_id: Option<UserId>,

    /// Manifest version (incremented on update)
    pub version: u64,

    /// Creation timestamp
    pub created_at: i64,

    /// Last update timestamp
    pub updated_at: i64,

    /// List of data segments
    pub segments: Vec<SegmentMetadata>,

    /// Last batch/segment index (used to generate next batch filename: batch-{N}.parquet)
    /// This is automatically updated when add_segment() is called.
    pub last_sequence_number: u64,

    /// File subfolder tracking for FILE columns.
    /// Only present if the table has FILE columns.
    /// Tracks current subfolder and file count for rotation.
    pub files: Option<FileSubfolderState>,

    /// Vector index metadata keyed by column name.
    /// This is embedded in manifest.json so index artifacts can be managed with
    /// the same atomic flush lifecycle used for segment metadata.
    #[serde(default)]
    // TODO: Dont include in json if empty
    pub vector_indexes: HashMap<String, VectorIndexMetadata>,
}

impl Manifest {
    pub fn new(table_id: TableId, user_id: Option<UserId>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            table_id,
            user_id,
            version: 1,
            created_at: now,
            updated_at: now,
            segments: Vec::new(),
            last_sequence_number: 0,
            files: None,
            vector_indexes: HashMap::new(),
        }
    }

    /// Create a manifest for a table with FILE columns
    pub fn new_with_files(table_id: TableId, user_id: Option<UserId>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            table_id,
            user_id,
            version: 1,
            created_at: now,
            updated_at: now,
            segments: Vec::new(),
            last_sequence_number: 0,
            files: Some(FileSubfolderState::new()),
            vector_indexes: HashMap::new(),
        }
    }

    /// Enable file tracking for this manifest
    pub fn enable_files(&mut self) {
        if self.files.is_none() {
            self.files = Some(FileSubfolderState::new());
        }
    }

    /// Allocate a file subfolder for a new file upload.
    /// Returns the subfolder name (e.g., "f0001").
    /// Panics if files are not enabled.
    pub fn allocate_file_subfolder(&mut self, max_files_per_folder: u32) -> String {
        let state = self.files.as_mut().expect("File tracking not enabled for this manifest");
        let subfolder = state.allocate_file(max_files_per_folder);
        self.updated_at = chrono::Utc::now().timestamp_millis();
        self.version += 1;
        subfolder
    }

    /// Get current file subfolder name without allocating
    pub fn current_file_subfolder(&self) -> Option<String> {
        self.files.as_ref().map(|s| s.subfolder_name())
    }
    // ...existing code...

    pub fn add_segment(&mut self, segment: SegmentMetadata) {
        // Extract batch number from segment path and update last_sequence_number
        if let Some(batch_num) = Self::extract_batch_number(&segment.path) {
            if batch_num >= self.last_sequence_number {
                self.last_sequence_number = batch_num;
            }
        }

        // Remove any existing segment with the same ID to prevent duplicates
        self.segments.retain(|s| s.id != segment.id);

        self.segments.push(segment);
        self.updated_at = chrono::Utc::now().timestamp_millis();
        self.version += 1;
    }

    /// Extract batch number from segment path (e.g., "batch-0.parquet" -> 0)
    fn extract_batch_number(path: &str) -> Option<u64> {
        let filename = std::path::Path::new(path).file_name()?.to_str()?;
        if filename.starts_with("batch-") && filename.ends_with(".parquet") {
            filename.strip_prefix("batch-")?.strip_suffix(".parquet")?.parse::<u64>().ok()
        } else {
            None
        }
    }

    pub fn update_sequence_number(&mut self, seq: u64) {
        if seq > self.last_sequence_number {
            self.last_sequence_number = seq;
            self.updated_at = chrono::Utc::now().timestamp_millis();
            // Version bump? Maybe not for just seq update if it happens often in memory
        }
    }

    /// Ensure vector metadata exists for a column and return mutable access to it.
    pub fn ensure_vector_index(
        &mut self,
        column_name: &str,
        dimensions: u32,
        metric: VectorMetric,
        engine: VectorEngine,
    ) -> &mut VectorIndexMetadata {
        let entry = self
            .vector_indexes
            .entry(column_name.to_string())
            .or_insert_with(|| VectorIndexMetadata::new(column_name, dimensions, metric, engine));

        entry.dimensions = dimensions;
        entry.metric = metric;
        entry.engine = engine;
        entry.enabled = true;
        entry.updated_at = chrono::Utc::now().timestamp_millis();
        self.updated_at = entry.updated_at;
        self.version += 1;
        entry
    }

    /// Update vector snapshot pointer and watermark for a column.
    pub fn record_vector_snapshot(
        &mut self,
        column_name: &str,
        dimensions: u32,
        metric: VectorMetric,
        engine: VectorEngine,
        snapshot_path: String,
        last_applied_seq: SeqId,
    ) {
        let entry = self.ensure_vector_index(column_name, dimensions, metric, engine);
        entry.record_snapshot(snapshot_path, last_applied_seq);
        self.updated_at = entry.updated_at;
        self.version += 1;
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::{NamespaceId, TableName};

    use super::*;

    #[test]
    fn test_manifest_cache_entry_json_roundtrip() {
        // This test ensures ManifestCacheEntry can be serialized/deserialized
        // via serde for API/cache round-trips.
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let mut manifest = Manifest::new(table_id, None);

        // Add segment with column stats using StoredScalarValue
        let mut column_stats = HashMap::new();
        column_stats.insert(
            1u64,
            ColumnStats {
                min: Some(StoredScalarValue::Int64(Some("42".to_string()))),
                max: Some(StoredScalarValue::Int64(Some("100".to_string()))),
                null_count: Some(0),
            },
        );
        column_stats.insert(
            2u64,
            ColumnStats {
                min: Some(StoredScalarValue::Utf8(Some("alice".to_string()))),
                max: Some(StoredScalarValue::Utf8(Some("zoe".to_string()))),
                null_count: Some(5),
            },
        );

        let segment = SegmentMetadata::new(
            "seg-1".to_string(),
            "batch-0.parquet".to_string(),
            column_stats,
            SeqId::from(0i64),
            SeqId::from(99i64),
            100,
            2048,
        );
        manifest.add_segment(segment);

        let entry =
            ManifestCacheEntry::new(manifest, Some("etag123".to_string()), 1000, SyncState::InSync);

        let encoded = serde_json::to_vec(&entry).expect("json encode failed");
        let decoded: ManifestCacheEntry =
            serde_json::from_slice(&encoded).expect("json decode failed");

        // Verify
        assert_eq!(decoded.etag, Some("etag123".to_string()));
        assert_eq!(decoded.sync_state, SyncState::InSync);
        assert_eq!(decoded.manifest.segments.len(), 1);

        let seg = &decoded.manifest.segments[0];
        let id_stats = seg.column_stats.get(&1u64).unwrap();
        assert_eq!(id_stats.min_as_i64(), Some(42));
        assert_eq!(id_stats.max_as_i64(), Some(100));

        let name_stats = seg.column_stats.get(&2u64).unwrap();
        assert_eq!(name_stats.min_as_str(), Some("alice".to_string()));
        assert_eq!(name_stats.max_as_str(), Some("zoe".to_string()));
    }

    #[test]
    fn test_column_stats_helper_methods() {
        let stats = ColumnStats {
            min: Some(StoredScalarValue::Int64(Some("42".to_string()))),
            max: Some(StoredScalarValue::Int64(Some("100".to_string()))),
            null_count: Some(0),
        };

        // Test i64 parsing
        assert_eq!(stats.min_as_i64(), Some(42));
        assert_eq!(stats.max_as_i64(), Some(100));

        // Test string values
        let string_stats = ColumnStats {
            min: Some(StoredScalarValue::Utf8(Some("alice".to_string()))),
            max: Some(StoredScalarValue::Utf8(Some("zoe".to_string()))),
            null_count: Some(5),
        };

        // min_as_str should return the string value
        assert_eq!(string_stats.min_as_str(), Some("alice".to_string()));
        assert_eq!(string_stats.max_as_str(), Some("zoe".to_string()));
    }

    #[test]
    fn test_manifest_cache_entry_is_stale() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let manifest = Manifest::new(table_id, None);
        // Use actual millisecond timestamps
        let base_time_ms = 1_700_000_000_000_i64; // Nov 2023
        let ttl_ms = 3600 * 1000; // 1 hour in milliseconds
        let entry = ManifestCacheEntry::new(
            manifest,
            Some("etag123".to_string()),
            base_time_ms,
            SyncState::InSync,
        );

        // Not stale within TTL (checked at 30 minutes after base time)
        assert!(!entry.is_stale(ttl_ms, base_time_ms + 1800 * 1000));

        // Stale after TTL (checked at 1 hour + 1 second after base time)
        assert!(entry.is_stale(ttl_ms, base_time_ms + ttl_ms + 1000));
    }

    #[test]
    fn test_manifest_cache_entry_state_transitions() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let manifest = Manifest::new(table_id, None);
        // Use actual millisecond timestamps
        let initial_time_ms = 1_700_000_000_000_i64; // Nov 2023
        let updated_time_ms = 1_700_000_100_000_i64; // 100 seconds later
        let mut entry = ManifestCacheEntry::new(manifest, None, initial_time_ms, SyncState::InSync);

        entry.mark_stale();
        assert_eq!(entry.sync_state, SyncState::Stale);

        entry.mark_in_sync(Some("new_etag".to_string()), updated_time_ms);
        assert_eq!(entry.sync_state, SyncState::InSync);
        assert_eq!(entry.etag, Some("new_etag".to_string()));
        assert_eq!(entry.last_refreshed_millis(), updated_time_ms);

        entry.mark_error();
        assert_eq!(entry.sync_state, SyncState::Error);
    }

    #[test]
    fn test_sync_state_syncing_transition() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let manifest = Manifest::new(table_id, None);
        let mut entry = ManifestCacheEntry::new(manifest, None, 1000, SyncState::PendingWrite);

        // Transition: PendingWrite -> Syncing (flush started)
        entry.mark_syncing();
        assert_eq!(entry.sync_state, SyncState::Syncing);

        // Transition: Syncing -> InSync (flush completed successfully)
        entry.mark_in_sync(Some("etag-after-flush".to_string()), 2000);
        assert_eq!(entry.sync_state, SyncState::InSync);
        assert_eq!(entry.etag, Some("etag-after-flush".to_string()));
    }

    #[test]
    fn test_sync_state_syncing_to_error() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let manifest = Manifest::new(table_id, None);
        let mut entry = ManifestCacheEntry::new(manifest, None, 1000, SyncState::InSync);

        // Transition: InSync -> Syncing (new flush started)
        entry.mark_syncing();
        assert_eq!(entry.sync_state, SyncState::Syncing);

        // Transition: Syncing -> Error (flush failed)
        entry.mark_error();
        assert_eq!(entry.sync_state, SyncState::Error);
    }

    #[test]
    fn test_sync_state_display() {
        assert_eq!(format!("{}", SyncState::InSync), "in_sync");
        assert_eq!(format!("{}", SyncState::PendingWrite), "pending_write");
        assert_eq!(format!("{}", SyncState::Syncing), "syncing");
        assert_eq!(format!("{}", SyncState::Stale), "stale");
        assert_eq!(format!("{}", SyncState::Error), "error");
    }

    #[test]
    fn test_sync_state_serialization() {
        // Test that SyncState serializes/deserializes correctly with snake_case
        let states = vec![
            (SyncState::InSync, "\"in_sync\""),
            (SyncState::PendingWrite, "\"pending_write\""),
            (SyncState::Syncing, "\"syncing\""),
            (SyncState::Stale, "\"stale\""),
            (SyncState::Error, "\"error\""),
        ];

        for (state, expected_json) in states {
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(
                json, expected_json,
                "SyncState {:?} should serialize to {}",
                state, expected_json
            );

            let deserialized: SyncState = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, state, "SyncState should round-trip through JSON");
        }
    }

    #[test]
    fn test_sync_state_full_flush_lifecycle() {
        // Simulate the complete flush lifecycle with the new Syncing state
        let table_id = TableId::new(NamespaceId::new("myns"), TableName::new("mytable"));
        let manifest = Manifest::new(table_id, None);
        let mut entry = ManifestCacheEntry::new(manifest, None, 1000, SyncState::InSync);

        // 1. Data is written to hot storage (RocksDB), manifest goes PendingWrite
        entry.mark_pending_write();
        assert_eq!(entry.sync_state, SyncState::PendingWrite);

        // 2. Flush job starts, write Parquet to temp location
        entry.mark_syncing();
        assert_eq!(entry.sync_state, SyncState::Syncing);

        // 3a. Success path: rename temp -> final, update manifest
        entry.mark_in_sync(Some("etag-v2".to_string()), 2000);
        assert_eq!(entry.sync_state, SyncState::InSync);

        // OR 3b. Failure path: mark as error
        entry.mark_syncing();
        entry.mark_error();
        assert_eq!(entry.sync_state, SyncState::Error);
    }

    #[test]
    fn test_manifest_add_segment() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let mut manifest = Manifest::new(table_id, None);

        assert_eq!(manifest.segments.len(), 0);

        let segment = SegmentMetadata::new(
            "uuid-1".to_string(),
            "segment-1.parquet".to_string(),
            HashMap::new(),
            SeqId::from(1000i64),
            SeqId::from(2000i64),
            50,
            512,
        );

        manifest.add_segment(segment);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.version, 2);
    }

    #[test]
    fn test_manifest_update_sequence_number() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let mut manifest = Manifest::new(table_id, None);

        assert_eq!(manifest.last_sequence_number, 0);

        manifest.update_sequence_number(100);
        assert_eq!(manifest.last_sequence_number, 100);

        // Should not decrease
        manifest.update_sequence_number(50);
        assert_eq!(manifest.last_sequence_number, 100);
    }

    #[test]
    fn test_manifest_tracks_batch_numbers() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let mut manifest = Manifest::new(table_id, None);

        assert_eq!(manifest.last_sequence_number, 0);

        // Add batch-0.parquet
        let segment0 = SegmentMetadata::new(
            "batch-0".to_string(),
            "batch-0.parquet".to_string(),
            HashMap::new(),
            SeqId::from(1000i64),
            SeqId::from(2000i64),
            50,
            512,
        );
        manifest.add_segment(segment0);
        assert_eq!(manifest.last_sequence_number, 0);
        assert_eq!(manifest.segments.len(), 1);

        // Add batch-1.parquet
        let segment1 = SegmentMetadata::new(
            "batch-1".to_string(),
            "batch-1.parquet".to_string(),
            HashMap::new(),
            SeqId::from(2001i64),
            SeqId::from(3000i64),
            50,
            512,
        );
        manifest.add_segment(segment1);
        assert_eq!(manifest.last_sequence_number, 1);
        assert_eq!(manifest.segments.len(), 2);

        // Add batch-5.parquet (skip some numbers)
        let segment5 = SegmentMetadata::new(
            "batch-5".to_string(),
            "batch-5.parquet".to_string(),
            HashMap::new(),
            SeqId::from(5001i64),
            SeqId::from(6000i64),
            50,
            512,
        );
        manifest.add_segment(segment5);
        assert_eq!(manifest.last_sequence_number, 5);
        assert_eq!(manifest.segments.len(), 3);
    }

    #[test]
    fn test_extract_batch_number() {
        assert_eq!(Manifest::extract_batch_number("batch-0.parquet"), Some(0));
        assert_eq!(Manifest::extract_batch_number("batch-1.parquet"), Some(1));
        assert_eq!(Manifest::extract_batch_number("batch-42.parquet"), Some(42));
        assert_eq!(Manifest::extract_batch_number("batch-999.parquet"), Some(999));

        // Invalid formats
        assert_eq!(Manifest::extract_batch_number("other-0.parquet"), None);
        assert_eq!(Manifest::extract_batch_number("batch-0.csv"), None);
        assert_eq!(Manifest::extract_batch_number("batch.parquet"), None);
    }

    #[test]
    fn test_manifest_prevent_duplicate_segments() {
        let table_id = TableId::new(NamespaceId::new("test"), TableName::new("table"));
        let mut manifest = Manifest::new(table_id, None);

        // Add first version of segment
        let segment1 = SegmentMetadata::new(
            "segment-1".to_string(),
            "segment-1.parquet".to_string(),
            HashMap::new(),
            SeqId::from(1000i64),
            SeqId::from(2000i64),
            50,
            512,
        );
        manifest.add_segment(segment1);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].min_seq, SeqId::from(1000i64));

        // Add updated version of same segment (same ID)
        let segment1_v2 = SegmentMetadata::new(
            "segment-1".to_string(),
            "segment-1.parquet".to_string(),
            HashMap::new(),
            SeqId::from(1500i64), // Changed min_seq
            SeqId::from(2500i64),
            60,
            600,
        );
        manifest.add_segment(segment1_v2);

        // Should still have 1 segment, but updated
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].min_seq, SeqId::from(1500i64));
        assert_eq!(manifest.segments[0].id, "segment-1");
    }

    // ========== SegmentStatus Tests ==========

    #[test]
    fn test_segment_status_display() {
        assert_eq!(format!("{}", SegmentStatus::InProgress), "in_progress");
        assert_eq!(format!("{}", SegmentStatus::Committed), "committed");
        assert_eq!(format!("{}", SegmentStatus::Tombstone), "tombstone");
    }

    #[test]
    fn test_segment_status_is_readable() {
        assert!(!SegmentStatus::InProgress.is_readable());
        assert!(SegmentStatus::Committed.is_readable());
        assert!(!SegmentStatus::Tombstone.is_readable());
    }

    #[test]
    fn test_segment_status_needs_cleanup() {
        assert!(SegmentStatus::InProgress.needs_cleanup()); // orphaned in_progress segments
        assert!(!SegmentStatus::Committed.needs_cleanup());
        assert!(SegmentStatus::Tombstone.needs_cleanup());
    }

    #[test]
    fn test_segment_in_progress_lifecycle() {
        // Create in_progress segment
        let mut segment = SegmentMetadata::in_progress(
            "seg-001".to_string(),
            "batch-001.parquet".to_string(),
            SeqId::from(1000i64),
            SeqId::from(2000i64),
            1, // schema_version
        );

        // Initially in_progress
        assert_eq!(segment.status, SegmentStatus::InProgress);
        assert!(!segment.is_readable());
        assert!(segment.needs_cleanup());
        assert_eq!(segment.row_count, 0);
        assert_eq!(segment.size_bytes, 0);

        // After successful write, mark committed
        segment.mark_committed(1000, 65536, HashMap::new());
        assert_eq!(segment.status, SegmentStatus::Committed);
        assert!(segment.is_readable());
        assert!(!segment.needs_cleanup());
        assert_eq!(segment.row_count, 1000);
        assert_eq!(segment.size_bytes, 65536);
    }

    #[test]
    fn test_segment_tombstone_lifecycle() {
        // Create committed segment
        let mut segment = SegmentMetadata::new(
            "seg-002".to_string(),
            "batch-002.parquet".to_string(),
            HashMap::new(),
            SeqId::from(3000i64),
            SeqId::from(4000i64),
            500,
            32768,
        );

        // Initially committed
        assert!(segment.is_readable());
        assert!(!segment.needs_cleanup());

        // After compaction or deletion, mark tombstone
        segment.mark_tombstone();
        assert_eq!(segment.status, SegmentStatus::Tombstone);
        assert!(!segment.is_readable());
        assert!(segment.needs_cleanup());
    }

    #[test]
    fn test_segment_status_serde() {
        // Test serialization/deserialization of SegmentStatus
        let segment = SegmentMetadata::in_progress(
            "seg-003".to_string(),
            "batch-003.parquet".to_string(),
            SeqId::from(5000i64),
            SeqId::from(6000i64),
            1,
        );

        let json = serde_json::to_string(&segment).unwrap();
        assert!(json.contains("\"status\":\"in_progress\""));

        let restored: SegmentMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.status, SegmentStatus::InProgress);
    }

    #[test]
    fn test_segment_status_default() {
        // Test that default status is Committed for backward compatibility
        assert_eq!(SegmentStatus::default(), SegmentStatus::Committed);
    }

    #[test]
    fn test_manifest_vector_index_roundtrip() {
        let table_id = TableId::new(NamespaceId::new("vec_ns"), TableName::new("emb"));
        let mut manifest = Manifest::new(table_id, Some(UserId::new("u1")));

        manifest.record_vector_snapshot(
            "embedding",
            384,
            VectorMetric::Cosine,
            VectorEngine::USearch,
            "vec-embedding-snapshot-1.vix".to_string(),
            SeqId::from(42i64),
        );

        let json = serde_json::to_string(&manifest).unwrap();
        let decoded: Manifest = serde_json::from_str(&json).unwrap();

        let meta = decoded.vector_indexes.get("embedding").expect("vector metadata exists");
        assert_eq!(meta.dimensions, 384);
        assert_eq!(meta.metric, VectorMetric::Cosine);
        assert_eq!(meta.engine, VectorEngine::USearch);
        assert_eq!(meta.last_applied_seq, SeqId::from(42i64));
        assert_eq!(meta.snapshot_version, 1);
        assert_eq!(meta.snapshot_path.as_deref(), Some("vec-embedding-snapshot-1.vix"));
    }

    #[test]
    fn test_manifest_vector_index_backward_compat_default() {
        let table_id = TableId::new(NamespaceId::new("ns1"), TableName::new("t1"));
        let manifest = Manifest::new(table_id, None);
        let mut raw = serde_json::to_value(&manifest).unwrap();
        if let Some(obj) = raw.as_object_mut() {
            obj.remove("vector_indexes");
        }

        let decoded: Manifest = serde_json::from_value(raw).unwrap();
        assert!(decoded.vector_indexes.is_empty());
    }
}
