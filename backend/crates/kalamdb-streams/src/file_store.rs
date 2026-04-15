use crate::config::StreamLogConfig;
use crate::error::{Result, StreamLogError};
use crate::record::StreamLogRecord;
use crate::store_trait::StreamLogStore;
use crate::time_bucket::StreamTimeBucket;
use crate::utils::{cleanup_empty_dir, parse_log_window, read_dirs, read_files};
use chrono::{Datelike, TimeZone, Timelike, Utc};
use dashmap::{DashMap, DashSet};
use kalamdb_commons::ids::StreamTableRowId;
use kalamdb_commons::models::{StreamTableRow, TableId, UserId};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Write buffer capacity per segment file handle (256 KB).
///
/// Larger buffers amortise `write()` syscalls — with ~500-byte records a 256 KB
/// buffer holds ~500 records before the OS sees a single `write()`.
const SEGMENT_BUF_CAPACITY: usize = 256 * 1024;

/// Cached state for an open segment file.
struct SegmentWriter {
    writer: BufWriter<File>,
    record_count: u32,
    last_write: Instant,
}

/// File-based stream log store with cached file handles and write buffering.
///
/// Optimised for high-throughput concurrent writes:
///
/// * **Cached file handles** — open segment files are kept in a sharded
///   `DashMap`, eliminating open / close syscall overhead per write.
/// * **Sharded write buffers** — each segment has its own 256 KB `BufWriter`,
///   reducing flush frequency while enabling per-user parallelism.
/// * **Directory cache** — avoids repeated `create_dir_all` syscalls.
/// * **Batch writes** — multiple records targeting the same segment share a
///   single lock acquisition.
pub struct FileStreamLogStore {
    config: StreamLogConfig,
    /// Cached open segment writers keyed by log-file path.
    segments: DashMap<PathBuf, Arc<Mutex<SegmentWriter>>>,
    /// Parent directories that have already been created.
    created_dirs: DashSet<PathBuf>,
}

impl fmt::Debug for FileStreamLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileStreamLogStore")
            .field("config", &self.config)
            .field("open_segments", &self.segments.len())
            .field("cached_dirs", &self.created_dirs.len())
            .finish()
    }
}

impl FileStreamLogStore {
    pub fn new(config: StreamLogConfig) -> Self {
        Self {
            config,
            segments: DashMap::new(),
            created_dirs: DashSet::new(),
        }
    }

    pub fn table_id(&self) -> &TableId {
        &self.config.table_id
    }

    // ── Lifecycle / maintenance ──────────────────────────────────────

    /// Flush all open segment writers to the OS page cache.
    ///
    /// This does **not** call `fsync` — stream tables are ephemeral and the
    /// kernel will write-back dirty pages asynchronously.
    pub fn flush_all(&self) -> Result<()> {
        let mut last_err: Option<StreamLogError> = None;
        for entry in self.segments.iter() {
            if let Ok(mut g) = entry.value().lock() {
                if let Err(e) = g.writer.flush() {
                    last_err = Some(StreamLogError::Io(e.to_string()));
                }
            }
        }
        match last_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Close segment writers that have been idle longer than `max_idle`.
    ///
    /// Writers are flushed before being dropped so buffered data is not lost.
    pub fn close_idle_segments(&self, max_idle: std::time::Duration) {
        let now = Instant::now();
        let mut to_remove: Vec<PathBuf> = Vec::new();
        for entry in self.segments.iter() {
            if let Ok(g) = entry.value().lock() {
                if now.duration_since(g.last_write) > max_idle {
                    to_remove.push(entry.key().clone());
                }
            }
        }
        for path in to_remove {
            if let Some((_, writer)) = self.segments.remove(&path) {
                if let Ok(mut g) = writer.lock() {
                    let _ = g.writer.flush();
                }
            }
        }
    }

    /// Number of currently cached segment file handles.
    pub fn open_segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn append_delete(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        row_id: &StreamTableRowId,
    ) -> Result<()> {
        self.ensure_table(table_id)?;
        let ts = row_id.seq().timestamp_millis();
        let window_start = self.window_start_ms(ts);
        let path = self.log_path(user_id, window_start);
        self.append_record(
            &path,
            StreamLogRecord::Delete {
                row_id: row_id.clone(),
            },
        )
    }

    pub fn delete_old_logs_with_count(&self, before_time: u64) -> Result<usize> {
        let mut deleted = 0usize;
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(0);
        }

        let bucket_dirs = read_dirs(base_dir)?;
        for bucket_dir in bucket_dirs {
            let shard_dirs = read_dirs(&bucket_dir)?;
            for shard_dir in shard_dirs {
                let user_dirs = read_dirs(&shard_dir)?;
                for user_dir in user_dirs {
                    let log_files = read_files(&user_dir)?;
                    for log_file in log_files {
                        if let Some(window_start) = parse_log_window(&log_file) {
                            let window_end =
                                window_start.saturating_add(self.config.bucket.duration_ms());
                            if window_end < before_time {
                                // Flush and drop the cached writer before deleting from disk.
                                if let Some((_, writer)) = self.segments.remove(&log_file) {
                                    if let Ok(mut g) = writer.lock() {
                                        let _ = g.writer.flush();
                                    }
                                }
                                if fs::remove_file(&log_file).is_ok() {
                                    deleted += 1;
                                }
                            }
                        }
                    }
                    cleanup_empty_dir(&user_dir);
                }
                cleanup_empty_dir(&shard_dir);
            }
            cleanup_empty_dir(&bucket_dir);
        }

        cleanup_empty_dir(base_dir);

        Ok(deleted)
    }

    pub fn has_logs_before(&self, before_time: u64) -> Result<bool> {
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(false);
        }

        let bucket_dirs = read_dirs(base_dir)?;
        for bucket_dir in bucket_dirs {
            let shard_dirs = read_dirs(&bucket_dir)?;
            for shard_dir in shard_dirs {
                let user_dirs = read_dirs(&shard_dir)?;
                for user_dir in user_dirs {
                    let log_files = read_files(&user_dir)?;
                    for log_file in log_files {
                        if let Some(window_start) = parse_log_window(&log_file) {
                            let window_end =
                                window_start.saturating_add(self.config.bucket.duration_ms());
                            if window_end < before_time {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    pub fn list_user_ids(&self) -> Result<Vec<UserId>> {
        let mut users = HashSet::new();
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(Vec::new());
        }

        let bucket_dirs = read_dirs(base_dir)?;
        for bucket_dir in bucket_dirs {
            let shard_dirs = read_dirs(&bucket_dir)?;
            for shard_dir in shard_dirs {
                let user_dirs = read_dirs(&shard_dir)?;
                for user_dir in user_dirs {
                    if let Some(name) = user_dir.file_name().and_then(|n| n.to_str()) {
                        users.insert(UserId::new(name));
                    }
                }
            }
        }

        Ok(users.into_iter().collect())
    }

    fn ensure_table(&self, table_id: &TableId) -> Result<()> {
        if table_id != &self.config.table_id {
            return Err(StreamLogError::InvalidInput(format!(
                "Stream log store configured for {} but got {}",
                self.config.table_id, table_id
            )));
        }
        Ok(())
    }

    fn window_start_ms(&self, ts_ms: u64) -> u64 {
        let dt = Utc.timestamp_millis_opt(ts_ms as i64).single();
        let dt = match dt {
            Some(val) => val,
            None => return ts_ms,
        };

        match self.config.bucket {
            StreamTimeBucket::Hour => {
                let truncated = dt
                    .with_minute(0)
                    .and_then(|v| v.with_second(0))
                    .and_then(|v| v.with_nanosecond(0));
                truncated.map(|v| v.timestamp_millis() as u64).unwrap_or(ts_ms)
            },
            StreamTimeBucket::Day => {
                let truncated =
                    dt.date_naive().and_hms_opt(0, 0, 0).map(|naive| Utc.from_utc_datetime(&naive));
                truncated.map(|v| v.timestamp_millis() as u64).unwrap_or(ts_ms)
            },
            StreamTimeBucket::Week => {
                let weekday = dt.weekday().num_days_from_monday() as i64;
                let date = dt.date_naive() - chrono::Duration::days(weekday);
                let naive = date.and_hms_opt(0, 0, 0);
                naive
                    .map(|v| Utc.from_utc_datetime(&v).timestamp_millis() as u64)
                    .unwrap_or(ts_ms)
            },
            StreamTimeBucket::Month => {
                let naive = chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                    .and_then(|d| d.and_hms_opt(0, 0, 0));
                naive
                    .map(|v| Utc.from_utc_datetime(&v).timestamp_millis() as u64)
                    .unwrap_or(ts_ms)
            },
        }
    }

    fn bucket_folder(&self, window_start_ms: u64) -> String {
        let dt = Utc
            .timestamp_millis_opt(window_start_ms as i64)
            .single()
            .unwrap_or_else(|| Utc.timestamp_millis_opt(0).single().unwrap());
        match self.config.bucket {
            StreamTimeBucket::Hour => dt.format("%Y%m%d%H").to_string(),
            StreamTimeBucket::Day => dt.format("%Y%m%d").to_string(),
            StreamTimeBucket::Week => dt.format("%G%V").to_string(),
            StreamTimeBucket::Month => dt.format("%Y%m").to_string(),
        }
    }

    fn log_path(&self, user_id: &UserId, window_start_ms: u64) -> PathBuf {
        let bucket_folder = self.bucket_folder(window_start_ms);
        let shard = self.config.shard_router.route_stream_user(user_id).folder_name();
        let user_dir = self.config.base_dir.join(bucket_folder).join(shard).join(user_id.as_str());
        user_dir.join(format!("{}.log", window_start_ms))
    }

    /// Ensure the parent directory of `path` exists, using the dir cache to
    /// skip redundant `create_dir_all` calls.
    fn ensure_parent_dir(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !self.created_dirs.contains(parent) {
                fs::create_dir_all(parent).map_err(|e| StreamLogError::Io(e.to_string()))?;
                self.created_dirs.insert(parent.to_path_buf());
            }
        }
        Ok(())
    }

    /// Return a cached writer for `path`, creating it (and its parent dirs) on
    /// first access.
    fn get_or_create_writer(&self, path: &Path) -> Result<Arc<Mutex<SegmentWriter>>> {
        // Fast path: already cached.
        if let Some(entry) = self.segments.get(path) {
            return Ok(Arc::clone(entry.value()));
        }

        // Slow path: open the file and insert into the cache.
        self.ensure_parent_dir(path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| StreamLogError::Io(e.to_string()))?;
        let writer = Arc::new(Mutex::new(SegmentWriter {
            writer: BufWriter::with_capacity(SEGMENT_BUF_CAPACITY, file),
            record_count: 0,
            last_write: Instant::now(),
        }));

        // Use `entry` API so a concurrent creation by another thread is
        // handled correctly — whichever was inserted first wins.
        Ok(self.segments.entry(path.to_path_buf()).or_insert(writer).value().clone())
    }

    /// Serialise `record` and write the length-prefixed frame to `writer`.
    #[inline]
    fn write_record_bytes(writer: &mut BufWriter<File>, record: &StreamLogRecord) -> Result<()> {
        let payload = flexbuffers::to_vec(record)
            .map_err(|e| StreamLogError::Serialization(e.to_string()))?;
        let len = payload.len() as u32;
        writer
            .write_all(&len.to_le_bytes())
            .map_err(|e| StreamLogError::Io(e.to_string()))?;
        writer.write_all(&payload).map_err(|e| StreamLogError::Io(e.to_string()))?;
        Ok(())
    }

    fn append_record(&self, path: &Path, record: StreamLogRecord) -> Result<()> {
        let seg = self.get_or_create_writer(path)?;
        let mut guard = seg
            .lock()
            .map_err(|e| StreamLogError::Io(format!("segment lock poisoned: {}", e)))?;
        Self::write_record_bytes(&mut guard.writer, &record)?;
        guard.record_count += 1;
        guard.last_write = Instant::now();
        Ok(())
    }

    /// Flush the writer for a specific segment path (if cached) so that
    /// subsequent disk reads see all buffered data.
    fn flush_segment(&self, path: &Path) {
        if let Some(entry) = self.segments.get(path) {
            if let Ok(mut g) = entry.value().lock() {
                let _ = g.writer.flush();
            }
        }
    }

    fn read_records(path: &Path) -> Result<Vec<StreamLogRecord>> {
        let file = File::open(path).map_err(|e| StreamLogError::Io(e.to_string()))?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {},
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    return Err(StreamLogError::Io(err.to_string()));
                },
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            reader.read_exact(&mut payload).map_err(|e| StreamLogError::Io(e.to_string()))?;
            let record = flexbuffers::from_slice::<StreamLogRecord>(&payload)
                .map_err(|e| StreamLogError::Serialization(e.to_string()))?;
            records.push(record);
        }
        Ok(records)
    }

    fn list_log_files_for_user(&self, user_id: &UserId) -> Result<Vec<(u64, PathBuf)>> {
        let mut entries = Vec::new();
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(entries);
        }

        let bucket_dirs = read_dirs(base_dir)?;
        for bucket_dir in bucket_dirs {
            let shard_dirs = read_dirs(&bucket_dir)?;
            for shard_dir in shard_dirs {
                let user_dir = shard_dir.join(user_id.as_str());
                if !user_dir.exists() {
                    continue;
                }
                let log_files = read_files(&user_dir)?;
                for log_file in log_files {
                    if let Some(window_start) = parse_log_window(&log_file) {
                        entries.push((window_start, log_file));
                    }
                }
            }
        }

        Ok(entries)
    }

    fn read_range_internal(
        &self,
        user_id: &UserId,
        start_time: u64,
        end_time: u64,
        limit: usize,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        let mut entries = self.list_log_files_for_user(user_id)?;
        if entries.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let bucket_ms = self.config.bucket.duration_ms();
        entries.retain(|(window_start, _)| {
            let window_end = window_start.saturating_add(bucket_ms);
            *window_start <= end_time && window_end >= start_time
        });
        entries.sort_by_key(|(window_start, _)| *window_start);

        let mut results: Vec<(StreamTableRowId, StreamTableRow)> = Vec::new();
        let mut deleted: HashSet<i64> = HashSet::new();

        for (_window_start, path) in &entries {
            self.flush_segment(path);
            let records = Self::read_records(path)?;
            for record in records {
                match record {
                    StreamLogRecord::Put { row_id, row } => {
                        let seq = row_id.seq().as_i64();
                        if deleted.contains(&seq) {
                            continue;
                        }
                        results.push((row_id, row));
                        if results.len() >= limit {
                            return Ok(results);
                        }
                    },
                    StreamLogRecord::Delete { row_id } => {
                        let seq = row_id.seq().as_i64();
                        deleted.insert(seq);
                        results.retain(|(existing_id, _)| existing_id.seq().as_i64() != seq);
                    },
                }
            }
        }

        Ok(results)
    }

    fn read_latest_internal(
        &self,
        user_id: &UserId,
        limit: usize,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        let mut entries = self.list_log_files_for_user(user_id)?;
        if entries.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        entries.sort_by(|a, b| b.0.cmp(&a.0));

        let mut results: Vec<(StreamTableRowId, StreamTableRow)> = Vec::new();
        let mut deleted: HashSet<i64> = HashSet::new();

        for (_window_start, path) in &entries {
            self.flush_segment(path);
            let records = Self::read_records(path)?;
            for record in records.into_iter().rev() {
                match record {
                    StreamLogRecord::Put { row_id, row } => {
                        let seq = row_id.seq().as_i64();
                        if deleted.contains(&seq) {
                            continue;
                        }
                        results.push((row_id, row));
                        if results.len() >= limit {
                            return Ok(results);
                        }
                    },
                    StreamLogRecord::Delete { row_id } => {
                        deleted.insert(row_id.seq().as_i64());
                    },
                }
            }
        }

        Ok(results)
    }
}

impl StreamLogStore for FileStreamLogStore {
    fn append_rows(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        rows: HashMap<StreamTableRowId, StreamTableRow>,
    ) -> Result<()> {
        self.ensure_table(table_id)?;

        // Group records by target segment so each file handle is locked once.
        let mut by_segment: HashMap<PathBuf, Vec<StreamLogRecord>> = HashMap::new();
        for (row_id, row) in rows {
            let ts = row_id.seq().timestamp_millis();
            let window_start = self.window_start_ms(ts);
            let path = self.log_path(user_id, window_start);
            by_segment.entry(path).or_default().push(StreamLogRecord::Put { row_id, row });
        }

        for (path, records) in by_segment {
            let seg = self.get_or_create_writer(&path)?;
            let mut guard = seg
                .lock()
                .map_err(|e| StreamLogError::Io(format!("segment lock poisoned: {}", e)))?;
            for record in &records {
                Self::write_record_bytes(&mut guard.writer, record)?;
            }
            guard.record_count += records.len() as u32;
            guard.last_write = Instant::now();
        }

        Ok(())
    }

    fn read_with_limit(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        limit: usize,
    ) -> Result<HashMap<StreamTableRowId, StreamTableRow>> {
        self.ensure_table(table_id)?;
        let rows = self.read_latest_internal(user_id, limit)?;
        Ok(rows.into_iter().collect())
    }

    fn read_in_time_range(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        start_time: u64,
        end_time: u64,
        limit: usize,
    ) -> Result<HashMap<StreamTableRowId, StreamTableRow>> {
        self.ensure_table(table_id)?;
        let rows = self.read_range_internal(user_id, start_time, end_time, limit)?;
        Ok(rows.into_iter().collect())
    }

    fn delete_old_logs(&self, before_time: u64) -> Result<()> {
        let _ = self.delete_old_logs_with_count(before_time)?;
        Ok(())
    }
}

impl Drop for FileStreamLogStore {
    fn drop(&mut self) {
        // Best-effort flush of all open segment writers on shutdown.
        let _ = self.flush_all();
    }
}

#[cfg(test)]
mod tests {
    use super::FileStreamLogStore;
    use crate::config::StreamLogConfig;
    use crate::store_trait::StreamLogStore;
    use crate::time_bucket::StreamTimeBucket;
    use chrono::{Datelike, TimeZone, Timelike};
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::ids::{SeqId, SnowflakeGenerator, StreamTableRowId};
    use kalamdb_commons::models::rows::Row;
    use kalamdb_commons::models::{NamespaceId, StreamTableRow, TableId, TableName, UserId};
    use kalamdb_sharding::ShardRouter;
    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use std::path::PathBuf;

    fn temp_base_dir(prefix: &str) -> PathBuf {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("{}_{}_{}", prefix, std::process::id(), now));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn seq_from_timestamp(ts_ms: u64) -> SeqId {
        let id =
            SnowflakeGenerator::max_id_for_timestamp(ts_ms).expect("max_id_for_timestamp failed");
        SeqId::new(id)
    }

    fn window_start_ms(bucket: StreamTimeBucket, ts_ms: u64) -> u64 {
        let dt = chrono::Utc
            .timestamp_millis_opt(ts_ms as i64)
            .single()
            .unwrap_or_else(|| chrono::Utc.timestamp_millis_opt(0).single().unwrap());

        match bucket {
            StreamTimeBucket::Hour => dt
                .with_minute(0)
                .and_then(|v| v.with_second(0))
                .and_then(|v| v.with_nanosecond(0))
                .map(|v| v.timestamp_millis() as u64)
                .unwrap_or(ts_ms),
            StreamTimeBucket::Day => dt
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .map(|naive| chrono::Utc.from_utc_datetime(&naive))
                .map(|v| v.timestamp_millis() as u64)
                .unwrap_or(ts_ms),
            StreamTimeBucket::Week => {
                let weekday = dt.weekday().num_days_from_monday() as i64;
                let date = dt.date_naive() - chrono::Duration::days(weekday);
                let naive = date.and_hms_opt(0, 0, 0);
                naive
                    .map(|v| chrono::Utc.from_utc_datetime(&v).timestamp_millis() as u64)
                    .unwrap_or(ts_ms)
            },
            StreamTimeBucket::Month => chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|v| chrono::Utc.from_utc_datetime(&v).timestamp_millis() as u64)
                .unwrap_or(ts_ms),
        }
    }

    fn bucket_folder(bucket: StreamTimeBucket, window_start_ms: u64) -> String {
        let dt = chrono::Utc
            .timestamp_millis_opt(window_start_ms as i64)
            .single()
            .unwrap_or_else(|| chrono::Utc.timestamp_millis_opt(0).single().unwrap());
        match bucket {
            StreamTimeBucket::Hour => dt.format("%Y%m%d%H").to_string(),
            StreamTimeBucket::Day => dt.format("%Y%m%d").to_string(),
            StreamTimeBucket::Week => dt.format("%G%V").to_string(),
            StreamTimeBucket::Month => dt.format("%Y%m").to_string(),
        }
    }

    fn build_row(user_id: &UserId, seq: SeqId) -> StreamTableRow {
        let values: BTreeMap<String, ScalarValue> = BTreeMap::new();
        StreamTableRow {
            user_id: user_id.clone(),
            _seq: seq,
            fields: Row::new(values),
        }
    }

    #[test]
    fn test_delete_old_logs_cleans_files_and_folders() {
        let base_dir = temp_base_dir("kalamdb_streams_delete_old");
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("events"));
        let shard_router = ShardRouter::new(4, 1);
        let bucket = StreamTimeBucket::Hour;

        let store = FileStreamLogStore::new(StreamLogConfig {
            base_dir: base_dir.clone(),
            shard_router: shard_router.clone(),
            bucket,
            table_id: table_id.clone(),
        });

        let user_id = UserId::new("user-1");
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let old_ts = now_ms.saturating_sub(3 * 60 * 60 * 1000);
        let new_ts = now_ms.saturating_sub(10 * 60 * 1000);

        let old_seq = seq_from_timestamp(old_ts);
        let new_seq = seq_from_timestamp(new_ts);
        let old_id = StreamTableRowId::new(user_id.clone(), old_seq);
        let new_id = StreamTableRowId::new(user_id.clone(), new_seq);

        let mut rows = HashMap::new();
        rows.insert(old_id.clone(), build_row(&user_id, old_seq));
        rows.insert(new_id.clone(), build_row(&user_id, new_seq));

        store.append_rows(&table_id, &user_id, rows).expect("append_rows failed");

        let shard_folder = shard_router.route_stream_user(&user_id).folder_name();
        let old_window = window_start_ms(bucket, old_ts);
        let new_window = window_start_ms(bucket, new_ts);
        let old_bucket = bucket_folder(bucket, old_window);
        let new_bucket = bucket_folder(bucket, new_window);

        let old_path = base_dir
            .join(old_bucket.clone())
            .join(&shard_folder)
            .join(user_id.as_str())
            .join(format!("{}.log", old_window));
        let new_path = base_dir
            .join(new_bucket.clone())
            .join(&shard_folder)
            .join(user_id.as_str())
            .join(format!("{}.log", new_window));

        assert!(old_path.exists(), "expected old log file to exist");
        assert!(new_path.exists(), "expected new log file to exist");
        assert!(
            store
                .has_logs_before(now_ms.saturating_sub(60 * 60 * 1000))
                .expect("has_logs_before failed"),
            "expected logs before cutoff"
        );

        let deleted = store
            .delete_old_logs_with_count(now_ms.saturating_sub(60 * 60 * 1000))
            .expect("delete_old_logs_with_count failed");
        assert!(deleted >= 1, "expected at least one log file deleted");

        assert!(!old_path.exists(), "expected old log file to be deleted");
        assert!(new_path.exists(), "expected new log file to remain");

        let old_bucket_dir = base_dir.join(old_bucket);
        assert!(!old_bucket_dir.exists(), "expected old bucket directory to be removed");
        assert!(base_dir.exists(), "expected base dir to remain");

        let _ = fs::remove_dir_all(&base_dir);
    }

    #[test]
    fn test_concurrent_writes_from_multiple_users() {
        let base_dir = temp_base_dir("kalamdb_streams_concurrent");
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("events"));
        let store = std::sync::Arc::new(FileStreamLogStore::new(StreamLogConfig {
            base_dir: base_dir.clone(),
            shard_router: ShardRouter::new(4, 1),
            bucket: StreamTimeBucket::Hour,
            table_id: table_id.clone(),
        }));

        let num_users: usize = 50;
        let writes_per_user: usize = 100;
        let mut handles = Vec::new();

        for i in 0..num_users {
            let store = std::sync::Arc::clone(&store);
            let tid = table_id.clone();
            handles.push(std::thread::spawn(move || {
                let user_id = UserId::new(format!("user-{}", i));
                for j in 0..writes_per_user {
                    let seq = SeqId::new((i * 10000 + j + 1) as i64);
                    let row_id = StreamTableRowId::new(user_id.clone(), seq);
                    let row = build_row(&user_id, seq);
                    let mut rows = HashMap::new();
                    rows.insert(row_id, row);
                    store.append_rows(&tid, &user_id, rows).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Flush before reading so buffered data is visible on disk.
        store.flush_all().unwrap();

        for i in 0..num_users {
            let user_id = UserId::new(format!("user-{}", i));
            let rows = store.read_with_limit(&table_id, &user_id, writes_per_user + 1).unwrap();
            assert_eq!(
                rows.len(),
                writes_per_user,
                "user-{} should have {} rows, got {}",
                i,
                writes_per_user,
                rows.len()
            );
        }

        assert!(store.open_segment_count() > 0);
        let _ = fs::remove_dir_all(&base_dir);
    }

    #[test]
    fn test_flush_all_and_close_idle() {
        let base_dir = temp_base_dir("kalamdb_streams_flush");
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("events"));
        let store = FileStreamLogStore::new(StreamLogConfig {
            base_dir: base_dir.clone(),
            shard_router: ShardRouter::new(4, 1),
            bucket: StreamTimeBucket::Hour,
            table_id: table_id.clone(),
        });

        let user_id = UserId::new("user-flush");
        let seq = SeqId::new(42);
        let row_id = StreamTableRowId::new(user_id.clone(), seq);
        let row = build_row(&user_id, seq);
        let mut rows = HashMap::new();
        rows.insert(row_id, row);
        store.append_rows(&table_id, &user_id, rows).unwrap();

        assert_eq!(store.open_segment_count(), 1);
        store.flush_all().unwrap();

        // After flush, segment is still open (not idle yet).
        assert_eq!(store.open_segment_count(), 1);

        // Closing with zero idle time should close all segments.
        store.close_idle_segments(std::time::Duration::ZERO);
        assert_eq!(store.open_segment_count(), 0);

        // Data should still be readable from disk.
        let read = store.read_with_limit(&table_id, &user_id, 10).unwrap();
        assert_eq!(read.len(), 1);

        let _ = fs::remove_dir_all(&base_dir);
    }
}
