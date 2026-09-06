use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};

use chrono::{Datelike, TimeZone, Timelike, Utc};
use dashmap::DashMap;
use kalamdb_commons::{
    ids::StreamTableRowId,
    models::{StreamTableRow, TableId, UserId},
};
use kalamdb_serialization::StorageSchema;

use crate::{
    config::StreamLogConfig,
    error::{Result, StreamLogError},
    record::{PersistedStreamLogRecord, StreamLogRecord},
    store_trait::StreamLogStore,
    time_bucket::StreamTimeBucket,
    utils::{cleanup_empty_dir, visit_dirs},
};

/// Write buffer capacity per segment file handle (64 KB).
///
/// This keeps per-active-segment memory bounded while still amortising small
/// append records into fewer `write()` syscalls.
const SEGMENT_BUF_CAPACITY: usize = 64 * 1024;

/// Maximum cached segment writers per stream table.
///
/// Each cached segment owns a file descriptor and a write buffer, so this cap
/// prevents high-cardinality stream users from turning into unbounded memory and
/// open-file growth.
const MAX_OPEN_SEGMENTS: usize = 256;

/// Number of cold segment writers to close when the cache exceeds its cap.
const SEGMENT_EVICT_BATCH: usize = 32;

/// Suffix for a per-user stream log file inside a window/shard directory.
const USER_LOG_FILE_SUFFIX: &str = ".log";

/// Prefix for window directories: `w<start_ms>-<duration_ms>`.
const WINDOW_DIR_PREFIX: &str = "w";

/// Cached state for an open segment file.
struct SegmentWriter {
    writer:       BufWriter<File>,
    record_count: u32,
    last_write:   Instant,
}

#[derive(Debug, Clone)]
struct LogFileEntry {
    window_start: u64,
    window_end:   u64,
    path:         PathBuf,
}

/// File-based stream log store with cached file handles and write buffering.
///
/// Optimised for high-throughput concurrent writes:
///
/// * **Cached file handles** — open segment files are kept in a sharded `DashMap`, eliminating open
///   / close syscall overhead per write.
/// * **Bounded write buffers** — each segment has its own 64 KB `BufWriter`, reducing flush
///   frequency while enabling per-user parallelism.
/// * **Bounded cache** — old segment writers are flushed and closed when too many users/windows are
///   active at once.
/// * **Batch writes** — multiple records targeting the same segment share a single lock
///   acquisition.
pub struct FileStreamLogStore {
    config:   StreamLogConfig,
    schema:   Arc<StorageSchema>,
    /// Cached open segment writers keyed by log-file path.
    segments: DashMap<PathBuf, Arc<Mutex<SegmentWriter>>>,
}

impl fmt::Debug for FileStreamLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileStreamLogStore")
            .field("config", &self.config)
            .field("open_segments", &self.segments.len())
            .finish()
    }
}

impl FileStreamLogStore {
    pub fn new(config: StreamLogConfig, schema: Arc<StorageSchema>) -> Self {
        Self {
            config,
            schema,
            segments: DashMap::new(),
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
            self.close_segment(&path);
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

    pub fn append_row(
        &self,
        table_id: &TableId,
        user_id: &UserId,
        row_id: &StreamTableRowId,
        row: &StreamTableRow,
    ) -> Result<()> {
        self.ensure_table(table_id)?;
        let ts = row_id.seq().timestamp_millis();
        let window_start = self.window_start_ms(ts);
        let path = self.log_path(user_id, window_start);
        self.append_record(
            &path,
            StreamLogRecord::Put {
                row_id: row_id.clone(),
                row:    row.clone(),
            },
        )
    }

    pub fn delete_old_logs_with_count(&self, before_time: u64) -> Result<usize> {
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(0);
        }

        let deleted = self.delete_expired_windows(before_time)?;
        cleanup_empty_dir(base_dir);

        Ok(deleted)
    }

    fn delete_expired_windows(&self, before_time: u64) -> Result<usize> {
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(0);
        }

        let mut expired_dirs = Vec::new();
        visit_dirs(base_dir, |window_dir| {
            if let Some((_window_start, window_end)) = Self::parse_window_dir(&window_dir) {
                if window_end <= before_time {
                    expired_dirs.push(window_dir);
                }
            }
            Ok(true)
        })?;

        let mut deleted = 0usize;
        for window_dir in expired_dirs {
            self.close_segments_under(&window_dir);
            match fs::remove_dir_all(&window_dir) {
                Ok(()) => {
                    deleted += 1;
                    self.close_segments_under(&window_dir);
                },
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {},
                Err(err) => return Err(StreamLogError::Io(err.to_string())),
            }
        }

        cleanup_empty_dir(base_dir);
        Ok(deleted)
    }

    pub fn has_logs_before(&self, before_time: u64) -> Result<bool> {
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(false);
        }

        self.has_expired_window(before_time)
    }

    fn has_expired_window(&self, before_time: u64) -> Result<bool> {
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(false);
        }

        let mut found = false;
        visit_dirs(base_dir, |window_dir| {
            if let Some((_window_start, window_end)) = Self::parse_window_dir(&window_dir) {
                if window_end <= before_time {
                    found = true;
                    return Ok(false);
                }
            }
            Ok(true)
        })?;
        Ok(found)
    }

    pub fn list_user_ids(&self) -> Result<Vec<UserId>> {
        let mut users = HashSet::new();
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(Vec::new());
        }

        self.collect_user_ids(&mut users)?;

        Ok(users.into_iter().collect())
    }

    fn close_segment(&self, path: &Path) -> bool {
        if let Some((_, writer)) = self.segments.remove(path) {
            if let Ok(mut g) = writer.lock() {
                let _ = g.writer.flush();
            }
            return true;
        }
        false
    }

    fn close_segments_under(&self, dir: &Path) -> usize {
        let paths: Vec<PathBuf> = self
            .segments
            .iter()
            .filter(|entry| entry.key().starts_with(dir))
            .map(|entry| entry.key().clone())
            .collect();
        let mut closed = 0usize;
        for path in paths {
            if self.close_segment(&path) {
                closed += 1;
            }
        }
        closed
    }

    fn prune_open_segments_if_needed(&self) {
        let current_len = self.segments.len();
        if current_len <= MAX_OPEN_SEGMENTS {
            return;
        }

        let over_limit = current_len.saturating_sub(MAX_OPEN_SEGMENTS);
        let remove_count = over_limit.saturating_add(SEGMENT_EVICT_BATCH).min(current_len);
        let mut candidates = Vec::with_capacity(current_len.min(remove_count * 2));

        for entry in self.segments.iter() {
            if let Ok(g) = entry.value().try_lock() {
                candidates.push((g.last_write, entry.key().clone()));
            }
        }

        candidates.sort_by_key(|(last_write, _)| *last_write);
        for (_, path) in candidates.into_iter().take(remove_count) {
            self.close_segment(&path);
        }
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
            StreamTimeBucket::Minute => {
                let truncated = dt.with_second(0).and_then(|v| v.with_nanosecond(0));
                truncated.map(|v| v.timestamp_millis() as u64).unwrap_or(ts_ms)
            },
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

    fn window_dir(&self, window_start_ms: u64) -> PathBuf {
        self.config.base_dir.join(format!(
            "{}{}-{}",
            WINDOW_DIR_PREFIX,
            window_start_ms,
            self.config.bucket.duration_ms()
        ))
    }

    fn parse_window_dir(path: &Path) -> Option<(u64, u64)> {
        let name = path.file_name()?.to_str()?;
        let body = name.strip_prefix(WINDOW_DIR_PREFIX)?;
        let (start, duration) = body.split_once('-')?;
        let window_start = start.parse::<u64>().ok()?;
        let duration_ms = duration.parse::<u64>().ok()?;
        Some((window_start, window_start.saturating_add(duration_ms)))
    }

    fn user_log_file_name(user_id: &UserId) -> String {
        format!("{}{}", user_id.as_str(), USER_LOG_FILE_SUFFIX)
    }

    fn parse_user_id_from_log_path(path: &Path) -> Option<UserId> {
        let name = path.file_name()?.to_str()?;
        let user_id = name.strip_suffix(USER_LOG_FILE_SUFFIX)?;
        UserId::try_new(user_id.to_string()).ok()
    }

    fn log_path(&self, user_id: &UserId, window_start_ms: u64) -> PathBuf {
        let shard = self.config.shard_router.route_stream_user(user_id).folder_name();
        self.window_dir(window_start_ms)
            .join(shard)
            .join(Self::user_log_file_name(user_id))
    }

    /// Ensure the parent directory of `path` exists, using the dir cache to
    /// skip redundant `create_dir_all` calls.
    fn ensure_parent_dir(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| StreamLogError::Io(e.to_string()))?;
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
            writer:       BufWriter::with_capacity(SEGMENT_BUF_CAPACITY, file),
            record_count: 0,
            last_write:   Instant::now(),
        }));

        // Use `entry` API so a concurrent creation by another thread is
        // handled correctly — whichever was inserted first wins.
        let segment = self.segments.entry(path.to_path_buf()).or_insert(writer).value().clone();
        self.prune_open_segments_if_needed();
        Ok(segment)
    }

    fn persist_record(&self, record: &StreamLogRecord) -> Result<PersistedStreamLogRecord> {
        match record {
            StreamLogRecord::Put { row_id, row } => {
                let payload = kalamdb_serialization::encode_stream_row(row, &self.schema)
                    .map_err(|e| StreamLogError::Serialization(e.to_string()))?
                    .into_bytes();
                Ok(PersistedStreamLogRecord::Put {
                    row_id: row_id.clone(),
                    payload,
                })
            },
            StreamLogRecord::Delete { row_id } => Ok(PersistedStreamLogRecord::Delete {
                row_id: row_id.clone(),
            }),
        }
    }

    fn hydrate_record(&self, persisted: PersistedStreamLogRecord) -> Result<StreamLogRecord> {
        match persisted {
            PersistedStreamLogRecord::Put { row_id, payload } => {
                let row = kalamdb_serialization::decode_stream_row(
                    &payload,
                    &self.schema,
                    row_id.user_id.clone(),
                    row_id.seq,
                )
                .map_err(|e| StreamLogError::Serialization(e.to_string()))?;
                Ok(StreamLogRecord::Put { row_id, row })
            },
            PersistedStreamLogRecord::Delete { row_id } => Ok(StreamLogRecord::Delete { row_id }),
        }
    }

    /// Serialise `record` and write the length-prefixed frame to `writer`.
    #[inline]
    fn write_record_bytes(
        &self,
        writer: &mut BufWriter<File>,
        record: &StreamLogRecord,
    ) -> Result<()> {
        let persisted = self.persist_record(record)?;
        let payload = kalamdb_serialization::encode_stream(&persisted)
            .map_err(|e| StreamLogError::Serialization(e.to_string()))?
            .into_bytes();
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
        self.write_record_bytes(&mut guard.writer, &record)?;
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

    fn visit_records<F>(&self, path: &Path, mut visitor: F) -> Result<bool>
    where
        F: FnMut(StreamLogRecord) -> Result<bool>,
    {
        let file = File::open(path).map_err(|e| StreamLogError::Io(e.to_string()))?;
        let mut reader = BufReader::new(file);
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
            match reader.read_exact(&mut payload) {
                Ok(_) => {},
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    return Err(StreamLogError::Io(err.to_string()));
                },
            }
            let persisted =
                kalamdb_serialization::decode_stream::<PersistedStreamLogRecord>(&payload)
                    .map_err(|e| StreamLogError::Serialization(e.to_string()))?;
            let record = self.hydrate_record(persisted)?;
            if !visitor(record)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn read_records(&self, path: &Path) -> Result<Vec<StreamLogRecord>> {
        let mut records = Vec::new();
        self.visit_records(path, |record| {
            records.push(record);
            Ok(true)
        })?;
        Ok(records)
    }

    fn collect_user_ids(&self, users: &mut HashSet<UserId>) -> Result<()> {
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(());
        }

        visit_dirs(base_dir, |window_dir| {
            if Self::parse_window_dir(&window_dir).is_none() {
                return Ok(true);
            }
            visit_dirs(&window_dir, |shard_dir| {
                for entry in
                    fs::read_dir(&shard_dir).map_err(|e| StreamLogError::Io(e.to_string()))?
                {
                    let entry = entry.map_err(|e| StreamLogError::Io(e.to_string()))?;
                    let path = entry.path();
                    if !path.is_file() {
                        continue;
                    }
                    if let Some(user_id) = Self::parse_user_id_from_log_path(&path) {
                        users.insert(user_id);
                    }
                }
                Ok(true)
            })
        })?;

        Ok(())
    }

    fn list_log_files_for_user(&self, user_id: &UserId) -> Result<Vec<LogFileEntry>> {
        let mut entries = Vec::new();
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(entries);
        }

        self.collect_log_files_for_user(user_id, &mut entries)?;

        Ok(entries)
    }

    fn collect_log_files_for_user(
        &self,
        user_id: &UserId,
        entries: &mut Vec<LogFileEntry>,
    ) -> Result<()> {
        let base_dir = &self.config.base_dir;
        if !base_dir.exists() {
            return Ok(());
        }

        let shard = self.config.shard_router.route_stream_user(user_id).folder_name();
        visit_dirs(base_dir, |window_dir| {
            let Some((window_start, window_end)) = Self::parse_window_dir(&window_dir) else {
                return Ok(true);
            };
            let log_file = window_dir.join(&shard).join(Self::user_log_file_name(user_id));
            if log_file.is_file() {
                entries.push(LogFileEntry {
                    window_start,
                    window_end,
                    path: log_file,
                });
            }
            Ok(true)
        })?;

        Ok(())
    }

    fn list_log_files_for_user_in_range(
        &self,
        user_id: &UserId,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<LogFileEntry>> {
        let mut entries = self.list_log_files_for_user(user_id)?;
        entries.retain(|entry| entry.window_start <= end_time && entry.window_end > start_time);
        entries.sort_by_key(|entry| entry.window_start);
        Ok(entries)
    }

    fn list_log_files_for_user_latest(&self, user_id: &UserId) -> Result<Vec<LogFileEntry>> {
        let mut entries = self.list_log_files_for_user(user_id)?;
        entries.sort_by(|a, b| b.window_start.cmp(&a.window_start));
        Ok(entries)
    }

    fn collect_range_from_entries(
        &self,
        entries: &[LogFileEntry],
        limit: usize,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        let mut results: Vec<(StreamTableRowId, StreamTableRow)> = Vec::new();
        let mut deleted: HashSet<i64> = HashSet::new();

        for entry in entries {
            self.flush_segment(&entry.path);
            let should_continue = self.visit_records(&entry.path, |record| {
                match record {
                    StreamLogRecord::Put { row_id, row } => {
                        let seq = row_id.seq().as_i64();
                        if deleted.contains(&seq) {
                            return Ok(true);
                        }
                        results.push((row_id, row));
                        if results.len() >= limit {
                            return Ok(false);
                        }
                    },
                    StreamLogRecord::Delete { row_id } => {
                        let seq = row_id.seq().as_i64();
                        deleted.insert(seq);
                        results.retain(|(existing_id, _)| existing_id.seq().as_i64() != seq);
                    },
                }
                Ok(true)
            })?;
            if !should_continue {
                break;
            }
        }

        Ok(results)
    }

    fn read_range_internal(
        &self,
        user_id: &UserId,
        start_time: u64,
        end_time: u64,
        limit: usize,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let entries = self.list_log_files_for_user_in_range(user_id, start_time, end_time)?;
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        self.collect_range_from_entries(&entries, limit)
    }

    fn read_latest_internal(
        &self,
        user_id: &UserId,
        limit: usize,
    ) -> Result<Vec<(StreamTableRowId, StreamTableRow)>> {
        let entries = self.list_log_files_for_user_latest(user_id)?;
        if entries.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let mut results: Vec<(StreamTableRowId, StreamTableRow)> = Vec::new();
        let mut deleted: HashSet<i64> = HashSet::new();

        for entry in &entries {
            self.flush_segment(&entry.path);
            let records = self.read_records(&entry.path)?;
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
                self.write_record_bytes(&mut guard.writer, record)?;
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
    use std::{
        collections::{BTreeMap, HashMap},
        fs,
        path::PathBuf,
        sync::Arc,
    };

    use chrono::{Datelike, TimeZone, Timelike};
    use datafusion::scalar::ScalarValue;
    use kalamdb_commons::{
        ids::{SeqId, SnowflakeGenerator, StreamTableRowId},
        models::{rows::Row, NamespaceId, StreamTableRow, TableId, TableName, UserId},
    };
    use kalamdb_serialization::StorageSchema;
    use kalamdb_sharding::ShardRouter;

    use super::{FileStreamLogStore, MAX_OPEN_SEGMENTS};
    use crate::{
        config::StreamLogConfig, store_trait::StreamLogStore, time_bucket::StreamTimeBucket,
    };

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
            StreamTimeBucket::Minute => dt
                .with_second(0)
                .and_then(|v| v.with_nanosecond(0))
                .map(|v| v.timestamp_millis() as u64)
                .unwrap_or(ts_ms),
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

    fn build_row(user_id: &UserId, seq: SeqId) -> StreamTableRow {
        let values: BTreeMap<String, ScalarValue> = BTreeMap::new();
        StreamTableRow {
            user_id: user_id.clone(),
            _seq:    seq,
            fields:  Row::new(values),
        }
    }

    fn create_store(
        base_dir: PathBuf,
        table_id: TableId,
        shard_router: ShardRouter,
        bucket: StreamTimeBucket,
    ) -> FileStreamLogStore {
        FileStreamLogStore::new(
            StreamLogConfig {
                base_dir,
                shard_router,
                bucket,
                table_id,
            },
            Arc::new(StorageSchema::new(1, Vec::new())),
        )
    }

    #[test]
    fn test_delete_old_logs_cleans_files_and_folders() {
        let base_dir = temp_base_dir("kalamdb_streams_delete_old");
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("events"));
        let shard_router = ShardRouter::new(4, 1);
        let bucket = StreamTimeBucket::Hour;

        let store = create_store(base_dir.clone(), table_id.clone(), shard_router.clone(), bucket);

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

        let old_window = window_start_ms(bucket, old_ts);
        let new_window = window_start_ms(bucket, new_ts);
        let old_path = store.log_path(&user_id, old_window);
        let new_path = store.log_path(&user_id, new_window);
        let old_window_dir = store.window_dir(old_window);

        assert!(old_path.exists(), "expected old log file to exist");
        assert!(new_path.exists(), "expected new log file to exist");
        assert_eq!(
            old_path.file_name().and_then(|name| name.to_str()),
            Some("user-1.log"),
            "expected stream rows to be stored directly as <user_id>.log"
        );
        assert_ne!(
            old_path
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str()),
            Some(user_id.as_str()),
            "expected no per-user directory in the stream file layout"
        );
        assert_eq!(store.open_segment_count(), 2);
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
        assert_eq!(store.open_segment_count(), 1);

        assert!(!old_window_dir.exists(), "expected old window directory to be removed");
        assert!(base_dir.exists(), "expected base dir to remain");

        let _ = fs::remove_dir_all(&base_dir);
    }

    #[test]
    fn test_concurrent_writes_from_multiple_users() {
        let base_dir = temp_base_dir("kalamdb_streams_concurrent");
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("events"));
        let store = std::sync::Arc::new(create_store(
            base_dir.clone(),
            table_id.clone(),
            ShardRouter::new(4, 1),
            StreamTimeBucket::Hour,
        ));

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
                    store.append_row(&tid, &user_id, &row_id, &row).unwrap();
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
    fn test_open_segment_cache_is_bounded() {
        let base_dir = temp_base_dir("kalamdb_streams_segment_cap");
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("events"));
        let store = create_store(
            base_dir.clone(),
            table_id.clone(),
            ShardRouter::new(4, 1),
            StreamTimeBucket::Hour,
        );

        let user_count = MAX_OPEN_SEGMENTS + 64;
        for i in 0..user_count {
            let user_id = UserId::new(format!("user-{}", i));
            let seq = SeqId::new((i + 1) as i64);
            let row_id = StreamTableRowId::new(user_id.clone(), seq);
            let row = build_row(&user_id, seq);
            store.append_row(&table_id, &user_id, &row_id, &row).unwrap();
        }

        assert!(
            store.open_segment_count() <= MAX_OPEN_SEGMENTS,
            "open segment cache exceeded cap: {} > {}",
            store.open_segment_count(),
            MAX_OPEN_SEGMENTS
        );

        let first_user = UserId::new("user-0");
        let read = store.read_with_limit(&table_id, &first_user, 10).unwrap();
        assert_eq!(read.len(), 1, "evicted segment should remain readable");

        let _ = fs::remove_dir_all(&base_dir);
    }

    #[test]
    fn test_flush_all_and_close_idle() {
        let base_dir = temp_base_dir("kalamdb_streams_flush");
        let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("events"));
        let store = create_store(
            base_dir.clone(),
            table_id.clone(),
            ShardRouter::new(4, 1),
            StreamTimeBucket::Hour,
        );

        let user_id = UserId::new("user-flush");
        let seq = SeqId::new(42);
        let row_id = StreamTableRowId::new(user_id.clone(), seq);
        let row = build_row(&user_id, seq);
        store.append_row(&table_id, &user_id, &row_id, &row).unwrap();

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
