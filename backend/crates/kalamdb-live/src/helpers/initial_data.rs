// backend/crates/kalamdb-live/src/helpers/initial_data.rs
//
// Initial data fetch for live query subscriptions.
// Provides "changes since timestamp" functionality to populate client state
// before real-time notifications begin.

use std::{collections::BTreeMap, fmt::Write, sync::Arc};

use datafusion::arrow::array::{Array, Int64Array};
use datafusion_common::ScalarValue;
use kalamdb_commons::{
    constants::SystemColumnNames,
    ids::SeqId,
    models::{rows::Row, ReadContext, TableId},
    Role, TableType,
};
use once_cell::sync::OnceCell;

use crate::{
    error::{LiveError, LiveResultExt},
    traits::{LiveSchemaLookup, LiveSqlExecutor},
};

/// Options for fetching initial data when subscribing to a live query
#[derive(Debug, Clone)]
pub struct InitialDataOptions {
    /// Fetch changes since this sequence ID (exclusive)
    /// If None, starts from the beginning (or end, depending on strategy)
    pub since_seq: Option<SeqId>,

    /// Fetch changes up to this sequence ID (inclusive)
    /// Used to define the snapshot boundary
    pub until_seq: Option<SeqId>,

    /// Fetch changes after this deterministic commit sequence (exclusive).
    pub since_commit_seq: Option<u64>,

    /// Fetch changes up to this deterministic commit sequence (inclusive).
    pub until_commit_seq: Option<u64>,

    /// Maximum number of rows to return (batch size)
    /// Default: 100
    pub limit: usize,

    /// Include soft-deleted rows (_deleted=true)
    /// Default: false
    pub include_deleted: bool,

    /// Fetch the last N rows (newest first) instead of from the beginning
    /// Default: false
    pub fetch_last: bool,
}

impl Default for InitialDataOptions {
    fn default() -> Self {
        Self {
            since_seq: None,
            until_seq: None,
            since_commit_seq: None,
            until_commit_seq: None,
            limit: 100,
            include_deleted: false,
            fetch_last: false,
        }
    }
}

impl InitialDataOptions {
    /// Create options to fetch changes since a specific sequence ID
    pub fn since(seq: SeqId) -> Self {
        Self {
            since_seq: Some(seq),
            until_seq: None,
            since_commit_seq: None,
            until_commit_seq: None,
            limit: 100,
            include_deleted: false,
            fetch_last: false,
        }
    }

    /// Create options to fetch the last N rows (legacy/simple mode)
    /// Note: This might need adjustment for SeqId-based logic
    pub fn last(limit: usize) -> Self {
        Self {
            since_seq: None,
            until_seq: None,
            since_commit_seq: None,
            until_commit_seq: None,
            limit,
            include_deleted: false,
            fetch_last: true,
        }
    }

    /// Create options for batch-based fetching
    pub fn batch(since_seq: Option<SeqId>, until_seq: Option<SeqId>, batch_size: usize) -> Self {
        Self {
            since_seq,
            until_seq,
            since_commit_seq: None,
            until_commit_seq: None,
            limit: batch_size,
            include_deleted: false,
            fetch_last: false,
        }
    }

    /// Set the maximum number of rows to return
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Include soft-deleted rows in the result
    pub fn with_deleted(mut self) -> Self {
        self.include_deleted = true;
        self
    }

    /// Set deterministic commit-sequence resume bounds.
    pub fn with_commit_range(
        mut self,
        since_commit_seq: Option<u64>,
        until_commit_seq: Option<u64>,
    ) -> Self {
        self.since_commit_seq = since_commit_seq;
        self.until_commit_seq = until_commit_seq;
        self
    }
}

/// Result of an initial data fetch
#[derive(Debug)]
pub struct InitialDataResult {
    /// The fetched rows (as Row objects)
    pub rows: Vec<Row>,

    /// Sequence ID of the last row in the result
    /// Used for pagination (passed as since_seq in next request)
    pub last_seq: Option<SeqId>,

    /// Deterministic commit sequence of the last row in the result.
    pub last_commit_seq: Option<u64>,

    /// Whether there are more rows available in the snapshot range
    pub has_more: bool,

    /// The snapshot boundary used for this fetch
    pub snapshot_end_seq: Option<SeqId>,

    /// Deterministic snapshot boundary used for this fetch.
    pub snapshot_end_commit_seq: Option<u64>,
}

/// Service for fetching initial data when subscribing to live queries
pub struct InitialDataFetcher {
    schema_lookup: Arc<dyn LiveSchemaLookup>,
    sql_executor: Arc<OnceCell<Arc<dyn LiveSqlExecutor>>>,
}

impl InitialDataFetcher {
    /// Create a new initial data fetcher.
    ///
    /// The SQL executor is set later via `set_sql_executor` because of
    /// bootstrap ordering (LiveQueryManager is created before SqlExecutor).
    pub fn new(schema_lookup: Arc<dyn LiveSchemaLookup>) -> Self {
        Self {
            schema_lookup,
            sql_executor: Arc::new(OnceCell::new()),
        }
    }

    /// Wire the SQL executor (called once during bootstrap).
    pub fn set_sql_executor(&self, executor: Arc<dyn LiveSqlExecutor>) {
        if self.sql_executor.set(executor).is_err() {
            log::warn!("LiveSqlExecutor already initialized in InitialDataFetcher");
        }
    }

    fn sql_executor(&self) -> Result<&Arc<dyn LiveSqlExecutor>, LiveError> {
        self.sql_executor
            .get()
            .ok_or_else(|| LiveError::InvalidOperation("SQL executor not initialized".into()))
    }

    /// Fetch initial data for a table
    ///
    /// # Arguments
    /// * `table_id` - Table identifier with namespace and table name
    /// * `table_type` - User or Shared table
    /// * `options` - Options for the fetch (timestamp, limit, etc.)
    /// * `where_clause` - Optional WHERE clause string to include in the query
    /// * `projections` - Optional column projections (None = SELECT *, all columns)
    ///
    /// # Returns
    /// InitialDataResult with rows and metadata
    pub async fn fetch_initial_data(
        &self,
        live_id: &kalamdb_commons::models::LiveQueryId,
        role: Role,
        table_id: &TableId,
        table_type: TableType,
        options: InitialDataOptions,
        where_clause: Option<&str>,
        projections: Option<&[String]>,
    ) -> Result<InitialDataResult, LiveError> {
        let limit = options.limit;
        if limit == 0 {
            return Ok(InitialDataResult {
                rows: Vec::new(),
                last_seq: None,
                last_commit_seq: None,
                has_more: false,
                snapshot_end_seq: None,
                snapshot_end_commit_seq: None,
            });
        }

        // Extract user_id from LiveId for RLS
        let user_id = live_id.user_id().clone();

        // Execute via trait — handles user scoping and RLS internally
        let table_name = table_id.full_name(); // "namespace.table"

        // Build SELECT clause: either specific columns or *
        // Always include _seq column for pagination, even if not in projections
        let has_commit_seq = self.table_has_column(table_id, SystemColumnNames::COMMIT_SEQ)?;
        let select_clause = if let Some(cols) = projections {
            // Ensure system resume columns are always included for pagination tracking.
            let mut columns = cols.to_vec();
            if !columns.iter().any(|c| c == SystemColumnNames::SEQ) {
                columns.push(SystemColumnNames::SEQ.to_string());
            }
            if has_commit_seq && !columns.iter().any(|c| c == SystemColumnNames::COMMIT_SEQ) {
                columns.push(SystemColumnNames::COMMIT_SEQ.to_string());
            }
            columns.join(", ")
        } else {
            "*".to_string()
        };

        let mut sql = format!("SELECT {} FROM {}", select_clause, table_name);

        let where_clauses =
            self.build_where_clauses(table_id, table_type, &options, where_clause)?;

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        // Add ORDER BY — use write! to avoid intermediate format! allocations
        if has_commit_seq && options.since_commit_seq.is_some() {
            let direction = if options.fetch_last { "DESC" } else { "ASC" };
            let _ = write!(
                sql,
                " ORDER BY {} {}, {} {}",
                SystemColumnNames::COMMIT_SEQ,
                direction,
                SystemColumnNames::SEQ,
                direction
            );
        } else if options.fetch_last {
            let _ = write!(sql, " ORDER BY {} DESC", SystemColumnNames::SEQ);
        } else {
            let _ = write!(sql, " ORDER BY {} ASC", SystemColumnNames::SEQ);
        }

        // Add LIMIT (fetch limit + 1 to check has_more)
        let _ = write!(sql, " LIMIT {}", limit + 1);

        let batches = self
            .sql_executor()?
            .execute_for_batches(&sql, user_id, role, ReadContext::Internal)
            .await?;

        // Convert batches to Rows
        // Pre-allocate with limit+1 since that's the max we'll fetch
        let mut rows_with_seq: Vec<(SeqId, Option<u64>, Row)> = Vec::with_capacity(limit + 1);

        for batch in batches {
            let schema = batch.schema();
            let seq_col_idx = schema.index_of(SystemColumnNames::SEQ).map_err(|_| {
                LiveError::Other(format!("Result missing {} column", SystemColumnNames::SEQ))
            })?;

            let seq_col = batch.column(seq_col_idx);
            let seq_array = seq_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .ok_or_else(|| {
                    LiveError::Other(format!("{} column is not Int64", SystemColumnNames::SEQ))
                })?;
            let commit_seq_array = if has_commit_seq {
                let commit_idx = schema.index_of(SystemColumnNames::COMMIT_SEQ).map_err(|_| {
                    LiveError::Other(format!(
                        "Result missing {} column",
                        SystemColumnNames::COMMIT_SEQ
                    ))
                })?;
                Some(batch.column(commit_idx))
            } else {
                None
            };

            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();

            for row_idx in 0..num_rows {
                let mut row_map = BTreeMap::new();
                for col_idx in 0..num_cols {
                    let col_name = schema.field(col_idx).name();
                    if col_name == SystemColumnNames::COMMIT_SEQ {
                        continue;
                    }
                    let col_array = batch.column(col_idx);
                    let value = ScalarValue::try_from_array(col_array, row_idx)
                        .into_serialization_error("Failed to convert to ScalarValue")?;
                    row_map.insert(col_name.clone(), value);
                }

                let seq_val = seq_array.value(row_idx);
                let seq_id = SeqId::from(seq_val);
                let commit_seq = commit_seq_array
                    .as_ref()
                    .and_then(|array| ScalarValue::try_from_array(array, row_idx).ok())
                    .and_then(|value| match value {
                        ScalarValue::UInt64(Some(commit_seq)) => Some(commit_seq),
                        ScalarValue::Int64(Some(commit_seq)) if commit_seq >= 0 => {
                            Some(commit_seq as u64)
                        },
                        _ => None,
                    });
                rows_with_seq.push((seq_id, commit_seq, Row::new(row_map)));
            }
        }

        if has_commit_seq && options.since_commit_seq.is_some() {
            rows_with_seq
                .sort_unstable_by_key(|(seq_id, commit_seq, _)| (commit_seq.unwrap_or(0), *seq_id));
        } else {
            rows_with_seq.sort_unstable_by_key(|(seq_id, _, _)| *seq_id);
        }
        if options.fetch_last {
            rows_with_seq.reverse();
        }

        // Determine has_more and slice to limit
        let total_fetched = rows_with_seq.len();
        let has_more = total_fetched > limit;

        // Truncate in-place instead of collecting into a new Vec
        if has_more {
            rows_with_seq.truncate(limit);
        }

        // If we fetched last rows (DESC), we need to reverse them to return in chronological order
        if options.fetch_last {
            rows_with_seq.reverse();
        }

        // Determine snapshot boundary
        let last_seq = rows_with_seq.last().map(|(seq, _, _)| *seq);
        let last_commit_seq = rows_with_seq.last().and_then(|(_, commit_seq, _)| *commit_seq);
        let snapshot_end_seq = options.until_seq.or(last_seq);
        let snapshot_end_commit_seq = options.until_commit_seq.or(last_commit_seq);

        let rows: Vec<Row> = rows_with_seq.into_iter().map(|(_, _, row)| row).collect();

        Ok(InitialDataResult {
            rows,
            last_seq,
            last_commit_seq,
            has_more,
            snapshot_end_seq,
            snapshot_end_commit_seq,
        })
    }

    /// Compute snapshot end sequence for a subscription.
    ///
    /// Compute the snapshot boundary from rows already materialized on this node.
    ///
    /// The boundary deliberately uses local `MAX(_seq)` instead of a wall-clock
    /// Snowflake upper bound. On a follower, the wall-clock bound can include
    /// leader commits that have not applied locally yet; using the local max keeps
    /// the initial snapshot and buffered notification gate aligned with this
    /// replica's actual storage state.
    pub async fn compute_snapshot_end_seq(
        &self,
        live_id: &kalamdb_commons::models::LiveQueryId,
        role: Role,
        table_id: &TableId,
        table_type: TableType,
        options: &InitialDataOptions,
        where_clause: Option<&str>,
    ) -> Result<Option<SeqId>, LiveError> {
        self.compute_snapshot_end_seq_sql_fallback(
            live_id,
            role,
            table_id,
            table_type,
            options,
            where_clause,
        )
        .await
    }

    /// Compute the deterministic commit-sequence snapshot boundary for tables
    /// that expose `_commit_seq`.
    pub async fn compute_snapshot_end_commit_seq(
        &self,
        live_id: &kalamdb_commons::models::LiveQueryId,
        role: Role,
        table_id: &TableId,
        table_type: TableType,
        options: &InitialDataOptions,
        where_clause: Option<&str>,
    ) -> Result<Option<u64>, LiveError> {
        if !self.table_has_column(table_id, SystemColumnNames::COMMIT_SEQ)? {
            return Ok(None);
        }

        let user_id = live_id.user_id().clone();
        let table_name = table_id.full_name();
        let mut sql = format!(
            "SELECT MAX({}) AS max_commit_seq FROM {}",
            SystemColumnNames::COMMIT_SEQ,
            table_name
        );

        let where_clauses =
            self.build_where_clauses(table_id, table_type, options, where_clause)?;
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        let batches = self
            .sql_executor()?
            .execute_for_batches(&sql, user_id, role, ReadContext::Internal)
            .await?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(None);
        }

        let batch = &batches[0];
        let value = ScalarValue::try_from_array(batch.column(0), 0)
            .into_serialization_error("Failed to convert max_commit_seq")?;

        match value {
            ScalarValue::UInt64(Some(commit_seq)) => Ok(Some(commit_seq)),
            ScalarValue::Int64(Some(commit_seq)) if commit_seq >= 0 => Ok(Some(commit_seq as u64)),
            ScalarValue::Null | ScalarValue::UInt64(None) | ScalarValue::Int64(None) => Ok(None),
            _ => Err(LiveError::Other("max_commit_seq column is not an integer".to_string())),
        }
    }

    async fn compute_snapshot_end_seq_sql_fallback(
        &self,
        live_id: &kalamdb_commons::models::LiveQueryId,
        role: Role,
        table_id: &TableId,
        table_type: TableType,
        options: &InitialDataOptions,
        where_clause: Option<&str>,
    ) -> Result<Option<SeqId>, LiveError> {
        let user_id = live_id.user_id().clone();

        let table_name = table_id.full_name();
        let mut sql =
            format!("SELECT MAX({}) AS max_seq FROM {}", SystemColumnNames::SEQ, table_name);

        let where_clauses =
            self.build_where_clauses(table_id, table_type, options, where_clause)?;
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        let batches = self
            .sql_executor()?
            .execute_for_batches(&sql, user_id, role, ReadContext::Internal)
            .await?;

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(None);
        }

        let batch = &batches[0];
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| LiveError::Other("max_seq column is not Int64".to_string()))?;

        if array.is_null(0) {
            return Ok(None);
        }

        Ok(Some(SeqId::from(array.value(0))))
    }

    fn build_where_clauses(
        &self,
        table_id: &TableId,
        table_type: TableType,
        options: &InitialDataOptions,
        where_clause: Option<&str>,
    ) -> Result<Vec<String>, LiveError> {
        let mut where_clauses = Vec::new();

        let has_commit_seq = self.table_has_column(table_id, SystemColumnNames::COMMIT_SEQ)?;

        if has_commit_seq {
            match (options.since_commit_seq, options.since_seq) {
                (Some(since_commit), Some(since_seq)) => where_clauses.push(format!(
                    "({commit_col} > {since_commit} OR ({commit_col} = {since_commit} AND \
                     {seq_col} > {since_seq}))",
                    commit_col = SystemColumnNames::COMMIT_SEQ,
                    seq_col = SystemColumnNames::SEQ,
                    since_seq = since_seq.as_i64()
                )),
                (Some(since_commit), None) => where_clauses.push(format!(
                    "{} > {}",
                    SystemColumnNames::COMMIT_SEQ,
                    since_commit
                )),
                (None, Some(since_seq)) => where_clauses.push(format!(
                    "{} > {}",
                    SystemColumnNames::SEQ,
                    since_seq.as_i64()
                )),
                (None, None) => {},
            }

            if let Some(until_commit_seq) = options.until_commit_seq {
                where_clauses.push(format!(
                    "{} <= {}",
                    SystemColumnNames::COMMIT_SEQ,
                    until_commit_seq
                ));
            } else if let Some(until_seq) = options.until_seq {
                where_clauses.push(format!("{} <= {}", SystemColumnNames::SEQ, until_seq.as_i64()));
            }
        } else {
            if let Some(since) = options.since_seq {
                where_clauses.push(format!("{} > {}", SystemColumnNames::SEQ, since.as_i64()));
            }
            if let Some(until) = options.until_seq {
                where_clauses.push(format!("{} <= {}", SystemColumnNames::SEQ, until.as_i64()));
            }
        }

        if !options.include_deleted
            && matches!(table_type, TableType::User | TableType::Shared)
            && self.table_has_column(table_id, SystemColumnNames::DELETED)?
        {
            where_clauses.push(format!("{} = false", SystemColumnNames::DELETED));
        }

        if let Some(where_sql) = where_clause {
            where_clauses.push(where_sql.to_string());
        }

        Ok(where_clauses)
    }

    fn table_has_column(&self, table_id: &TableId, column_name: &str) -> Result<bool, LiveError> {
        let schema = self.schema_lookup.get_arrow_schema(table_id)?;
        Ok(schema.field_with_name(column_name).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };
    use async_trait::async_trait;
    use datafusion::arrow::record_batch::RecordBatch;
    use kalamdb_commons::{
        models::{LiveQueryId, NamespaceId, TableName, UserId},
        schemas::TableDefinition,
    };
    use parking_lot::Mutex;

    use super::*;

    struct EmptySchemaLookup;

    impl LiveSchemaLookup for EmptySchemaLookup {
        fn get_table_definition(&self, _table_id: &TableId) -> Option<Arc<TableDefinition>> {
            None
        }

        fn get_arrow_schema(&self, _table_id: &TableId) -> Result<Arc<Schema>, LiveError> {
            Ok(Arc::new(Schema::empty()))
        }
    }

    struct MaxSeqExecutor {
        seen_sql: Mutex<Option<String>>,
    }

    #[async_trait]
    impl LiveSqlExecutor for MaxSeqExecutor {
        async fn execute_for_batches(
            &self,
            sql: &str,
            _user_id: kalamdb_commons::models::UserId,
            _role: Role,
            _read_context: ReadContext,
        ) -> Result<Vec<RecordBatch>, LiveError> {
            *self.seen_sql.lock() = Some(sql.to_string());
            let schema = Arc::new(Schema::new(vec![Field::new("max_seq", DataType::Int64, true)]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))])
                .map_err(|err| LiveError::Other(err.to_string()))?;
            Ok(vec![batch])
        }
    }

    #[test]
    fn test_initial_data_options_default() {
        let options = InitialDataOptions::default();
        assert_eq!(options.since_seq, None);
        assert_eq!(options.limit, 100);
        assert!(!options.include_deleted);
        assert!(!options.fetch_last);
    }

    #[test]
    fn test_initial_data_options_since() {
        let seq = SeqId::new(12345);
        let options = InitialDataOptions::since(seq);
        assert_eq!(options.since_seq, Some(seq));
        assert_eq!(options.limit, 100);
        assert!(!options.include_deleted);
        assert!(!options.fetch_last);
    }

    #[test]
    fn test_initial_data_options_last() {
        let options = InitialDataOptions::last(50);
        assert_eq!(options.since_seq, None);
        assert_eq!(options.limit, 50);
        assert!(!options.include_deleted);
        assert!(options.fetch_last);
    }

    #[test]
    fn test_initial_data_options_builder() {
        let seq = SeqId::new(12345);
        let options = InitialDataOptions::since(seq).with_limit(200).with_deleted();

        assert_eq!(options.since_seq, Some(seq));
        assert_eq!(options.limit, 200);
        assert!(options.include_deleted);
    }

    #[tokio::test]
    async fn snapshot_boundary_uses_local_max_seq_query() {
        let fetcher = InitialDataFetcher::new(Arc::new(EmptySchemaLookup));
        let executor = Arc::new(MaxSeqExecutor {
            seen_sql: Mutex::new(None),
        });
        fetcher.set_sql_executor(executor.clone());

        let table_id = TableId::new(NamespaceId::from("app"), TableName::from("items"));
        let live_id = LiveQueryId::new(
            UserId::new("u1"),
            kalamdb_commons::models::ConnectionId::new("c1"),
            "sub1".to_string(),
        );
        let options = InitialDataOptions::default().with_deleted();

        let boundary = fetcher
            .compute_snapshot_end_seq(
                &live_id,
                Role::User,
                &table_id,
                TableType::User,
                &options,
                None,
            )
            .await
            .expect("snapshot boundary");

        assert_eq!(boundary, Some(SeqId::from(42)));
        assert_eq!(
            executor.seen_sql.lock().as_deref(),
            Some("SELECT MAX(_seq) AS max_seq FROM app.items")
        );
    }
}
