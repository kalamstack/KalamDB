// backend/crates/kalamdb-live/src/helpers/initial_data.rs
//
// Initial data fetch for live query subscriptions.
// Provides "changes since timestamp" functionality to populate client state
// before real-time notifications begin.

use crate::error::{LiveError, LiveResultExt};
use crate::traits::{LiveSchemaLookup, LiveSqlExecutor};
use datafusion::arrow::array::{Array, Int64Array};
use datafusion_common::ScalarValue;
use kalamdb_commons::constants::SystemColumnNames;
use kalamdb_commons::ids::SeqId;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::{ReadContext, TableId};
use kalamdb_commons::Role;
use kalamdb_commons::TableType;
use once_cell::sync::OnceCell;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Options for fetching initial data when subscribing to a live query
#[derive(Debug, Clone)]
pub struct InitialDataOptions {
    /// Fetch changes since this sequence ID (exclusive)
    /// If None, starts from the beginning (or end, depending on strategy)
    pub since_seq: Option<SeqId>,

    /// Fetch changes up to this sequence ID (inclusive)
    /// Used to define the snapshot boundary
    pub until_seq: Option<SeqId>,

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
}

/// Result of an initial data fetch
#[derive(Debug)]
pub struct InitialDataResult {
    /// The fetched rows (as Row objects)
    pub rows: Vec<Row>,

    /// Sequence ID of the last row in the result
    /// Used for pagination (passed as since_seq in next request)
    pub last_seq: Option<SeqId>,

    /// Whether there are more rows available in the snapshot range
    pub has_more: bool,

    /// The snapshot boundary used for this fetch
    pub snapshot_end_seq: Option<SeqId>,
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
    pub fn new(
        schema_lookup: Arc<dyn LiveSchemaLookup>,
    ) -> Self {
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
                has_more: false,
                snapshot_end_seq: None,
            });
        }

        // Extract user_id from LiveId for RLS
        let user_id = live_id.user_id().clone();

        // Execute via trait — handles user scoping and RLS internally
        let table_name = table_id.full_name(); // "namespace.table"

        // Build SELECT clause: either specific columns or *
        // Always include _seq column for pagination, even if not in projections
        let select_clause = if let Some(cols) = projections {
            // Ensure _seq is always included for pagination tracking
            let mut columns = cols.to_vec();
            if !columns.iter().any(|c| c == SystemColumnNames::SEQ) {
                columns.push(SystemColumnNames::SEQ.to_string());
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

        // Add ORDER BY
        if options.fetch_last {
            sql.push_str(&format!(" ORDER BY {} DESC", SystemColumnNames::SEQ));
        } else {
            sql.push_str(&format!(" ORDER BY {} ASC", SystemColumnNames::SEQ));
        }

        // Add LIMIT (fetch limit + 1 to check has_more)
        sql.push_str(&format!(" LIMIT {}", limit + 1));

        let batches = self
            .sql_executor()?
            .execute_for_batches(&sql, user_id, role, ReadContext::Internal)
            .await?;

        // Convert batches to Rows
        // Pre-allocate with limit+1 since that's the max we'll fetch
        let mut rows_with_seq: Vec<(SeqId, Row)> = Vec::with_capacity(limit + 1);

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

            let num_rows = batch.num_rows();
            let num_cols = batch.num_columns();

            for row_idx in 0..num_rows {
                let mut row_map = BTreeMap::new();
                for col_idx in 0..num_cols {
                    let col_name = schema.field(col_idx).name();
                    let col_array = batch.column(col_idx);
                    let value = ScalarValue::try_from_array(col_array, row_idx)
                        .into_serialization_error("Failed to convert to ScalarValue")?;
                    row_map.insert(col_name.clone(), value);
                }

                let seq_val = seq_array.value(row_idx);
                let seq_id = SeqId::from(seq_val);
                rows_with_seq.push((seq_id, Row::new(row_map)));
            }
        }

        rows_with_seq.sort_unstable_by_key(|(seq_id, _)| *seq_id);
        if options.fetch_last {
            rows_with_seq.reverse();
        }

        // Determine has_more and slice to limit
        let total_fetched = rows_with_seq.len();
        let has_more = total_fetched > limit;

        let mut batch_rows = if has_more {
            rows_with_seq.into_iter().take(limit).collect::<Vec<_>>()
        } else {
            rows_with_seq
        };

        // If we fetched last rows (DESC), we need to reverse them to return in chronological order
        if options.fetch_last {
            batch_rows.reverse();
        }

        // Determine snapshot boundary
        let last_seq = batch_rows.last().map(|(seq, _)| *seq);
        let snapshot_end_seq = options.until_seq.or(last_seq);

        let rows: Vec<Row> = batch_rows.into_iter().map(|(_, row)| row).collect();

        Ok(InitialDataResult {
            rows,
            last_seq,
            has_more,
            snapshot_end_seq,
        })
    }

    /// Compute snapshot end sequence for a subscription
    ///
    /// Uses MAX(_seq) with the same filters as initial data to define a snapshot boundary.
    pub async fn compute_snapshot_end_seq(
        &self,
        live_id: &kalamdb_commons::models::LiveQueryId,
        role: Role,
        table_id: &TableId,
        table_type: TableType,
        options: &InitialDataOptions,
        where_clause: Option<&str>,
    ) -> Result<Option<SeqId>, LiveError> {
        // Extract user_id from LiveId for RLS
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

        if let Some(since) = options.since_seq {
            where_clauses.push(format!("{} > {}", SystemColumnNames::SEQ, since.as_i64()));
        }
        if let Some(until) = options.until_seq {
            where_clauses.push(format!("{} <= {}", SystemColumnNames::SEQ, until.as_i64()));
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

    fn table_has_column(
        &self,
        table_id: &TableId,
        column_name: &str,
    ) -> Result<bool, LiveError> {
        let schema = self.schema_lookup.get_arrow_schema(table_id)?;
        Ok(schema.field_with_name(column_name).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

