//! Unified KalamDB table provider trait.
//!
//! `KalamTableProvider` extends DataFusion's `TableProvider` with KalamDB-specific
//! DML operations (INSERT, and future methods). All table types — User, Shared,
//! Stream, and System — implement this trait, giving a single provider abstraction
//! for the entire caching and execution layer.
//!
//! System tables use the default `insert_rows()` which returns an error (read-only).
//! User/Shared/Stream providers override it with actual batch-insert logic.

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::scalar::ScalarValue;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;

use crate::error::KalamDbError;

/// Unified table provider that extends DataFusion's `TableProvider` with
/// KalamDB-specific DML operations.
///
/// Stored in `CachedTableData` as the single provider reference. The fast INSERT
/// path calls `insert_rows()` directly, bypassing DataFusion's optimizer (~2.6ms).
///
/// All table types implement this trait:
/// - **User/Shared/Stream**: Override `insert_rows()` with batch-insert logic.
/// - **System**: Use the default (returns error — system tables are read-only).
#[async_trait]
pub trait KalamTableProvider: TableProvider + Send + Sync {
    /// Insert rows directly, returning the number of rows inserted.
    ///
    /// Default implementation returns an error for read-only tables (system tables).
    /// User/Shared/Stream providers override with `BaseTableProvider::insert_batch()`.
    async fn insert_rows(&self, user_id: &UserId, rows: Vec<Row>) -> Result<usize, KalamDbError> {
        let _ = (user_id, rows);
        Err(KalamDbError::InvalidOperation(
            "INSERT not supported for this table type".into(),
        ))
    }

    /// Insert rows and return the generated sequence IDs.
    ///
    /// Used by INSERT ... RETURNING _seq to return auto-generated row IDs.
    /// Default implementation calls `insert_rows` and returns an empty vec.
    async fn insert_rows_returning(
        &self,
        user_id: &UserId,
        rows: Vec<Row>,
    ) -> Result<Vec<ScalarValue>, KalamDbError> {
        let _count = self.insert_rows(user_id, rows).await?;
        Ok(Vec::new())
    }

    /// Update a single row addressed by primary key.
    ///
    /// Returns `Ok(true)` if a row was updated, `Ok(false)` if no matching row exists.
    async fn update_row_by_pk(
        &self,
        user_id: &UserId,
        pk_value: &str,
        updates: Row,
    ) -> Result<bool, KalamDbError> {
        let _ = (user_id, pk_value, updates);
        Err(KalamDbError::InvalidOperation(
            "UPDATE not supported for this table type".into(),
        ))
    }

    /// Delete a single row addressed by primary key.
    ///
    /// Returns `Ok(true)` if a row was deleted, `Ok(false)` if no matching row exists.
    async fn delete_row_by_pk(
        &self,
        user_id: &UserId,
        pk_value: &str,
    ) -> Result<bool, KalamDbError> {
        let _ = (user_id, pk_value);
        Err(KalamDbError::InvalidOperation(
            "DELETE not supported for this table type".into(),
        ))
    }
}
