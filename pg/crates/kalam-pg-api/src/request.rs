use crate::session::{RemoteSessionContext, TenantContext};
use crate::filter::ScanFilter;
use kalam_pg_common::KalamPgError;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::{TableId, TableType};

fn validate_write_scope(
    table_type: TableType,
    tenant_context: &TenantContext,
    operation: &str,
) -> Result<(), KalamPgError> {
    tenant_context.validate()?;
    if matches!(table_type, TableType::User | TableType::Stream)
        && tenant_context.effective_user_id().is_none()
    {
        return Err(KalamPgError::Validation(format!(
            "user_id is required for {} {}",
            table_type, operation
        )));
    }

    Ok(())
}

/// Scan request built from PostgreSQL planner state.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub tenant_context: TenantContext,
    pub remote_session: Option<RemoteSessionContext>,
    pub projection: Option<Vec<String>>,
    pub filters: Vec<ScanFilter>,
    pub limit: Option<usize>,
}

impl ScanRequest {
    /// Create a minimal scan request for a table.
    pub fn new(table_id: TableId, table_type: TableType, tenant_context: TenantContext) -> Self {
        Self {
            table_id,
            table_type,
            tenant_context,
            remote_session: None,
            projection: None,
            filters: Vec::new(),
            limit: None,
        }
    }

    /// Validate the request before execution.
    pub fn validate(&self) -> Result<(), KalamPgError> {
        self.tenant_context.validate()?;
        if let Some(remote_session) = &self.remote_session {
            remote_session.validate()?;
        }
        if matches!(self.table_type, TableType::User | TableType::Stream)
            && self.tenant_context.effective_user_id().is_none()
        {
            return Err(KalamPgError::Validation(format!(
                "user_id is required for {} table scans",
                self.table_type
            )));
        }
        Ok(())
    }
}

/// Insert request built from PostgreSQL modify callbacks.
#[derive(Debug, Clone)]
pub struct InsertRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub tenant_context: TenantContext,
    pub remote_session: Option<RemoteSessionContext>,
    pub rows: Vec<Row>,
}

impl InsertRequest {
    /// Create an insert request with rows already converted from PostgreSQL tuples.
    pub fn new(
        table_id: TableId,
        table_type: TableType,
        tenant_context: TenantContext,
        rows: Vec<Row>,
    ) -> Self {
        Self {
            table_id,
            table_type,
            tenant_context,
            remote_session: None,
            rows,
        }
    }

    /// Validate the request before execution.
    pub fn validate(&self) -> Result<(), KalamPgError> {
        validate_write_scope(self.table_type, &self.tenant_context, "inserts")?;
        if let Some(remote_session) = &self.remote_session {
            remote_session.validate()?;
        }
        if self.rows.is_empty() {
            return Err(KalamPgError::Validation(
                "insert request must include at least one row".to_string(),
            ));
        }

        Ok(())
    }
}

/// Update request built from PostgreSQL modify callbacks.
#[derive(Debug, Clone)]
pub struct UpdateRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub tenant_context: TenantContext,
    pub remote_session: Option<RemoteSessionContext>,
    pub pk_value: String,
    pub updates: Row,
}

impl UpdateRequest {
    /// Create an update request with a primary-key lookup and replacement values.
    pub fn new(
        table_id: TableId,
        table_type: TableType,
        tenant_context: TenantContext,
        pk_value: String,
        updates: Row,
    ) -> Self {
        Self {
            table_id,
            table_type,
            tenant_context,
            remote_session: None,
            pk_value,
            updates,
        }
    }

    /// Validate the request before execution.
    pub fn validate(&self) -> Result<(), KalamPgError> {
        validate_write_scope(self.table_type, &self.tenant_context, "updates")?;
        if let Some(remote_session) = &self.remote_session {
            remote_session.validate()?;
        }
        if self.pk_value.trim().is_empty() {
            return Err(KalamPgError::Validation(
                "update request must include a primary-key value".to_string(),
            ));
        }
        if self.updates.is_empty() {
            return Err(KalamPgError::Validation(
                "update request must include at least one column assignment".to_string(),
            ));
        }

        Ok(())
    }
}

/// Delete request built from PostgreSQL modify callbacks.
#[derive(Debug, Clone)]
pub struct DeleteRequest {
    pub table_id: TableId,
    pub table_type: TableType,
    pub tenant_context: TenantContext,
    pub remote_session: Option<RemoteSessionContext>,
    pub pk_value: String,
}

impl DeleteRequest {
    /// Create a delete request for a single primary-key value.
    pub fn new(
        table_id: TableId,
        table_type: TableType,
        tenant_context: TenantContext,
        pk_value: String,
    ) -> Self {
        Self {
            table_id,
            table_type,
            tenant_context,
            remote_session: None,
            pk_value,
        }
    }

    /// Validate the request before execution.
    pub fn validate(&self) -> Result<(), KalamPgError> {
        validate_write_scope(self.table_type, &self.tenant_context, "deletes")?;
        if let Some(remote_session) = &self.remote_session {
            remote_session.validate()?;
        }
        if self.pk_value.trim().is_empty() {
            return Err(KalamPgError::Validation(
                "delete request must include a primary-key value".to_string(),
            ));
        }

        Ok(())
    }
}
