//! System table initialization entrypoint.
//!
//! System schema lifecycle is now reconciled by
//! `SchemaRegistry::initialize_tables()` and persisted in `system.schemas`.
//! This function remains as a compatibility no-op for callers that still invoke
//! it during bootstrap.

use std::sync::Arc;

use kalamdb_store::StorageBackend;

use crate::error::SystemError;

/// Compatibility no-op.
///
/// System tables are now initialized and evolved through schema reconciliation
/// in `SchemaRegistry`, not via a separate version key.
pub async fn initialize_system_tables(
    _storage_backend: Arc<dyn StorageBackend>,
) -> Result<(), SystemError> {
    log::debug!(
        "initialize_system_tables() is a no-op; system schema reconciliation is handled by \
         SchemaRegistry"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use kalamdb_store::test_utils::InMemoryBackend;

    use super::*;

    #[tokio::test]
    async fn initialization_is_noop_and_succeeds() {
        let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
        let result = initialize_system_tables(backend).await;
        assert!(result.is_ok());
    }
}
