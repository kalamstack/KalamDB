use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use kalamdb_commons::models::{TableId, UserId};
use kalamdb_filestore::StorageCached;
use kalamdb_system::Manifest;
use kalamdb_vector::{
    flush_shared_scope_vectors as flush_shared_scope_vectors_impl,
    flush_user_scope_vectors as flush_user_scope_vectors_impl, VectorFlushError,
    VectorManifestStore,
};

use crate::{app_context::AppContext, error::KalamDbError, manifest::ManifestService};

struct CoreVectorManifestStore<'a> {
    manifest_service: &'a ManifestService,
}

impl<'a> CoreVectorManifestStore<'a> {
    fn new(manifest_service: &'a ManifestService) -> Self {
        Self { manifest_service }
    }
}

impl VectorManifestStore for CoreVectorManifestStore<'_> {
    fn ensure_manifest_initialized(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
    ) -> Result<Manifest, VectorFlushError> {
        self.manifest_service
            .ensure_manifest_initialized(table_id, user_id)
            .map_err(|e| VectorFlushError::new(format!("Failed to initialize manifest: {}", e)))
    }

    fn persist_manifest(
        &self,
        table_id: &TableId,
        user_id: Option<&UserId>,
        manifest: &Manifest,
    ) -> Result<(), VectorFlushError> {
        self.manifest_service
            .persist_manifest(table_id, user_id, manifest)
            .map_err(|e| {
                VectorFlushError::new(format!(
                    "Failed to persist manifest with vector metadata: {}",
                    e
                ))
            })
    }
}

fn to_kalam_error(err: VectorFlushError) -> KalamDbError {
    KalamDbError::Other(err.to_string())
}

pub fn flush_user_scope_vectors(
    app_context: &Arc<AppContext>,
    table_id: &TableId,
    user_id: &UserId,
    schema: &SchemaRef,
    storage_cached: &StorageCached,
) -> Result<(), KalamDbError> {
    let backend = app_context.storage_backend();
    let manifest_service = app_context.manifest_service();
    let manifest_store = CoreVectorManifestStore::new(manifest_service.as_ref());

    flush_user_scope_vectors_impl(
        backend,
        &manifest_store,
        table_id,
        user_id,
        schema,
        storage_cached,
    )
    .map_err(to_kalam_error)
}

pub fn flush_shared_scope_vectors(
    app_context: &Arc<AppContext>,
    table_id: &TableId,
    schema: &SchemaRef,
    storage_cached: &StorageCached,
) -> Result<(), KalamDbError> {
    let backend = app_context.storage_backend();
    let manifest_service = app_context.manifest_service();
    let manifest_store = CoreVectorManifestStore::new(manifest_service.as_ref());

    flush_shared_scope_vectors_impl(backend, &manifest_store, table_id, schema, storage_cached)
        .map_err(to_kalam_error)
}
