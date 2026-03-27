use crate::request::{DeleteRequest, InsertRequest, ScanRequest, UpdateRequest};
use crate::response::{MutationResponse, ScanResponse};
use async_trait::async_trait;
use kalam_pg_common::KalamPgError;

/// Backend executor abstraction for remote mode.
#[async_trait]
pub trait KalamBackendExecutor: Send + Sync {
    async fn scan(&self, request: ScanRequest) -> Result<ScanResponse, KalamPgError>;

    async fn insert(&self, request: InsertRequest) -> Result<MutationResponse, KalamPgError>;

    async fn update(&self, request: UpdateRequest) -> Result<MutationResponse, KalamPgError>;

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, KalamPgError>;
}
