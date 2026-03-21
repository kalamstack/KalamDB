use async_trait::async_trait;
use kalam_pg_api::request::{DeleteRequest, InsertRequest, ScanRequest, UpdateRequest};
use kalam_pg_api::response::{MutationResponse, ScanResponse};
use kalam_pg_api::KalamBackendExecutor;
use kalam_pg_client::RemoteKalamClient;
use kalam_pg_common::KalamPgError;

/// Remote backend executor that delegates to a KalamDB server via gRPC.
pub struct RemoteBackendExecutor {
    client: RemoteKalamClient,
    session_id: String,
}

impl RemoteBackendExecutor {
    pub fn new(client: RemoteKalamClient, session_id: String) -> Self {
        Self { client, session_id }
    }
}

#[async_trait]
impl KalamBackendExecutor for RemoteBackendExecutor {
    async fn scan(&self, request: ScanRequest) -> Result<ScanResponse, KalamPgError> {
        let user_id = request
            .tenant_context
            .effective_user_id()
            .map(|u| u.as_str().to_string());

        let columns = request.projection.unwrap_or_default();
        let limit = request.limit.map(|l| l as u64);

        self.client
            .scan(
                request.table_id.namespace_id().as_str(),
                request.table_id.table_name().as_str(),
                request.table_type.as_str(),
                &self.session_id,
                user_id.as_deref(),
                columns,
                limit,
            )
            .await
    }

    async fn insert(&self, request: InsertRequest) -> Result<MutationResponse, KalamPgError> {
        let user_id = request
            .tenant_context
            .effective_user_id()
            .map(|u| u.as_str().to_string());

        let rows_json: Vec<String> = request
            .rows
            .iter()
            .map(|row| serde_json::to_string(row).unwrap_or_default())
            .collect();

        self.client
            .insert(
                request.table_id.namespace_id().as_str(),
                request.table_id.table_name().as_str(),
                request.table_type.as_str(),
                &self.session_id,
                user_id.as_deref(),
                rows_json,
            )
            .await
    }

    async fn update(&self, request: UpdateRequest) -> Result<MutationResponse, KalamPgError> {
        let user_id = request
            .tenant_context
            .effective_user_id()
            .map(|u| u.as_str().to_string());

        let updates_json =
            serde_json::to_string(&request.updates).unwrap_or_else(|_| "{}".to_string());

        self.client
            .update(
                request.table_id.namespace_id().as_str(),
                request.table_id.table_name().as_str(),
                request.table_type.as_str(),
                &self.session_id,
                user_id.as_deref(),
                &request.pk_value,
                &updates_json,
            )
            .await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, KalamPgError> {
        let user_id = request
            .tenant_context
            .effective_user_id()
            .map(|u| u.as_str().to_string());

        self.client
            .delete(
                request.table_id.namespace_id().as_str(),
                request.table_id.table_name().as_str(),
                request.table_type.as_str(),
                &self.session_id,
                user_id.as_deref(),
                &request.pk_value,
            )
            .await
    }
}
