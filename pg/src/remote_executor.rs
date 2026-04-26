use async_trait::async_trait;
use datafusion_common::ScalarValue;
use kalam_pg_api::{
    request::{DeleteRequest, InsertRequest, ScanRequest, UpdateRequest},
    response::{MutationResponse, ScanResponse},
    KalamBackendExecutor,
};
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
        request.validate()?;
        let user_id = request.tenant_context.effective_user_id().map(|u| u.as_str().to_string());

        let columns = request.projection.unwrap_or_default();
        let limit = request.limit.map(|l| l as u64);

        // Convert ScanFilter::Eq to (column, value) pairs for gRPC pushdown
        let filters: Vec<(String, String)> = request
            .filters
            .iter()
            .filter_map(|f| match f {
                kalam_pg_api::ScanFilter::Eq { column, value } => {
                    scalar_to_string(value).map(|v| (column.clone(), v))
                },
            })
            .collect();

        self.client
            .scan(
                request.table_id.namespace_id().as_str(),
                request.table_id.table_name().as_str(),
                request.table_type.as_str(),
                &self.session_id,
                user_id.as_deref(),
                columns,
                limit,
                filters,
            )
            .await
    }

    async fn insert(&self, request: InsertRequest) -> Result<MutationResponse, KalamPgError> {
        request.validate()?;
        let user_id = request.tenant_context.effective_user_id().map(|u| u.as_str().to_string());

        let rows_json: Vec<String> = request
            .rows
            .iter()
            .map(|row| {
                serde_json::to_string(row).map_err(|error| {
                    KalamPgError::Execution(format!(
                        "failed to serialize insert row for {}: {}",
                        request.table_id.full_name(),
                        error
                    ))
                })
            })
            .collect::<Result<_, _>>()?;

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
        request.validate()?;
        let user_id = request.tenant_context.effective_user_id().map(|u| u.as_str().to_string());

        let updates_json = serde_json::to_string(&request.updates).map_err(|error| {
            KalamPgError::Execution(format!(
                "failed to serialize update payload for {} pk {}: {}",
                request.table_id.full_name(),
                request.pk_value,
                error
            ))
        })?;

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
        request.validate()?;
        let user_id = request.tenant_context.effective_user_id().map(|u| u.as_str().to_string());

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

/// Convert a DataFusion ScalarValue to a plain string for gRPC filter transport.
fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
        ScalarValue::Int8(Some(v)) => Some(v.to_string()),
        ScalarValue::Int16(Some(v)) => Some(v.to_string()),
        ScalarValue::Int32(Some(v)) => Some(v.to_string()),
        ScalarValue::Int64(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
        ScalarValue::Float32(Some(v)) => Some(v.to_string()),
        ScalarValue::Float64(Some(v)) => Some(v.to_string()),
        _ => None,
    }
}
