use std::io::Cursor;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use kalam_pg_api::{MutationResponse, ScanResponse};
use kalam_pg_common::{KalamPgError, RemoteServerConfig};
use kalamdb_pg::{
    DeleteRpcRequest, InsertRpcRequest, KalamPgService, OpenSessionRequest, PgServiceClient,
    PingRequest, ScanRpcRequest, UpdateRpcRequest,
};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSessionHandle {
    pub session_id: String,
    pub current_schema: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RemoteKalamClient {
    channel: Channel,
    authorization_header: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl RemoteKalamClient {
    pub async fn connect(
        config: RemoteServerConfig,
        authorization_header: Option<String>,
    ) -> Result<Self, KalamPgError> {
        let endpoint = Endpoint::from_shared(format!("http://{}:{}", config.host, config.port))
            .map_err(|error| KalamPgError::Execution(error.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| KalamPgError::Execution(error.to_string()))?;

        let authorization_header = authorization_header
            .map(|value| KalamPgService::authorization_metadata_value(&value))
            .transpose()
            .map_err(|error| KalamPgError::Execution(error.to_string()))?;

        Ok(Self {
            channel,
            authorization_header,
        })
    }

    pub async fn ping(&self) -> Result<(), KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(PingRequest {});
        client
            .ping(request)
            .await
            .map_err(|error| KalamPgError::Execution(error.to_string()))?;
        Ok(())
    }

    pub async fn open_session(
        &self,
        session_id: &str,
        current_schema: Option<&str>,
    ) -> Result<RemoteSessionHandle, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(OpenSessionRequest {
            session_id: session_id.trim().to_string(),
            current_schema: current_schema
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        });
        let response = client
            .open_session(request)
            .await
            .map_err(|error| KalamPgError::Execution(error.to_string()))?
            .into_inner();

        Ok(RemoteSessionHandle {
            session_id: response.session_id,
            current_schema: response.current_schema,
        })
    }

    pub async fn scan(
        &self,
        namespace: &str,
        table_name: &str,
        table_type: &str,
        session_id: &str,
        user_id: Option<&str>,
        columns: Vec<String>,
        limit: Option<u64>,
    ) -> Result<ScanResponse, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(ScanRpcRequest {
            namespace: namespace.to_string(),
            table_name: table_name.to_string(),
            table_type: table_type.to_string(),
            session_id: session_id.to_string(),
            user_id: user_id.map(str::to_string),
            columns,
            limit,
        });
        let response = client
            .scan(request)
            .await
            .map_err(|error| KalamPgError::Execution(error.to_string()))?
            .into_inner();

        let batches = Self::decode_ipc_batches(&response.ipc_batches)?;
        Ok(ScanResponse::new(batches))
    }

    pub async fn insert(
        &self,
        namespace: &str,
        table_name: &str,
        table_type: &str,
        session_id: &str,
        user_id: Option<&str>,
        rows_json: Vec<String>,
    ) -> Result<MutationResponse, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(InsertRpcRequest {
            namespace: namespace.to_string(),
            table_name: table_name.to_string(),
            table_type: table_type.to_string(),
            session_id: session_id.to_string(),
            user_id: user_id.map(str::to_string),
            rows_json,
        });
        let response = client
            .insert(request)
            .await
            .map_err(|error| KalamPgError::Execution(error.to_string()))?
            .into_inner();

        Ok(MutationResponse {
            affected_rows: response.affected_rows,
        })
    }

    pub async fn update(
        &self,
        namespace: &str,
        table_name: &str,
        table_type: &str,
        session_id: &str,
        user_id: Option<&str>,
        pk_value: &str,
        updates_json: &str,
    ) -> Result<MutationResponse, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(UpdateRpcRequest {
            namespace: namespace.to_string(),
            table_name: table_name.to_string(),
            table_type: table_type.to_string(),
            session_id: session_id.to_string(),
            user_id: user_id.map(str::to_string),
            pk_value: pk_value.to_string(),
            updates_json: updates_json.to_string(),
        });
        let response = client
            .update(request)
            .await
            .map_err(|error| KalamPgError::Execution(error.to_string()))?
            .into_inner();

        Ok(MutationResponse {
            affected_rows: response.affected_rows,
        })
    }

    pub async fn delete(
        &self,
        namespace: &str,
        table_name: &str,
        table_type: &str,
        session_id: &str,
        user_id: Option<&str>,
        pk_value: &str,
    ) -> Result<MutationResponse, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(DeleteRpcRequest {
            namespace: namespace.to_string(),
            table_name: table_name.to_string(),
            table_type: table_type.to_string(),
            session_id: session_id.to_string(),
            user_id: user_id.map(str::to_string),
            pk_value: pk_value.to_string(),
        });
        let response = client
            .delete(request)
            .await
            .map_err(|error| KalamPgError::Execution(error.to_string()))?
            .into_inner();

        Ok(MutationResponse {
            affected_rows: response.affected_rows,
        })
    }

    fn authorized_request<T>(&self, payload: T) -> Request<T> {
        let mut request = Request::new(payload);
        if let Some(authorization_header) = &self.authorization_header {
            request
                .metadata_mut()
                .insert("authorization", authorization_header.clone());
        }
        request
    }

    /// Decode Arrow IPC bytes received from the gRPC response into RecordBatches.
    fn decode_ipc_batches(
        ipc_batches: &[bytes::Bytes],
    ) -> Result<Vec<RecordBatch>, KalamPgError> {
        let mut batches = Vec::with_capacity(ipc_batches.len());
        for ipc_bytes in ipc_batches {
            let cursor = Cursor::new(ipc_bytes.as_ref());
            let reader = StreamReader::try_new(cursor, None)
                .map_err(|e| KalamPgError::Execution(format!("failed to read IPC stream: {}", e)))?;
            for batch_result in reader {
                let batch = batch_result.map_err(|e| {
                    KalamPgError::Execution(format!("failed to decode IPC batch: {}", e))
                })?;
                batches.push(batch);
            }
        }
        Ok(batches)
    }
}
