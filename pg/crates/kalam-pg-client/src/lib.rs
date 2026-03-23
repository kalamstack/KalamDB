use std::io::Cursor;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use kalam_pg_api::{MutationResponse, ScanResponse};
use kalam_pg_common::{KalamPgError, RemoteServerConfig};
use kalamdb_pg::{
    BeginTransactionRequest, CloseSessionRequest, CommitTransactionRequest, DeleteRpcRequest,
    InsertRpcRequest, OpenSessionRequest, PgServiceClient, PingRequest,
    RollbackTransactionRequest, ScanRpcRequest, UpdateRpcRequest,
};
#[cfg(feature = "tls")]
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

/// Load PEM material from either an inline PEM string or a file path.
#[cfg(feature = "tls")]
fn load_pem(value: &str) -> Result<Vec<u8>, String> {
    if value.contains("-----BEGIN") {
        Ok(value.as_bytes().to_vec())
    } else {
        std::fs::read(value).map_err(|e| format!("Failed reading PEM file '{}': {}", value, e))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSessionHandle {
    pub session_id: String,
    pub current_schema: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RemoteKalamClient {
    channel: Channel,
    /// "host:port" used in error messages.
    server_addr: String,
    /// Value to send as the gRPC `authorization` metadata header.
    auth_header: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

impl RemoteKalamClient {
    pub async fn connect(
        config: RemoteServerConfig,
    ) -> Result<Self, KalamPgError> {
        let server_addr = format!("{}:{}", config.host, config.port);
        let mut endpoint = Endpoint::from_shared(config.endpoint_uri())
            .map_err(|error| KalamPgError::Execution(error.to_string()))?;

        // Apply connection and request timeout if configured
        if config.timeout_ms > 0 {
            let timeout = Duration::from_millis(config.timeout_ms);
            endpoint = endpoint.connect_timeout(timeout).timeout(timeout);
        }

        if config.tls_enabled() {
            #[cfg(feature = "tls")]
            {
                let tls = Self::build_tls_config(&config)?;
                endpoint = endpoint
                    .tls_config(tls)
                    .map_err(|error| KalamPgError::Execution(error.to_string()))?;
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(KalamPgError::Execution(
                    "TLS requested but kalam-pg-client was compiled without the 'tls' feature".to_string(),
                ));
            }
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|error| Self::connect_err(&error, &server_addr))?;

        let auth_header = config
            .auth_header
            .as_deref()
            .filter(|v| !v.is_empty())
            .and_then(|v| v.parse::<tonic::metadata::MetadataValue<_>>().ok());

        Ok(Self {
            channel,
            server_addr,
            auth_header,
        })
    }

    /// Convert a transport-level connection error into a user-readable message.
    fn connect_err(error: &tonic::transport::Error, server_addr: &str) -> KalamPgError {
        let detail = error.to_string();
        // Detect OS-level connection failures (refused, timed out, unreachable)
        if detail.contains("connection refused")
            || detail.contains("Connection refused")
            || detail.contains("os error 111")
            || detail.contains("os error 61")
        {
            KalamPgError::ServerUnreachable(server_addr.to_string())
        } else if detail.contains("timed out") || detail.contains("deadline") {
            KalamPgError::Execution(format!(
                "connection to KalamDB server at {server_addr} timed out – \
                 check the server is running and the port is correct"
            ))
        } else if detail.contains("certificate") || detail.contains("tls") || detail.contains("TLS") {
            KalamPgError::Execution(format!(
                "TLS handshake with KalamDB server at {server_addr} failed: {detail}"
            ))
        } else {
            KalamPgError::Execution(format!(
                "could not connect to KalamDB server at {server_addr}: {detail}"
            ))
        }
    }

    /// Convert a gRPC status error into a user-readable KalamPgError.
    fn grpc_err(status: tonic::Status, server_addr: &str) -> KalamPgError {
        use tonic::Code;
        match status.code() {
            Code::Unavailable => {
                let msg = status.message();
                if msg.contains("connection refused")
                    || msg.contains("Connection refused")
                    || msg.is_empty()
                    || msg.contains("transport error")
                {
                    KalamPgError::ServerUnreachable(server_addr.to_string())
                } else {
                    KalamPgError::Execution(format!(
                        "KalamDB server at {server_addr} is unavailable: {msg}"
                    ))
                }
            }
            Code::NotFound => {
                let msg = status.message();
                KalamPgError::Execution(format!("not found: {msg}"))
            }
            Code::Unauthenticated => KalamPgError::Execution(
                "authentication failed – check the auth_header in CREATE SERVER OPTIONS".to_string(),
            ),
            Code::PermissionDenied => KalamPgError::Execution(format!(
                "permission denied: {}",
                status.message()
            )),
            Code::DeadlineExceeded => KalamPgError::Execution(format!(
                "request to KalamDB server at {server_addr} timed out"
            )),
            Code::AlreadyExists => KalamPgError::Execution(format!(
                "already exists: {}",
                status.message()
            )),
            Code::InvalidArgument => KalamPgError::Execution(format!(
                "invalid argument: {}",
                status.message()
            )),
            Code::Unimplemented => KalamPgError::Execution(format!(
                "operation not supported by KalamDB server at {server_addr}: {}",
                status.message()
            )),
            Code::Internal => KalamPgError::Execution(format!(
                "internal KalamDB error: {}",
                status.message()
            )),
            _ => KalamPgError::Execution(format!(
                "KalamDB error ({}): {}",
                status.code(),
                status.message()
            )),
        }
    }

    /// Build a TLS configuration from the remote server config.
    /// Supports both inline PEM strings and file paths.
    #[cfg(feature = "tls")]
    fn build_tls_config(config: &RemoteServerConfig) -> Result<ClientTlsConfig, KalamPgError> {
        let ca_pem = config
            .ca_cert
            .as_deref()
            .ok_or_else(|| KalamPgError::Execution("ca_cert is required for TLS".to_string()))?;
        let ca_bytes = load_pem(ca_pem)
            .map_err(|e| KalamPgError::Execution(format!("ca_cert: {e}")))?;
        let mut tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_bytes));

        if let (Some(cert_str), Some(key_str)) =
            (config.client_cert.as_deref(), config.client_key.as_deref())
        {
            let cert_bytes = load_pem(cert_str)
                .map_err(|e| KalamPgError::Execution(format!("client_cert: {e}")))?;
            let key_bytes = load_pem(key_str)
                .map_err(|e| KalamPgError::Execution(format!("client_key: {e}")))?;
            tls = tls.identity(Identity::from_pem(cert_bytes, key_bytes));
        }

        Ok(tls)
    }

    pub async fn ping(&self) -> Result<(), KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(PingRequest {});
        client
            .ping(request)
            .await
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?;
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
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?
            .into_inner();

        Ok(RemoteSessionHandle {
            session_id: response.session_id,
            current_schema: response.current_schema,
        })
    }

    /// Close a session and free server-side resources.
    pub async fn close_session(
        &self,
        session_id: &str,
    ) -> Result<(), KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(CloseSessionRequest {
            session_id: session_id.trim().to_string(),
        });
        client
            .close_session(request)
            .await
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?;
        Ok(())
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
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?
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
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?
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
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?
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
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?
            .into_inner();

        Ok(MutationResponse {
            affected_rows: response.affected_rows,
        })
    }

    /// Begin a new transaction within the given session.
    pub async fn begin_transaction(
        &self,
        session_id: &str,
    ) -> Result<String, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(BeginTransactionRequest {
            session_id: session_id.to_string(),
        });
        let response = client
            .begin_transaction(request)
            .await
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?
            .into_inner();
        Ok(response.transaction_id)
    }

    /// Commit an active transaction.
    pub async fn commit_transaction(
        &self,
        session_id: &str,
        transaction_id: &str,
    ) -> Result<String, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(CommitTransactionRequest {
            session_id: session_id.to_string(),
            transaction_id: transaction_id.to_string(),
        });
        let response = client
            .commit_transaction(request)
            .await
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?
            .into_inner();
        Ok(response.transaction_id)
    }

    /// Rollback an active transaction. Idempotent for already-rolled-back handles.
    pub async fn rollback_transaction(
        &self,
        session_id: &str,
        transaction_id: &str,
    ) -> Result<String, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let request = self.authorized_request(RollbackTransactionRequest {
            session_id: session_id.to_string(),
            transaction_id: transaction_id.to_string(),
        });
        let response = client
            .rollback_transaction(request)
            .await
            .map_err(|e| Self::grpc_err(e, &self.server_addr))?
            .into_inner();
        Ok(response.transaction_id)
    }

    fn authorized_request<T>(&self, payload: T) -> Request<T> {
        let mut req = Request::new(payload);
        if let Some(val) = &self.auth_header {
            req.metadata_mut().insert("authorization", val.clone());
        }
        req
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
