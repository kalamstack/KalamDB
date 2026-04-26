use std::{io::Cursor, time::Duration};

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use kalam_pg_api::{MutationResponse, ScanResponse};
use kalam_pg_common::{KalamPgError, RemoteAuthMode, RemoteServerConfig};
use kalamdb_pg::{
    BeginTransactionRequest, CloseSessionRequest, CommitTransactionRequest, DeleteRpcRequest,
    ExecuteQueryRpcRequest, ExecuteSqlRpcRequest, InsertRpcRequest, OpenSessionRequest,
    PgServiceClient, PingRequest, RollbackTransactionRequest, ScanFilterExpression, ScanRpcRequest,
    UpdateRpcRequest,
};
#[cfg(feature = "tls")]
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use tonic::{
    transport::{Channel, Endpoint},
    Request,
};

/// Load PEM material from either an inline PEM string or a file path.
fn load_pem(value: &str) -> Result<Vec<u8>, String> {
    if value.contains("-----BEGIN") {
        Ok(value.as_bytes().to_vec())
    } else {
        std::fs::read(value).map_err(|e| format!("Failed reading PEM file '{}': {}", value, e))
    }
}

/// Build a `Basic base64(user:pass)` authorization metadata value.
fn build_basic_auth_metadata(
    user: &str,
    password: &str,
) -> Result<tonic::metadata::MetadataValue<tonic::metadata::Ascii>, KalamPgError> {
    use base64::Engine;
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", user, password));
    format!("Basic {}", encoded)
        .parse::<tonic::metadata::MetadataValue<_>>()
        .map_err(|error| {
            KalamPgError::Validation(format!("failed to build Basic auth metadata: {}", error))
        })
}

/// Auth metadata to send on `open_session` only.
#[derive(Debug, Clone)]
enum OpenSessionAuth {
    /// No auth metadata.
    None,
    /// Pre-shared static header value (e.g. `Bearer <shared-secret>`).
    StaticHeader(tonic::metadata::MetadataValue<tonic::metadata::Ascii>),
    /// Basic auth with username/password, sent as `Basic base64(user:pass)`.
    BasicCredentials(tonic::metadata::MetadataValue<tonic::metadata::Ascii>),
}

impl OpenSessionAuth {
    fn from_config(config: &RemoteServerConfig) -> Result<Self, KalamPgError> {
        match config.effective_auth_mode() {
            RemoteAuthMode::None => Ok(Self::None),
            RemoteAuthMode::StaticHeader => {
                let value = config
                    .auth_header
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        KalamPgError::Validation(
                            "auth_mode 'static_header' requires a non-empty auth_header"
                                .to_string(),
                        )
                    })?
                    .parse::<tonic::metadata::MetadataValue<_>>()
                    .map_err(|error| {
                        KalamPgError::Validation(format!(
                            "invalid auth_header metadata value: {}",
                            error
                        ))
                    })?;
                Ok(Self::StaticHeader(value))
            },
            RemoteAuthMode::AccountLogin => {
                let login_user = config
                    .login_user
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        KalamPgError::Validation(
                            "auth_mode 'account_login' requires server option 'login_user'"
                                .to_string(),
                        )
                    })?;
                let login_password = config
                    .login_password
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        KalamPgError::Validation(
                            "auth_mode 'account_login' requires server option 'login_password'"
                                .to_string(),
                        )
                    })?;
                let metadata = build_basic_auth_metadata(login_user, login_password)?;
                Ok(Self::BasicCredentials(metadata))
            },
        }
    }

    fn authorization_metadata(
        &self,
    ) -> Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>> {
        match self {
            Self::None => None,
            Self::StaticHeader(value) => Some(value.clone()),
            Self::BasicCredentials(value) => Some(value.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSessionHandle {
    pub session_id: String,
    pub current_schema: Option<String>,
    /// Lease expiry (epoch ms). Client should re-authenticate before this time.
    pub lease_expires_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct RemoteKalamClient {
    channel: Channel,
    config: RemoteServerConfig,
    /// "host:port" used in error messages.
    server_addr: String,
    /// Auth metadata to send on `open_session` only.
    open_session_auth: OpenSessionAuth,
}

impl RemoteKalamClient {
    pub async fn connect(config: RemoteServerConfig) -> Result<Self, KalamPgError> {
        config.validate()?;

        let server_addr = format!("{}:{}", config.host, config.port);
        let mut endpoint = Endpoint::from_shared(config.endpoint_uri())
            .map_err(|error| KalamPgError::Execution(error.to_string()))?;

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
                    "TLS requested but kalam-pg-client was compiled without the 'tls' feature"
                        .to_string(),
                ));
            }
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|error| Self::connect_err(&error, &server_addr))?;
        let open_session_auth = OpenSessionAuth::from_config(&config)?;

        Ok(Self {
            channel,
            config,
            server_addr,
            open_session_auth,
        })
    }

    fn should_retry_cleanup_status(status: &tonic::Status) -> bool {
        use tonic::Code;

        matches!(
            status.code(),
            Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown
        )
    }

    async fn reconnect(&self) -> Result<Self, KalamPgError> {
        Self::connect(self.config.clone()).await
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
                "connection to KalamDB server at {server_addr} timed out – check the server is \
                 running and the port is correct"
            ))
        } else if detail.contains("certificate") || detail.contains("tls") || detail.contains("TLS")
        {
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
            },
            Code::NotFound => {
                let msg = status.message();
                KalamPgError::Execution(format!("not found: {msg}"))
            },
            Code::Unauthenticated => KalamPgError::Execution(
                "authentication failed – check auth_mode, auth_header, or account_login \
                 credentials in CREATE SERVER OPTIONS"
                    .to_string(),
            ),
            Code::PermissionDenied => {
                KalamPgError::Execution(format!("permission denied: {}", status.message()))
            },
            Code::DeadlineExceeded => KalamPgError::Execution(format!(
                "request to KalamDB server at {server_addr} timed out"
            )),
            Code::AlreadyExists => {
                KalamPgError::Execution(format!("already exists: {}", status.message()))
            },
            Code::InvalidArgument => {
                KalamPgError::Execution(format!("invalid argument: {}", status.message()))
            },
            Code::Unimplemented => KalamPgError::Execution(format!(
                "operation not supported by KalamDB server at {server_addr}: {}",
                status.message()
            )),
            Code::Internal => {
                KalamPgError::Execution(format!("internal KalamDB error: {}", status.message()))
            },
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
        let ca_bytes =
            load_pem(ca_pem).map_err(|e| KalamPgError::Execution(format!("ca_cert: {e}")))?;
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

    /// Create a plain gRPC request (no auth metadata).
    fn plain_request<T>(payload: T) -> Request<T> {
        Request::new(payload)
    }

    /// Create a gRPC request with auth metadata for `open_session`.
    fn authenticated_request<T>(&self, payload: T) -> Request<T> {
        let mut req = Request::new(payload);
        if let Some(val) = self.open_session_auth.authorization_metadata() {
            req.metadata_mut().insert("authorization", val);
        }
        req
    }

    pub async fn ping(&self) -> Result<(), KalamPgError> {
        // Ping uses auth metadata directly (pre-session health check).
        let request = self.authenticated_request(PingRequest {});
        let mut client = PgServiceClient::new(self.channel.clone());
        client
            .ping(request)
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?;
        Ok(())
    }

    /// Open an authenticated session. Auth metadata is sent only on this call.
    /// Returns the server-accepted session handle.
    pub async fn open_session(
        &self,
        session_id: Option<&str>,
        current_schema: Option<&str>,
    ) -> Result<RemoteSessionHandle, KalamPgError> {
        let request = self.authenticated_request(OpenSessionRequest {
            session_id: session_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("")
                .to_string(),
            current_schema: current_schema
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        });
        let mut client = PgServiceClient::new(self.channel.clone());
        let response = client
            .open_session(request)
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
            .into_inner();

        Ok(RemoteSessionHandle {
            session_id: response.session_id,
            current_schema: response.current_schema,
            lease_expires_at_ms: response.lease_expires_at_ms,
        })
    }

    /// Close a session and free server-side resources.
    pub async fn close_session(&self, session_id: &str) -> Result<(), KalamPgError> {
        match self.close_session_attempt(session_id).await {
            Ok(()) => Ok(()),
            Err(status) if Self::should_retry_cleanup_status(&status) => {
                let fresh_client = self.reconnect().await?;
                fresh_client
                    .close_session_attempt(session_id)
                    .await
                    .map_err(|status| Self::grpc_err(status, &fresh_client.server_addr))
            },
            Err(status) => Err(Self::grpc_err(status, &self.server_addr)),
        }
    }

    async fn close_session_attempt(&self, session_id: &str) -> Result<(), tonic::Status> {
        let mut client = PgServiceClient::new(self.channel.clone());
        client
            .close_session(Self::plain_request(CloseSessionRequest {
                session_id: session_id.trim().to_string(),
            }))
            .await?;
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
        filters: Vec<(String, String)>,
    ) -> Result<ScanResponse, KalamPgError> {
        let grpc_filters = filters
            .into_iter()
            .map(|(column, value)| ScanFilterExpression {
                column,
                op: "eq".to_string(),
                value,
            })
            .collect();
        let mut client = PgServiceClient::new(self.channel.clone());
        let response = client
            .scan(Self::plain_request(ScanRpcRequest {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                table_type: table_type.to_string(),
                session_id: session_id.to_string(),
                user_id: user_id.map(str::to_string),
                columns,
                limit,
                filters: grpc_filters,
            }))
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
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
        let response = client
            .insert(Self::plain_request(InsertRpcRequest {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                table_type: table_type.to_string(),
                session_id: session_id.to_string(),
                user_id: user_id.map(str::to_string),
                rows_json,
            }))
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
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
        let response = client
            .update(Self::plain_request(UpdateRpcRequest {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                table_type: table_type.to_string(),
                session_id: session_id.to_string(),
                user_id: user_id.map(str::to_string),
                pk_value: pk_value.to_string(),
                updates_json: updates_json.to_string(),
            }))
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
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
        let response = client
            .delete(Self::plain_request(DeleteRpcRequest {
                namespace: namespace.to_string(),
                table_name: table_name.to_string(),
                table_type: table_type.to_string(),
                session_id: session_id.to_string(),
                user_id: user_id.map(str::to_string),
                pk_value: pk_value.to_string(),
            }))
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
            .into_inner();

        Ok(MutationResponse {
            affected_rows: response.affected_rows,
        })
    }

    /// Begin a new transaction within the given session.
    pub async fn begin_transaction(&self, session_id: &str) -> Result<String, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let response = client
            .begin_transaction(Self::plain_request(BeginTransactionRequest {
                session_id: session_id.to_string(),
            }))
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
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
        let response = client
            .commit_transaction(Self::plain_request(CommitTransactionRequest {
                session_id: session_id.to_string(),
                transaction_id: transaction_id.to_string(),
            }))
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
            .into_inner();
        Ok(response.transaction_id)
    }

    /// Rollback an active transaction. Idempotent for already-rolled-back handles.
    pub async fn rollback_transaction(
        &self,
        session_id: &str,
        transaction_id: &str,
    ) -> Result<String, KalamPgError> {
        match self.rollback_transaction_attempt(session_id, transaction_id).await {
            Ok(response) => Ok(response),
            Err(status) if Self::should_retry_cleanup_status(&status) => {
                let fresh_client = self.reconnect().await?;
                fresh_client
                    .rollback_transaction_attempt(session_id, transaction_id)
                    .await
                    .map_err(|status| Self::grpc_err(status, &fresh_client.server_addr))
            },
            Err(status) => Err(Self::grpc_err(status, &self.server_addr)),
        }
    }

    async fn rollback_transaction_attempt(
        &self,
        session_id: &str,
        transaction_id: &str,
    ) -> Result<String, tonic::Status> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let response = client
            .rollback_transaction(Self::plain_request(RollbackTransactionRequest {
                session_id: session_id.to_string(),
                transaction_id: transaction_id.to_string(),
            }))
            .await?
            .into_inner();
        Ok(response.transaction_id)
    }

    /// Execute a DDL SQL statement on the KalamDB backend.
    pub async fn execute_sql(&self, sql: &str, session_id: &str) -> Result<String, KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let response = client
            .execute_sql(Self::plain_request(ExecuteSqlRpcRequest {
                sql: sql.to_string(),
                session_id: session_id.to_string(),
            }))
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
            .into_inner();
        Ok(response.message)
    }

    /// Execute an arbitrary SQL statement and return JSON rows.
    pub async fn execute_query(
        &self,
        sql: &str,
        session_id: &str,
    ) -> Result<(String, Vec<String>), KalamPgError> {
        let mut client = PgServiceClient::new(self.channel.clone());
        let response = client
            .execute_query(Self::plain_request(ExecuteQueryRpcRequest {
                sql: sql.to_string(),
                session_id: session_id.to_string(),
            }))
            .await
            .map_err(|status| Self::grpc_err(status, &self.server_addr))?
            .into_inner();
        let batches = Self::decode_ipc_batches(&response.ipc_batches)?;
        let json_rows = Self::batches_to_json_rows(&batches);
        Ok((response.message, json_rows))
    }

    /// Decode Arrow IPC bytes received from the gRPC response into RecordBatches.
    fn decode_ipc_batches(ipc_batches: &[bytes::Bytes]) -> Result<Vec<RecordBatch>, KalamPgError> {
        let mut batches = Vec::with_capacity(ipc_batches.len());
        for ipc_bytes in ipc_batches {
            let cursor = Cursor::new(ipc_bytes.as_ref());
            let reader = StreamReader::try_new(cursor, None).map_err(|e| {
                KalamPgError::Execution(format!("failed to read IPC stream: {}", e))
            })?;
            for batch_result in reader {
                let batch = batch_result.map_err(|e| {
                    KalamPgError::Execution(format!("failed to decode IPC batch: {}", e))
                })?;
                batches.push(batch);
            }
        }
        Ok(batches)
    }

    /// Serialize Arrow RecordBatches into a Vec of JSON object strings (one per row).
    fn batches_to_json_rows(batches: &[RecordBatch]) -> Vec<String> {
        use arrow::{
            array::{
                Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
                Int64Array, Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array,
                UInt64Array, UInt8Array,
            },
            datatypes::DataType,
        };

        let mut rows = Vec::new();
        for batch in batches {
            let schema = batch.schema();
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            for row_idx in 0..batch.num_rows() {
                let mut obj = serde_json::Map::new();
                for (col_idx, col) in batch.columns().iter().enumerate() {
                    let field_name = field_names[col_idx].to_string();
                    let value = if col.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        match col.data_type() {
                            DataType::Utf8 => {
                                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                                serde_json::Value::String(arr.value(row_idx).to_string())
                            },
                            DataType::LargeUtf8 => {
                                let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                                serde_json::Value::String(arr.value(row_idx).to_string())
                            },
                            DataType::Int8 => {
                                let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                                serde_json::Value::Number(arr.value(row_idx).into())
                            },
                            DataType::Int16 => {
                                let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                                serde_json::Value::Number(arr.value(row_idx).into())
                            },
                            DataType::Int32 => {
                                let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                serde_json::Value::Number(arr.value(row_idx).into())
                            },
                            DataType::Int64 => {
                                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                serde_json::Value::Number(arr.value(row_idx).into())
                            },
                            DataType::UInt8 => {
                                let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                                serde_json::Value::Number(arr.value(row_idx).into())
                            },
                            DataType::UInt16 => {
                                let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                                serde_json::Value::Number(arr.value(row_idx).into())
                            },
                            DataType::UInt32 => {
                                let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                                serde_json::Value::Number(arr.value(row_idx).into())
                            },
                            DataType::UInt64 => {
                                let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                                serde_json::Value::Number(arr.value(row_idx).into())
                            },
                            DataType::Float32 => {
                                let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                                let v = arr.value(row_idx) as f64;
                                serde_json::Number::from_f64(v)
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            },
                            DataType::Float64 => {
                                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                                serde_json::Number::from_f64(arr.value(row_idx))
                                    .map(serde_json::Value::Number)
                                    .unwrap_or(serde_json::Value::Null)
                            },
                            DataType::Boolean => {
                                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                                serde_json::Value::Bool(arr.value(row_idx))
                            },
                            _ => {
                                // For all other types, produce a string representation.
                                serde_json::Value::String(format!("{:?}", col.data_type()))
                            },
                        }
                    };
                    obj.insert(field_name, value);
                }
                rows.push(serde_json::Value::Object(obj).to_string());
            }
        }
        rows
    }
}
