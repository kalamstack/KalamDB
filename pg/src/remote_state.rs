use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex, OnceLock},
};

use kalam_pg_api::KalamBackendExecutor;
use kalam_pg_client::RemoteKalamClient;
use kalam_pg_common::{KalamPgError, RemoteServerConfig};
use pgrx::pg_sys;

use crate::remote_executor::RemoteBackendExecutor;

/// Global state holding the remote connection + tokio runtime for the PostgreSQL extension
/// in remote mode for a single backend/config pair.
pub struct RemoteExtensionState {
    client: RemoteKalamClient,
    runtime: Arc<tokio::runtime::Runtime>,
    session_id: String,
}

impl RemoteExtensionState {
    pub fn executor(&self) -> Result<Arc<dyn KalamBackendExecutor>, KalamPgError> {
        Ok(Arc::new(RemoteBackendExecutor::new(
            self.client.clone(),
            self.session_id.clone(),
        )))
    }

    pub fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self.runtime
    }

    pub fn client(&self) -> &RemoteKalamClient {
        &self.client
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

trait HasSessionId {
    fn session_id(&self) -> &str;
}

impl HasSessionId for RemoteExtensionState {
    fn session_id(&self) -> &str {
        self.session_id()
    }
}

struct RemoteStateRegistry<T> {
    by_config: HashMap<RemoteServerConfig, Arc<T>>,
    by_session_id: HashMap<String, Arc<T>>,
    exit_handler_registered: bool,
}

impl<T> Default for RemoteStateRegistry<T> {
    fn default() -> Self {
        Self {
            by_config: HashMap::new(),
            by_session_id: HashMap::new(),
            exit_handler_registered: false,
        }
    }
}

impl<T: HasSessionId> RemoteStateRegistry<T> {
    fn get_by_config(&self, config: &RemoteServerConfig) -> Option<Arc<T>> {
        self.by_config.get(config).cloned()
    }

    fn get_by_session_id(&self, session_id: &str) -> Option<Arc<T>> {
        self.by_session_id.get(session_id).cloned()
    }

    fn insert(&mut self, config: RemoteServerConfig, state: Arc<T>) {
        self.by_session_id.insert(state.session_id().to_string(), Arc::clone(&state));
        self.by_config.insert(config, state);
    }

    #[cfg_attr(test, allow(dead_code))]
    fn all_states(&self) -> Vec<Arc<T>> {
        self.by_session_id.values().cloned().collect()
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.by_config.clear();
        self.by_session_id.clear();
        self.exit_handler_registered = false;
    }
}

/// Per-process registry for remote extension states keyed by remote server config.
static REMOTE_STATES: OnceLock<Mutex<RemoteStateRegistry<RemoteExtensionState>>> = OnceLock::new();

fn remote_state_registry() -> &'static Mutex<RemoteStateRegistry<RemoteExtensionState>> {
    REMOTE_STATES.get_or_init(|| Mutex::new(RemoteStateRegistry::default()))
}

fn pg_backend_session_id(config: &RemoteServerConfig, backend_pid: u32) -> String {
    let mut hasher = DefaultHasher::new();
    config.hash(&mut hasher);
    let config_hash = hasher.finish();
    format!("pg-{}-{:x}", backend_pid, config_hash)
}

#[cfg(not(test))]
fn current_backend_pid() -> u32 {
    unsafe { pg_sys::MyProcPid as u32 }
}

#[cfg(test)]
fn current_backend_pid() -> u32 {
    std::process::id()
}

fn build_remote_extension_state(
    config: &RemoteServerConfig,
) -> Result<RemoteExtensionState, KalamPgError> {
    let runtime =
        Arc::new(tokio::runtime::Builder::new_current_thread().enable_all().build().map_err(
            |e| KalamPgError::Execution(format!("failed to build tokio runtime: {}", e)),
        )?);

    let client = runtime.block_on(async { RemoteKalamClient::connect(config.clone()).await })?;
    let session_id = pg_backend_session_id(config, current_backend_pid());
    let session =
        runtime.block_on(async { client.open_session(Some(session_id.as_str()), None).await })?;

    Ok(RemoteExtensionState {
        client,
        runtime,
        session_id: session.session_id,
    })
}

fn register_exit_handler_once(registry: &mut RemoteStateRegistry<RemoteExtensionState>) {
    if registry.exit_handler_registered {
        return;
    }

    #[cfg(not(test))]
    unsafe {
        pg_sys::on_proc_exit(Some(on_proc_exit_close_sessions), pg_sys::Datum::from(0));
    }

    registry.exit_handler_registered = true;
}

/// Return the remote extension state for the provided remote session id.
pub fn get_remote_extension_state_for_session(
    session_id: &str,
) -> Option<Arc<RemoteExtensionState>> {
    let registry = remote_state_registry().lock().unwrap_or_else(|e| e.into_inner());
    registry.get_by_session_id(session_id)
}

/// Bootstrap or retrieve the remote extension state.
///
/// The `host`, `port`, and `auth_header` are parsed from the foreign server OPTIONS.
pub fn ensure_remote_extension_state(
    config: RemoteServerConfig,
) -> Result<Arc<RemoteExtensionState>, KalamPgError> {
    let mut registry = remote_state_registry().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(state) = registry.get_by_config(&config) {
        return Ok(state);
    }

    let state = Arc::new(build_remote_extension_state(&config)?);
    registry.insert(config, Arc::clone(&state));
    register_exit_handler_once(&mut registry);
    Ok(state)
}

/// PostgreSQL process-exit callback that closes all KalamDB sessions opened by this backend.
///
/// # Safety
/// Called by PostgreSQL at backend process exit. Must not panic through FFI.
#[cfg_attr(test, allow(dead_code))]
unsafe extern "C-unwind" fn on_proc_exit_close_sessions(_code: i32, _arg: pg_sys::Datum) {
    let states = {
        let registry = remote_state_registry().lock().unwrap_or_else(|e| e.into_inner());
        registry.all_states()
    };

    for state in states {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state
                .runtime
                .block_on(async { state.client.close_session(&state.session_id).await })
        }));

        match result {
            Ok(Ok(())) => {
                eprintln!("pg_kalam: session {} closed", state.session_id);
            },
            Ok(Err(e)) => {
                eprintln!("pg_kalam: failed to close session {}: {}", state.session_id, e);
            },
            Err(_panic) => {
                eprintln!("pg_kalam: panic closing session {}", state.session_id);
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc, Mutex,
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use kalamdb_pg::{
        BeginTransactionRequest, BeginTransactionResponse, CloseSessionRequest,
        CloseSessionResponse, CommitTransactionRequest, CommitTransactionResponse,
        DeleteRpcRequest, DeleteRpcResponse, ExecuteQueryRpcRequest, ExecuteQueryRpcResponse,
        ExecuteSqlRpcRequest, ExecuteSqlRpcResponse, InsertRpcRequest, InsertRpcResponse,
        OpenSessionRequest, OpenSessionResponse, PgService, PgServiceServer, PingRequest,
        PingResponse, RollbackTransactionRequest, RollbackTransactionResponse, ScanRpcRequest,
        ScanRpcResponse, UpdateRpcRequest, UpdateRpcResponse,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status};

    use super::*;

    #[derive(Default)]
    struct CountingState {
        open_session_calls: AtomicUsize,
        recorded_session_ids: Mutex<Vec<String>>,
    }

    impl CountingState {
        fn record_session_id(&self, session_id: impl Into<String>) {
            self.recorded_session_ids
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(session_id.into());
        }

        fn recorded_session_ids(&self) -> Vec<String> {
            self.recorded_session_ids.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }
    }

    #[derive(Clone)]
    struct CountingService {
        state: Arc<CountingState>,
    }

    impl CountingService {
        fn new(state: Arc<CountingState>) -> Self {
            Self { state }
        }
    }

    #[async_trait]
    impl PgService for CountingService {
        async fn ping(
            &self,
            _request: Request<PingRequest>,
        ) -> Result<Response<PingResponse>, Status> {
            Ok(Response::new(PingResponse { ok: true }))
        }

        async fn open_session(
            &self,
            request: Request<OpenSessionRequest>,
        ) -> Result<Response<OpenSessionResponse>, Status> {
            let count = self.state.open_session_calls.fetch_add(1, Ordering::Relaxed);
            let server_issued_id = format!("srv-session-{}", count);
            let request = request.into_inner();
            let session_id = if request.session_id.trim().is_empty() {
                server_issued_id
            } else {
                request.session_id.clone()
            };
            self.state.record_session_id(session_id.clone());
            Ok(Response::new(OpenSessionResponse {
                session_id,
                current_schema: request.current_schema,
                lease_expires_at_ms: 0,
            }))
        }

        async fn close_session(
            &self,
            _request: Request<CloseSessionRequest>,
        ) -> Result<Response<CloseSessionResponse>, Status> {
            Ok(Response::new(CloseSessionResponse {}))
        }

        async fn scan(
            &self,
            _request: Request<ScanRpcRequest>,
        ) -> Result<Response<ScanRpcResponse>, Status> {
            Ok(Response::new(ScanRpcResponse {
                ipc_batches: Vec::new(),
                schema_ipc: None,
            }))
        }

        async fn insert(
            &self,
            _request: Request<InsertRpcRequest>,
        ) -> Result<Response<InsertRpcResponse>, Status> {
            Ok(Response::new(InsertRpcResponse { affected_rows: 0 }))
        }

        async fn update(
            &self,
            _request: Request<UpdateRpcRequest>,
        ) -> Result<Response<UpdateRpcResponse>, Status> {
            Ok(Response::new(UpdateRpcResponse { affected_rows: 0 }))
        }

        async fn delete(
            &self,
            _request: Request<DeleteRpcRequest>,
        ) -> Result<Response<DeleteRpcResponse>, Status> {
            Ok(Response::new(DeleteRpcResponse { affected_rows: 0 }))
        }

        async fn begin_transaction(
            &self,
            request: Request<BeginTransactionRequest>,
        ) -> Result<Response<BeginTransactionResponse>, Status> {
            let session_id = request.get_ref().session_id.clone();
            self.state.record_session_id(session_id.clone());
            Ok(Response::new(BeginTransactionResponse {
                transaction_id: format!("tx-{}", session_id),
            }))
        }

        async fn commit_transaction(
            &self,
            request: Request<CommitTransactionRequest>,
        ) -> Result<Response<CommitTransactionResponse>, Status> {
            Ok(Response::new(CommitTransactionResponse {
                transaction_id: request.into_inner().transaction_id,
            }))
        }

        async fn rollback_transaction(
            &self,
            request: Request<RollbackTransactionRequest>,
        ) -> Result<Response<RollbackTransactionResponse>, Status> {
            Ok(Response::new(RollbackTransactionResponse {
                transaction_id: request.into_inner().transaction_id,
            }))
        }

        async fn execute_sql(
            &self,
            request: Request<ExecuteSqlRpcRequest>,
        ) -> Result<Response<ExecuteSqlRpcResponse>, Status> {
            self.state.record_session_id(request.get_ref().session_id.clone());
            Ok(Response::new(ExecuteSqlRpcResponse {
                success: true,
                message: "ok".to_string(),
            }))
        }

        async fn execute_query(
            &self,
            request: Request<ExecuteQueryRpcRequest>,
        ) -> Result<Response<ExecuteQueryRpcResponse>, Status> {
            self.state.record_session_id(request.get_ref().session_id.clone());
            Ok(Response::new(ExecuteQueryRpcResponse {
                success: true,
                message: "ok".to_string(),
                ipc_batches: Vec::<Bytes>::new(),
            }))
        }
    }

    fn clear_registry() {
        remote_state_registry().lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    fn start_counting_server() -> (RemoteServerConfig, Arc<CountingState>) {
        let state = Arc::new(CountingState::default());
        let (port_tx, port_rx) = mpsc::channel();
        let service_for_thread = CountingService::new(Arc::clone(&state));

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("test runtime");
            runtime.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
                let addr: SocketAddr = listener.local_addr().expect("listener addr");
                port_tx.send(addr.port()).expect("send port");
                let incoming = TcpListenerStream::new(listener);
                tonic::transport::Server::builder()
                    .add_service(PgServiceServer::new(service_for_thread))
                    .serve_with_incoming(incoming)
                    .await
                    .expect("serve counting pg grpc");
            });
        });

        let port = port_rx.recv_timeout(Duration::from_secs(3)).expect("receive port");
        std::thread::sleep(Duration::from_millis(50));

        (
            RemoteServerConfig {
                host: "127.0.0.1".to_string(),
                port,
                ..Default::default()
            },
            state,
        )
    }

    #[test]
    fn same_config_reuses_remote_state_and_session() {
        clear_registry();
        let (config, service) = start_counting_server();

        let first = ensure_remote_extension_state(config.clone()).expect("first state");
        let second = ensure_remote_extension_state(config).expect("second state");

        let transaction_id = first
            .runtime()
            .block_on(async { first.client().begin_transaction(first.session_id()).await })
            .expect("begin transaction");
        first
            .runtime()
            .block_on(async { first.client().execute_sql("SELECT 1", first.session_id()).await })
            .expect("execute sql");
        first
            .runtime()
            .block_on(async { first.client().execute_query("SELECT 1", first.session_id()).await })
            .expect("execute query");
        first
            .runtime()
            .block_on(async {
                first.client().commit_transaction(first.session_id(), &transaction_id).await
            })
            .expect("commit transaction");

        let recorded_session_ids = service.recorded_session_ids();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.session_id(), second.session_id());
        assert!(first.session_id().starts_with("pg-"));
        assert_eq!(service.open_session_calls.load(Ordering::Relaxed), 1);
        assert!(
            recorded_session_ids.iter().all(|session_id| session_id == first.session_id()),
            "all follow-up RPCs should reuse the original session id"
        );
        assert!(recorded_session_ids.len() >= 4, "expected multiple RPCs to be recorded");
    }

    #[test]
    fn different_configs_open_distinct_remote_sessions() {
        clear_registry();
        let (config, service) = start_counting_server();

        let first = ensure_remote_extension_state(config.clone()).expect("first state");
        let second = ensure_remote_extension_state(RemoteServerConfig {
            auth_header: Some("Bearer second".to_string()),
            ..config
        })
        .expect("second state");

        assert!(!Arc::ptr_eq(&first, &second));
        assert_ne!(first.session_id(), second.session_id());
        assert_eq!(service.open_session_calls.load(Ordering::Relaxed), 2);
    }
}
