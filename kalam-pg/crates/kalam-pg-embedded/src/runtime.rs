use datafusion::physical_plan::collect;
use kalam_pg_api::{
    DeleteRequest, InsertRequest, KalamBackendExecutor, MutationResponse, ScanRequest,
    ScanResponse, TenantContext, UpdateRequest,
};
use kalam_pg_common::{EmbeddedRuntimeConfig, KalamPgError};
use kalamdb_commons::models::{NamespaceId, NodeId, StorageId, UserId};
use kalamdb_commons::Role;
use kalamdb_commons::TableType;
use kalamdb_configs::ServerConfig;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::executor::SqlExecutor;
use kalamdb_core::sql::ExecutionContext;
use kalamdb_store::open_storage_backend;
use kalamdb_system::providers::storages::models::StorageType;
use kalamdb_system::{Namespace, Storage};
use kalamdb_tables::utils::row_utils::system_user_id;
use log::info;
use std::fs::create_dir_all;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Once};
use std::time::{SystemTime, UNIX_EPOCH};

/// Embedded runtime wrapper around the existing shared [`AppContext`].
pub struct EmbeddedKalamRuntime {
    app_context: Arc<AppContext>,
    runtime_config: EmbeddedRuntimeConfig,
    tokio_runtime: Arc<tokio::runtime::Runtime>,
    http_server: Option<kalamdb_server::lifecycle::RunningTestHttpServer>,
}

impl EmbeddedKalamRuntime {
    /// Bootstrap a new in-process KalamDB runtime backed by RocksDB + Parquet storage.
    pub fn bootstrap(runtime_config: EmbeddedRuntimeConfig) -> Result<Self, KalamPgError> {
        install_embedded_logging();

        let tokio_runtime = build_tokio_runtime()?;
        let server_config = embedded_server_config(&runtime_config)?;
        let app_context = {
            let _guard = tokio_runtime.enter();
            build_app_context(&runtime_config, &server_config)?
        };
        ensure_sql_executor(&app_context);

        tokio_runtime.block_on(async {
            bootstrap_app_context(&app_context).await?;
            Ok::<(), KalamPgError>(())
        })?;
        app_context.wire_raft_appliers();

        ensure_default_namespace(&app_context)?;
        ensure_default_local_storage(&app_context, &runtime_config)?;

        let http_server = tokio_runtime.block_on(async {
            let components = kalamdb_server::lifecycle::prepare_components(
                &server_config,
                Arc::clone(&app_context),
            )
            .await
            .map_err(|err| KalamPgError::Execution(err.to_string()))?;

            if runtime_config.http.enabled {
                let server = kalamdb_server::lifecycle::run_detached(
                    &server_config,
                    components,
                    Arc::clone(&app_context),
                )
                .await
                .map_err(|err| KalamPgError::Execution(err.to_string()))?;

                info!(
                    "Embedded KalamDB HTTP server listening on {}:{}",
                    runtime_config.http.host, runtime_config.http.port
                );

                Ok::<Option<kalamdb_server::lifecycle::RunningTestHttpServer>, KalamPgError>(Some(
                    server,
                ))
            } else {
                Ok(None)
            }
        })?;

        Ok(Self {
            app_context,
            runtime_config,
            tokio_runtime,
            http_server,
        })
    }

    /// Wrap an already-initialized [`AppContext`].
    pub fn from_app_context(app_context: Arc<AppContext>) -> Result<Self, KalamPgError> {
        ensure_sql_executor(&app_context);
        Ok(Self {
            app_context,
            runtime_config: EmbeddedRuntimeConfig::default(),
            tokio_runtime: build_tokio_runtime()?,
            http_server: None,
        })
    }

    /// Access the shared application context.
    pub fn app_context(&self) -> &Arc<AppContext> {
        &self.app_context
    }

    /// Access the embedded runtime configuration.
    pub fn runtime_config(&self) -> &EmbeddedRuntimeConfig {
        &self.runtime_config
    }

    pub fn http_base_url(&self) -> Option<&str> {
        self.http_server.as_ref().map(|server| server.base_url.as_str())
    }

    /// Block the embedded Tokio runtime on an async operation.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.tokio_runtime.block_on(future)
    }

    fn execution_context(&self, tenant_context: &TenantContext) -> ExecutionContext {
        match tenant_context.effective_user_id() {
            Some(user_id) => ExecutionContext::new(
                user_id.clone(),
                Role::User,
                self.app_context.base_session_context(),
            ),
            None => ExecutionContext::anonymous(self.app_context.base_session_context()),
        }
    }

    fn mutation_user_id<'a>(
        table_type: TableType,
        tenant_context: &'a TenantContext,
    ) -> Result<&'a UserId, KalamPgError> {
        match table_type {
            TableType::User | TableType::Stream => {
                tenant_context.effective_user_id().ok_or_else(|| {
                    KalamPgError::Validation(format!(
                        "user_id is required for {} mutations",
                        table_type
                    ))
                })
            },
            TableType::Shared => Ok(system_user_id()),
            _ => Err(KalamPgError::Unsupported(format!(
                "{} mutations are not supported in embedded mode",
                table_type
            ))),
        }
    }
}

fn build_tokio_runtime() -> Result<Arc<tokio::runtime::Runtime>, KalamPgError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("kalam-pg-embedded")
        .build()
        .map(Arc::new)
        .map_err(|err| KalamPgError::Execution(err.to_string()))
}

fn ensure_sql_executor(app_context: &Arc<AppContext>) {
    if app_context.try_sql_executor().is_none() {
        app_context.set_sql_executor(Arc::new(SqlExecutor::new(Arc::clone(app_context), false)));
    }
}

fn build_app_context(
    runtime_config: &EmbeddedRuntimeConfig,
    server_config: &ServerConfig,
) -> Result<Arc<AppContext>, KalamPgError> {
    let rocksdb_dir = server_config.storage.rocksdb_dir();
    let storage_base_path = server_config.storage.storage_dir();

    let (storage_backend, _) = open_storage_backend(&rocksdb_dir, &server_config.storage.rocksdb)
        .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    let node_id = parse_node_id(&runtime_config.node_id)?;

    Ok(AppContext::init(
        storage_backend,
        node_id,
        storage_base_path.to_string_lossy().to_string(),
        server_config.clone(),
    ))
}

fn embedded_server_config(
    runtime_config: &EmbeddedRuntimeConfig,
) -> Result<ServerConfig, KalamPgError> {
    let data_path = normalize_embedded_root(&runtime_config.storage_base_path)?;
    let logs_path = data_path.join("logs");
    let staging_path = data_path.join("staging");

    for path in [
        data_path.clone(),
        data_path.join("rocksdb"),
        data_path.join("storage"),
        data_path.join("snapshots"),
        data_path.join("streams"),
        data_path.join("exports"),
        logs_path.clone(),
        staging_path.clone(),
    ] {
        create_dir_all(&path).map_err(|err| KalamPgError::Execution(err.to_string()))?;
    }

    let mut server_config = ServerConfig::default();
    server_config.server.host = runtime_config.http.host.clone();
    server_config.server.port = runtime_config.http.port;
    server_config.storage.data_path = data_path.to_string_lossy().to_string();
    server_config.logging.logs_path = logs_path.to_string_lossy().to_string();
    server_config.logging.log_to_console = true;
    server_config.files.staging_path = staging_path.to_string_lossy().to_string();

    Ok(server_config)
}

fn install_embedded_logging() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .with_target(true)
            .with_level(true)
            .compact()
            .try_init();
        tracing_log::LogTracer::init().ok();
        tracing::info!("embedded PostgreSQL logging initialized");
    });
}

fn normalize_embedded_root(storage_base_path: &PathBuf) -> Result<PathBuf, KalamPgError> {
    if storage_base_path.as_os_str().is_empty() {
        return Err(KalamPgError::Validation(
            "embedded storage_base_path must not be empty".to_string(),
        ));
    }

    Ok(storage_base_path.clone())
}

fn parse_node_id(node_id: &str) -> Result<NodeId, KalamPgError> {
    node_id.trim().parse::<u64>().map(NodeId::new).map_err(|err| {
        KalamPgError::Validation(format!("invalid embedded node_id '{}': {}", node_id, err))
    })
}

async fn bootstrap_app_context(app_context: &Arc<AppContext>) -> Result<(), KalamPgError> {
    let executor = app_context.executor();
    executor.start().await.map_err(|err| KalamPgError::Execution(err.to_string()))?;
    executor
        .initialize_cluster()
        .await
        .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    Ok(())
}

fn ensure_default_namespace(app_context: &Arc<AppContext>) -> Result<(), KalamPgError> {
    let default_namespace = NamespaceId::default();
    let namespaces = app_context.system_tables().namespaces();

    if namespaces
        .get_namespace_by_id(&default_namespace)
        .map_err(|err| KalamPgError::Execution(err.to_string()))?
        .is_none()
    {
        namespaces
            .create_namespace(Namespace {
                namespace_id: default_namespace,
                name: "default".to_string(),
                created_at: current_timestamp_millis()?,
                options: Some("{}".to_string()),
                table_count: 0,
            })
            .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    }

    Ok(())
}

fn ensure_default_local_storage(
    app_context: &Arc<AppContext>,
    runtime_config: &EmbeddedRuntimeConfig,
) -> Result<(), KalamPgError> {
    let storages = app_context.system_tables().storages();
    let storage_id = StorageId::from("local");

    if storages
        .get_storage_by_id(&storage_id)
        .map_err(|err| KalamPgError::Execution(err.to_string()))?
        .is_none()
    {
        let base_directory = runtime_config.storage_base_path.join("storage");
        storages
            .create_storage(Storage {
                storage_id,
                storage_name: "Local Storage".to_string(),
                description: Some("Embedded local storage for PostgreSQL".to_string()),
                storage_type: StorageType::Filesystem,
                base_directory: base_directory.to_string_lossy().to_string(),
                credentials: None,
                config_json: None,
                shared_tables_template: "shared/{namespace}/{table}".to_string(),
                user_tables_template: "user/{namespace}/{table}/{userId}".to_string(),
                created_at: current_timestamp_millis()?,
                updated_at: current_timestamp_millis()?,
            })
            .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    }

    Ok(())
}

fn current_timestamp_millis() -> Result<i64, KalamPgError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| KalamPgError::Execution(err.to_string()))?;
    Ok(duration.as_millis() as i64)
}

#[async_trait::async_trait]
impl KalamBackendExecutor for EmbeddedKalamRuntime {
    async fn scan(&self, request: ScanRequest) -> Result<ScanResponse, KalamPgError> {
        request.validate()?;

        let provider =
            self.app_context.schema_registry().get_provider(&request.table_id).ok_or_else(
                || {
                    KalamPgError::Execution(format!(
                        "table provider not registered for {}",
                        request.table_id
                    ))
                },
            )?;

        let schema = provider.schema();
        let projection = request
            .projection
            .as_ref()
            .map(|columns| {
                columns
                    .iter()
                    .map(|column_name| {
                        schema
                            .fields()
                            .iter()
                            .position(|field| field.name() == column_name)
                            .ok_or_else(|| {
                                KalamPgError::Validation(format!(
                                    "projection column '{}' not found on {}",
                                    column_name, request.table_id
                                ))
                            })
                    })
                    .collect::<Result<Vec<usize>, KalamPgError>>()
            })
            .transpose()?;

        let exec_ctx = self.execution_context(&request.tenant_context);
        let session = exec_ctx.create_session_with_user();
        let plan = provider
            .scan(&session.state(), projection.as_ref(), &request.filters, request.limit)
            .await?;
        let batches = collect(plan, session.task_ctx()).await?;

        Ok(ScanResponse::new(batches))
    }

    async fn insert(&self, request: InsertRequest) -> Result<MutationResponse, KalamPgError> {
        request.validate()?;

        let provider = self
            .app_context
            .schema_registry()
            .get_kalam_provider(&request.table_id)
            .ok_or_else(|| {
                KalamPgError::Execution(format!(
                    "kalam provider not registered for {}",
                    request.table_id
                ))
            })?;
        let user_id = Self::mutation_user_id(request.table_type, &request.tenant_context)?;
        let inserted = provider
            .insert_rows(user_id, request.rows)
            .await
            .map_err(|err| KalamPgError::Execution(err.to_string()))?;

        Ok(MutationResponse {
            affected_rows: inserted as u64,
        })
    }

    async fn update(&self, request: UpdateRequest) -> Result<MutationResponse, KalamPgError> {
        request.validate()?;

        let provider = self
            .app_context
            .schema_registry()
            .get_kalam_provider(&request.table_id)
            .ok_or_else(|| {
                KalamPgError::Execution(format!(
                    "kalam provider not registered for {}",
                    request.table_id
                ))
            })?;
        let user_id = Self::mutation_user_id(request.table_type, &request.tenant_context)?;
        let updated = provider
            .update_row_by_pk(user_id, &request.pk_value, request.updates)
            .await
            .map_err(|err| KalamPgError::Execution(err.to_string()))?;

        Ok(MutationResponse {
            affected_rows: updated as u64,
        })
    }

    async fn delete(&self, request: DeleteRequest) -> Result<MutationResponse, KalamPgError> {
        request.validate()?;

        let provider = self
            .app_context
            .schema_registry()
            .get_kalam_provider(&request.table_id)
            .ok_or_else(|| {
                KalamPgError::Execution(format!(
                    "kalam provider not registered for {}",
                    request.table_id
                ))
            })?;
        let user_id = Self::mutation_user_id(request.table_type, &request.tenant_context)?;
        let deleted = provider
            .delete_row_by_pk(user_id, &request.pk_value)
            .await
            .map_err(|err| KalamPgError::Execution(err.to_string()))?;

        Ok(MutationResponse {
            affected_rows: deleted as u64,
        })
    }
}
