//! Server lifecycle management helpers.
//!
//! This module bootstraps application services and exposes production and
//! integration-test lifecycle entry points. Transport construction and ordered
//! shutdown live in their dedicated modules.

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use chrono::Utc;
use kalamdb_api::limiter::RateLimiter;
use kalamdb_auth::CachedUsersRepo;
use kalamdb_commons::{AuthType, Role, StorageId, UserId};
use kalamdb_configs::ServerConfig;
use kalamdb_core::{
    functions::TriggerDispatcherRuntime,
    sql::{
        datafusion_session::DataFusionSessionFactory,
        executor::{handler_registry::HandlerRegistry, SqlExecutor},
    },
};
use kalamdb_dba::{ensure_dba_notification_policies, initialize_dba_namespace};
use kalamdb_jobs::{AppContextJobsExt, JobsManagerRuntime};
use kalamdb_live::{ConnectionsManager, LiveQueryManager};
use kalamdb_postgres_wire::{
    format_startup_log_segment, PostgresWireListener, PostgresWireRuntimeDeps,
};
use kalamdb_store::open_storage_backend;
use kalamdb_system::providers::storages::models::StorageMode;
use log::{debug, info, warn};

pub use crate::http_server::effective_max_blocking_threads;
use crate::{
    http_runtime::AuthRuntimeMode,
    http_server::{effective_workers, HttpServerRuntime},
    shutdown::{shutdown_background_services, shutdown_server, wait_for_termination},
    startup,
};

/// Aggregated application components that need to be shared across the
/// HTTP server and shutdown handling.
pub struct ApplicationComponents {
    pub session_factory:     Arc<DataFusionSessionFactory>,
    pub sql_executor:        Arc<SqlExecutor>,
    pub rate_limiter:        Arc<RateLimiter>,
    pub live_query_manager:  Arc<LiveQueryManager>,
    pub user_repo:           Arc<dyn kalamdb_auth::UserRepository>,
    pub connection_registry: Arc<ConnectionsManager>,
    pub jobs_runtime:        Option<JobsManagerRuntime>,
    pub trigger_runtime:     Option<TriggerDispatcherRuntime>,
}

async fn fail_bootstrap<T>(
    app_context: Arc<kalamdb_core::app_context::AppContext>,
    error: anyhow::Error,
) -> Result<T> {
    if let Err(cleanup_error) =
        shutdown_background_services(None, app_context, std::time::Duration::ZERO).await
    {
        warn!("Startup cleanup failed while preserving the original error: {cleanup_error}");
    }
    Err(error)
}

#[derive(Clone, Copy)]
enum AppContextMode {
    Global,
    Isolated,
}

fn create_app_context(
    config: &ServerConfig,
    mode: AppContextMode,
) -> Result<Arc<kalamdb_core::app_context::AppContext>> {
    let phase_start = std::time::Instant::now();
    let db_path = config.storage.rocksdb_dir();
    std::fs::create_dir_all(&db_path)?;

    let (backend, partition_count) = open_storage_backend(&db_path, &config.storage.rocksdb)?;
    info!(
        "Storage backend initialized at {} with {} partitions ({:.2}ms)",
        db_path.display(),
        partition_count,
        phase_start.elapsed().as_secs_f64() * 1000.0
    );
    if !config.storage.rocksdb.sync_writes {
        debug!("Async writes enabled (sync_writes=false) for high throughput");
    }

    let node_id = config
        .cluster
        .as_ref()
        .map(|cluster| kalamdb_commons::NodeId::new(cluster.node_id))
        .unwrap_or_else(|| kalamdb_commons::NodeId::new(1));
    let storage_dir = config.storage.storage_dir().to_string_lossy().into_owned();
    let phase_start = std::time::Instant::now();
    let app_context = match mode {
        AppContextMode::Global => kalamdb_core::app_context::AppContext::init(
            backend,
            node_id,
            storage_dir,
            config.clone(),
        ),
        AppContextMode::Isolated => kalamdb_core::app_context::AppContext::create_isolated(
            backend,
            node_id,
            storage_dir,
            config.clone(),
        ),
    };
    info!(
        "Startup: AppContext initialized in {:.2}ms",
        phase_start.elapsed().as_secs_f64() * 1000.0
    );
    Ok(app_context)
}

async fn finish_bootstrap(
    config: &ServerConfig,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
    use_root_password_env: bool,
) -> Result<(ApplicationComponents, Arc<kalamdb_core::app_context::AppContext>)> {
    app_context.wire_raft_appliers();

    let phase_start = std::time::Instant::now();
    if let Err(error) = startup::create_default_storage_if_needed(config, &app_context) {
        return fail_bootstrap(app_context, error).await;
    }
    debug!(
        "Storage initialization completed ({:.2}ms)",
        phase_start.elapsed().as_secs_f64() * 1000.0
    );

    let components =
        match prepare_components(config, app_context.clone(), use_root_password_env).await {
            Ok(components) => components,
            Err(error) => return fail_bootstrap(app_context, error).await,
        };
    Ok((components, app_context))
}

/// Prepare services and background tasks for an already-initialized [`AppContext`].
pub async fn prepare_components(
    config: &ServerConfig,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
    use_root_password_env: bool,
) -> Result<ApplicationComponents> {
    let prepare_start = std::time::Instant::now();

    let live_query_manager = app_context.live_query_manager();
    let session_factory = app_context.session_factory();
    let users_provider = app_context.system_tables().users();
    let cached_user_repo = Arc::new(CachedUsersRepo::new(users_provider));
    let user_repo: Arc<dyn kalamdb_auth::UserRepository> = Arc::clone(&cached_user_repo) as _;
    app_context.set_cached_user_repo(cached_user_repo);

    let handler_registry = Arc::new(HandlerRegistry::new());
    kalamdb_handlers::register_all_handlers(
        &handler_registry,
        app_context.clone(),
        config.auth.local.enforce_password_complexity,
    );

    let sql_executor = Arc::new(SqlExecutor::new(app_context.clone(), handler_registry));

    app_context.set_sql_executor(sql_executor.clone());

    let tables_start = std::time::Instant::now();
    sql_executor.load_existing_tables().await?;
    info!(
        "Startup: schema/table load completed in {:.2}ms",
        tables_start.elapsed().as_secs_f64() * 1000.0
    );

    let dba_start = std::time::Instant::now();
    initialize_dba_namespace(app_context.clone())?;
    ensure_dba_notification_policies(app_context.clone()).await?;
    debug!(
        "Startup: DBA namespace initialized in {:.2}ms",
        dba_start.elapsed().as_secs_f64() * 1000.0
    );

    let raft_restore_start = std::time::Instant::now();
    app_context.restore_raft_state_machines().await;
    info!(
        "Startup: Raft state-machine restore completed in {:.2}ms",
        raft_restore_start.elapsed().as_secs_f64() * 1000.0
    );

    // Initialize job system (executors, manager, waker) — extracted to kalamdb-jobs crate
    kalamdb_jobs::init_job_manager(&app_context);

    let rate_limiter = Arc::new(RateLimiter::with_config(&config.rate_limit));
    let connection_registry = app_context.connection_registry();

    let users_provider_for_init = app_context.system_tables().users();
    create_default_system_user(
        users_provider_for_init.clone(),
        config.auth.root_password.clone(),
        use_root_password_env,
    )
    .await?;

    let max_concurrent = config.jobs.max_concurrent;
    debug!("Starting JobsManager run loop with max {} concurrent jobs", max_concurrent);
    let jobs_runtime =
        JobsManagerRuntime::start(app_context.job_manager(), max_concurrent as usize);
    let trigger_runtime = TriggerDispatcherRuntime::start(Arc::clone(&app_context));

    info!(
        "Startup: prepare_components completed in {:.2}ms",
        prepare_start.elapsed().as_secs_f64() * 1000.0
    );

    Ok(ApplicationComponents {
        session_factory,
        sql_executor,
        rate_limiter,
        live_query_manager,
        user_repo,
        connection_registry,
        jobs_runtime: Some(jobs_runtime),
        trigger_runtime: Some(trigger_runtime),
    })
}

/// Initialize the storage backend, DataFusion, services, rate limiter, and flush scheduler.
pub async fn bootstrap(
    config: &ServerConfig,
) -> Result<(ApplicationComponents, Arc<kalamdb_core::app_context::AppContext>)> {
    let app_context = create_app_context(config, AppContextMode::Global)?;
    app_context.log_runtime_metrics();

    // Start the executor (always Raft - single-node or cluster)
    let phase_start = std::time::Instant::now();
    let is_cluster_mode = config.cluster.is_some();

    if is_cluster_mode {
        // Multi-node cluster mode
        let cluster_config = config.cluster.as_ref().unwrap();
        info!(
            "Starting cluster node {} in cluster '{}' (rpc={}, api={}, peers={})",
            cluster_config.node_id,
            cluster_config.cluster_id,
            cluster_config.rpc_addr,
            cluster_config.api_addr,
            cluster_config.peers.len()
        );
        debug!("╔═══════════════════════════════════════════════════════════════════╗");
        debug!("║                     Multi-Node Cluster Mode                       ║");
        debug!("╚═══════════════════════════════════════════════════════════════════╝");
        debug!(
            "Cluster: {} | Node: {} | Peers: {}",
            cluster_config.cluster_id,
            cluster_config.node_id,
            cluster_config.peers.len()
        );
        debug!(
            "Shards: {} user, {} shared",
            cluster_config.user_shards, cluster_config.shared_shards
        );

        debug!("RPC: {} | API: {}", cluster_config.rpc_addr, cluster_config.api_addr);
        debug!(
            "Heartbeat: {}ms | Election timeout: {:?}ms",
            cluster_config.heartbeat_interval_ms, cluster_config.election_timeout_ms
        );
        for peer in &cluster_config.peers {
            debug!("  Peer {}: rpc={}, api={}", peer.node_id, peer.rpc_addr, peer.api_addr);
        }

        if let Err(error) = app_context.executor().start().await {
            return fail_bootstrap(
                app_context,
                anyhow::anyhow!("Failed to start Raft cluster: {}", error),
            )
            .await;
        }

        // Auto-bootstrap: node_id=1 is the designated bootstrap node
        let should_bootstrap = cluster_config.peers.is_empty() || cluster_config.node_id == 1;

        if should_bootstrap {
            if !cluster_config.peers.is_empty() {
                info!(
                    "Node {} is the bootstrap node; initializing cluster membership and admitting \
                     configured peers",
                    cluster_config.node_id
                );
            }
            if let Err(error) = app_context.executor().initialize_cluster().await {
                return fail_bootstrap(
                    app_context,
                    anyhow::anyhow!("Failed to initialize cluster: {}", error),
                )
                .await;
            }
        } else {
            info!(
                "Node {} is ready and waiting for bootstrap node 1 to admit it to the cluster",
                cluster_config.node_id
            );
        }

        info!(
            "Cluster node {} started Raft services in {:.2}ms",
            cluster_config.node_id,
            phase_start.elapsed().as_secs_f64() * 1000.0
        );
    } else {
        // Single-node mode (lightweight Raft)
        debug!("Single-node mode - initializing lightweight Raft");

        if let Err(error) = app_context.executor().start().await {
            return fail_bootstrap(app_context, anyhow::anyhow!("Failed to start Raft: {}", error))
                .await;
        }
        if let Err(error) = app_context.executor().initialize_cluster().await {
            return fail_bootstrap(
                app_context,
                anyhow::anyhow!("Failed to initialize single-node Raft: {}", error),
            )
            .await;
        }

        debug!(
            "✓ Single-node Raft initialized ({:.2}ms)",
            phase_start.elapsed().as_secs_f64() * 1000.0
        );
    }

    finish_bootstrap(config, app_context, true).await
}

async fn bootstrap_isolated_inner(
    config: &ServerConfig,
    initialize_cluster: bool,
) -> Result<(ApplicationComponents, Arc<kalamdb_core::app_context::AppContext>)> {
    let bootstrap_start = std::time::Instant::now();
    let app_context = create_app_context(config, AppContextMode::Isolated)?;

    // Start Raft (same as bootstrap)
    if let Err(error) = app_context.executor().start().await {
        return fail_bootstrap(app_context, anyhow::anyhow!("Failed to start Raft: {}", error))
            .await;
    }
    if initialize_cluster {
        if let Err(error) = app_context.executor().initialize_cluster().await {
            return fail_bootstrap(
                app_context,
                anyhow::anyhow!("Failed to initialize single-node Raft: {}", error),
            )
            .await;
        }
    }

    let result = finish_bootstrap(config, app_context, false).await;

    debug!(
        "🚀 Server bootstrap (isolated) completed in {:.2}ms",
        bootstrap_start.elapsed().as_secs_f64() * 1000.0
    );
    result
}

/// Bootstrap the server for tests with isolated AppContext.
///
/// Unlike `bootstrap()`, this does NOT use the global AppContext singleton.
/// Each call creates a completely fresh AppContext instance, which is essential
/// for test isolation where each test needs its own independent state.
///
/// **Warning**: Only use this in tests! Production code should use `bootstrap()`.
pub async fn bootstrap_isolated(
    config: &ServerConfig,
) -> Result<(ApplicationComponents, Arc<kalamdb_core::app_context::AppContext>)> {
    bootstrap_isolated_inner(config, true).await
}

/// Bootstrap the server for tests with isolated AppContext without bootstrapping
/// the local node as a fresh cluster leader.
///
/// This is used by multi-node integration tests for follower/joiner nodes that
/// must start their RPC and HTTP surfaces before the initial leader adds them to
/// cluster membership.
pub async fn bootstrap_isolated_without_cluster_init(
    config: &ServerConfig,
) -> Result<(ApplicationComponents, Arc<kalamdb_core::app_context::AppContext>)> {
    bootstrap_isolated_inner(config, false).await
}

fn log_server_started(
    config: &ServerConfig,
    elapsed_ms: f64,
    http_version: &str,
    bind_addr: &str,
    ui_status: &str,
) {
    let ui_segment = if crate::http_runtime::should_print_terminal_hyperlinks(config) {
        crate::http_runtime::format_startup_ui_status_with_links(config, ui_status)
    } else {
        crate::http_runtime::format_startup_ui_status_plain(config, ui_status)
    };
    let pgwire_segment = format_startup_log_segment(&config.postgres_wire);
    let message = format!(
        "🚀 Server started in {elapsed_ms:.2}ms ({http_version} on {bind_addr}{pgwire_segment} | \
         UI: {ui_segment})"
    );

    if crate::http_runtime::should_print_terminal_hyperlinks(config) {
        // tracing-subscriber escapes OSC-8 sequences in log messages, so write
        // trusted startup output directly to the interactive console instead.
        let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ");
        println!("{timestamp}  INFO main kalamdb_server::lifecycle: {message}");
        return;
    }

    info!("{message}");
}

/// Start the HTTP server and manage graceful shutdown.
pub async fn run(
    config: &ServerConfig,
    mut components: ApplicationComponents,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
    main_start: std::time::Instant,
) -> Result<()> {
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    debug!("Starting HTTP server on {}", bind_addr);
    debug!("Endpoints: POST /v1/api/sql, GET /v1/ws");

    // Log server configuration for debugging
    debug!(
        "Server config: workers={}, max_connections={}, backlog={}, blocking_threads={}, \
         body_limit={}MB",
        effective_workers(config.server.workers),
        config.performance.max_connections,
        config.performance.backlog,
        effective_max_blocking_threads(config.performance.worker_max_blocking_threads),
        config.rate_limit.request_body_limit_bytes / (1024 * 1024)
    );

    if config.rate_limit.enable_connection_protection {
        debug!(
            "Connection protection: max_conn_per_ip={}, max_req_per_ip_per_sec={}, \
             ban_duration={}s",
            config.rate_limit.max_connections_per_ip,
            config.rate_limit.max_requests_per_ip_per_sec,
            config.rate_limit.ban_duration_seconds
        );
    } else {
        warn!("Connection protection is DISABLED - server may be vulnerable to DoS attacks");
    }

    if config.security.cors.allowed_origins.is_empty()
        || config.security.cors.allowed_origins.iter().any(|origin| origin == "*")
    {
        debug!("CORS: allowing any origin");
    } else {
        debug!("CORS: allowed origins={:?}", config.security.cors.allowed_origins);
    }

    let jobs_runtime = components
        .jobs_runtime
        .take()
        .expect("jobs runtime must be initialized before server startup");
    let trigger_runtime = components.trigger_runtime.take();
    let job_drain_timeout = std::time::Duration::from_secs(config.shutdown.flush.timeout.into());
    let mut http_server = match HttpServerRuntime::start_configured(
        config,
        &components,
        app_context.clone(),
        AuthRuntimeMode::AlreadyConfigured,
    ) {
        Ok(server) => server,
        Err(error) => {
            if let Some(runtime) = trigger_runtime {
                runtime.shutdown().await;
            }
            if let Err(cleanup_error) =
                shutdown_background_services(Some(jobs_runtime), app_context, job_drain_timeout)
                    .await
            {
                warn!("Startup cleanup failed after HTTP bind error: {cleanup_error}");
            }
            return Err(error.into());
        },
    };
    debug!("Admin UI: {} (at /ui)", http_server.ui_status());
    let connection_registry = components.connection_registry.clone();
    let mut postgres_wire_listener = match PostgresWireListener::start(
        config,
        PostgresWireRuntimeDeps {
            app_context:  app_context.clone(),
            sql_executor: components.sql_executor.clone(),
            user_repo:    components.user_repo.clone(),
        },
    )
    .await
    {
        Ok(listener) => listener,
        Err(error) => {
            if let Some(runtime) = trigger_runtime {
                runtime.shutdown().await;
            }
            return shutdown_server(
                crate::shutdown::TerminationReason::PostgresWireStopped(Err(error)),
                http_server,
                None,
                jobs_runtime,
                connection_registry,
                app_context,
                job_drain_timeout,
            )
            .await;
        },
    };

    log_server_started(
        config,
        main_start.elapsed().as_secs_f64() * 1000.0,
        http_server.http_version(),
        &http_server.bind_addr().to_string(),
        http_server.ui_status(),
    );

    let reason = wait_for_termination(&mut http_server, &mut postgres_wire_listener).await;
    if let Some(runtime) = trigger_runtime {
        runtime.shutdown().await;
    }
    let shutdown_result = shutdown_server(
        reason,
        http_server,
        postgres_wire_listener,
        jobs_runtime,
        connection_registry,
        app_context.clone(),
        job_drain_timeout,
    )
    .await;
    drop(components);
    drop(app_context);
    info!("Server shutdown complete");
    shutdown_result
}

/// A running HTTP server instance intended for integration tests.
///
/// This starts the same Actix app wiring as the production server (middleware stack,
/// route registration, app_data wiring, auth config, rate limiting, etc.) but binds
/// to an ephemeral port and provides an explicit shutdown handle.
pub struct RunningTestHttpServer {
    pub base_url:        String,
    pub bind_addr:       SocketAddr,
    pub app_context:     Arc<kalamdb_core::app_context::AppContext>,
    http_server:         HttpServerRuntime,
    jobs_runtime:        JobsManagerRuntime,
    trigger_runtime:     Option<TriggerDispatcherRuntime>,
    connection_registry: Arc<ConnectionsManager>,
    job_drain_timeout:   std::time::Duration,
}

impl RunningTestHttpServer {
    pub async fn shutdown(self) {
        println!("Shutting down test HTTP server at {}", self.base_url);
        if let Some(runtime) = self.trigger_runtime {
            runtime.shutdown().await;
        }
        let result = shutdown_server(
            crate::shutdown::TerminationReason::Explicit,
            self.http_server,
            None,
            self.jobs_runtime,
            self.connection_registry,
            self.app_context,
            self.job_drain_timeout,
        )
        .await;
        if let Err(error) = result {
            log::warn!("Test server shutdown failed: {}", error);
        }

        println!("Test HTTP server shutdown complete");
    }
}

/// Start the HTTP server for integration tests on a random available port.
///
/// Notes:
/// - Does not install Ctrl+C handling.
/// - Caller must invoke `shutdown()` to stop the server.
pub async fn run_for_tests(
    config: &ServerConfig,
    mut components: ApplicationComponents,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
) -> Result<RunningTestHttpServer> {
    let jobs_runtime = components
        .jobs_runtime
        .take()
        .expect("jobs runtime must be initialized before test server startup");
    let trigger_runtime = components.trigger_runtime.take();
    let job_drain_timeout = std::time::Duration::from_secs(config.shutdown.flush.timeout.into());
    let http_server = match HttpServerRuntime::start_ephemeral(
        config,
        &components,
        app_context.clone(),
        AuthRuntimeMode::Configure,
    ) {
        Ok(server) => server,
        Err(error) => {
            if let Some(runtime) = trigger_runtime {
                runtime.shutdown().await;
            }
            if let Err(cleanup_error) =
                shutdown_background_services(Some(jobs_runtime), app_context, job_drain_timeout)
                    .await
            {
                warn!("Test startup cleanup failed after HTTP bind error: {cleanup_error}");
            }
            return Err(error);
        },
    };
    let bind_addr = http_server.bind_addr();
    let base_url = format!("http://{}", bind_addr);

    Ok(RunningTestHttpServer {
        base_url,
        bind_addr,
        app_context,
        http_server,
        jobs_runtime,
        trigger_runtime,
        connection_registry: components.connection_registry.clone(),
        job_drain_timeout,
    })
}

/// Start the HTTP server without Ctrl+C handling and bind to the configured address.
pub async fn run_detached(
    config: &ServerConfig,
    mut components: ApplicationComponents,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
) -> Result<RunningTestHttpServer> {
    let jobs_runtime = components
        .jobs_runtime
        .take()
        .expect("jobs runtime must be initialized before detached server startup");
    let trigger_runtime = components.trigger_runtime.take();
    let job_drain_timeout = std::time::Duration::from_secs(config.shutdown.flush.timeout.into());
    let http_server = match HttpServerRuntime::start_configured(
        config,
        &components,
        app_context.clone(),
        AuthRuntimeMode::Configure,
    ) {
        Ok(server) => server,
        Err(error) => {
            if let Some(runtime) = trigger_runtime {
                runtime.shutdown().await;
            }
            if let Err(cleanup_error) =
                shutdown_background_services(Some(jobs_runtime), app_context, job_drain_timeout)
                    .await
            {
                warn!("Detached startup cleanup failed after HTTP bind error: {cleanup_error}");
            }
            return Err(error);
        },
    };
    let bind_addr = http_server.bind_addr();
    let base_url = format!("http://{}", bind_addr);

    Ok(RunningTestHttpServer {
        base_url,
        bind_addr,
        app_context,
        http_server,
        jobs_runtime,
        trigger_runtime,
        connection_registry: components.connection_registry.clone(),
        job_drain_timeout,
    })
}

/// T125-T127: Create default system user on database initialization
///
/// Creates a default system user with:
/// - Username: "root" (AUTH::DEFAULT_SYSTEM_USERNAME)
/// - Auth type: Internal (localhost-only by default)
/// - Role: System
/// - Random password for emergency remote access
///
/// Periodic scheduler for stream table TTL eviction runs in background,
/// checking all STREAM tables and creating eviction jobs for tables
/// that have TTL configured.
///
/// On first startup, logs the credentials to stdout for the administrator to save.
///
/// # Arguments
/// * `users_provider` - UsersTableProvider for system.users table
///
/// # Returns
/// Result indicating success or failure
async fn create_default_system_user(
    users_provider: Arc<kalamdb_system::UsersTableProvider>,
    config_root_password: Option<String>,
    use_root_password_env: bool,
) -> Result<()> {
    use kalamdb_commons::constants::AuthConstants;
    use kalamdb_system::User;

    // Check if root user already exists
    let existing_user = users_provider.get_user_by_id(&UserId::root());

    match existing_user {
        Ok(Some(_)) => {
            // User already exists, skip creation
            debug!(
                "System user '{}' already exists, skipping initialization",
                AuthConstants::DEFAULT_SYSTEM_USERNAME
            );
            Ok(())
        },
        Ok(None) | Err(_) => {
            // User doesn't exist, create new system user
            let user_id = UserId::root();
            let username = AuthConstants::DEFAULT_SYSTEM_USERNAME.to_string();
            let email = format!("{}@localhost", AuthConstants::DEFAULT_SYSTEM_USERNAME);
            let role = Role::System; // Highest privilege level
            let created_at = chrono::Utc::now().timestamp_millis();

            // Check for root password from environment variable or config file.
            // Priority: KALAMDB_ROOT_PASSWORD env var > config auth.root_password > empty
            // (localhost-only)
            let root_password_from_env = if use_root_password_env {
                std::env::var("KALAMDB_ROOT_PASSWORD").ok().filter(|p| !p.is_empty())
            } else {
                None
            };
            let root_password_from_config = config_root_password.filter(|p| !p.is_empty());
            let root_password = root_password_from_env.or(root_password_from_config);

            let password_hash = match root_password {
                Some(password) => {
                    // Hash the provided password for remote access
                    bcrypt::hash(&password, bcrypt::DEFAULT_COST)
                        .map_err(|e| anyhow::anyhow!("Failed to hash root password: {}", e))?
                },
                None => {
                    // T126: Create with EMPTY password hash for localhost-only access
                    // This allows passwordless authentication from localhost (127.0.0.1, ::1)
                    // For remote access, set a password using: ALTER USER root SET PASSWORD '...'
                    String::new() // Empty = localhost-only, no password required
                },
            };
            let has_password = !password_hash.is_empty();

            let user = User {
                user_id,
                password_hash,
                role,
                name: None,
                email: Some(email),
                auth_type: AuthType::Password,
                auth_data: None,
                storage_mode: StorageMode::Table,
                storage_id: Some(StorageId::local()),
                failed_login_attempts: 0,
                locked_until: None,
                last_login_at: None,
                created_at,
                updated_at: created_at,
                last_seen: None,
                deleted_at: None,
                invite_expires_at: None,
                invited_by: None,
            };

            users_provider.create_user(user)?;

            // T127: Log system user information to stdout
            if has_password {
                info!(
                    "✓ Created system user '{}' (remote access enabled via KALAMDB_ROOT_PASSWORD)",
                    username
                );
            } else {
                info!(
                    "✓ Created system user '{}' (localhost-only access, no password required)",
                    username
                );
                info!(
                    "  To enable remote access, set a password: ALTER USER root SET PASSWORD '...'"
                );
                info!("  Or set KALAMDB_ROOT_PASSWORD environment variable before startup");
            }

            Ok(())
        },
    }
}
