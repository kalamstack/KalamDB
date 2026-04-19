//! Server lifecycle management helpers.
//!
//! This module encapsulates the heavy lifting previously handled directly
//! in `main.rs`: bootstrapping databases and services, wiring the HTTP
//! server, and coordinating graceful shutdown.

use crate::{middleware, routes};
use actix_web::{web, App, HttpServer};
use anyhow::Result;
use kalamdb_api::limiter::RateLimiter;
use kalamdb_auth::CachedUsersRepo;
use kalamdb_commons::{AuthType, Role, StorageId, UserId};
use kalamdb_configs::ServerConfig;
use kalamdb_core::sql::datafusion_session::DataFusionSessionFactory;
use kalamdb_core::sql::executor::handler_registry::HandlerRegistry;
use kalamdb_core::sql::executor::SqlExecutor;
use kalamdb_dba::{initialize_dba_namespace, start_stats_recorder};
use kalamdb_jobs::AppContextJobsExt;
use kalamdb_live::{ConnectionsManager, LiveQueryManager};
use kalamdb_store::open_storage_backend;
use kalamdb_system::providers::storages::models::StorageMode;
use log::debug;
use log::{info, warn};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use tracing_actix_web::{RootSpanBuilder, TracingLogger};

/// Resolve the effective number of actix-web worker threads.
///
/// Precedence: `KALAMDB_SERVER_WORKERS` env var > server.toml `workers` > auto.
/// When auto (`configured == 0`), uses `num_cpus` but caps at 4 to keep
/// idle RSS low (~2 MB per worker). For high-throughput production
/// deployments, set `workers` explicitly in server.toml or the env var.
fn effective_workers(configured: usize) -> usize {
    // Environment variable takes highest priority
    if let Ok(val) = std::env::var("KALAMDB_SERVER_WORKERS") {
        if let Ok(n) = val.parse::<usize>() {
            if n > 0 {
                return n;
            }
        }
    }
    if configured == 0 {
        num_cpus::get().min(4)
    } else {
        configured
    }
}

/// Custom root span builder that forces `parent: None` on every request span.
///
/// The default `TracingLogger` inherits whatever span is "current" on the
/// thread when a new request arrives.  With `tracing-opentelemetry` in the
/// subscriber stack, this means that a request processed on a thread that
/// still has another request's span entered will be incorrectly parented
/// under that span in Jaeger.  By explicitly setting `parent: None` we
/// guarantee every HTTP request starts a fresh, independent trace.
struct KalamDbRootSpanBuilder;

impl RootSpanBuilder for KalamDbRootSpanBuilder {
    fn on_request_start(request: &actix_web::dev::ServiceRequest) -> tracing::Span {
        let method = request.method().as_str();
        let path = request.uri().path();
        // `parent: None` ensures this span is always a root span,
        // preventing cross-request span contamination.
        tracing::info_span!(
            parent: None,
            "HTTP request",
            http.method = %method,
            http.route = %path,
            http.status_code = tracing::field::Empty,
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
        )
    }

    fn on_request_end<B: actix_web::body::MessageBody>(
        span: tracing::Span,
        outcome: &Result<actix_web::dev::ServiceResponse<B>, actix_web::Error>,
    ) {
        match outcome {
            Ok(response) => {
                let status = response.status().as_u16();
                span.record("http.status_code", status);
                if status >= 500 {
                    span.record("otel.status_code", "ERROR");
                } else {
                    span.record("otel.status_code", "OK");
                }
            },
            Err(err) => {
                span.record("otel.status_code", "ERROR");
                span.record("http.status_code", 500u16);
                tracing::error!(parent: &span, error = %err, "HTTP request error");
            },
        }
    }
}

/// Aggregated application components that need to be shared across the
/// HTTP server and shutdown handling.
pub struct ApplicationComponents {
    pub session_factory: Arc<DataFusionSessionFactory>,
    pub sql_executor: Arc<SqlExecutor>,
    pub rate_limiter: Arc<RateLimiter>,
    pub live_query_manager: Arc<LiveQueryManager>,
    pub user_repo: Arc<dyn kalamdb_auth::UserRepository>,
    pub connection_registry: Arc<ConnectionsManager>,
}

/// Prepare services and background tasks for an already-initialized [`AppContext`].
pub async fn prepare_components(
    config: &ServerConfig,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
    use_root_password_env: bool,
) -> Result<ApplicationComponents> {
    let live_query_manager = app_context.live_query_manager();
    let session_factory = app_context.session_factory();
    let users_provider = app_context.system_tables().users();
    let user_repo: Arc<dyn kalamdb_auth::UserRepository> =
        Arc::new(CachedUsersRepo::new(users_provider));

    let handler_registry = Arc::new(HandlerRegistry::new());
    kalamdb_handlers::register_all_handlers(
        &handler_registry,
        app_context.clone(),
        config.auth.enforce_password_complexity,
    );

    let sql_executor = Arc::new(SqlExecutor::new(app_context.clone(), handler_registry));

    app_context.set_sql_executor(sql_executor.clone());

    sql_executor.load_existing_tables().await?;
    initialize_dba_namespace(app_context.clone())?;

    app_context.restore_raft_state_machines().await;

    // Initialize job system (executors, manager, waker) — extracted to kalamdb-jobs crate
    kalamdb_jobs::init_job_manager(&app_context);

    let job_manager = app_context.job_manager();
    let max_concurrent = config.jobs.max_concurrent;
    tokio::spawn(async move {
        debug!("Starting JobsManager run loop with max {} concurrent jobs", max_concurrent);
        if let Err(e) = job_manager.run_loop(max_concurrent as usize).await {
            log::error!("JobsManager run loop failed: {}", e);
        }
    });

    let rate_limiter = Arc::new(RateLimiter::with_config(&config.rate_limit));
    let connection_registry = app_context.connection_registry();

    let users_provider_for_init = app_context.system_tables().users();
    create_default_system_user(
        users_provider_for_init.clone(),
        config.auth.root_password.clone(),
        use_root_password_env,
    )
    .await?;
    if config.retention.enable_dba_stats {
        if let Err(error) = start_stats_recorder(app_context.clone()).await {
            log::error!("Failed to start DBA stats recorder: {}", error);
        }
    } else {
        log::info!("DBA stats recorder disabled via config (retention.enable_dba_stats = false)");
    }

    Ok(ApplicationComponents {
        session_factory,
        sql_executor,
        rate_limiter,
        live_query_manager,
        user_repo,
        connection_registry,
    })
}

/// Initialize the storage backend, DataFusion, services, rate limiter, and flush scheduler.
pub async fn bootstrap(
    config: &ServerConfig,
) -> Result<(ApplicationComponents, Arc<kalamdb_core::app_context::AppContext>)> {
    // Initialize the storage backend
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

    // Initialize core stores from generic backend (uses kalamdb_store::StorageBackend)
    // Phase 5: AppContext now creates all dependencies internally!
    // Uses constants from kalamdb_commons for table prefixes
    let phase_start = std::time::Instant::now();

    // Node ID: use cluster.node_id (u64) if cluster mode, otherwise default to 1 for standalone
    let node_id = if let Some(cluster) = &config.cluster {
        kalamdb_commons::NodeId::new(cluster.node_id)
    } else {
        kalamdb_commons::NodeId::new(1) // Standalone mode uses node ID 1
    };

    let app_context = kalamdb_core::app_context::AppContext::init(
        backend.clone(),
        node_id,
        config.storage.storage_dir().to_string_lossy().into_owned(),
        config.clone(), // ServerConfig needs to be cloned for Arc storage in AppContext
    );
    debug!(
        "AppContext initialized with all stores, managers, registries, and providers ({:.2}ms)",
        phase_start.elapsed().as_secs_f64() * 1000.0
    );

    // Log runtime snapshot (CPU/memory/threads) using shared sysinfo helper
    app_context.log_runtime_metrics();

    // Start the executor (always Raft - single-node or cluster)
    let phase_start = std::time::Instant::now();
    let is_cluster_mode = config.cluster.is_some();

    if is_cluster_mode {
        // Multi-node cluster mode
        let cluster_config = config.cluster.as_ref().unwrap();
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

        app_context
            .executor()
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start Raft cluster: {}", e))?;

        // Auto-bootstrap: node_id=1 is the designated bootstrap node
        let should_bootstrap = cluster_config.peers.is_empty() || cluster_config.node_id == 1;

        if should_bootstrap {
            if !cluster_config.peers.is_empty() {
                info!("Node {} is bootstrap node - initializing cluster", cluster_config.node_id);
            }
            app_context
                .executor()
                .initialize_cluster()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to initialize cluster: {}", e))?;
        } else {
            info!("Node {} waiting for bootstrap node (node_id=1)", cluster_config.node_id);
        }

        info!("✓ Raft cluster started ({:.2}ms)", phase_start.elapsed().as_secs_f64() * 1000.0);
    } else {
        // Single-node mode (lightweight Raft)
        debug!("Single-node mode - initializing lightweight Raft");

        app_context
            .executor()
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start Raft: {}", e))?;
        app_context
            .executor()
            .initialize_cluster()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize single-node Raft: {}", e))?;

        debug!(
            "✓ Single-node Raft initialized ({:.2}ms)",
            phase_start.elapsed().as_secs_f64() * 1000.0
        );
    }

    // Ensure Raft appliers are registered after Raft has started.
    // Some Raft initialization flows may recreate state machines; re-wiring here keeps
    // metadata/data replication applying into local providers (system tables, schema registry, etc.).
    app_context.wire_raft_appliers();

    // NOTE: restore_raft_state_machines() is called LATER after system tables, storages,
    // and user/shared tables are initialized. The state machine restoration may need to
    // apply commands that interact with these providers.

    // Manifest cache uses lazy loading via get_or_load() - no pre-loading needed
    // When a manifest is needed, get_or_load() checks hot cache → RocksDB → returns None
    // This avoids loading manifests that may never be accessed

    // Seed default storage if necessary (using SystemTablesRegistry)
    let phase_start = std::time::Instant::now();
    let storages_provider = app_context.system_tables().storages();
    let existing_storages = storages_provider.scan_all_storages()?;
    let storage_count = existing_storages.num_rows();

    //TODO: Extract as a separate function create_default_storage_if_needed
    if storage_count == 0 {
        info!("No storages found, creating default 'local' storage");
        let now = chrono::Utc::now().timestamp_millis();
        let default_storage = kalamdb_system::Storage {
            storage_id: StorageId::from("local"),
            storage_name: "Local Filesystem".to_string(),
            description: Some("Default local filesystem storage".to_string()),
            storage_type: kalamdb_system::providers::storages::models::StorageType::Filesystem,
            base_directory: config.storage.storage_dir().to_string_lossy().into_owned(),
            credentials: None,
            config_json: None,
            shared_tables_template: config.storage.shared_tables_template.clone(), // Need clone for Storage struct
            user_tables_template: config.storage.user_tables_template.clone(), // Need clone for Storage struct
            created_at: now,
            updated_at: now,
        };
        storages_provider.insert_storage(default_storage)?;
        info!("Default 'local' storage created successfully");
    } else {
        debug!("Found {} existing storage(s)", storage_count);
    }
    debug!(
        "Storage initialization completed ({:.2}ms)",
        phase_start.elapsed().as_secs_f64() * 1000.0
    );

    let components = prepare_components(config, app_context.clone(), true).await?;

    Ok((components, app_context))
}

async fn bootstrap_isolated_inner(
    config: &ServerConfig,
    initialize_cluster: bool,
) -> Result<(ApplicationComponents, Arc<kalamdb_core::app_context::AppContext>)> {
    let bootstrap_start = std::time::Instant::now();

    // Initialize the storage backend
    let phase_start = std::time::Instant::now();
    let db_path = config.storage.rocksdb_dir();
    std::fs::create_dir_all(&db_path)?;

    let (backend, partition_count) = open_storage_backend(&db_path, &config.storage.rocksdb)?;
    debug!(
        "Storage backend initialized at {} with {} partitions ({:.2}ms)",
        db_path.display(),
        partition_count,
        phase_start.elapsed().as_secs_f64() * 1000.0
    );

    // Node ID: use cluster.node_id (u64) if cluster mode, otherwise default to 1 for standalone
    let node_id = if let Some(cluster) = &config.cluster {
        kalamdb_commons::NodeId::new(cluster.node_id)
    } else {
        kalamdb_commons::NodeId::new(1) // Standalone mode uses node ID 1
    };

    // Use create_isolated instead of init to bypass the global singleton
    let app_context = kalamdb_core::app_context::AppContext::create_isolated(
        backend.clone(),
        node_id,
        config.storage.storage_dir().to_string_lossy().into_owned(),
        config.clone(),
    );

    // Start Raft (same as bootstrap)
    app_context
        .executor()
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start Raft: {}", e))?;
    if initialize_cluster {
        app_context
            .executor()
            .initialize_cluster()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize single-node Raft: {}", e))?;
    }

    app_context.wire_raft_appliers();

    // NOTE: restore_raft_state_machines() is called LATER after system tables, storages,
    // and user/shared tables are initialized. The state machine restoration may need to
    // apply commands that interact with these providers.

    // Seed default storage if necessary
    let storages_provider = app_context.system_tables().storages();
    let existing_storages = storages_provider.scan_all_storages()?;
    if existing_storages.num_rows() == 0 {
        let now = chrono::Utc::now().timestamp_millis();
        let default_storage = kalamdb_system::Storage {
            storage_id: StorageId::from("local"),
            storage_name: "Local Filesystem".to_string(),
            description: Some("Default local filesystem storage".to_string()),
            storage_type: kalamdb_system::providers::storages::models::StorageType::Filesystem,
            base_directory: config.storage.storage_dir().to_string_lossy().into_owned(),
            credentials: None,
            config_json: None,
            shared_tables_template: config.storage.shared_tables_template.clone(),
            user_tables_template: config.storage.user_tables_template.clone(),
            created_at: now,
            updated_at: now,
        };
        storages_provider.insert_storage(default_storage)?;
    }

    let components = prepare_components(config, app_context.clone(), false).await?;

    debug!(
        "🚀 Server bootstrap (isolated) completed in {:.2}ms",
        bootstrap_start.elapsed().as_secs_f64() * 1000.0
    );

    Ok((components, app_context))
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

/// Start the HTTP server and manage graceful shutdown.
pub async fn run(
    config: &ServerConfig,
    components: ApplicationComponents,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
    main_start: std::time::Instant,
) -> Result<()> {
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    debug!("Starting HTTP server on {}", bind_addr);
    debug!("Endpoints: POST /v1/api/sql, GET /v1/ws");

    // Log server configuration for debugging
    debug!(
        "Server config: workers={}, max_connections={}, backlog={}, blocking_threads={}, body_limit={}MB",
        effective_workers(config.server.workers),
        config.performance.max_connections,
        config.performance.backlog,
        config.performance.worker_max_blocking_threads,
        config.rate_limit.request_body_limit_bytes / (1024 * 1024)
    );

    if config.rate_limit.enable_connection_protection {
        debug!(
            "Connection protection: max_conn_per_ip={}, max_req_per_ip_per_sec={}, ban_duration={}s",
            config.rate_limit.max_connections_per_ip,
            config.rate_limit.max_requests_per_ip_per_sec,
            config.rate_limit.ban_duration_seconds
        );
    } else {
        warn!("Connection protection is DISABLED - server may be vulnerable to DoS attacks");
    }

    if config.security.cors.allowed_origins.is_empty()
        || config.security.cors.allowed_origins.contains(&"*".to_string())
    {
        debug!("CORS: allowing any origin");
    } else {
        debug!("CORS: allowed origins={:?}", config.security.cors.allowed_origins);
    }

    // Get JobsManager for graceful shutdown
    let job_manager_shutdown = app_context.job_manager();
    let shutdown_timeout_secs = config.shutdown.flush.timeout;

    let session_factory = components.session_factory.clone();
    let sql_executor = components.sql_executor.clone();
    let rate_limiter = components.rate_limiter.clone();
    let live_query_manager = components.live_query_manager.clone();
    let user_repo = components.user_repo.clone();
    let connection_registry = components.connection_registry.clone();

    // Create connection protection middleware from config
    let connection_protection = middleware::ConnectionProtection::from_server_config(config);

    // Build CORS middleware from config (uses actix-cors)
    let cors_config = config.clone();

    let app_context_for_handler = app_context.clone();
    let connection_registry_for_handler = connection_registry.clone();

    // Initialize shared JWT configuration for kalamdb-auth
    kalamdb_auth::services::unified::init_auth_config(&config.auth, &config.oauth);
    kalamdb_auth::init_trusted_proxy_ranges(&config.security.trusted_proxy_ranges)?;

    // Share auth settings with HTTP handlers
    let auth_settings = config.auth.clone();
    let ui_path = config.server.ui_path.clone();
    let ui_runtime_config =
        kalamdb_api::ui::UiRuntimeConfig::new(config.server.configured_public_origin());

    // Log UI serving status
    let ui_status = if kalamdb_api::routes::is_embedded_ui_available() {
        "embedded in binary"
    } else if let Some(ref _path) = ui_path {
        "filesystem"
    } else {
        "disabled"
    };
    debug!("Admin UI: {} (at /ui)", ui_status);

    let server = HttpServer::new(move || {
        let mut app = App::new()
            // Connection protection (first middleware - drops bad requests early)
            .wrap(connection_protection.clone())
            // Tracing middleware (creates a root span per HTTP request)
            // Uses KalamDbRootSpanBuilder to force `parent: None` on each request,
            // preventing cross-request span contamination in OTel/Jaeger.
            .wrap(TracingLogger::<KalamDbRootSpanBuilder>::new())
            .wrap(middleware::build_cors_from_config(&cors_config))
            .app_data(web::Data::new(app_context_for_handler.clone()))
            .app_data(web::Data::new(session_factory.clone()))
            .app_data(web::Data::new(sql_executor.clone()))
            .app_data(web::Data::new(rate_limiter.clone()))
            .app_data(web::Data::new(live_query_manager.clone()))
            .app_data(web::Data::new(user_repo.clone()))
            .app_data(web::Data::new(connection_registry_for_handler.clone()))
            .app_data(web::Data::new(auth_settings.clone()))
            .configure(routes::configure);

        // Add UI routes - prefer embedded, fallback to filesystem path
        #[cfg(feature = "embedded-ui")]
        if kalamdb_api::routes::is_embedded_ui_available() {
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_embedded_ui_routes(cfg, runtime_config.clone());
            });
        } else if let Some(ref path) = ui_path {
            let path: String = path.clone();
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_ui_routes(
                    cfg,
                    &path,
                    runtime_config.clone(),
                );
            });
        }

        #[cfg(not(feature = "embedded-ui"))]
        if let Some(ref path) = ui_path {
            let path: String = path.clone();
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_ui_routes(
                    cfg,
                    &path,
                    runtime_config.clone(),
                );
            });
        }

        app
    })
    // Set backlog BEFORE bind() - this affects the listen queue size
    .backlog(config.performance.backlog);

    // Bind with HTTP/2 support if enabled, otherwise use HTTP/1.1 only
    let http_version = if config.server.enable_http2 {
        "HTTP/2"
    } else {
        "HTTP/1.1"
    };
    let server = if config.server.enable_http2 {
        debug!("HTTP/2 support enabled (h2c - HTTP/2 cleartext)");
        server.bind_auto_h2c(&bind_addr)?
    } else {
        debug!("HTTP/1.1 only mode");
        server.bind(&bind_addr)?
    };

    info!(
        "🚀 Server started in {:.2}ms ({} on {} | UI: {})",
        main_start.elapsed().as_secs_f64() * 1000.0,
        http_version,
        bind_addr,
        ui_status
    );

    let server = server
    .workers(effective_workers(config.server.workers))
    // Per-worker max concurrent connections (default: 25000)
    .max_connections(config.performance.max_connections)
    // Blocking thread pool size per worker for RocksDB and CPU-intensive ops
    .worker_max_blocking_threads(config.performance.worker_max_blocking_threads)
    // Enable HTTP keep-alive for connection reuse (improves throughput 2-3x)
    // Connections stay open for reuse, reducing TCP handshake overhead
    .keep_alive(std::time::Duration::from_secs(config.performance.keepalive_timeout))
    // Client must send request headers within this time
    .client_request_timeout(std::time::Duration::from_secs(config.performance.client_request_timeout))
    // Allow time for graceful connection shutdown
    .client_disconnect_timeout(std::time::Duration::from_secs(config.performance.client_disconnect_timeout))
    .run();

    let server_handle = server.handle();
    let server_task = tokio::spawn(server);

    tokio::select! {
        result = server_task => {
            if let Err(e) = result {
                log::error!("Server task failed: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, initiating graceful shutdown...");

            // Stop accepting new HTTP connections
            server_handle.stop(true).await;

            // Gracefully shutdown WebSocket connections
            info!("Shutting down WebSocket connections...");
            connection_registry.shutdown(std::time::Duration::from_secs(5)).await;

            info!(
                "Waiting up to {}s for active jobs to complete...",
                shutdown_timeout_secs
            );

            // Signal shutdown to JobsManager (non-async, uses AtomicBool)
            job_manager_shutdown.shutdown();

            // Wait for active jobs with timeout
            let timeout = std::time::Duration::from_secs(shutdown_timeout_secs as u64);
            let start = std::time::Instant::now();

            loop {
                // Check for Running jobs
                let filter = kalamdb_system::JobFilter {
                    status: Some(kalamdb_system::JobStatus::Running),
                    ..Default::default()
                };

                match job_manager_shutdown.list_jobs(filter).await {
                    Ok(jobs) if jobs.is_empty() => {
                        info!("All jobs completed successfully");
                        break;
                    }
                    Ok(jobs) => {
                        if start.elapsed() > timeout {
                            warn!("Timeout waiting for {} active jobs to complete", jobs.len());
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                    Err(e) => {
                        log::error!("Error checking job status during shutdown: {}", e);
                        break;
                    }
                }
            }

            // Ensure all file descriptors are released
            info!("Performing cleanup to release file descriptors...");

            // Shutdown the executor (Raft cluster shutdown in cluster mode)
            if app_context.executor().is_cluster_mode() {
                info!("Shutting down Raft cluster...");
                if let Err(e) = app_context.executor().shutdown().await {
                    log::error!("Error shutting down Raft cluster: {}", e);
                }
            }

            drop(components);
            drop(app_context);

            debug!("Graceful shutdown complete");
        }
    }

    info!("Server shutdown complete");
    Ok(())
}

/// A running HTTP server instance intended for integration tests.
///
/// This starts the same Actix app wiring as the production server (middleware stack,
/// route registration, app_data wiring, auth config, rate limiting, etc.) but binds
/// to an ephemeral port and provides an explicit shutdown handle.
pub struct RunningTestHttpServer {
    pub base_url: String,
    pub bind_addr: SocketAddr,
    pub app_context: Arc<kalamdb_core::app_context::AppContext>,
    server_handle: actix_web::dev::ServerHandle,
    server_task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl RunningTestHttpServer {
    pub async fn shutdown(self) {
        println!("Shutting down test HTTP server at {}", self.base_url);
        // Stop the HTTP server first
        self.server_handle.stop(false).await;
        let _ = self.server_task.await;

        // Stop JobsManager background loop before tearing down storage/executor
        self.app_context.job_manager().shutdown();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Then shutdown the Raft executor to cleanly stop all Raft groups
        // This prevents "Fatal(Stopped)" errors in subsequent tests
        log::debug!("Shutting down Raft executor for test server...");
        println!("Shutting down Raft executor for test server...");
        if let Err(e) = self.app_context.executor().shutdown().await {
            log::warn!("Failed to shutdown Raft executor: {}", e);
        }

        println!("Test HTTP server shutdown complete");
        // Brief delay to allow background tasks to clean up
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Start the HTTP server for integration tests on a random available port.
///
/// Notes:
/// - Does not install Ctrl+C handling.
/// - Caller must invoke `shutdown()` to stop the server.
pub async fn run_for_tests(
    config: &ServerConfig,
    components: ApplicationComponents,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
) -> Result<RunningTestHttpServer> {
    let bind_ip = if config.server.host.is_empty() {
        "127.0.0.1"
    } else {
        config.server.host.as_str()
    };

    let listener = TcpListener::bind((bind_ip, 0))?;
    let bind_addr = listener.local_addr()?;

    let session_factory = components.session_factory.clone();
    let sql_executor = components.sql_executor.clone();
    let rate_limiter = components.rate_limiter.clone();
    let live_query_manager = components.live_query_manager.clone();
    let user_repo = components.user_repo.clone();
    let connection_registry = components.connection_registry.clone();

    let connection_protection = middleware::ConnectionProtection::from_server_config(config);
    let cors_config = config.clone();

    let app_context_for_handler = app_context.clone();
    let connection_registry_for_handler = connection_registry.clone();

    kalamdb_auth::services::unified::init_auth_config(&config.auth, &config.oauth);
    kalamdb_auth::init_trusted_proxy_ranges(&config.security.trusted_proxy_ranges)?;
    let auth_settings = config.auth.clone();
    let ui_path = config.server.ui_path.clone();
    let ui_runtime_config =
        kalamdb_api::ui::UiRuntimeConfig::new(config.server.configured_public_origin());

    let server = HttpServer::new(move || {
        let mut app = App::new()
            .wrap(connection_protection.clone())
            .wrap(TracingLogger::<KalamDbRootSpanBuilder>::new())
            .wrap(middleware::build_cors_from_config(&cors_config))
            .app_data(web::Data::new(app_context_for_handler.clone()))
            .app_data(web::Data::new(session_factory.clone()))
            .app_data(web::Data::new(sql_executor.clone()))
            .app_data(web::Data::new(rate_limiter.clone()))
            .app_data(web::Data::new(live_query_manager.clone()))
            .app_data(web::Data::new(user_repo.clone()))
            .app_data(web::Data::new(connection_registry_for_handler.clone()))
            .app_data(web::Data::new(auth_settings.clone()))
            .configure(routes::configure);

        #[cfg(feature = "embedded-ui")]
        if kalamdb_api::routes::is_embedded_ui_available() {
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_embedded_ui_routes(cfg, runtime_config.clone());
            });
        } else if let Some(ref path) = ui_path {
            let path: String = path.clone();
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_ui_routes(cfg, &path, runtime_config.clone());
            });
        }

        #[cfg(not(feature = "embedded-ui"))]
        if let Some(ref path) = ui_path {
            let path: String = path.clone();
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_ui_routes(cfg, &path, runtime_config.clone());
            });
        }

        app
    })
    .backlog(config.performance.backlog);

    let server = server
        .listen(listener)?
        .workers(effective_workers(config.server.workers))
        .max_connections(config.performance.max_connections)
        .worker_max_blocking_threads(config.performance.worker_max_blocking_threads)
        .keep_alive(std::time::Duration::from_secs(config.performance.keepalive_timeout))
        .client_request_timeout(std::time::Duration::from_secs(
            config.performance.client_request_timeout,
        ))
        .client_disconnect_timeout(std::time::Duration::from_secs(
            config.performance.client_disconnect_timeout,
        ))
        .run();

    let server_handle = server.handle();
    let server_task = tokio::spawn(server);
    let base_url = format!("http://{}", bind_addr);

    Ok(RunningTestHttpServer {
        base_url,
        bind_addr,
        app_context,
        server_handle,
        server_task,
    })
}

/// Start the HTTP server without Ctrl+C handling and bind to the configured address.
pub async fn run_detached(
    config: &ServerConfig,
    components: ApplicationComponents,
    app_context: Arc<kalamdb_core::app_context::AppContext>,
) -> Result<RunningTestHttpServer> {
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);

    let session_factory = components.session_factory.clone();
    let sql_executor = components.sql_executor.clone();
    let rate_limiter = components.rate_limiter.clone();
    let live_query_manager = components.live_query_manager.clone();
    let user_repo = components.user_repo.clone();
    let connection_registry = components.connection_registry.clone();

    let connection_protection = middleware::ConnectionProtection::from_server_config(config);
    let cors_config = config.clone();

    let app_context_for_handler = app_context.clone();
    let connection_registry_for_handler = connection_registry.clone();

    kalamdb_auth::services::unified::init_auth_config(&config.auth, &config.oauth);
    kalamdb_auth::init_trusted_proxy_ranges(&config.security.trusted_proxy_ranges)?;
    let auth_settings = config.auth.clone();
    let ui_path = config.server.ui_path.clone();
    let ui_runtime_config =
        kalamdb_api::ui::UiRuntimeConfig::new(config.server.configured_public_origin());

    let server = HttpServer::new(move || {
        let mut app = App::new()
            .wrap(connection_protection.clone())
            .wrap(TracingLogger::<KalamDbRootSpanBuilder>::new())
            .wrap(middleware::build_cors_from_config(&cors_config))
            .app_data(web::Data::new(app_context_for_handler.clone()))
            .app_data(web::Data::new(session_factory.clone()))
            .app_data(web::Data::new(sql_executor.clone()))
            .app_data(web::Data::new(rate_limiter.clone()))
            .app_data(web::Data::new(live_query_manager.clone()))
            .app_data(web::Data::new(user_repo.clone()))
            .app_data(web::Data::new(connection_registry_for_handler.clone()))
            .app_data(web::Data::new(auth_settings.clone()))
            .configure(routes::configure);

        #[cfg(feature = "embedded-ui")]
        if kalamdb_api::routes::is_embedded_ui_available() {
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_embedded_ui_routes(cfg, runtime_config.clone());
            });
        } else if let Some(ref path) = ui_path {
            let path: String = path.clone();
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_ui_routes(cfg, &path, runtime_config.clone());
            });
        }

        #[cfg(not(feature = "embedded-ui"))]
        if let Some(ref path) = ui_path {
            let path: String = path.clone();
            let runtime_config = ui_runtime_config.clone();
            app = app.configure(move |cfg| {
                kalamdb_api::routes::configure_ui_routes(cfg, &path, runtime_config.clone());
            });
        }

        app
    })
    .backlog(config.performance.backlog);

    let server = if config.server.enable_http2 {
        server.bind_auto_h2c(&bind_addr)?
    } else {
        server.bind(&bind_addr)?
    };

    let server = server
        .workers(effective_workers(config.server.workers))
        .max_connections(config.performance.max_connections)
        .worker_max_blocking_threads(config.performance.worker_max_blocking_threads)
        .keep_alive(std::time::Duration::from_secs(config.performance.keepalive_timeout))
        .client_request_timeout(std::time::Duration::from_secs(
            config.performance.client_request_timeout,
        ))
        .client_disconnect_timeout(std::time::Duration::from_secs(
            config.performance.client_disconnect_timeout,
        ))
        .run();

    let server_handle = server.handle();
    let server_task = tokio::spawn(server);
    let bind_addr = bind_addr.parse()?;
    let base_url = format!("http://{}", bind_addr);

    Ok(RunningTestHttpServer {
        base_url,
        bind_addr,
        app_context,
        server_handle,
        server_task,
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
            // Priority: KALAMDB_ROOT_PASSWORD env var > config auth.root_password > empty (localhost-only)
            let root_password_from_env = if use_root_password_env {
                std::env::var("KALAMDB_ROOT_PASSWORD").ok().filter(|p| !p.is_empty())
            } else {
                None
            };
            let root_password_from_config = config_root_password.clone().filter(|p| !p.is_empty());
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
                email: Some(email),
                auth_type: AuthType::Internal, // System user uses Internal auth
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

// /// Check for security issues with remote access configuration
// ///
// /// Informs users about password requirements for remote access

// async fn check_remote_access_security(
//     config: &ServerConfig,
//     users_provider: Arc<kalamdb_system::UsersTableProvider>,
// ) -> Result<()> {
//     use kalamdb_commons::constants::AuthConstants;

//     // Check if root user exists and has empty password
//     // Always show this info if root has no password, regardless of allow_remote_access setting
//     if let Ok(Some(user)) =
//         users_provider.get_user_by_username(AuthConstants::DEFAULT_SYSTEM_USERNAME)
//     {
//         if user.password_hash.is_empty() {
//             // Root user has no password - this is secure for localhost-only but warn about limitations
//             warn!("╔═══════════════════════════════════════════════════════════════════╗");
//             warn!("║                    ⚠️  SECURITY NOTICE ⚠️                           ║");
//             warn!("╠═══════════════════════════════════════════════════════════════════╣");
//             warn!("║                                                                   ║");
//             warn!("║  Root user has NO PASSWORD (localhost-only access enabled)       ║");
//             warn!("║                                                                   ║");
//             warn!("║  SECURITY ENFORCEMENT:                                           ║");
//             warn!("║  • Remote authentication is BLOCKED for users with no password   ║");
//             warn!("║  • Root can only connect from localhost (127.0.0.1)              ║");
//             warn!("║  • This configuration is secure by design                        ║");
//             warn!("║                                                                   ║");
//             warn!("║  TO ENABLE REMOTE ACCESS:                                        ║");
//             warn!("║  Set a strong password for the root user:                        ║");
//             warn!("║     ALTER USER root SET PASSWORD 'strong-password-here';         ║");
//             warn!("║                                                                   ║");
//             warn!(
//                 "║  Note: allow_remote_access config is currently: {}               ║",
//                 if config.auth.allow_remote_access {
//                     "ENABLED "
//                 } else {
//                     "DISABLED"
//                 }
//             );
//             warn!("║  (Remote access still requires password for system users)        ║");
//             warn!("║                                                                   ║");
//             warn!("╚═══════════════════════════════════════════════════════════════════╝");
//         }
//     }

//     Ok(())
// }
