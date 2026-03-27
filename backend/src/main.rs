// KalamDB Server entrypoint
//!
//! The heavy lifting (initialization, middleware wiring, graceful shutdown)
//! lives in dedicated modules so this file remains a thin orchestrator.

use kalamdb_core::metrics::{BUILD_DATE, SERVER_VERSION};

mod logging;

use anyhow::{anyhow, Result};
use kalamdb_configs::ServerConfig;
use kalamdb_server::lifecycle::{bootstrap, run};
use log::info;
use std::collections::HashSet;
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::path::{Path, PathBuf};

fn resolve_bind_addrs(addr: &str, label: &str) -> Result<HashSet<SocketAddr>> {
    let addrs: Vec<SocketAddr> = addr
        .to_socket_addrs()
        .map_err(|e| anyhow!("Invalid {} address '{}': {}", label, addr, e))?
        .collect();

    if addrs.is_empty() {
        return Err(anyhow!(
            "Invalid {} address '{}': resolved to no socket addresses",
            label,
            addr
        ));
    }

    Ok(addrs.into_iter().collect())
}

fn ensure_any_addr_bindable(
    addrs: &HashSet<SocketAddr>,
    label: &str,
    original_addr: &str,
) -> Result<()> {
    let mut last_error: Option<(SocketAddr, std::io::Error)> = None;

    for addr in addrs {
        match TcpListener::bind(addr) {
            Ok(listener) => {
                drop(listener);
                return Ok(());
            },
            Err(err) => last_error = Some((*addr, err)),
        }
    }

    if let Some((addr, err)) = last_error {
        if err.kind() == std::io::ErrorKind::AddrInUse {
            return Err(anyhow!(
                "{} port check failed: '{}' (resolved as {}) is already in use",
                label,
                original_addr,
                addr
            ));
        }

        return Err(anyhow!(
            "{} port check failed: unable to bind '{}' (resolved as {}): {}",
            label,
            original_addr,
            addr,
            err
        ));
    }

    Err(anyhow!("{} port check failed: unable to bind '{}'", label, original_addr))
}

fn validate_startup_ports(config: &ServerConfig) -> Result<()> {
    let http_addr = format!("{}:{}", config.server.host, config.server.port);
    let http_addrs = resolve_bind_addrs(&http_addr, "HTTP")?;

    if let Some(cluster) = &config.cluster {
        let rpc_addrs = resolve_bind_addrs(&cluster.rpc_addr, "Raft RPC")?;

        if !http_addrs.is_disjoint(&rpc_addrs) {
            return Err(anyhow!(
                "Invalid configuration: HTTP '{}' and Raft RPC '{}' resolve to at least one identical socket address. Configure distinct ports.",
                http_addr,
                cluster.rpc_addr
            ));
        }

        ensure_any_addr_bindable(&rpc_addrs, "Raft RPC", &cluster.rpc_addr)?;
    }

    ensure_any_addr_bindable(&http_addrs, "HTTP", &http_addr)?;

    Ok(())
}

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Raise the process file-descriptor limit to the OS hard maximum.
/// This is critical for benchmarks and production workloads that open many
/// RocksDB files, Parquet segments, and WebSocket connections simultaneously.
#[cfg(unix)]
fn raise_fd_limit() {
    use std::mem::MaybeUninit;

    unsafe {
        let mut rlim = MaybeUninit::<libc::rlimit>::zeroed().assume_init();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            let old_soft = rlim.rlim_cur;
            // On macOS kern.maxfilesperproc is typically 10240-24576;
            // request the hard limit (or a sane floor of 65536).
            let target = rlim.rlim_max.max(65_536);
            rlim.rlim_cur = target;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &rlim) != 0 {
                // macOS may reject values above kern.maxfilesperproc;
                // fall back to hard limit as-is.
                rlim.rlim_cur = rlim.rlim_max;
                let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
            }
            // Re-read to report actual value
            libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim);
            if rlim.rlim_cur != old_soft {
                eprintln!("📂 Raised open-file limit: {} → {}", old_soft, rlim.rlim_cur);
            }
        }
    }
}

fn resolve_config_path() -> PathBuf {
    if let Some(arg_path) = std::env::args().nth(1) {
        PathBuf::from(arg_path)
    } else {
        let cwd_path = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("server.toml");
        if cwd_path.exists() {
            cwd_path
        } else {
            let exe_dir = std::env::current_exe()
                .ok()
                .and_then(|path| path.parent().map(|dir| dir.to_path_buf()))
                .unwrap_or_else(|| PathBuf::from("."));
            exe_dir.join("server.toml")
        }
    }
}

fn load_server_config(config_path: &Path) -> ServerConfig {
    if !config_path.exists() {
        eprintln!("❌ FATAL: Config file not found: {}", config_path.display());
        eprintln!("❌ Server cannot start without valid configuration");
        std::process::exit(1);
    }

    let mut config = match ServerConfig::from_file(config_path) {
        Ok(cfg) => {
            eprintln!(
                "✅ Loaded config from: {}",
                std::fs::canonicalize(config_path)
                    .unwrap_or_else(|_| config_path.to_path_buf())
                    .display()
            );
            cfg
        },
        Err(e) => {
            eprintln!("❌ FATAL: Failed to load {}: {}", config_path.display(), e);
            eprintln!("❌ Server cannot start without valid configuration");
            std::process::exit(1);
        },
    };

    if let Err(e) = config.apply_env_overrides() {
        eprintln!("❌ FATAL: Failed to apply environment overrides: {}", e);
        eprintln!("❌ Server cannot start without valid configuration");
        std::process::exit(1);
    }

    if let Err(e) = config.finalize() {
        eprintln!("❌ FATAL: Invalid configuration after overrides: {}", e);
        eprintln!("❌ Server cannot start without valid configuration");
        std::process::exit(1);
    }

    if let Err(e) = validate_startup_ports(&config) {
        eprintln!("❌ FATAL: Port preflight check failed: {}", e);
        eprintln!("❌ Server cannot start until both HTTP and Raft RPC ports are available");
        std::process::exit(1);
    }

    config
}

fn resolve_tokio_worker_threads(config: &ServerConfig) -> usize {
    std::env::var("KALAMDB_TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .or_else(|| {
            (config.performance.tokio_worker_threads > 0)
                .then_some(config.performance.tokio_worker_threads)
        })
        .unwrap_or_else(|| num_cpus::get().min(4))
}

// Build the tokio runtime manually so we can honour KALAMDB_TOKIO_WORKER_THREADS
// or `performance.tokio_worker_threads` from server.toml and reduce idle RSS
// from over-provisioned thread stacks on high-core-count hosts / Docker.
fn main() -> Result<()> {
    // Raise file-descriptor limit BEFORE any I/O (RocksDB, Parquet, sockets).
    #[cfg(unix)]
    raise_fd_limit();

    let config = load_server_config(&resolve_config_path());
    let worker_threads = resolve_tokio_worker_threads(&config);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("Failed to build tokio runtime");

    runtime.block_on(async_main(config))
}

async fn async_main(config: ServerConfig) -> Result<()> {
    let main_start = std::time::Instant::now();

    // ========================================================================
    // JWT CONFIG INITIALIZATION
    // ========================================================================
    // Initialize auth JWT config from server.toml (after env overrides are applied).
    kalamdb_auth::services::unified::init_auth_config(&config.auth, &config.oauth);
    kalamdb_auth::init_trusted_proxy_ranges(&config.security.trusted_proxy_ranges)?;

    // ========================================================================
    // Security: Validate critical configuration at startup
    // ========================================================================

    // Check JWT secret strength
    const INSECURE_JWT_SECRETS: &[&str] = &[
        "CHANGE_ME_IN_PRODUCTION",
        "kalamdb-dev-secret-key-change-in-production",
        "your-secret-key-at-least-32-chars-change-me-in-production",
        "test",
        "secret",
        "password",
    ];

    let jwt_secret = &config.auth.jwt_secret;
    let is_insecure_secret = INSECURE_JWT_SECRETS.iter().any(|s| jwt_secret == *s);
    let is_short_secret = jwt_secret.len() < 32;

    if is_insecure_secret || is_short_secret {
        eprintln!("╔═══════════════════════════════════════════════════════════════════╗");
        eprintln!("║               ⚠️  SECURITY WARNING: JWT SECRET ⚠️                  ║");
        eprintln!("╠═══════════════════════════════════════════════════════════════════╣");
        if is_insecure_secret {
            eprintln!("║  The configured JWT secret is a known default/placeholder.       ║");
            eprintln!("║  This is INSECURE and allows token forgery!                       ║");
        }
        if is_short_secret {
            eprintln!(
                "║  JWT secret is too short ({} chars). Minimum 32 chars required.  ║",
                jwt_secret.len()
            );
        }
        eprintln!("║                                                                   ║");
        eprintln!("║  To fix: Set a strong, unique secret in server.toml:             ║");
        eprintln!("║    [auth]                                                         ║");
        eprintln!("║    jwt_secret = \"your-unique-32-char-minimum-secret-here\"         ║");
        eprintln!("║                                                                   ║");
        eprintln!("║  Or set via environment variable:                                ║");
        eprintln!("║    export KALAMDB_JWT_SECRET=\"$(openssl rand -base64 32)\"         ║");
        eprintln!("║                                                                   ║");
        eprintln!("║  Generate a secure random secret:                                ║");
        eprintln!("║    openssl rand -base64 32                                        ║");
        eprintln!("║    # or                                                           ║");
        eprintln!("║    cat /dev/urandom | head -c 32 | base64                        ║");
        eprintln!("║                                                                   ║");

        // In production mode (not localhost), refuse to start
        let host = &config.server.host;
        let is_localhost = host == "127.0.0.1" || host == "localhost" || host == "::1";

        if !is_localhost {
            eprintln!("║  FATAL: Refusing to start with insecure JWT secret on non-local  ║");
            eprintln!("║         address. This prevents token forgery attacks.             ║");
            eprintln!("║                                                                   ║");
            eprintln!("║  KalamDB will not start on {} with the current JWT secret.       ║", host);
            eprintln!("╚═══════════════════════════════════════════════════════════════════╝");
            std::process::exit(1);
        } else {
            eprintln!("║  ⚠️ Allowing insecure secret for localhost development only.      ║");
            eprintln!("║  This configuration would be REJECTED for production use.        ║");
            eprintln!("╚═══════════════════════════════════════════════════════════════════╝");
        }
    }

    // Logging before any other side effects
    // Use .jsonl extension for JSON format, .log for compact format
    let log_extension = if config.logging.format.eq_ignore_ascii_case("json") {
        "jsonl"
    } else {
        "log"
    };
    let server_log_path = format!("{}/server.{}", config.logging.logs_path, log_extension);
    logging::init_logging(
        &config.logging.level,
        &server_log_path,
        config.logging.log_to_console,
        Some(&config.logging.targets),
        &config.logging.format,
        &config.logging.otlp,
    )
    .map_err(|error| {
        anyhow::anyhow!("Failed to initialize logging at '{}': {}", server_log_path, error)
    })?;

    // Display enhanced version information
    info!("KalamDB Server v{:<10} | Build: {}", SERVER_VERSION, BUILD_DATE);

    // Build application state and kick off background services
    let (components, app_context) = bootstrap(&config).await?;

    // Run HTTP server until termination signal is received
    let run_result = run(&config, components, app_context, main_start).await;
    logging::shutdown_telemetry();
    run_result
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;

    /// Verify the global allocator can allocate, write, read, and free memory.
    /// Under mimalloc this runs through the replaced global allocator; under the
    /// system allocator it still passes — the key assertion is that alloc/dealloc
    /// round-trips work and memory is not leaked.
    #[test]
    fn allocator_alloc_dealloc_roundtrip() {
        let layout = Layout::array::<u8>(4096).unwrap();

        unsafe {
            // Allocate 4 KiB
            let ptr = std::alloc::alloc(layout);
            assert!(!ptr.is_null(), "allocation must succeed");

            // Write and read back
            std::ptr::write_bytes(ptr, 0xAB, 4096);
            assert_eq!(*ptr, 0xAB);
            assert_eq!(*ptr.add(4095), 0xAB);

            // Free
            std::alloc::dealloc(ptr, layout);
        }
    }

    /// Stress test: allocate many small blocks (mimalloc's sweet spot),
    /// touch them, and free in reverse order. Validates the allocator
    /// handles high-churn small allocations without corruption.
    #[test]
    fn allocator_small_alloc_stress() {
        const COUNT: usize = 10_000;
        const SIZE: usize = 64;
        let layout = Layout::from_size_align(SIZE, 8).unwrap();

        let mut ptrs = Vec::with_capacity(COUNT);
        unsafe {
            for i in 0..COUNT {
                let ptr = std::alloc::alloc(layout);
                assert!(!ptr.is_null(), "allocation {i} must succeed");
                // Write a sentinel byte
                std::ptr::write_bytes(ptr, (i & 0xFF) as u8, SIZE);
                ptrs.push(ptr);
            }

            // Verify and free in reverse order
            for (i, ptr) in ptrs.iter().enumerate().rev() {
                let expected = (i & 0xFF) as u8;
                assert_eq!(**ptr, expected, "data corruption at allocation {i}");
                std::alloc::dealloc(*ptr, layout);
            }
        }
    }

    /// Confirm that the mimalloc global allocator is actually installed
    /// by checking the type name of the ALLOC static.
    #[cfg(feature = "mimalloc")]
    #[test]
    fn mimalloc_is_global_allocator() {
        let name = std::any::type_name_of_val(&super::ALLOC);
        assert!(
            name.contains("MiMalloc"),
            "expected MiMalloc global allocator, got: {name}"
        );
    }
}
