use std::process::Command;
use std::time::Instant;

use crate::benchmarks::enabled_in_default_suite;
use crate::client::KalamClient;
use crate::config::Config;

/// Result of a single pre-flight check.
#[derive(Debug)]
pub struct CheckResult {
    pub name: String,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
}

impl CheckResult {
    fn pass(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            status: CheckStatus::Pass,
            detail: detail.into(),
        }
    }
    fn warn(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            status: CheckStatus::Warn,
            detail: detail.into(),
        }
    }
    fn fail(name: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            status: CheckStatus::Fail,
            detail: detail.into(),
        }
    }

    fn icon(&self) -> &str {
        match self.status {
            CheckStatus::Pass => "✅",
            CheckStatus::Warn => "⚠️ ",
            CheckStatus::Fail => "❌",
        }
    }

    fn color_code(&self) -> &str {
        match self.status {
            CheckStatus::Pass => "\x1b[32m",
            CheckStatus::Warn => "\x1b[33m",
            CheckStatus::Fail => "\x1b[31m",
        }
    }
}

/// Run all pre-flight checks. Returns true if no checks failed (warnings are OK).
pub async fn run_preflight_checks(client: &KalamClient, config: &Config) -> bool {
    println!("Pre-flight Checks");
    println!("─────────────────────────────────────────────────");

    let mut checks: Vec<CheckResult> = Vec::new();

    // 1. File descriptor limit (Unix only)
    checks.push(check_fd_limit());
    checks.push(check_macos_maxfiles_limits());

    // 2. Endpoint reachability (all configured URLs)
    checks.push(check_all_urls_reachable(client, config).await);

    // 2b. scale benchmark capacity feasibility
    checks.push(check_subscriber_scale_target_capacity(config));
    checks.push(check_connection_scale_target_capacity(config));

    // 3. SQL connectivity
    checks.push(check_sql_connectivity(client).await);

    // 4. Admin permissions
    checks.push(check_admin_permissions(client, config).await);

    // 5. Clean state (no leftover bench namespaces)
    checks.push(check_clean_state(client).await);

    // 6. bcrypt cost (measure user creation speed as proxy)
    checks.push(check_bcrypt_cost(client).await);

    // Print results
    for check in &checks {
        println!(
            "  {} {}{:<24}\x1b[0m {}",
            check.icon(),
            check.color_code(),
            check.name,
            check.detail,
        );
    }
    println!();

    let failed = checks.iter().any(|c| c.status == CheckStatus::Fail);
    let warned = checks.iter().any(|c| c.status == CheckStatus::Warn);

    if failed {
        println!("\x1b[31m  ✖ Pre-flight checks FAILED. Fix the issues above before running benchmarks.\x1b[0m\n");
        return false;
    }
    if warned {
        println!("\x1b[33m  ⚡ Some warnings detected — benchmarks will proceed but results may be affected.\x1b[0m\n");
    } else {
        println!("\x1b[32m  ✔ All pre-flight checks passed.\x1b[0m\n");
    }

    true
}

fn check_subscriber_scale_target_capacity(config: &Config) -> CheckResult {
    if !will_run_subscriber_scale(config) {
        return CheckResult::pass(
            "subscriber_scale target",
            "Skipped (subscriber_scale not selected)",
        );
    }

    let ws_targets = resolve_ws_targets(&config.urls);
    let allow_single = std::env::var("KALAMDB_ALLOW_SINGLE_WS_TARGET").ok().as_deref() == Some("1");

    if ws_targets.is_empty() {
        return CheckResult::fail(
            "subscriber_scale target",
            "No WebSocket targets resolved from --urls",
        );
    }

    let subscriptions_per_connection = benchmark_ws_subscriptions_per_connection();
    let required_ws_connections =
        (config.max_subscribers as usize).div_ceil(subscriptions_per_connection.max(1));
    let required_per_target = required_ws_connections.div_ceil(ws_targets.len());
    let single_target_ws_limit = detected_single_target_ws_limit().unwrap_or(32_000);
    let single_target_ws_limit_label = human_count(single_target_ws_limit);

    if ws_targets.len() == 1 && required_ws_connections > single_target_ws_limit && !allow_single {
        return CheckResult::fail(
            "subscriber_scale target",
            format!(
                "Single WS target ({}) would require about {} WebSocket connections at {} subscriptions/connection, which likely hits the local ephemeral-port ceiling on this host near {}. Use --urls with multiple endpoints or set KALAMDB_ALLOW_SINGLE_WS_TARGET=1.",
                ws_targets[0],
                required_ws_connections,
                subscriptions_per_connection,
                single_target_ws_limit_label
            ),
        );
    }

    if ws_targets.len() == 1 && required_ws_connections > single_target_ws_limit && allow_single {
        return CheckResult::warn(
            "subscriber_scale target",
            format!(
                "Forced via KALAMDB_ALLOW_SINGLE_WS_TARGET=1 on single target {} (estimated {} WebSocket connections at {} subscriptions/connection; ephemeral-port risk on this host near {}).",
                ws_targets[0],
                required_ws_connections,
                subscriptions_per_connection,
                single_target_ws_limit_label
            ),
        );
    }

    CheckResult::pass(
        "subscriber_scale target",
        format!(
            "{} WS target(s), client pools up to {} subscriptions per WS connection, requires about {} WS connection(s) total (~{} per target) for max_subscribers={}",
            ws_targets.len(),
            subscriptions_per_connection,
            required_ws_connections,
            required_per_target,
            config.max_subscribers
        ),
    )
}

fn benchmark_ws_subscriptions_per_connection() -> usize {
    std::env::var("KALAMDB_BENCH_WS_SUBSCRIPTIONS_PER_CONNECTION")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100)
}

fn check_connection_scale_target_capacity(config: &Config) -> CheckResult {
    if !will_run_connection_scale(config) {
        return CheckResult::pass(
            "connection_scale target",
            "Skipped (connection_scale not selected)",
        );
    }

    let ws_targets = resolve_ws_targets(&config.urls);
    let allow_single = std::env::var("KALAMDB_ALLOW_SINGLE_WS_TARGET").ok().as_deref() == Some("1");

    if ws_targets.is_empty() {
        return CheckResult::fail(
            "connection_scale target",
            "No WebSocket targets resolved from --urls",
        );
    }

    let required_ws_connections = config.max_subscribers as usize;
    let required_per_target = required_ws_connections.div_ceil(ws_targets.len());
    let single_target_ws_limit = detected_single_target_ws_limit().unwrap_or(32_000);
    let single_target_ws_limit_label = human_count(single_target_ws_limit);

    if ws_targets.len() == 1 && required_ws_connections > single_target_ws_limit && !allow_single {
        return CheckResult::fail(
            "connection_scale target",
            format!(
                "Single WS target ({}) would require about {} WebSocket connections with one subscription/connection, which likely hits the local ephemeral-port ceiling on this host near {}. Use --urls with multiple endpoints or set KALAMDB_ALLOW_SINGLE_WS_TARGET=1.",
                ws_targets[0],
                required_ws_connections,
                single_target_ws_limit_label,
            ),
        );
    }

    if ws_targets.len() == 1 && required_ws_connections > single_target_ws_limit && allow_single {
        return CheckResult::warn(
            "connection_scale target",
            format!(
                "Forced via KALAMDB_ALLOW_SINGLE_WS_TARGET=1 on single target {} (estimated {} WebSocket connections with one subscription/connection; ephemeral-port risk on this host near {}).",
                ws_targets[0],
                required_ws_connections,
                single_target_ws_limit_label,
            ),
        );
    }

    CheckResult::pass(
        "connection_scale target",
        format!(
            "{} WS target(s), one subscription per connection, requires about {} WS connection(s) total (~{} per target) for max_subscribers={}",
            ws_targets.len(),
            required_ws_connections,
            required_per_target,
            config.max_subscribers,
        ),
    )
}

fn will_run_subscriber_scale(config: &Config) -> bool {
    will_run_named_benchmark(config, "subscriber_scale", "scale")
}

fn will_run_connection_scale(config: &Config) -> bool {
    will_run_named_benchmark(config, "connection_scale", "scale")
}

fn will_run_named_benchmark(config: &Config, name: &str, category: &str) -> bool {
    if !config.bench.is_empty() {
        return config.bench.iter().any(|benchmark| benchmark == name);
    }

    if let Some(filter) = &config.filter {
        let f = filter.to_lowercase();
        return name.contains(&f) || category.contains(&f);
    }

    enabled_in_default_suite(name)
}

fn resolve_ws_targets(urls: &[String]) -> Vec<String> {
    let mut targets = Vec::new();

    for raw in urls {
        if let Some(target) = normalize_ws_endpoint(raw) {
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
    }

    targets
}

fn normalize_ws_endpoint(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let normalized = trimmed.trim_end_matches('/');
    let mut url = if normalized.starts_with("ws://") || normalized.starts_with("wss://") {
        normalized.to_string()
    } else if normalized.starts_with("http://") || normalized.starts_with("https://") {
        normalized.replace("http://", "ws://").replace("https://", "wss://")
    } else {
        format!("ws://{}", normalized)
    };

    let authority_start = url.find("://").map(|idx| idx + 3).unwrap_or(0);
    let has_path = url[authority_start..].contains('/');
    if !has_path {
        url.push_str("/v1/ws");
    }

    Some(url)
}

fn detected_single_target_ws_limit() -> Option<usize> {
    let bind_count = configured_ws_local_bind_address_count().max(1);

    #[cfg(target_os = "macos")]
    {
        macos_high_ephemeral_port_capacity().map(|limit| limit.saturating_mul(bind_count))
    }

    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
fn macos_high_ephemeral_port_capacity() -> Option<usize> {
    let first = read_sysctl_usize("net.inet.ip.portrange.hifirst")?;
    let last = read_sysctl_usize("net.inet.ip.portrange.hilast")?;
    (last >= first).then_some(last - first + 1)
}

#[cfg(target_os = "macos")]
fn read_sysctl_usize(name: &str) -> Option<usize> {
    let output = Command::new("sysctl").args(["-n", name]).output().ok()?;
    if !output.status.success() {
        return None;
    }

    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<usize>()
        .ok()
}

fn human_count(value: usize) -> String {
    let value = value as f64;
    if value >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.1}K", value / 1_000.0)
    } else {
        (value as usize).to_string()
    }
}

fn configured_ws_local_bind_address_count() -> usize {
    std::env::var("KALAMDB_BENCH_WS_LOCAL_BIND_ADDRESSES")
        .ok()
        .map(|raw| raw.split(',').filter(|entry| !entry.trim().is_empty()).count())
        .unwrap_or(0)
}

/// Check the process file descriptor limit.
fn check_fd_limit() -> CheckResult {
    #[cfg(unix)]
    {
        // Try to read the soft limit via getrlimit
        let output = Command::new("sh").arg("-c").arg("ulimit -n").output();
        match output {
            Ok(out) => {
                let val = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if val.eq_ignore_ascii_case("unlimited") {
                    CheckResult::pass(
                        "File descriptors",
                        "unlimited (soft limit is not constraining benchmark sockets)",
                    )
                } else if let Ok(n) = val.parse::<u64>() {
                    if n >= 8192 {
                        CheckResult::pass("File descriptors", format!("{} (>= 8192)", n))
                    } else if n >= 1024 {
                        CheckResult::warn(
                            "File descriptors",
                            format!("{} — recommend >= 8192 (`ulimit -n 65536`)", n),
                        )
                    } else {
                        CheckResult::fail(
                            "File descriptors",
                            format!("{} — too low, run `ulimit -n 65536`", n),
                        )
                    }
                } else {
                    CheckResult::warn("File descriptors", format!("Could not parse: {}", val))
                }
            },
            Err(e) => CheckResult::warn("File descriptors", format!("Could not check: {}", e)),
        }
    }
    #[cfg(not(unix))]
    {
        CheckResult::pass("File descriptors", "N/A (non-Unix)")
    }
}

#[cfg(target_os = "macos")]
const TARGET_KERN_MAXFILES: u64 = 524_288;
#[cfg(target_os = "macos")]
const TARGET_KERN_MAXFILESPERPROC: u64 = 262_144;
#[cfg(target_os = "macos")]
const TARGET_LAUNCHCTL_SOFT: u64 = 262_144;
#[cfg(target_os = "macos")]
const TARGET_LAUNCHCTL_HARD: u64 = 524_288;

fn check_macos_maxfiles_limits() -> CheckResult {
    #[cfg(target_os = "macos")]
    {
        let kern_maxfiles = read_u64_cmd("sysctl", &["-n", "kern.maxfiles"]);
        let kern_maxfilesperproc = read_u64_cmd("sysctl", &["-n", "kern.maxfilesperproc"]);
        let launchctl_limits = read_launchctl_maxfiles();

        let mut issues = Vec::new();

        match kern_maxfiles {
            Ok(v) if v >= TARGET_KERN_MAXFILES => {},
            Ok(v) => issues.push(format!("kern.maxfiles={} (< {})", v, TARGET_KERN_MAXFILES)),
            Err(e) => issues.push(format!("kern.maxfiles check failed: {}", e)),
        }

        match kern_maxfilesperproc {
            Ok(v) if v >= TARGET_KERN_MAXFILESPERPROC => {},
            Ok(v) => issues
                .push(format!("kern.maxfilesperproc={} (< {})", v, TARGET_KERN_MAXFILESPERPROC)),
            Err(e) => issues.push(format!("kern.maxfilesperproc check failed: {}", e)),
        }

        match launchctl_limits {
            Ok((soft, hard_opt)) => {
                if soft < TARGET_LAUNCHCTL_SOFT {
                    issues.push(format!("launchctl soft={} (< {})", soft, TARGET_LAUNCHCTL_SOFT));
                }
                match hard_opt {
                    Some(hard) => {
                        if hard < TARGET_LAUNCHCTL_HARD {
                            issues.push(format!(
                                "launchctl hard={} (< {})",
                                hard, TARGET_LAUNCHCTL_HARD
                            ));
                        }
                    },
                    None => {
                        // "unlimited" hard limit is acceptable.
                    },
                }
            },
            Err(e) => issues.push(format!("launchctl maxfiles check failed: {}", e)),
        }

        if issues.is_empty() {
            CheckResult::pass(
                "Kernel maxfiles",
                format!(
                    "kern.maxfiles >= {}, kern.maxfilesperproc >= {}, launchctl maxfiles >= {}/{}",
                    TARGET_KERN_MAXFILES,
                    TARGET_KERN_MAXFILESPERPROC,
                    TARGET_LAUNCHCTL_SOFT,
                    TARGET_LAUNCHCTL_HARD
                ),
            )
        } else {
            CheckResult::warn(
                "Kernel maxfiles",
                format!(
                    "{}; run: sudo sysctl -w kern.maxfiles={}; sudo sysctl -w kern.maxfilesperproc={}; sudo launchctl limit maxfiles {} {}",
                    issues.join(", "),
                    TARGET_KERN_MAXFILES,
                    TARGET_KERN_MAXFILESPERPROC,
                    TARGET_LAUNCHCTL_SOFT,
                    TARGET_LAUNCHCTL_HARD
                ),
            )
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        CheckResult::pass("Kernel maxfiles", "N/A (non-macOS)")
    }
}

#[cfg(target_os = "macos")]
fn read_u64_cmd(cmd: &str, args: &[&str]) -> Result<u64, String> {
    let output = Command::new(cmd).args(args).output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!("exit {}", output.status));
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    value.parse::<u64>().map_err(|e| format!("parse '{}': {}", value, e))
}

#[cfg(target_os = "macos")]
fn read_launchctl_maxfiles() -> Result<(u64, Option<u64>), String> {
    let output = Command::new("launchctl")
        .args(["limit", "maxfiles"])
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!("exit {}", output.status));
    }

    let line = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        return Err(format!("unexpected output '{}'", line.trim()));
    }

    let soft = parts[1]
        .parse::<u64>()
        .map_err(|e| format!("parse soft '{}': {}", parts[1], e))?;
    let hard = if parts[2].eq_ignore_ascii_case("unlimited") {
        None
    } else {
        Some(
            parts[2]
                .parse::<u64>()
                .map_err(|e| format!("parse hard '{}': {}", parts[2], e))?,
        )
    };

    Ok((soft, hard))
}

/// Check that all configured URLs are reachable and usable for SQL.
async fn check_all_urls_reachable(client: &KalamClient, config: &Config) -> CheckResult {
    match client.validate_sql_on_all_urls("SELECT 1").await {
        Ok(_) => CheckResult::pass(
            "URLs reachable",
            format!("All {} URL(s) responded", config.urls.len()),
        ),
        Err(failures) => {
            let mut detail = format!("{}/{} URL(s) failed", failures.len(), config.urls.len());
            if let Some(first) = failures.first() {
                detail.push_str(&format!(" (e.g. {})", first));
            }
            CheckResult::fail("URLs reachable", detail)
        },
    }
}

/// Check SQL connectivity with SELECT 1.
async fn check_sql_connectivity(client: &KalamClient) -> CheckResult {
    match client.sql_ok("SELECT 1").await {
        Ok(_) => CheckResult::pass("SQL connectivity", "SELECT 1 returned OK"),
        Err(e) => CheckResult::fail("SQL connectivity", format!("Failed: {}", e)),
    }
}

/// Check admin permissions by creating and dropping a test namespace.
async fn check_admin_permissions(client: &KalamClient, config: &Config) -> CheckResult {
    let test_ns = format!("{}_preflight_check", config.namespace);
    match client.sql_ok(&format!("CREATE NAMESPACE IF NOT EXISTS {}", test_ns)).await {
        Ok(_) => {
            let _ = client.sql(&format!("DROP NAMESPACE IF EXISTS {}", test_ns)).await;
            CheckResult::pass("Admin permissions", "Can create/drop namespaces")
        },
        Err(e) => CheckResult::fail("Admin permissions", format!("Cannot create namespace: {}", e)),
    }
}

/// Check for leftover benchmark namespaces.
async fn check_clean_state(client: &KalamClient) -> CheckResult {
    match client.sql_ok("SHOW NAMESPACES").await {
        Ok(resp) => {
            let mut stale = Vec::new();
            if let Some(result) = resp.results.first() {
                if let Some(rows) = &result.rows {
                    for row in rows {
                        if let Some(serde_json::Value::String(ns)) = row.first() {
                            if ns.starts_with("bench_") {
                                stale.push(ns.clone());
                            }
                        }
                    }
                }
            }
            if stale.is_empty() {
                CheckResult::pass("Clean state", "No leftover bench_* namespaces")
            } else {
                CheckResult::warn(
                    "Clean state",
                    format!(
                        "Found {} stale bench_* namespace(s) — they won't interfere (unique run IDs)",
                        stale.len()
                    ),
                )
            }
        },
        Err(e) => CheckResult::warn("Clean state", format!("Could not check: {}", e)),
    }
}

/// Measure user creation speed as a proxy for bcrypt cost.
/// If creating a user takes > 50ms, bcrypt cost is likely too high for benchmarks.
async fn check_bcrypt_cost(client: &KalamClient) -> CheckResult {
    let test_user =
        format!("bench_preflight_bcrypt_test_{}", chrono::Utc::now().timestamp_millis());
    let test_pass = "TestPass123!";

    let start = Instant::now();
    let create_result = client
        .sql(&format!(
            "CREATE USER '{}' WITH PASSWORD '{}' ROLE 'user'",
            test_user, test_pass
        ))
        .await;
    let elapsed = start.elapsed();

    // Always try to clean up
    let _ = client.sql(&format!("DROP USER IF EXISTS '{}'", test_user)).await;

    match create_result {
        Ok(resp) => {
            let ms = elapsed.as_millis();
            if resp.status == "success"
                || resp.error.as_ref().map_or(false, |e| e.message.contains("already exists"))
            {
                if ms < 50 {
                    CheckResult::pass(
                        "bcrypt cost",
                        format!("User creation took {}ms (cost is low ✓)", ms),
                    )
                } else if ms < 200 {
                    CheckResult::warn(
                        "bcrypt cost",
                        format!(
                            "User creation took {}ms — set bcrypt_cost = 4 in server.toml for benchmarks",
                            ms
                        ),
                    )
                } else {
                    CheckResult::fail(
                        "bcrypt cost",
                        format!(
                            "User creation took {}ms — bcrypt_cost is too high, set to 4 in server.toml",
                            ms
                        ),
                    )
                }
            } else {
                let msg = resp.error.map(|e| e.message).unwrap_or_default();
                CheckResult::warn("bcrypt cost", format!("Could not test: {}", msg))
            }
        },
        Err(e) => CheckResult::warn("bcrypt cost", format!("Could not test: {}", e)),
    }
}
