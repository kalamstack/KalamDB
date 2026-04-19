use std::fs;
use std::path::Path;
use std::time::Instant;

use clap::Parser;

mod benchmarks;
mod client;
mod comparison;
mod config;
mod metrics;
mod preflight;
mod reporter;
mod runner;
mod system_info;
mod verdict;

use client::KalamClient;
use config::Config;
use reporter::html_reporter;
use reporter::json_reporter;
use system_info::collect_system_info;

#[tokio::main]
async fn main() {
    let mut config = Config::parse();
    let kalamdb_version = load_kalamdb_version();
    let system = collect_system_info();

    if config.list_benches {
        println!("Available benchmarks:");
        for bench in benchmarks::all_benchmarks() {
            println!("  {:<28} {}", bench.name(), bench.description());
        }
        return;
    }

    // Append a short timestamp to the namespace to ensure a clean run every time.
    // This avoids PK conflicts from tables that weren't fully dropped.
    let run_id = chrono::Utc::now().format("%H%M%S").to_string();
    config.namespace = format!("{}_{}", config.namespace, run_id);

    println!("╔════════════════════════════════════════════════╗");
    println!("║   KalamDB Benchmark Suite v{:<18}║", kalamdb_version);
    println!("╚════════════════════════════════════════════════╝");
    println!();
    println!("  Servers:     {}", config.urls.join(", "));
    println!("  Namespace:   {}", config.namespace);
    println!("  Iterations:  {}", config.iterations);
    println!("  Warmup:      {}", config.warmup);
    println!("  Concurrency: {}", config.concurrency);
    println!("  Max Subs:    {}", config.max_subscribers);
    if let Some(ref f) = config.filter {
        println!("  Filter:      {}", f);
    }
    if !config.bench.is_empty() {
        println!("  Benches:     {}", config.bench.join(", "));
    }
    println!("  Machine:     {} ({})", system.machine_model, system.hostname);
    println!(
        "  CPU:         {} ({} logical / {} physical)",
        system.cpu_model, system.cpu_logical_cores, system.cpu_physical_cores
    );
    println!(
        "  Memory:      {} total, {} available ({:.1}% used)",
        format_bytes(system.total_memory_bytes),
        format_bytes(system.available_memory_bytes),
        system.used_memory_percent
    );
    println!(
        "  Platform:    {} {} ({})",
        system.os_name, system.os_version, system.architecture
    );
    println!();

    // Build client (login to get Bearer token)
    print!("Authenticating... ");
    let client = match KalamClient::login(&config.urls, &config.user, &config.password).await {
        Ok(c) => {
            println!("OK");
            c
        },
        Err(e) => {
            eprintln!("FAILED");
            eprintln!("  {}", e);
            eprintln!(
                "Make sure all servers are running at [{}] and credentials are correct.",
                config.urls.join(", ")
            );
            std::process::exit(1);
        },
    };

    println!();

    // ── Pre-flight checks ──────────────────────────────────────────
    if !preflight::run_preflight_checks(&client, &config).await {
        std::process::exit(1);
    }

    // ── Load previous run for comparison ───────────────────────────
    let previous = comparison::load_previous_run(&config.output_dir);
    if let Some(ref prev) = previous {
        println!("  📊 Comparing against previous run: {}\n", prev.timestamp);
    } else {
        println!("  📊 No previous run found — skipping comparison\n");
    }

    // Create fresh namespace for this run
    let _ = run_sql_ok_on_all_urls(
        &client,
        &format!("CREATE NAMESPACE IF NOT EXISTS {}", config.namespace),
    )
    .await;

    // Run benchmarks
    let overall_start = Instant::now();
    let results = runner::run_all(&client, &config, previous.as_ref()).await;

    if results.is_empty() {
        eprintln!("No benchmarks matched selection. Use --list-benches to see valid names.");
        std::process::exit(1);
    }
    let overall_elapsed = overall_start.elapsed();
    // ── Summary with verdict breakdown ─────────────────────────────
    let passed = results.iter().filter(|r| r.success).count();
    let failed = results.iter().filter(|r| !r.success).count();
    let excellent = results
        .iter()
        .filter(|r| verdict::evaluate(r) == verdict::Verdict::Excellent)
        .count();
    let acceptable = results
        .iter()
        .filter(|r| verdict::evaluate(r) == verdict::Verdict::Acceptable)
        .count();
    let slow = results
        .iter()
        .filter(|r| verdict::evaluate(r) == verdict::Verdict::Slow)
        .count();

    println!("\n════════════════════════════════════════════════");
    println!(
        "  Completed {} benchmarks in {:.2}s",
        results.len(),
        overall_elapsed.as_secs_f64()
    );
    println!("  Passed: {}  Failed: {}", passed, failed);
    println!(
        "  Verdicts: \x1b[32m🟢 {} excellent\x1b[0m  \x1b[33m🟡 {} acceptable\x1b[0m  \x1b[31m🔴 {} slow\x1b[0m",
        excellent, acceptable, slow
    );
    println!("════════════════════════════════════════════════\n");

    if failed == 0 {
        match json_reporter::write_json_report(
            &results,
            &config,
            &config.output_dir,
            &kalamdb_version,
            &system,
            overall_elapsed,
        ) {
            Ok(path) => println!("  JSON report: {}", path),
            Err(e) => eprintln!("  Failed to write JSON report: {}", e),
        }

        match html_reporter::write_html_report(
            &results,
            &config,
            &config.output_dir,
            &kalamdb_version,
            previous.as_ref(),
            &system,
            overall_elapsed,
        ) {
            Ok(path) => println!("  HTML report: {}", path),
            Err(e) => eprintln!("  Failed to write HTML report: {}", e),
        }
    } else {
        println!("  Skipping report generation because {} benchmark(s) failed.", failed);
    }

    // Clean up the benchmark namespace
    let _ =
        run_sql_ok_on_all_urls(&client, &format!("DROP NAMESPACE IF EXISTS {}", config.namespace))
            .await;

    println!();
    if failed > 0 {
        std::process::exit(1);
    }
}

fn load_kalamdb_version() -> String {
    let fallback = env!("CARGO_PKG_VERSION").to_string();
    let root_manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.toml");

    let content = match fs::read_to_string(root_manifest) {
        Ok(value) => value,
        Err(_) => return fallback,
    };

    parse_workspace_version(&content).unwrap_or(fallback)
}

async fn run_sql_ok_on_all_urls(client: &KalamClient, sql: &str) -> Result<(), Vec<String>> {
    let mut failures = Vec::new();

    for url in client.urls() {
        if let Err(err) = client.sql_ok_on_url(&url, sql).await {
            failures.push(format!("{} -> {}", url, err));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

fn parse_workspace_version(manifest_content: &str) -> Option<String> {
    let mut in_workspace_package = false;

    for line in manifest_content.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            in_workspace_package = trimmed == "[workspace.package]";
            continue;
        }

        if in_workspace_package && trimmed.starts_with("version") {
            let value = trimmed.split_once('=')?.1.trim().trim_matches('"').to_string();
            if !value.is_empty() {
                return Some(value);
            }
        }
    }

    None
}

fn format_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;

    let value = bytes as f64;
    if value >= GIB {
        format!("{:.2} GiB", value / GIB)
    } else {
        format!("{:.1} MiB", value / MIB)
    }
}
