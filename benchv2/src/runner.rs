use std::time::Instant;

use crate::benchmarks::{all_benchmarks, enabled_in_default_suite, Benchmark};
use crate::client::KalamClient;
use crate::comparison::{self, PreviousRun};
use crate::config::Config;
use crate::metrics::BenchmarkResult;
use crate::verdict;

/// Runs all registered benchmarks according to config, returning results.
pub async fn run_all(
    client: &KalamClient,
    config: &Config,
    previous: Option<&PreviousRun>,
) -> Vec<BenchmarkResult> {
    let benchmarks = all_benchmarks();
    let mut results = Vec::new();

    let mut selected = Vec::new();
    for bench in benchmarks {
        if config.bench.is_empty()
            && config.filter.is_none()
            && !enabled_in_default_suite(bench.name())
        {
            continue;
        }

        if !config.bench.is_empty() {
            let bench_name = bench.name();
            if !config.bench.iter().any(|selected_name| selected_name == bench_name) {
                continue;
            }
        }

        // Apply filter if set
        if let Some(ref filter) = config.filter {
            let name_lower = bench.name().to_lowercase();
            let cat_lower = bench.category().to_lowercase();
            let filter_lower = filter.to_lowercase();
            if !name_lower.contains(&filter_lower) && !cat_lower.contains(&filter_lower) {
                continue;
            }
        }

        selected.push(bench);
    }

    let total = selected.len();
    for (idx, bench) in selected.iter().enumerate() {
        println!("\n[{}/{}] {} — {}", idx + 1, total, bench.name(), bench.description());

        let result = run_single(bench.as_ref(), client, config).await;

        // Verdict
        let v = verdict::evaluate(&result);

        if result.success {
            // Build optional comparison delta
            let delta = previous
                .and_then(|prev| comparison::compare(&result, prev))
                .map(|c| format!("  {}", c.display()))
                .unwrap_or_default();

            println!(
                "  ✅ {} iterations | mean={:.0}µs  p50={:.0}µs  p95={:.0}µs  p99={:.0}µs  ops/s={:.0}  │ {}{}",
                result.iterations,
                result.mean_us,
                result.median_us,
                result.p95_us,
                result.p99_us,
                result.ops_per_sec,
                v.display(),
                delta,
            );
        } else {
            println!(
                "  ❌ FAILED: {}  │ {}",
                result.error.as_deref().unwrap_or("unknown"),
                v.display(),
            );
        }

        results.push(result);
    }

    results
}

/// Run a single benchmark: setup → warmup → timed iterations → teardown.
async fn run_single(
    bench: &dyn Benchmark,
    client: &KalamClient,
    config: &Config,
) -> BenchmarkResult {
    // --- Setup ---
    print!("  Setting up...");
    if let Err(e) = bench.setup(client, config).await {
        return BenchmarkResult::failed(bench.name(), bench.category(), bench.description(), e);
    }
    println!(" done");

    // Scale tests run exactly once with no warmup
    let (warmup_count, iter_count) = if bench.single_run() {
        (0, 1)
    } else {
        (config.warmup, config.iterations)
    };

    // --- Warmup ---
    if warmup_count > 0 {
        print!("  Warmup ({} iterations)...", warmup_count);
        for i in 0..warmup_count {
            if let Err(e) = bench.run(client, config, 10_000 + i).await {
                let _ = bench.teardown(client, config).await;
                return BenchmarkResult::failed(
                    bench.name(),
                    bench.category(),
                    bench.description(),
                    format!("Warmup failed: {}", e),
                );
            }
        }
        println!(" done");
    }

    // --- Timed iterations ---
    print!(
        "  Running {} iteration{}...",
        iter_count,
        if iter_count == 1 { "" } else { "s" }
    );
    let mut durations = Vec::with_capacity(iter_count as usize);
    for i in 0..iter_count {
        let start = Instant::now();
        if let Err(e) = bench.run(client, config, i).await {
            let _ = bench.teardown(client, config).await;
            return BenchmarkResult::failed(
                bench.name(),
                bench.category(),
                bench.description(),
                format!("Iteration {} failed: {}", i, e),
            );
        }
        durations.push(start.elapsed());
    }
    println!(" done");

    // --- Teardown ---
    print!("  Tearing down...");
    let _ = bench.teardown(client, config).await;
    println!(" done");

    BenchmarkResult::from_durations(bench.name(), bench.category(), bench.description(), durations)
}
