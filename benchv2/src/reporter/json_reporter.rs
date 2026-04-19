use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::config::Config;
use crate::metrics::{BenchmarkReport, BenchmarkResult, ReportConfig, ReportSummary, SystemInfo};

/// Write a JSON report to disk and return the file path.
pub fn write_json_report(
    results: &[BenchmarkResult],
    config: &Config,
    output_dir: &str,
    version: &str,
    system: &SystemInfo,
    wall_clock_duration: Duration,
) -> Result<String, String> {
    fs::create_dir_all(output_dir).map_err(|e| format!("Failed to create output dir: {}", e))?;

    let timestamp = chrono::Utc::now();
    let version_slug = version.replace('.', "-").replace("-alpha", "-a").replace("-beta", "-b");
    let filename = format!("bench-{}-{}.json", timestamp.format("%Y-%m-%d-%H%M%S"), version_slug);
    let path = Path::new(output_dir).join(&filename);

    let passed = results.iter().filter(|r| r.success).count() as u32;
    let failed = results.iter().filter(|r| !r.success).count() as u32;
    let measured_duration_ms: f64 = results.iter().map(|r| r.total_us as f64 / 1000.0).sum();
    let total_duration_ms = wall_clock_duration.as_secs_f64() * 1000.0;

    let report = BenchmarkReport {
        version: version.to_string(),
        server_url: config.urls.join(", "),
        timestamp: timestamp.to_rfc3339(),
        config: ReportConfig {
            iterations: config.iterations,
            warmup: config.warmup,
            concurrency: config.concurrency,
            namespace: config.namespace.clone(),
            max_subscribers: Some(config.max_subscribers),
        },
        system: system.clone(),
        results: results.to_vec(),
        summary: ReportSummary {
            total_benchmarks: results.len() as u32,
            passed,
            failed,
            total_duration_ms,
            measured_duration_ms,
        },
    };

    let json =
        serde_json::to_string_pretty(&report).map_err(|e| format!("Serialize error: {}", e))?;

    fs::write(&path, &json).map_err(|e| format!("Write error: {}", e))?;

    Ok(path.display().to_string())
}
