use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::metrics::{BenchmarkReport, BenchmarkResult};

/// Previous run data loaded from the most recent JSON report.
#[derive(Debug)]
pub struct PreviousRun {
    pub timestamp: String,
    pub results: HashMap<String, PreviousBenchmark>,
}

/// A single benchmark's previous metrics.
#[derive(Debug, Clone)]
pub struct PreviousBenchmark {
    pub mean_us: f64,
    pub median_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub ops_per_sec: f64,
}

/// Delta between current and previous run for a single benchmark.
#[derive(Debug)]
pub struct Comparison {
    /// Percentage change in mean latency (negative = faster = better)
    pub mean_pct: f64,
    /// Percentage change in ops/sec (positive = better)
    pub ops_pct: f64,
    /// Previous mean for display
    pub prev_mean_us: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonTrend {
    Faster,
    Same,
    Slower,
}

impl Comparison {
    pub fn trend(&self) -> ComparisonTrend {
        let abs_pct = self.mean_pct.abs();

        if abs_pct < 3.0 {
            ComparisonTrend::Same
        } else if self.mean_pct < 0.0 {
            ComparisonTrend::Faster
        } else {
            ComparisonTrend::Slower
        }
    }

    /// Format the comparison as a terminal-printable delta string.
    /// Shows the change in mean latency relative to previous run.
    pub fn display(&self) -> String {
        let abs_pct = self.mean_pct.abs();

        match self.trend() {
            ComparisonTrend::Same => {
                format!("\x1b[90m~ {:.0}µs prior\x1b[0m", self.prev_mean_us)
            }
            ComparisonTrend::Faster => {
                format!(
                    "\x1b[32m↑{:.0}% faster\x1b[0m \x1b[90m(was {:.0}µs)\x1b[0m",
                    abs_pct, self.prev_mean_us
                )
            }
            ComparisonTrend::Slower => {
                format!(
                    "\x1b[31m↓{:.0}% slower\x1b[0m \x1b[90m(was {:.0}µs)\x1b[0m",
                    abs_pct, self.prev_mean_us
                )
            }
        }
    }

    /// HTML version of the comparison delta.
    pub fn html(&self) -> String {
        let abs_pct = self.mean_pct.abs();
        let tooltip = format!("previous mean: {:.0}µs", self.prev_mean_us);

        match self.trend() {
            ComparisonTrend::Same => format!(
                "<span class=\"delta delta-same\" title=\"{}\">~ {:.0}µs prior</span>",
                tooltip, self.prev_mean_us
            ),
            ComparisonTrend::Faster => format!(
                "<span class=\"delta delta-faster\" title=\"{}\">↑{:.0}% faster</span>",
                tooltip, abs_pct
            ),
            ComparisonTrend::Slower => format!(
                "<span class=\"delta delta-slower\" title=\"{}\">↓{:.0}% slower</span>",
                tooltip, abs_pct
            ),
        }
    }
}

/// Load the most recent benchmark report from the results directory.
/// Returns None if no previous reports exist or parsing fails.
pub fn load_previous_run(output_dir: &str) -> Option<PreviousRun> {
    let dir = Path::new(output_dir);
    if !dir.exists() || !dir.is_dir() {
        return None;
    }

    // Find all JSON report files, sorted by name (which includes timestamp)
    let mut json_files: Vec<_> = fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().extension().map_or(false, |ext| ext == "json")
                && e.path()
                    .file_name()
                    .map_or(false, |n| n.to_string_lossy().starts_with("bench-"))
        })
        .collect();

    if json_files.is_empty() {
        return None;
    }

    // Sort by filename descending — most recent first
    json_files.sort_by(|a, b| b.file_name().cmp(&a.file_name()));

    let latest = &json_files[0];
    let content = fs::read_to_string(latest.path()).ok()?;
    let report: BenchmarkReport = serde_json::from_str(&content).ok()?;

    let mut results = HashMap::new();
    for r in &report.results {
        results.insert(
            r.name.clone(),
            PreviousBenchmark {
                mean_us: r.mean_us,
                median_us: r.median_us,
                p95_us: r.p95_us,
                p99_us: r.p99_us,
                ops_per_sec: r.ops_per_sec,
            },
        );
    }

    Some(PreviousRun {
        timestamp: report.timestamp,
        results,
    })
}

/// Compare a current result against the previous run.
pub fn compare(result: &BenchmarkResult, previous: &PreviousRun) -> Option<Comparison> {
    let prev = previous.results.get(&result.name)?;
    if prev.mean_us <= 0.0 || !result.success {
        return None;
    }

    let mean_pct = ((result.mean_us - prev.mean_us) / prev.mean_us) * 100.0;
    let ops_pct = if prev.ops_per_sec > 0.0 {
        ((result.ops_per_sec - prev.ops_per_sec) / prev.ops_per_sec) * 100.0
    } else {
        0.0
    };

    Some(Comparison {
        mean_pct,
        ops_pct,
        prev_mean_us: prev.mean_us,
    })
}
