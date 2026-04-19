use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Additional benchmark-specific detail rendered in reports.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchmarkDetail {
    /// Short label for the detail value.
    pub label: String,
    /// Human-readable value.
    pub value: String,
}

impl BenchmarkDetail {
    /// Create a benchmark detail entry.
    pub fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
        }
    }
}

/// Statistics for a single benchmark operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Name of the benchmark (e.g. "single_insert", "select_by_pk")
    pub name: String,
    /// Category grouping (e.g. "Insert", "Select", "Update", "Delete")
    pub category: String,
    /// Human-readable description
    pub description: String,
    /// Full description shown in report tooltips.
    #[serde(default)]
    pub full_description: String,
    /// Benchmark-specific details shown alongside the description.
    #[serde(default)]
    pub details: Vec<BenchmarkDetail>,
    /// Number of iterations measured (excluding warmup)
    pub iterations: u32,
    /// Individual iteration durations in microseconds
    #[serde(skip)]
    pub raw_durations_us: Vec<u64>,
    /// Total wall-clock time in microseconds
    pub total_us: u64,
    /// Mean latency in microseconds
    pub mean_us: f64,
    /// Median (p50) latency in microseconds
    pub median_us: f64,
    /// p95 latency in microseconds
    pub p95_us: f64,
    /// p99 latency in microseconds
    pub p99_us: f64,
    /// Minimum latency in microseconds
    pub min_us: f64,
    /// Maximum latency in microseconds
    pub max_us: f64,
    /// Standard deviation in microseconds
    pub stddev_us: f64,
    /// Operations per second (throughput)
    pub ops_per_sec: f64,
    /// Whether the benchmark succeeded
    pub success: bool,
    /// Error message if the benchmark failed
    pub error: Option<String>,
}

impl BenchmarkResult {
    /// Create a new result from a list of duration measurements.
    pub fn from_durations(
        name: &str,
        category: &str,
        description: &str,
        durations: Vec<Duration>,
    ) -> Self {
        let mut us_values: Vec<u64> = durations.iter().map(|d| d.as_micros() as u64).collect();
        us_values.sort();

        let n = us_values.len() as f64;
        let total_us: u64 = us_values.iter().sum();
        let mean_us = if n > 0.0 { total_us as f64 / n } else { 0.0 };

        let median_us = percentile(&us_values, 50.0);
        let p95_us = percentile(&us_values, 95.0);
        let p99_us = percentile(&us_values, 99.0);
        let min_us = us_values.first().copied().unwrap_or(0) as f64;
        let max_us = us_values.last().copied().unwrap_or(0) as f64;

        let variance = if n > 1.0 {
            us_values
                .iter()
                .map(|&v| {
                    let diff = v as f64 - mean_us;
                    diff * diff
                })
                .sum::<f64>()
                / (n - 1.0)
        } else {
            0.0
        };
        let stddev_us = variance.sqrt();

        let total_secs = total_us as f64 / 1_000_000.0;
        let ops_per_sec = if total_secs > 0.0 {
            n / total_secs
        } else {
            0.0
        };

        Self {
            name: name.to_string(),
            category: category.to_string(),
            description: description.to_string(),
            full_description: description.to_string(),
            details: Vec::new(),
            iterations: us_values.len() as u32,
            raw_durations_us: us_values,
            total_us,
            mean_us,
            median_us,
            p95_us,
            p99_us,
            min_us,
            max_us,
            stddev_us,
            ops_per_sec,
            success: true,
            error: None,
        }
    }

    /// Create a failed result.
    pub fn failed(name: &str, category: &str, description: &str, error: String) -> Self {
        Self {
            name: name.to_string(),
            category: category.to_string(),
            description: description.to_string(),
            full_description: description.to_string(),
            details: Vec::new(),
            iterations: 0,
            raw_durations_us: vec![],
            total_us: 0,
            mean_us: 0.0,
            median_us: 0.0,
            p95_us: 0.0,
            p99_us: 0.0,
            min_us: 0.0,
            max_us: 0.0,
            stddev_us: 0.0,
            ops_per_sec: 0.0,
            success: false,
            error: Some(error),
        }
    }

    /// Attach richer report context to a benchmark result.
    #[must_use]
    pub fn with_report_context(
        mut self,
        full_description: impl Into<String>,
        details: Vec<BenchmarkDetail>,
    ) -> Self {
        let full_description = full_description.into();
        if !full_description.is_empty() {
            self.full_description = full_description;
        }
        self.details = details;
        self
    }
}

/// Compute the k-th percentile from a sorted slice of u64 values.
fn percentile(sorted: &[u64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0] as f64;
    }
    let idx = (pct / 100.0) * (sorted.len() - 1) as f64;
    let lower = idx.floor() as usize;
    let upper = idx.ceil() as usize;
    let frac = idx - lower as f64;
    sorted[lower] as f64 * (1.0 - frac) + sorted[upper] as f64 * frac
}

/// Full benchmark report containing all results + metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub version: String,
    pub server_url: String,
    pub timestamp: String,
    pub config: ReportConfig,
    #[serde(default)]
    pub system: SystemInfo,
    pub results: Vec<BenchmarkResult>,
    pub summary: ReportSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SystemInfo {
    pub hostname: String,
    pub machine_model: String,
    pub os_name: String,
    pub os_version: String,
    pub kernel_version: String,
    pub architecture: String,
    pub cpu_model: String,
    pub cpu_logical_cores: usize,
    pub cpu_physical_cores: usize,
    pub total_memory_bytes: u64,
    pub available_memory_bytes: u64,
    pub used_memory_bytes: u64,
    pub used_memory_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportConfig {
    pub iterations: u32,
    pub warmup: u32,
    pub concurrency: u32,
    pub namespace: String,
    #[serde(default)]
    pub max_subscribers: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSummary {
    pub total_benchmarks: u32,
    pub passed: u32,
    pub failed: u32,
    /// End-to-end wall-clock duration for the report run.
    pub total_duration_ms: f64,
    /// Sum of measured benchmark iteration durations.
    #[serde(default)]
    pub measured_duration_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_result_attaches_report_context() {
        let result = BenchmarkResult::from_durations(
            "subscriber_scale",
            "Scale",
            "short description",
            vec![Duration::from_micros(3_700_434)],
        )
        .with_report_context(
            "full description",
            vec![BenchmarkDetail::new("Batch/Wave", "1.0K / 500")],
        );

        assert_eq!(result.full_description, "full description");
        assert_eq!(result.details.len(), 1);
        assert_eq!(result.total_us, 3_700_434);
    }
}
