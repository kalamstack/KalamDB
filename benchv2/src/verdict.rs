use crate::metrics::BenchmarkResult;

/// Verdict level for a benchmark result based on its mean latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Well within expected range
    Excellent,
    /// Acceptable but could be better
    Acceptable,
    /// Slower than expected
    Slow,
    /// Benchmark failed — no performance verdict
    Failed,
}

impl Verdict {
    /// ANSI-colored icon + label for terminal output.
    pub fn display(&self) -> &str {
        match self {
            Verdict::Excellent => "\x1b[32m🟢 excellent\x1b[0m",
            Verdict::Acceptable => "\x1b[33m🟡 acceptable\x1b[0m",
            Verdict::Slow => "\x1b[31m🔴 slow\x1b[0m",
            Verdict::Failed => "\x1b[31m⛔ failed\x1b[0m",
        }
    }

    /// Short label (no ANSI) for reports.
    pub fn label(&self) -> &str {
        match self {
            Verdict::Excellent => "excellent",
            Verdict::Acceptable => "acceptable",
            Verdict::Slow => "slow",
            Verdict::Failed => "failed",
        }
    }

    /// CSS class for HTML reports.
    pub fn css_class(&self) -> &str {
        match self {
            Verdict::Excellent => "verdict-excellent",
            Verdict::Acceptable => "verdict-acceptable",
            Verdict::Slow => "verdict-slow",
            Verdict::Failed => "verdict-failed",
        }
    }

    /// HTML badge for the HTML reporter.
    pub fn html_badge(&self) -> &str {
        match self {
            Verdict::Excellent => "<span class=\"verdict verdict-excellent\">🟢 Excellent</span>",
            Verdict::Acceptable => {
                "<span class=\"verdict verdict-acceptable\">🟡 Acceptable</span>"
            },
            Verdict::Slow => "<span class=\"verdict verdict-slow\">🔴 Slow</span>",
            Verdict::Failed => "<span class=\"verdict verdict-failed\">⛔ Failed</span>",
        }
    }
}

/// Threshold definition: (excellent_ceil_us, acceptable_ceil_us).
/// - mean_us <= excellent_ceil  →  Excellent
/// - mean_us <= acceptable_ceil →  Acceptable
/// - mean_us >  acceptable_ceil →  Slow
struct Threshold {
    excellent_us: f64,
    acceptable_us: f64,
}

impl Threshold {
    const fn new(excellent_ms: f64, acceptable_ms: f64) -> Self {
        Self {
            excellent_us: excellent_ms * 1000.0,
            acceptable_us: acceptable_ms * 1000.0,
        }
    }
}

/// Get the performance threshold for a specific benchmark.
/// First tries an exact name match, then falls back to category-based defaults.
fn threshold_for(name: &str, category: &str) -> Threshold {
    // Per-benchmark overrides (name-based)
    match name {
        // DDL — inherently slower due to metadata ops
        "create_table" => Threshold::new(100.0, 500.0),
        "drop_table" => Threshold::new(100.0, 500.0),
        "alter_table" => Threshold::new(100.0, 500.0),

        // Single-row DML — should be sub-2ms
        "single_insert" => Threshold::new(2.0, 10.0),
        "single_update" => Threshold::new(2.0, 10.0),
        "single_delete" => Threshold::new(2.0, 10.0),
        "point_lookup" => Threshold::new(2.0, 5.0),

        // Bulk operations
        "bulk_insert" => Threshold::new(50.0, 200.0),
        "bulk_delete" => Threshold::new(50.0, 200.0),

        // Selects
        "select_all" => Threshold::new(3.0, 15.0),
        "select_by_filter" => Threshold::new(2.0, 10.0),
        "select_count" => Threshold::new(2.0, 10.0),
        "select_order_by" => Threshold::new(3.0, 15.0),

        // Aggregates and joins
        "aggregate_query" => Threshold::new(5.0, 20.0),
        "multi_table_join" => Threshold::new(10.0, 50.0),

        // Large/wide payloads
        "large_payload_insert" => Threshold::new(5.0, 20.0),
        "wide_column_insert" => Threshold::new(5.0, 20.0),

        // Sequential CRUD (multi-op)
        "sequential_crud" => Threshold::new(5.0, 20.0),

        // Concurrent benchmarks
        "concurrent_insert" => Threshold::new(10.0, 50.0),
        "concurrent_select" => Threshold::new(10.0, 50.0),
        "concurrent_update" => Threshold::new(10.0, 50.0),
        "concurrent_mixed_dml" => Threshold::new(15.0, 50.0),
        "namespace_isolation" => Threshold::new(10.0, 50.0),

        // Subscription benchmarks
        "subscribe_initial_load" => Threshold::new(20.0, 100.0),
        "subscribe_change_latency" => Threshold::new(30.0, 100.0),
        "reconnect_subscribe" => Threshold::new(20.0, 80.0),

        // Flushed storage
        "flushed_parquet_query" => Threshold::new(50.0, 200.0),

        // User management
        "create_user" => Threshold::new(5.0, 50.0),
        "drop_user" => Threshold::new(5.0, 50.0),

        // Load/stress tests (single_run, measured differently)
        "load_subscriber" | "load_publisher" | "load_consumer" | "load_mixed_rw"
        | "connection_storm" | "wide_fanout_query" => Threshold::new(5000.0, 30000.0),

        "sql_1k_concurrent" | "sql_1k_users" => Threshold::new(5000.0, 20000.0),
        "subscriber_scale" => Threshold::new(60000.0, 300000.0),

        // Fallback: use category
        _ => threshold_for_category(category),
    }
}

/// Category-based fallback thresholds.
fn threshold_for_category(category: &str) -> Threshold {
    match category {
        "DDL" => Threshold::new(100.0, 500.0),
        "Insert" => Threshold::new(3.0, 15.0),
        "Select" => Threshold::new(3.0, 15.0),
        "Update" => Threshold::new(3.0, 15.0),
        "Delete" => Threshold::new(3.0, 15.0),
        "Concurrent" => Threshold::new(15.0, 50.0),
        "Subscribe" => Threshold::new(30.0, 100.0),
        "Storage" => Threshold::new(50.0, 200.0),
        "Load" => Threshold::new(5000.0, 30000.0),
        "Scale" => Threshold::new(60000.0, 300000.0),
        "User" => Threshold::new(5.0, 50.0),
        _ => Threshold::new(10.0, 100.0),
    }
}

/// Compute the verdict for a benchmark result.
pub fn evaluate(result: &BenchmarkResult) -> Verdict {
    if !result.success {
        return Verdict::Failed;
    }
    let t = threshold_for(&result.name, &result.category);
    if result.mean_us <= t.excellent_us {
        Verdict::Excellent
    } else if result.mean_us <= t.acceptable_us {
        Verdict::Acceptable
    } else {
        Verdict::Slow
    }
}
