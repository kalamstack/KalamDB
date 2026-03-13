use std::sync::atomic::{AtomicUsize, Ordering};
use sysinfo::System;

/// Global counter for active WebSocket sessions
/// This is updated by kalamdb-api when sessions start/stop
static ACTIVE_WEBSOCKET_SESSIONS: AtomicUsize = AtomicUsize::new(0);

/// Increment the active WebSocket session count
pub fn increment_websocket_sessions() -> usize {
    ACTIVE_WEBSOCKET_SESSIONS.fetch_add(1, Ordering::SeqCst) + 1
}

/// Decrement the active WebSocket session count
pub fn decrement_websocket_sessions() -> usize {
    ACTIVE_WEBSOCKET_SESSIONS.fetch_sub(1, Ordering::SeqCst) - 1
}

/// Get the current active WebSocket session count
pub fn get_websocket_session_count() -> usize {
    ACTIVE_WEBSOCKET_SESSIONS.load(Ordering::SeqCst)
}

/// Health metrics snapshot
#[derive(Debug, Clone)]
pub struct HealthMetrics {
    pub memory_mb: Option<u64>,
    pub cpu_usage: Option<f32>,
    pub open_files: usize,
    pub open_file_breakdown: Option<OpenFileBreakdown>,
    pub namespace_count: usize,
    pub table_count: usize,
    pub storage_partition_count: Option<usize>,
    pub subscription_count: usize,
    pub connection_count: usize,
    pub ws_session_count: usize,
    pub jobs_running: usize,
    pub jobs_queued: usize,
    pub jobs_failed: usize,
    pub jobs_total: usize,
}

/// Aggregated counts for health metrics.
#[derive(Debug, Clone, Copy)]
pub struct HealthCounts {
    pub namespace_count: usize,
    pub table_count: usize,
    pub storage_partition_count: Option<usize>,
    pub subscription_count: usize,
    pub connection_count: usize,
    pub jobs_running: usize,
    pub jobs_queued: usize,
    pub jobs_failed: usize,
    pub jobs_total: usize,
}

/// Descriptor class breakdown captured from `lsof`.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenFileBreakdown {
    pub total: usize,
    pub regular: usize,
    pub directories: usize,
    pub kqueue: usize,
    pub unix: usize,
    pub ipv4: usize,
    pub other: usize,
}

/// Monitor for system health and job statistics
pub struct HealthMonitor;

impl HealthMonitor {
    /// Collect open file metrics without refreshing process CPU/memory stats.
    pub fn collect_open_file_metrics() -> (usize, Option<OpenFileBreakdown>) {
        #[cfg(unix)]
        let open_file_breakdown = Self::collect_open_file_breakdown();
        #[cfg(not(unix))]
        let open_file_breakdown = None;

        let open_files = open_file_breakdown.map(|breakdown| breakdown.total).unwrap_or(0);
        (open_files, open_file_breakdown)
    }

    /// Collect system health metrics
    ///
    /// Returns a structured snapshot of current health metrics.
    /// This is a low-level collector that doesn't log - consumers can log or report metrics as needed.
    pub fn collect_system_metrics() -> (Option<u64>, Option<f32>, usize, Option<OpenFileBreakdown>)
    {
        let mut sys = System::new_all();
        sys.refresh_all();

        // Get process info
        let pid = match sysinfo::get_current_pid() {
            Ok(pid) => pid,
            Err(e) => {
                log::warn!("Failed to get current process ID for health metrics: {}", e);
                return (None, None, 0, None);
            },
        };

        let process = sys.process(pid);

        let (open_files, open_file_breakdown) = Self::collect_open_file_metrics();

        if let Some(proc) = process {
            let memory_mb = proc.memory() / 1024 / 1024;
            let cpu_usage = proc.cpu_usage();
            (Some(memory_mb), Some(cpu_usage), open_files, open_file_breakdown)
        } else {
            (None, None, open_files, open_file_breakdown)
        }
    }

    /// Build complete health metrics snapshot
    pub fn build_metrics(
        memory_mb: Option<u64>,
        cpu_usage: Option<f32>,
        open_files: usize,
        open_file_breakdown: Option<OpenFileBreakdown>,
        counts: HealthCounts,
    ) -> HealthMetrics {
        HealthMetrics {
            memory_mb,
            cpu_usage,
            open_files,
            open_file_breakdown,
            namespace_count: counts.namespace_count,
            table_count: counts.table_count,
            storage_partition_count: counts.storage_partition_count,
            subscription_count: counts.subscription_count,
            connection_count: counts.connection_count,
            ws_session_count: get_websocket_session_count(),
            jobs_running: counts.jobs_running,
            jobs_queued: counts.jobs_queued,
            jobs_failed: counts.jobs_failed,
            jobs_total: counts.jobs_total,
        }
    }

    /// Log health metrics to debug output
    pub fn log_metrics(metrics: &HealthMetrics) {
        let open_files_text = Self::format_open_files(metrics);
        let storage_partitions = metrics
            .storage_partition_count
            .map(|count| count.to_string())
            .unwrap_or_else(|| "n/a".to_string());

        if let (Some(memory_mb), Some(cpu_usage)) = (metrics.memory_mb, metrics.cpu_usage) {
            log::debug!(
                "Health metrics: Memory: {} MB | CPU: {:.2}% | Open Files: {} | Storage Partitions: {} | Namespaces: {} | Tables: {} | Subscriptions: {} ({} connections, {} ws sessions) | Jobs: {} running, {} queued, {} failed (total: {})",
                memory_mb,
                cpu_usage,
                open_files_text,
                storage_partitions,
                metrics.namespace_count,
                metrics.table_count,
                metrics.subscription_count,
                metrics.connection_count,
                metrics.ws_session_count,
                metrics.jobs_running,
                metrics.jobs_queued,
                metrics.jobs_failed,
                metrics.jobs_total
            );
        } else {
            log::debug!(
                "Health metrics: Open Files: {} | Storage Partitions: {} | Namespaces: {} | Tables: {} | Subscriptions: {} ({} connections, {} ws sessions) | Jobs: {} running, {} queued, {} failed (total: {})",
                open_files_text,
                storage_partitions,
                metrics.namespace_count,
                metrics.table_count,
                metrics.subscription_count,
                metrics.connection_count,
                metrics.ws_session_count,
                metrics.jobs_running,
                metrics.jobs_queued,
                metrics.jobs_failed,
                metrics.jobs_total
            );
        }
    }

    /// Format a concise health log line from system.stats key/value pairs.
    pub fn format_log_from_pairs(metrics: &[(String, String)]) -> String {
        let metric_map: std::collections::HashMap<&str, &str> =
            metrics.iter().map(|(name, value)| (name.as_str(), value.as_str())).collect();

        let open_files_text = Self::format_open_files_from_metric_map(&metric_map);
        let storage_partitions =
            metric_map.get("storage_partition_count").copied().unwrap_or("n/a");
        let namespaces = metric_map.get("total_namespaces").copied().unwrap_or("n/a");
        let tables = metric_map.get("total_tables").copied().unwrap_or("n/a");
        let subscriptions = metric_map.get("active_subscriptions").copied().unwrap_or("n/a");
        let connections = metric_map.get("active_connections").copied().unwrap_or("n/a");
        let ws_sessions = metric_map.get("websocket_sessions").copied().unwrap_or("n/a");
        let jobs_running = metric_map.get("jobs_running").copied().unwrap_or("n/a");
        let jobs_queued = metric_map.get("jobs_queued").copied().unwrap_or("n/a");
        let jobs_failed = metric_map.get("jobs_failed").copied().unwrap_or("n/a");
        let jobs_total = metric_map.get("total_jobs").copied().unwrap_or("n/a");

        let mut segments = Vec::new();

        if let Some(memory_mb) = metric_map.get("memory_usage_mb").copied() {
            segments.push(format!("Memory: {} MB", memory_mb));
        }
        if let Some(cpu_usage) = metric_map.get("cpu_usage_percent").copied() {
            segments.push(format!("CPU: {}%", cpu_usage));
        }

        segments.push(format!("Open Files: {}", open_files_text));
        segments.push(format!("Storage Partitions: {}", storage_partitions));
        segments.push(format!("Namespaces: {}", namespaces));
        segments.push(format!("Tables: {}", tables));
        segments.push(format!(
            "Subscriptions: {} ({} connections, {} ws sessions)",
            subscriptions, connections, ws_sessions
        ));
        segments.push(format!(
            "Jobs: {} running, {} queued, {} failed (total: {})",
            jobs_running, jobs_queued, jobs_failed, jobs_total
        ));

        let schema_cache_size = metric_map.get("schema_cache_size").copied();
        let schema_cache_total = metric_map.get("schema_cache_total_entries").copied();
        let plan_cache_size = metric_map.get("plan_cache_size").copied();
        let topic_cache_topics = metric_map.get("topic_cache_topic_count").copied();
        let topic_cache_routes = metric_map.get("topic_cache_total_routes").copied();
        let interned_strings = metric_map.get("string_interner_unique_strings").copied();

        let mut cache_parts = Vec::new();
        if let Some(schema_size) = schema_cache_size {
            if let Some(schema_total) = schema_cache_total {
                cache_parts.push(format!("Schema {} latest/{} total", schema_size, schema_total));
            } else {
                cache_parts.push(format!("Schema {}", schema_size));
            }
        }
        if let Some(plan_size) = plan_cache_size {
            cache_parts.push(format!("Plan {}", plan_size));
        }
        if let Some(topic_topics) = topic_cache_topics {
            if let Some(topic_routes) = topic_cache_routes {
                cache_parts.push(format!("Topics {} / {} routes", topic_topics, topic_routes));
            } else {
                cache_parts.push(format!("Topics {}", topic_topics));
            }
        }
        if let Some(interned) = interned_strings {
            cache_parts.push(format!("Strings {}", interned));
        }
        if !cache_parts.is_empty() {
            segments.push(format!("Caches: {}", cache_parts.join(", ")));
        }

        format!("Health metrics: {}", segments.join(" | "))
    }

    /// Log health metrics that were sourced from system.stats rows.
    pub fn log_system_stats(metrics: &[(String, String)]) {
        log::debug!("{}", Self::format_log_from_pairs(metrics));
    }

    fn format_open_files(metrics: &HealthMetrics) -> String {
        if let Some(breakdown) = metrics.open_file_breakdown {
            format!(
                "{} total ({} reg, {} dir, {} kqueue, {} unix, {} ipv4, {} other)",
                breakdown.total,
                breakdown.regular,
                breakdown.directories,
                breakdown.kqueue,
                breakdown.unix,
                breakdown.ipv4,
                breakdown.other
            )
        } else {
            metrics.open_files.to_string()
        }
    }

    fn format_open_files_from_metric_map(
        metric_map: &std::collections::HashMap<&str, &str>,
    ) -> String {
        let total = metric_map.get("open_files_total").copied();
        let regular = metric_map.get("open_files_regular").copied();
        let directories = metric_map.get("open_files_directories").copied();
        let kqueue = metric_map.get("open_files_kqueue").copied();
        let unix = metric_map.get("open_files_unix").copied();
        let ipv4 = metric_map.get("open_files_ipv4").copied();
        let other = metric_map.get("open_files_other").copied();

        match (total, regular, directories, kqueue, unix, ipv4, other) {
            (
                Some(total),
                Some(regular),
                Some(directories),
                Some(kqueue),
                Some(unix),
                Some(ipv4),
                Some(other),
            ) => format!(
                "{} total ({} reg, {} dir, {} kqueue, {} unix, {} ipv4, {} other)",
                total, regular, directories, kqueue, unix, ipv4, other
            ),
            (Some(total), ..) => total.to_string(),
            _ => "n/a".to_string(),
        }
    }

    /// Count open file descriptors for the current process (Unix only)
    #[cfg(unix)]
    fn collect_open_file_breakdown() -> Option<OpenFileBreakdown> {
        use std::process::Command;

        let output = Command::new("lsof")
            .arg("-n")
            .arg("-P")
            .arg("-p")
            .arg(std::process::id().to_string())
            .arg("-Fft")
            .output()
            .ok()?;

        if !output.status.success() {
            return None;
        }

        let stdout = String::from_utf8(output.stdout).ok()?;
        let mut breakdown = OpenFileBreakdown::default();
        let mut current_type: Option<&str> = None;

        for line in stdout.lines() {
            if let Some(rest) = line.strip_prefix('f') {
                if !rest.is_empty() {
                    if let Some(fd_type) = current_type.take() {
                        breakdown.total += 1;
                        Self::increment_fd_type(&mut breakdown, fd_type);
                    }
                }
                continue;
            }

            if let Some(rest) = line.strip_prefix('t') {
                current_type = Some(rest);
            }
        }

        if let Some(fd_type) = current_type {
            breakdown.total += 1;
            Self::increment_fd_type(&mut breakdown, fd_type);
        }

        Some(breakdown)
    }

    #[cfg(unix)]
    fn increment_fd_type(breakdown: &mut OpenFileBreakdown, fd_type: &str) {
        match fd_type {
            "REG" => breakdown.regular += 1,
            "DIR" => breakdown.directories += 1,
            "KQUEUE" => breakdown.kqueue += 1,
            "unix" => breakdown.unix += 1,
            "IPv4" => breakdown.ipv4 += 1,
            _ => breakdown.other += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HealthMonitor;

    #[test]
    fn format_log_from_pairs_includes_core_health_segments() {
        let metrics = vec![
            ("memory_usage_mb".to_string(), "109".to_string()),
            ("cpu_usage_percent".to_string(), "0.23".to_string()),
            ("open_files_total".to_string(), "437".to_string()),
            ("open_files_regular".to_string(), "68".to_string()),
            ("open_files_directories".to_string(), "250".to_string()),
            ("open_files_kqueue".to_string(), "80".to_string()),
            ("open_files_unix".to_string(), "28".to_string()),
            ("open_files_ipv4".to_string(), "6".to_string()),
            ("open_files_other".to_string(), "5".to_string()),
            ("storage_partition_count".to_string(), "247".to_string()),
            ("total_namespaces".to_string(), "29".to_string()),
            ("total_tables".to_string(), "30".to_string()),
            ("active_subscriptions".to_string(), "24".to_string()),
            ("active_connections".to_string(), "1".to_string()),
            ("websocket_sessions".to_string(), "1".to_string()),
            ("jobs_running".to_string(), "0".to_string()),
            ("jobs_queued".to_string(), "0".to_string()),
            ("jobs_failed".to_string(), "0".to_string()),
            ("total_jobs".to_string(), "100".to_string()),
            ("schema_cache_size".to_string(), "30".to_string()),
            ("schema_cache_total_entries".to_string(), "45".to_string()),
            ("plan_cache_size".to_string(), "7".to_string()),
            ("topic_cache_topic_count".to_string(), "3".to_string()),
            ("topic_cache_total_routes".to_string(), "12".to_string()),
            ("string_interner_unique_strings".to_string(), "88".to_string()),
        ];

        let line = HealthMonitor::format_log_from_pairs(&metrics);

        assert!(line.contains("Health metrics: Memory: 109 MB | CPU: 0.23%"));
        assert!(line.contains(
            "Open Files: 437 total (68 reg, 250 dir, 80 kqueue, 28 unix, 6 ipv4, 5 other)"
        ));
        assert!(line.contains("Jobs: 0 running, 0 queued, 0 failed (total: 100)"));
        assert!(line.contains(
            "Caches: Schema 30 latest/45 total, Plan 7, Topics 3 / 12 routes, Strings 88"
        ));
    }
}
