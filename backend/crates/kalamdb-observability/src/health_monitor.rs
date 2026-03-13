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

        // Get open file descriptor count (Unix only)
        #[cfg(unix)]
        let open_file_breakdown = Self::collect_open_file_breakdown();
        #[cfg(not(unix))]
        let open_file_breakdown = None;

        let open_files = open_file_breakdown.map(|breakdown| breakdown.total).unwrap_or(0);

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
