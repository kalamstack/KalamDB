use std::sync::Mutex;
use std::time::Instant;
use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

/// Reusable System instance to avoid repeated allocation/deallocation.
/// sysinfo docs explicitly recommend reusing the same System instance.
/// Creating System::new_all() every 30s causes severe heap fragmentation
/// that jemalloc retains across arenas (~600MB+ over hours).
pub static SHARED_SYSTEM: Mutex<Option<System>> = Mutex::new(None);

/// Snapshot of runtime/system metrics gathered from sysinfo.
#[derive(Debug, Clone)]
pub struct RuntimeMetrics {
    pub uptime_seconds: u64,
    pub uptime_human: String,
    pub memory_bytes: Option<u64>,
    pub memory_mb: Option<u64>,
    pub cpu_usage_percent: Option<f32>,
    pub system_total_memory_mb: u64,
    pub system_used_memory_mb: u64,
    pub thread_count: Option<usize>,
    pub pid: Option<u32>,
}

impl RuntimeMetrics {
    /// Render as key/value pairs for system.stats.
    pub fn as_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();

        pairs.push(("server_uptime_seconds".to_string(), self.uptime_seconds.to_string()));
        pairs.push(("server_uptime_human".to_string(), self.uptime_human.clone()));

        if let Some(bytes) = self.memory_bytes {
            pairs.push(("memory_usage_bytes".to_string(), bytes.to_string()));
        }
        if let Some(mb) = self.memory_mb {
            pairs.push(("memory_usage_mb".to_string(), mb.to_string()));
        }
        if let Some(cpu) = self.cpu_usage_percent {
            pairs.push(("cpu_usage_percent".to_string(), format!("{:.2}", cpu)));
        }

        pairs.push(("system_total_memory_mb".to_string(), self.system_total_memory_mb.to_string()));
        pairs.push(("system_used_memory_mb".to_string(), self.system_used_memory_mb.to_string()));

        if let Some(t) = self.thread_count {
            pairs.push(("thread_count".to_string(), t.to_string()));
        }

        if let Some(pid) = self.pid {
            pairs.push(("pid".to_string(), pid.to_string()));
        }

        pairs
    }

    /// Render a concise log line for the console.
    pub fn to_log_string(&self) -> String {
        format!(
            "uptime={} mem={}MB used={}MB cpu={} pid={} threads={} sys_mem={}MB/{}MB",
            self.uptime_human,
            self.memory_mb.unwrap_or(0),
            self.system_used_memory_mb,
            self.cpu_usage_percent
                .map(|v| format!("{:.2}%", v))
                .unwrap_or_else(|| "N/A".to_string()),
            self.pid.map(|p| p.to_string()).unwrap_or_else(|| "N/A".to_string()),
            self.thread_count.map(|t| t.to_string()).unwrap_or_else(|| "N/A".to_string()),
            self.system_used_memory_mb,
            self.system_total_memory_mb,
        )
    }
}

/// Collect runtime metrics from sysinfo using the server start time for uptime.
///
/// Reuses a shared System instance to avoid heap fragmentation from repeated
/// System::new_all() calls. Only refreshes the current process and memory info.
pub fn collect_runtime_metrics(start_time: Instant) -> RuntimeMetrics {
    let uptime_seconds = start_time.elapsed().as_secs();
    let days = uptime_seconds / 86400;
    let hours = (uptime_seconds % 86400) / 3600;
    let minutes = (uptime_seconds % 3600) / 60;
    let uptime_human = if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m", hours, minutes)
    } else {
        format!("{}m", minutes)
    };

    let mut guard = SHARED_SYSTEM.lock().unwrap_or_else(|e| e.into_inner());
    let sys = guard.get_or_insert_with(|| {
        System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()))
    });

    // Only refresh what we need: current process memory/cpu + system memory
    let process_refresh = ProcessRefreshKind::nothing()
        .with_memory()
        .with_cpu();
    if let Ok(pid) = sysinfo::get_current_pid() {
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            process_refresh,
        );
    }
    sys.refresh_memory_specifics(MemoryRefreshKind::everything());

    let mut memory_bytes = None;
    let mut memory_mb = None;
    let mut cpu_usage_percent = None;
    #[allow(unused_mut)]
    let mut thread_count = None;
    let mut pid_num = None;

    if let Ok(pid) = sysinfo::get_current_pid() {
        if let Some(proc) = sys.process(pid) {
            pid_num = Some(proc.pid().as_u32());
            let mem_bytes = proc.memory();
            memory_bytes = Some(mem_bytes);
            memory_mb = Some(mem_bytes / 1024 / 1024);
            cpu_usage_percent = Some(proc.cpu_usage());
            #[cfg(unix)]
            {
                if let Ok(entries) = std::fs::read_dir("/proc/self/task") {
                    thread_count = Some(entries.count());
                }
            }
        }
    }

    let system_total_memory_mb = sys.total_memory() / 1024 / 1024;
    let system_used_memory_mb = sys.used_memory() / 1024 / 1024;

    RuntimeMetrics {
        uptime_seconds,
        uptime_human,
        memory_bytes,
        memory_mb,
        cpu_usage_percent,
        system_total_memory_mb,
        system_used_memory_mb,
        thread_count,
        pid: pid_num,
    }
}

// Public constants for server version info (used by compute_metrics and potentially other modules)
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_DATE: &str = match option_env!("BUILD_DATE") {
    Some(v) => v,
    None => "unknown",
};
pub const GIT_BRANCH: &str = match option_env!("GIT_BRANCH") {
    Some(v) => v,
    None => "unknown",
};
pub const GIT_COMMIT_HASH: &str = match option_env!("GIT_COMMIT_HASH") {
    Some(v) => v,
    None => "unknown",
};
