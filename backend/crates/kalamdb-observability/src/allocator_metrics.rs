#[derive(Debug, Clone, Default)]
pub struct AllocatorMetrics {
    pub allocator_name: &'static str,
    pub allocator_version: Option<u32>,
    pub elapsed_ms: Option<u64>,
    pub user_ms: Option<u64>,
    pub system_ms: Option<u64>,
    pub process_rss_bytes: Option<u64>,
    pub process_peak_rss_bytes: Option<u64>,
    pub process_commit_bytes: Option<u64>,
    pub process_peak_commit_bytes: Option<u64>,
    pub page_faults: Option<u64>,
    pub reserved_current_bytes: Option<u64>,
    pub reserved_peak_bytes: Option<u64>,
    pub committed_current_bytes: Option<u64>,
    pub committed_peak_bytes: Option<u64>,
    pub reset_current_bytes: Option<u64>,
    pub purged_current_bytes: Option<u64>,
    pub page_committed_current_bytes: Option<u64>,
    pub pages_current: Option<u64>,
    pub pages_abandoned_current: Option<u64>,
    pub threads_current: Option<u64>,
    pub malloc_requested_current_bytes: Option<u64>,
    pub malloc_requested_peak_bytes: Option<u64>,
    pub malloc_normal_current_bytes: Option<u64>,
    pub malloc_huge_current_bytes: Option<u64>,
    pub segments_current: Option<u64>,
    pub segments_abandoned_current: Option<u64>,
    pub segments_cache_current: Option<u64>,
    pub arena_count: Option<u64>,
    pub purge_delay_ms: Option<i64>,
    pub purge_decommits: Option<bool>,
    pub target_segments_per_thread: Option<u64>,
}

impl AllocatorMetrics {
    pub fn as_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        pairs.push(("allocator_name".to_string(), self.allocator_name.to_string()));

        push_u64(&mut pairs, "allocator_version", self.allocator_version.map(u64::from));
        push_u64(&mut pairs, "mimalloc_elapsed_ms", self.elapsed_ms);
        push_u64(&mut pairs, "mimalloc_user_ms", self.user_ms);
        push_u64(&mut pairs, "mimalloc_system_ms", self.system_ms);
        push_u64(&mut pairs, "mimalloc_process_rss_bytes", self.process_rss_bytes);
        push_u64(&mut pairs, "mimalloc_process_peak_rss_bytes", self.process_peak_rss_bytes);
        push_u64(&mut pairs, "mimalloc_process_commit_bytes", self.process_commit_bytes);
        push_u64(&mut pairs, "mimalloc_process_peak_commit_bytes", self.process_peak_commit_bytes);
        push_u64(&mut pairs, "mimalloc_page_faults", self.page_faults);
        push_u64(&mut pairs, "mimalloc_reserved_current_bytes", self.reserved_current_bytes);
        push_u64(&mut pairs, "mimalloc_reserved_peak_bytes", self.reserved_peak_bytes);
        push_u64(&mut pairs, "mimalloc_committed_current_bytes", self.committed_current_bytes);
        push_u64(&mut pairs, "mimalloc_committed_peak_bytes", self.committed_peak_bytes);
        push_u64(&mut pairs, "mimalloc_reset_current_bytes", self.reset_current_bytes);
        push_u64(&mut pairs, "mimalloc_purged_current_bytes", self.purged_current_bytes);
        push_u64(
            &mut pairs,
            "mimalloc_page_committed_current_bytes",
            self.page_committed_current_bytes,
        );
        push_u64(&mut pairs, "mimalloc_pages_current", self.pages_current);
        push_u64(&mut pairs, "mimalloc_pages_abandoned_current", self.pages_abandoned_current);
        push_u64(&mut pairs, "mimalloc_threads_current", self.threads_current);
        push_u64(
            &mut pairs,
            "mimalloc_malloc_requested_current_bytes",
            self.malloc_requested_current_bytes,
        );
        push_u64(
            &mut pairs,
            "mimalloc_malloc_requested_peak_bytes",
            self.malloc_requested_peak_bytes,
        );
        push_u64(
            &mut pairs,
            "mimalloc_malloc_normal_current_bytes",
            self.malloc_normal_current_bytes,
        );
        push_u64(&mut pairs, "mimalloc_malloc_huge_current_bytes", self.malloc_huge_current_bytes);
        push_u64(&mut pairs, "mimalloc_segments_current", self.segments_current);
        push_u64(
            &mut pairs,
            "mimalloc_segments_abandoned_current",
            self.segments_abandoned_current,
        );
        push_u64(&mut pairs, "mimalloc_segments_cache_current", self.segments_cache_current);
        push_u64(&mut pairs, "mimalloc_arena_count", self.arena_count);
        push_i64(&mut pairs, "mimalloc_purge_delay_ms", self.purge_delay_ms);
        push_bool(&mut pairs, "mimalloc_purge_decommits_enabled", self.purge_decommits);
        push_u64(
            &mut pairs,
            "mimalloc_target_segments_per_thread",
            self.target_segments_per_thread,
        );

        pairs
    }
}

fn push_u64(pairs: &mut Vec<(String, String)>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        pairs.push((key.to_string(), value.to_string()));
    }
}

fn push_i64(pairs: &mut Vec<(String, String)>, key: &str, value: Option<i64>) {
    if let Some(value) = value {
        pairs.push((key.to_string(), value.to_string()));
    }
}

fn push_bool(pairs: &mut Vec<(String, String)>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        pairs.push((key.to_string(), value.to_string()));
    }
}

#[cfg(feature = "mimalloc")]
mod imp {
    use super::AllocatorMetrics;
    use libmimalloc_sys::{
        mi_collect, mi_option_get, mi_option_is_enabled, mi_option_t, mi_process_info,
        mi_stats_merge, mi_version,
    };
    use std::mem::MaybeUninit;

    const MI_BIN_HUGE: usize = 73;
    const MI_OPTION_PURGE_DECOMMITS: mi_option_t = 5;
    const MI_OPTION_PURGE_DELAY: mi_option_t = 15;
    const MI_OPTION_TARGET_SEGMENTS_PER_THREAD: mi_option_t = 35;

    #[repr(C)]
    #[derive(Debug, Clone, Copy, Default)]
    struct MiStatCount {
        total: i64,
        peak: i64,
        current: i64,
    }

    #[repr(C)]
    #[derive(Debug, Clone, Copy, Default)]
    struct MiStatCounter {
        total: i64,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct MiStats {
        version: i32,
        pages: MiStatCount,
        reserved: MiStatCount,
        committed: MiStatCount,
        reset: MiStatCount,
        purged: MiStatCount,
        page_committed: MiStatCount,
        pages_abandoned: MiStatCount,
        threads: MiStatCount,
        malloc_normal: MiStatCount,
        malloc_huge: MiStatCount,
        malloc_requested: MiStatCount,
        mmap_calls: MiStatCounter,
        commit_calls: MiStatCounter,
        reset_calls: MiStatCounter,
        purge_calls: MiStatCounter,
        arena_count: MiStatCounter,
        malloc_normal_count: MiStatCounter,
        malloc_huge_count: MiStatCounter,
        malloc_guarded_count: MiStatCounter,
        arena_rollback_count: MiStatCounter,
        arena_purges: MiStatCounter,
        pages_extended: MiStatCounter,
        pages_retire: MiStatCounter,
        page_searches: MiStatCounter,
        segments: MiStatCount,
        segments_abandoned: MiStatCount,
        segments_cache: MiStatCount,
        _segments_reserved: MiStatCount,
        _stat_reserved: [MiStatCount; 4],
        _stat_counter_reserved: [MiStatCounter; 4],
        malloc_bins: [MiStatCount; MI_BIN_HUGE + 1],
        page_bins: [MiStatCount; MI_BIN_HUGE + 1],
    }

    unsafe extern "C" {
        fn mi_stats_get(stats_size: usize, stats: *mut MiStats);
    }

    pub fn collect_allocator_metrics() -> Option<AllocatorMetrics> {
        let mut elapsed = 0usize;
        let mut user = 0usize;
        let mut system = 0usize;
        let mut current_rss = 0usize;
        let mut peak_rss = 0usize;
        let mut current_commit = 0usize;
        let mut peak_commit = 0usize;
        let mut page_faults = 0usize;

        unsafe {
            mi_process_info(
                &mut elapsed,
                &mut user,
                &mut system,
                &mut current_rss,
                &mut peak_rss,
                &mut current_commit,
                &mut peak_commit,
                &mut page_faults,
            );

            // mi_stats_get snapshots the global stats state. Merge the current
            // thread-local counters first so point-in-time values reflect the
            // allocations made by the calling thread.
            mi_stats_merge();
        }

        let mut stats = MaybeUninit::<MiStats>::zeroed();
        unsafe {
            mi_stats_get(std::mem::size_of::<MiStats>(), stats.as_mut_ptr());
        }
        let stats = unsafe { stats.assume_init() };

        Some(AllocatorMetrics {
            allocator_name: "mimalloc",
            allocator_version: Some(unsafe { mi_version() as u32 }),
            elapsed_ms: Some(elapsed as u64),
            user_ms: Some(user as u64),
            system_ms: Some(system as u64),
            process_rss_bytes: Some(current_rss as u64),
            process_peak_rss_bytes: Some(peak_rss as u64),
            process_commit_bytes: Some(current_commit as u64),
            process_peak_commit_bytes: Some(peak_commit as u64),
            page_faults: Some(page_faults as u64),
            reserved_current_bytes: clamp_count(stats.reserved.current),
            reserved_peak_bytes: clamp_count(stats.reserved.peak),
            committed_current_bytes: clamp_count(stats.committed.current),
            committed_peak_bytes: clamp_count(stats.committed.peak),
            reset_current_bytes: clamp_count(stats.reset.current),
            purged_current_bytes: clamp_count(stats.purged.current),
            page_committed_current_bytes: clamp_count(stats.page_committed.current),
            pages_current: clamp_count(stats.pages.current),
            pages_abandoned_current: clamp_count(stats.pages_abandoned.current),
            threads_current: clamp_count(stats.threads.current),
            malloc_requested_current_bytes: clamp_count(stats.malloc_requested.current),
            malloc_requested_peak_bytes: clamp_count(stats.malloc_requested.peak),
            malloc_normal_current_bytes: clamp_count(stats.malloc_normal.current),
            malloc_huge_current_bytes: clamp_count(stats.malloc_huge.current),
            segments_current: clamp_count(stats.segments.current),
            segments_abandoned_current: clamp_count(stats.segments_abandoned.current),
            segments_cache_current: clamp_count(stats.segments_cache.current),
            arena_count: clamp_counter(stats.arena_count.total),
            purge_delay_ms: Some(unsafe { mi_option_get(MI_OPTION_PURGE_DELAY) } as i64),
            purge_decommits: Some(unsafe { mi_option_is_enabled(MI_OPTION_PURGE_DECOMMITS) }),
            target_segments_per_thread: Some(unsafe {
                mi_option_get(MI_OPTION_TARGET_SEGMENTS_PER_THREAD) as u64
            }),
        })
    }

    pub fn force_allocator_collection(force: bool) {
        unsafe {
            mi_collect(force);
        }
    }

    fn clamp_count(value: i64) -> Option<u64> {
        u64::try_from(value).ok()
    }

    fn clamp_counter(value: i64) -> Option<u64> {
        u64::try_from(value).ok()
    }
}

#[cfg(not(feature = "mimalloc"))]
mod imp {
    use super::AllocatorMetrics;

    pub fn collect_allocator_metrics() -> Option<AllocatorMetrics> {
        None
    }

    pub fn force_allocator_collection(_force: bool) {}
}

pub use imp::{collect_allocator_metrics, force_allocator_collection};
