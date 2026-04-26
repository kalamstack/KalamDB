use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

fn epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

static LAST_ACTIVITY_MS: AtomicU64 = AtomicU64::new(0);

/// Record server activity using the current wall-clock timestamp.
pub fn record_activity_now() -> u64 {
    let now = epoch_millis();
    LAST_ACTIVITY_MS.store(now, Ordering::Release);
    now
}

/// Return the last recorded activity timestamp in epoch milliseconds.
pub fn last_activity_ms() -> Option<u64> {
    let last = LAST_ACTIVITY_MS.load(Ordering::Acquire);
    (last != 0).then_some(last)
}

/// Return how long the server has been idle based on the last recorded activity.
pub fn idle_duration() -> Option<Duration> {
    let last = last_activity_ms()?;
    Some(Duration::from_millis(epoch_millis().saturating_sub(last)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_activity_updates_idle_duration() {
        record_activity_now();
        let idle = idle_duration().expect("idle duration should be available after activity");
        assert!(idle < Duration::from_secs(1));
    }
}
