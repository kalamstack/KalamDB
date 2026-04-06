//! Exponential backoff with jitter.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
}

pub fn jittered_exponential_backoff(base: Duration, attempt: u32, max: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    if base_ms == 0 {
        return Duration::from_millis(0);
    }

    let exp = 1u64.checked_shl(attempt.min(30)).unwrap_or(u64::MAX);
    let mut delay_ms = base_ms.saturating_mul(exp);
    let max_ms = max.as_millis() as u64;
    if max_ms > 0 {
        delay_ms = delay_ms.min(max_ms);
    }

    let jitter_seed = now_nanos() % 1000;
    let jitter_factor = 0.8 + (jitter_seed as f64 / 1000.0) * 0.4;
    let jittered = (delay_ms as f64 * jitter_factor).round() as u64;

    Duration::from_millis(jittered)
}
