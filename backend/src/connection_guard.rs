//! IP-based connection guard for DoS protection.

use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use kalamdb_configs::RateLimitSettings;
use moka::sync::Cache;
use parking_lot::Mutex;

#[derive(Debug)]
struct TokenBucket {
    capacity: u32,
    tokens: u32,
    last_refill: Instant,
    tokens_per_sec: f64,
}

impl TokenBucket {
    #[inline]
    fn new(capacity: u32, refill_rate: u32, window: Duration) -> Self {
        let tokens_per_sec = refill_rate as f64 / window.as_secs_f64();
        Self {
            capacity,
            tokens: capacity,
            last_refill: Instant::now(),
            tokens_per_sec,
        }
    }

    #[inline]
    fn try_consume(&mut self, tokens: u32) -> bool {
        self.refill();

        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    #[inline]
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);

        if elapsed.as_millis() < 10 {
            return;
        }

        let tokens_to_add = (self.tokens_per_sec * elapsed.as_secs_f64()) as u32;
        if tokens_to_add > 0 {
            self.tokens = self.capacity.min(self.tokens + tokens_to_add);
            self.last_refill = now;
        }
    }
}

struct IpState {
    active_connections: AtomicU32,
    request_bucket: Mutex<TokenBucket>,
    banned_until: Mutex<Option<Instant>>,
    violation_count: AtomicU32,
}

impl IpState {
    fn new(max_requests_per_sec: u32) -> Self {
        Self {
            active_connections: AtomicU32::new(0),
            request_bucket: Mutex::new(TokenBucket::new(
                max_requests_per_sec,
                max_requests_per_sec,
                Duration::from_secs(1),
            )),
            banned_until: Mutex::new(None),
            violation_count: AtomicU32::new(0),
        }
    }

    #[inline]
    fn is_banned(&self) -> Option<Instant> {
        let mut banned_until = self.banned_until.lock();
        if let Some(until) = *banned_until {
            if Instant::now() < until {
                return Some(until);
            }
            *banned_until = None;
        }
        None
    }

    #[inline]
    fn set_banned_until(&self, until: Instant) {
        *self.banned_until.lock() = Some(until);
    }

    #[inline]
    fn clear_ban(&self) {
        *self.banned_until.lock() = None;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionGuardResult {
    Allowed,
    Banned { until: Instant },
    TooManyConnections { current: u32, max: u32 },
    RateLimitExceeded,
    BodyTooLarge { size: usize, max: usize },
}

pub struct ConnectionGuard {
    max_connections_per_ip: u32,
    max_requests_per_ip_per_sec: u32,
    request_body_limit_bytes: usize,
    ban_duration: Duration,
    enabled: bool,
    ip_states: Cache<IpAddr, Arc<IpState>>,
}

impl ConnectionGuard {
    pub fn new() -> Self {
        Self::with_config(&RateLimitSettings::default())
    }

    pub fn with_config(config: &RateLimitSettings) -> Self {
        let cache_ttl = Duration::from_secs(config.cache_ttl_seconds);
        let max_requests = config.max_requests_per_ip_per_sec;

        let ip_states = Cache::builder()
            .max_capacity(config.cache_max_entries)
            .time_to_idle(cache_ttl)
            .eviction_listener(move |_ip, state: Arc<IpState>, _cause| {
                let active = state.active_connections.load(Ordering::Relaxed);
                if active > 0 {
                    log::debug!("[CONN_GUARD] Evicting IP with {} active connections", active);
                }
            })
            .build();

        Self {
            max_connections_per_ip: config.max_connections_per_ip,
            max_requests_per_ip_per_sec: max_requests,
            request_body_limit_bytes: config.request_body_limit_bytes,
            ban_duration: Duration::from_secs(config.ban_duration_seconds),
            enabled: config.enable_connection_protection,
            ip_states,
        }
    }

    #[inline]
    fn get_or_create_state(&self, ip: IpAddr) -> Arc<IpState> {
        let max_requests = self.max_requests_per_ip_per_sec;
        self.ip_states.get_with(ip, || Arc::new(IpState::new(max_requests)))
    }

    pub fn check_request(&self, ip: IpAddr, body_size: Option<usize>) -> ConnectionGuardResult {
        if !self.enabled {
            return ConnectionGuardResult::Allowed;
        }

        if let Some(size) = body_size {
            if size > self.request_body_limit_bytes {
                return ConnectionGuardResult::BodyTooLarge {
                    size,
                    max: self.request_body_limit_bytes,
                };
            }
        }

        if ip.is_loopback() {
            return ConnectionGuardResult::Allowed;
        }

        let state = self.get_or_create_state(ip);

        if let Some(banned_until) = state.is_banned() {
            return ConnectionGuardResult::Banned {
                until: banned_until,
            };
        }

        if !state.request_bucket.lock().try_consume(1) {
            let violations = state.violation_count.fetch_add(1, Ordering::Relaxed) + 1;

            if violations >= 10 {
                let ban_multiplier = (violations / 10).min(6);
                let ban_duration = self.ban_duration * ban_multiplier;
                state.set_banned_until(Instant::now() + ban_duration);

                log::warn!(
                    "[CONN_GUARD] IP {} banned for {:?} after {} violations",
                    ip,
                    ban_duration,
                    violations
                );
            }

            return ConnectionGuardResult::RateLimitExceeded;
        }

        ConnectionGuardResult::Allowed
    }

    pub fn register_connection(&self, ip: IpAddr) -> ConnectionGuardResult {
        if !self.enabled {
            return ConnectionGuardResult::Allowed;
        }

        if ip.is_loopback() {
            return ConnectionGuardResult::Allowed;
        }

        let state = self.get_or_create_state(ip);

        if let Some(banned_until) = state.is_banned() {
            return ConnectionGuardResult::Banned {
                until: banned_until,
            };
        }

        let current = state.active_connections.load(Ordering::Relaxed);
        if current >= self.max_connections_per_ip {
            let violations = state.violation_count.fetch_add(1, Ordering::Relaxed) + 1;

            if violations >= 5 {
                state.set_banned_until(Instant::now() + self.ban_duration);

                log::warn!(
                    "[CONN_GUARD] IP {} banned for {:?} - exceeded connection limit {} times",
                    ip,
                    self.ban_duration,
                    violations
                );
            }

            return ConnectionGuardResult::TooManyConnections {
                current,
                max: self.max_connections_per_ip,
            };
        }

        state.active_connections.fetch_add(1, Ordering::Relaxed);
        ConnectionGuardResult::Allowed
    }

    #[inline]
    pub fn unregister_connection(&self, ip: IpAddr) {
        if !self.enabled {
            return;
        }

        if let Some(state) = self.ip_states.get(&ip) {
            state
                .active_connections
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_sub(1))
                })
                .ok();
        }
    }

    #[inline]
    pub fn get_connection_count(&self, ip: IpAddr) -> u32 {
        self.ip_states
            .get(&ip)
            .map(|state| state.active_connections.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    #[inline]
    pub fn is_banned(&self, ip: IpAddr) -> bool {
        self.ip_states.get(&ip).and_then(|state| state.is_banned()).is_some()
    }

    pub fn ban_ip(&self, ip: IpAddr, duration: Duration) {
        let state = self.get_or_create_state(ip);
        state.set_banned_until(Instant::now() + duration);
        state.violation_count.fetch_add(10, Ordering::Relaxed);

        log::warn!("[CONN_GUARD] IP {} manually banned for {:?}", ip, duration);
    }

    pub fn unban_ip(&self, ip: IpAddr) {
        if let Some(state) = self.ip_states.get(&ip) {
            state.clear_ban();
            log::info!("[CONN_GUARD] IP {} unbanned", ip);
        }
    }

    pub fn get_stats(&self) -> ConnectionGuardStats {
        let total_ips = self.ip_states.iter().count();
        let mut banned_ips = 0usize;
        let mut total_connections = 0u32;
        let mut total_violations = 0u32;

        self.ip_states.iter().for_each(|(_, state)| {
            if state.is_banned().is_some() {
                banned_ips += 1;
            }
            total_connections += state.active_connections.load(Ordering::Relaxed);
            total_violations += state.violation_count.load(Ordering::Relaxed);
        });

        ConnectionGuardStats {
            tracked_ips: total_ips,
            banned_ips,
            total_active_connections: total_connections,
            total_violations,
        }
    }
}

impl Default for ConnectionGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionGuardStats {
    pub tracked_ips: usize,
    pub banned_ips: usize,
    pub total_active_connections: u32,
    pub total_violations: u32,
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    fn test_config(
        max_conn: u32,
        max_req: u32,
        body_limit: usize,
        ban_secs: u64,
    ) -> RateLimitSettings {
        RateLimitSettings {
            max_connections_per_ip: max_conn,
            max_requests_per_ip_per_sec: max_req,
            request_body_limit_bytes: body_limit,
            ban_duration_seconds: ban_secs,
            enable_connection_protection: true,
            ..Default::default()
        }
    }

    #[test]
    fn test_connection_guard_allows_normal_requests() {
        let guard = ConnectionGuard::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        assert_eq!(guard.check_request(ip, Some(1000)), ConnectionGuardResult::Allowed);
    }

    #[test]
    fn test_connection_guard_body_size_limit() {
        let config = test_config(100, 200, 1000, 300);
        let guard = ConnectionGuard::with_config(&config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        let result = guard.check_request(ip, Some(2000));
        assert!(matches!(result, ConnectionGuardResult::BodyTooLarge { .. }));
    }

    #[test]
    fn test_connection_guard_connection_limit() {
        let config = test_config(2, 200, 10_000_000, 300);
        let guard = ConnectionGuard::with_config(&config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        assert_eq!(guard.register_connection(ip), ConnectionGuardResult::Allowed);
        assert_eq!(guard.register_connection(ip), ConnectionGuardResult::Allowed);

        let result = guard.register_connection(ip);
        assert!(matches!(result, ConnectionGuardResult::TooManyConnections { .. }));

        guard.unregister_connection(ip);
        assert_eq!(guard.register_connection(ip), ConnectionGuardResult::Allowed);
    }

    #[test]
    fn test_connection_guard_rate_limit() {
        let config = test_config(100, 5, 10_000_000, 300);
        let guard = ConnectionGuard::with_config(&config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        for _ in 0..5 {
            assert_eq!(guard.check_request(ip, None), ConnectionGuardResult::Allowed);
        }

        assert_eq!(guard.check_request(ip, None), ConnectionGuardResult::RateLimitExceeded);
    }

    #[test]
    fn test_connection_guard_ban_after_violations() {
        let config = test_config(100, 1, 10_000_000, 1);
        let guard = ConnectionGuard::with_config(&config);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        for _ in 0..15 {
            guard.check_request(ip, None);
        }

        assert!(guard.is_banned(ip));

        thread::sleep(Duration::from_millis(1100));

        assert!(!guard.is_banned(ip));
    }

    #[test]
    fn test_connection_guard_manual_ban() {
        let guard = ConnectionGuard::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        assert!(!guard.is_banned(ip));

        guard.ban_ip(ip, Duration::from_millis(100));

        assert!(guard.is_banned(ip));
        let result = guard.check_request(ip, None);
        assert!(matches!(result, ConnectionGuardResult::Banned { .. }));

        guard.unban_ip(ip);
        assert!(!guard.is_banned(ip));
    }

    #[test]
    fn test_connection_guard_stats() {
        let guard = ConnectionGuard::new();
        let ip1: IpAddr = "192.168.1.1".parse().unwrap();
        let ip2: IpAddr = "192.168.1.2".parse().unwrap();

        guard.register_connection(ip1);
        guard.register_connection(ip1);
        guard.register_connection(ip2);

        let stats = guard.get_stats();
        assert_eq!(stats.tracked_ips, 2);
        assert_eq!(stats.total_active_connections, 3);
    }

    #[test]
    fn test_connection_guard_localhost_exempt() {
        let config = test_config(100, 1, 10_000_000, 300);
        let guard = ConnectionGuard::with_config(&config);
        let localhost: IpAddr = "127.0.0.1".parse().unwrap();

        for _ in 0..100 {
            assert_eq!(guard.check_request(localhost, None), ConnectionGuardResult::Allowed);
        }
    }

    #[test]
    fn test_connection_guard_concurrent_access() {
        let guard = Arc::new(ConnectionGuard::new());
        let ip: IpAddr = "192.168.1.100".parse().unwrap();

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let guard = Arc::clone(&guard);
                thread::spawn(move || {
                    for _ in 0..50 {
                        guard.check_request(ip, None);
                        thread::sleep(Duration::from_micros(100));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
