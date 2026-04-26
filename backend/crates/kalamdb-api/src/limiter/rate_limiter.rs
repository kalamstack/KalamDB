//! User and connection-based rate limiter
//!
//! Uses Moka cache for automatic TTL-based cleanup and high concurrency.
//! Optimized for zero-copy access patterns where possible.

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use kalamdb_commons::models::{ConnectionInfo, UserId};
use kalamdb_configs::RateLimitSettings;
use kalamdb_live::ConnectionId;
use moka::sync::Cache;
use parking_lot::Mutex;

use super::token_bucket::TokenBucket;

/// Rate limiter for users and connections
///
/// Provides three layers of rate limiting:
/// 1. Per-user query rate limiting (token bucket)
/// 2. Per-user subscription limits (counter)
/// 3. Per-connection message rate limiting (token bucket)
///
/// Uses Moka cache with TTL for automatic cleanup of inactive entries.
pub struct RateLimiter {
    /// Max queries per second per user
    max_queries_per_sec: u32,
    /// Max subscriptions per user
    max_subscriptions_per_user: u32,
    /// Max messages per second per connection
    max_messages_per_sec: u32,
    /// Max auth requests per second per IP
    max_auth_requests_per_ip_per_sec: u32,
    /// User query rate buckets - keyed by user_id string
    user_query_buckets: Cache<Arc<str>, Arc<Mutex<TokenBucket>>>,
    /// User subscription counts - keyed by user_id string
    user_subscription_counts: Cache<Arc<str>, Arc<AtomicU32>>,
    /// Auth rate buckets - keyed by client IP
    auth_ip_buckets: Cache<Arc<str>, Arc<Mutex<TokenBucket>>>,
    /// Connection message rate buckets - keyed by connection_id
    connection_message_buckets: Cache<ConnectionId, Arc<Mutex<TokenBucket>>>,
}

impl RateLimiter {
    /// Create a new rate limiter with default config
    pub fn new() -> Self {
        Self::with_config(&RateLimitSettings::default())
    }

    /// Create a new rate limiter from config settings
    pub fn with_config(config: &RateLimitSettings) -> Self {
        let cache_ttl = Duration::from_secs(config.cache_ttl_seconds);

        let user_query_buckets = Cache::builder()
            .max_capacity(config.cache_max_entries)
            .time_to_idle(cache_ttl)
            .build();

        let user_subscription_counts = Cache::builder()
            .max_capacity(config.cache_max_entries)
            .time_to_idle(cache_ttl)
            .build();

        let auth_ip_buckets = Cache::builder()
            .max_capacity(config.cache_max_entries)
            .time_to_idle(cache_ttl)
            .build();

        let connection_message_buckets = Cache::builder()
            .max_capacity(config.cache_max_entries)
            .time_to_idle(cache_ttl)
            .build();

        Self {
            max_queries_per_sec: config.max_queries_per_sec,
            max_subscriptions_per_user: config.max_subscriptions_per_user,
            max_messages_per_sec: config.max_messages_per_sec,
            max_auth_requests_per_ip_per_sec: config.max_auth_requests_per_ip_per_sec,
            user_query_buckets,
            user_subscription_counts,
            auth_ip_buckets,
            connection_message_buckets,
        }
    }

    /// Check if a user can execute a query
    ///
    /// Returns `true` if allowed, `false` if rate limit exceeded.
    /// Uses `Arc<str>` for zero-copy key reuse.
    #[inline]
    pub fn check_query_rate(&self, user_id: &UserId) -> bool {
        // Use Arc<str> for efficient key - avoids repeated String allocations
        let user_key: Arc<str> = Arc::from(user_id.as_str());
        let max_queries = self.max_queries_per_sec;

        let bucket = self.user_query_buckets.get_with(user_key, || {
            Arc::new(Mutex::new(TokenBucket::new(max_queries, max_queries, Duration::from_secs(1))))
        });

        let mut guard = bucket.lock();
        guard.try_consume(1)
    }

    /// Check if a user can create a new subscription
    ///
    /// Returns `true` if allowed, `false` if limit exceeded.
    #[inline]
    pub fn check_subscription_limit(&self, user_id: &UserId) -> bool {
        let user_key: Arc<str> = Arc::from(user_id.as_str());

        self.user_subscription_counts
            .get(&user_key)
            .map(|count| count.load(Ordering::Relaxed) < self.max_subscriptions_per_user)
            .unwrap_or(true)
    }

    /// Increment user subscription count
    #[inline]
    pub fn increment_subscription(&self, user_id: &UserId) {
        let user_key: Arc<str> = Arc::from(user_id.as_str());

        let count =
            self.user_subscription_counts.get_with(user_key, || Arc::new(AtomicU32::new(0)));

        count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement user subscription count
    #[inline]
    pub fn decrement_subscription(&self, user_id: &UserId) {
        let user_key: Arc<str> = Arc::from(user_id.as_str());

        if let Some(count) = self.user_subscription_counts.get(&user_key) {
            count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
                .ok();
        }
    }

    /// Check if a connection can send a message
    ///
    /// Returns `true` if allowed, `false` if rate limit exceeded.
    #[inline]
    pub fn check_message_rate(&self, connection_id: &ConnectionId) -> bool {
        let max_messages = self.max_messages_per_sec;

        let bucket = self.connection_message_buckets.get_with(connection_id.clone(), || {
            Arc::new(Mutex::new(TokenBucket::new(
                max_messages,
                max_messages,
                Duration::from_secs(1),
            )))
        });

        let mut guard = bucket.lock();
        guard.try_consume(1)
    }

    /// Check if a client IP can attempt authentication
    ///
    /// Returns `true` if allowed, `false` if rate limit exceeded.
    #[inline]
    pub fn check_auth_rate(&self, connection_info: &ConnectionInfo) -> bool {
        let max_auth_requests = self.max_auth_requests_per_ip_per_sec;
        let ip_key = connection_info.remote_addr.as_deref().unwrap_or("unknown");
        let key: Arc<str> = Arc::from(ip_key);

        let bucket = self.auth_ip_buckets.get_with(key, || {
            Arc::new(Mutex::new(TokenBucket::new(
                max_auth_requests,
                max_auth_requests,
                Duration::from_secs(1),
            )))
        });

        let mut guard = bucket.lock();
        guard.try_consume(1)
    }

    /// Clean up rate limit state for a connection
    ///
    /// Called when a connection closes. The entry will be evicted from cache.
    #[inline]
    pub fn cleanup_connection(&self, connection_id: &ConnectionId) {
        self.connection_message_buckets.invalidate(connection_id);
    }

    /// Get current rate limit stats for a user
    ///
    /// Returns (available_queries, subscription_count)
    pub fn get_user_stats(&self, user_id: &UserId) -> (u32, u32) {
        let user_key: Arc<str> = Arc::from(user_id.as_str());

        let available_queries = if let Some(bucket) = self.user_query_buckets.get(&user_key) {
            let mut guard = bucket.lock();
            guard.available_tokens()
        } else {
            self.max_queries_per_sec
        };

        let subscription_count = self
            .user_subscription_counts
            .get(&user_key)
            .map(|count| count.load(Ordering::Relaxed))
            .unwrap_or(0);

        (available_queries, subscription_count)
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    fn test_config(max_queries: u32, max_subs: u32, max_msgs: u32) -> RateLimitSettings {
        RateLimitSettings {
            max_queries_per_sec: max_queries,
            max_subscriptions_per_user: max_subs,
            max_messages_per_sec: max_msgs,
            ..Default::default()
        }
    }

    #[test]
    fn test_query_rate_limiting() {
        let config = test_config(5, 100, 100);
        let limiter = RateLimiter::with_config(&config);
        let user_id = UserId::from("user-123");

        // Should allow first 5 queries
        for _ in 0..5 {
            assert!(limiter.check_query_rate(&user_id));
        }

        // 6th query should be denied
        assert!(!limiter.check_query_rate(&user_id));
    }

    #[test]
    fn test_subscription_limit() {
        let config = test_config(100, 3, 100);
        let limiter = RateLimiter::with_config(&config);
        let user_id = UserId::from("user-123");

        // Should allow first 3 subscriptions
        for _ in 0..3 {
            assert!(limiter.check_subscription_limit(&user_id));
            limiter.increment_subscription(&user_id);
        }

        // 4th subscription should be denied
        assert!(!limiter.check_subscription_limit(&user_id));

        // Decrement and should allow again
        limiter.decrement_subscription(&user_id);
        assert!(limiter.check_subscription_limit(&user_id));
    }

    #[test]
    fn test_auth_rate_limiting() {
        let config = RateLimitSettings {
            max_auth_requests_per_ip_per_sec: 2,
            ..Default::default()
        };
        let limiter = RateLimiter::with_config(&config);
        let connection_info = ConnectionInfo::new(Some("10.0.0.1".to_string()));

        assert!(limiter.check_auth_rate(&connection_info));
        assert!(limiter.check_auth_rate(&connection_info));
        assert!(!limiter.check_auth_rate(&connection_info));
    }

    #[test]
    fn test_message_rate_limiting() {
        let config = test_config(100, 100, 5);
        let limiter = RateLimiter::with_config(&config);
        let conn_id = ConnectionId::new("conn-123");

        // Should allow first 5 messages
        for _ in 0..5 {
            assert!(limiter.check_message_rate(&conn_id));
        }

        // 6th message should be denied
        assert!(!limiter.check_message_rate(&conn_id));
    }

    #[test]
    fn test_connection_cleanup() {
        let limiter = RateLimiter::new();
        let conn_id = ConnectionId::new("conn-123");

        // Send messages to create bucket
        for _ in 0..5 {
            limiter.check_message_rate(&conn_id);
        }

        // Cleanup
        limiter.cleanup_connection(&conn_id);

        // Should be able to send again (new bucket created)
        assert!(limiter.check_message_rate(&conn_id));
    }

    #[test]
    fn test_user_stats() {
        let config = test_config(10, 5, 100);
        let limiter = RateLimiter::with_config(&config);
        let user_id = UserId::from("user-123");

        // Initial stats
        let (queries, subs) = limiter.get_user_stats(&user_id);
        assert_eq!(queries, 10);
        assert_eq!(subs, 0);

        // Consume some queries
        limiter.check_query_rate(&user_id);
        limiter.check_query_rate(&user_id);

        // Add subscriptions
        limiter.increment_subscription(&user_id);
        limiter.increment_subscription(&user_id);

        // Check stats
        let (queries, subs) = limiter.get_user_stats(&user_id);
        assert_eq!(queries, 8);
        assert_eq!(subs, 2);
    }

    #[test]
    fn test_rate_limiter_concurrent_access() {
        use std::sync::Arc as StdArc;

        let limiter = StdArc::new(RateLimiter::new());
        let user_id = UserId::from("user-concurrent");

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let limiter = limiter.clone();
                let user_id = user_id.clone();
                thread::spawn(move || {
                    for _ in 0..10 {
                        limiter.check_query_rate(&user_id);
                        thread::sleep(Duration::from_millis(1));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Should complete without deadlock
    }
}
