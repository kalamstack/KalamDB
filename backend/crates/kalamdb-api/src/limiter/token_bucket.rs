//! Token bucket implementation for rate limiting
//!
//! Uses a simple token bucket algorithm with continuous refill based on elapsed time.
//! Optimized for minimal allocations and fast path operations.

use std::time::{Duration, Instant};

/// Token bucket for rate limiting
///
/// This implementation uses continuous refill based on elapsed time,
/// providing smooth rate limiting rather than bursty window-based limits.
#[derive(Debug)]
pub struct TokenBucket {
    /// Maximum tokens in the bucket
    capacity: u32,

    /// Current number of tokens
    tokens: u32,

    /// Last refill time
    last_refill: Instant,

    /// Pre-computed tokens per second for fast refill calculation
    tokens_per_sec: f64,
}

impl TokenBucket {
    /// Create a new token bucket
    ///
    /// # Arguments
    /// * `capacity` - Maximum tokens the bucket can hold
    /// * `refill_rate` - Tokens to add per window
    /// * `window` - Duration of the refill window
    #[inline]
    pub fn new(capacity: u32, refill_rate: u32, window: Duration) -> Self {
        let tokens_per_sec = refill_rate as f64 / window.as_secs_f64();
        Self {
            capacity,
            tokens: capacity,
            last_refill: Instant::now(),
            tokens_per_sec,
        }
    }

    /// Try to consume tokens
    ///
    /// Returns `true` if successful, `false` if insufficient tokens.
    /// This is the hot path - optimized for speed.
    #[inline]
    pub fn try_consume(&mut self, tokens: u32) -> bool {
        self.refill();

        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    /// Refill tokens based on elapsed time
    ///
    /// Uses pre-computed tokens_per_sec to avoid division in hot path.
    #[inline]
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);

        // Fast path: if very little time has passed, skip calculation
        if elapsed.as_millis() < 10 {
            return;
        }

        // Calculate tokens to add using pre-computed rate
        let elapsed_secs = elapsed.as_secs_f64();
        let tokens_to_add = (self.tokens_per_sec * elapsed_secs) as u32;

        if tokens_to_add > 0 {
            self.tokens = self.capacity.min(self.tokens + tokens_to_add);
            self.last_refill = now;
        }
    }

    /// Get current token count (after refill)
    #[inline]
    pub fn available_tokens(&mut self) -> u32 {
        self.refill();
        self.tokens
    }

    /// Get the capacity of this bucket
    #[inline]
    #[allow(dead_code)]
    pub fn capacity(&self) -> u32 {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10, 10, Duration::from_secs(1));

        // Should succeed - have 10 tokens
        assert!(bucket.try_consume(5));
        assert_eq!(bucket.available_tokens(), 5);

        // Should succeed - have 5 tokens left
        assert!(bucket.try_consume(5));
        assert_eq!(bucket.available_tokens(), 0);

        // Should fail - no tokens left
        assert!(!bucket.try_consume(1));
    }

    #[test]
    fn test_token_bucket_refill() {
        let mut bucket = TokenBucket::new(10, 10, Duration::from_millis(100));

        // Consume all tokens
        assert!(bucket.try_consume(10));
        assert_eq!(bucket.available_tokens(), 0);

        // Wait for refill
        thread::sleep(Duration::from_millis(150));

        // Should have refilled
        assert!(bucket.try_consume(10));
    }

    #[test]
    fn test_token_bucket_partial_refill() {
        let mut bucket = TokenBucket::new(100, 100, Duration::from_secs(1));

        // Consume all tokens
        assert!(bucket.try_consume(100));

        // Wait for partial refill (50ms = ~5 tokens)
        thread::sleep(Duration::from_millis(50));

        // Should have some tokens but not full
        let available = bucket.available_tokens();
        assert!(available > 0 && available < 100);
    }
}
