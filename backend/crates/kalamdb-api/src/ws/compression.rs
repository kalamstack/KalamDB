//! WebSocket message compression utilities
//!
//! Provides gzip compression for WebSocket messages to reduce bandwidth.
//! Compression is enabled by default for all messages over the threshold.

use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

/// Default compression threshold in bytes (512 bytes)
/// Messages smaller than this are sent uncompressed
pub const COMPRESSION_THRESHOLD: usize = 512;

/// Compress data using gzip
///
/// Returns compressed bytes on success, or the original data if compression fails
pub fn compress_gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    if encoder.write_all(data).is_ok() {
        if let Ok(compressed) = encoder.finish() {
            // Only use compressed if it's actually smaller
            if compressed.len() < data.len() {
                return compressed;
            }
        }
    }
    // Fallback to original data
    data.to_vec()
}

/// Check if data should be compressed based on size threshold
#[inline]
pub fn should_compress(data: &[u8]) -> bool {
    data.len() >= COMPRESSION_THRESHOLD
}

/// Compress data if it exceeds the threshold
///
/// Returns (data, is_compressed) tuple
pub fn maybe_compress(data: &[u8]) -> (Vec<u8>, bool) {
    if should_compress(data) {
        let compressed = compress_gzip(data);
        // Check if compression was effective
        if compressed.len() < data.len() && is_gzip(&compressed) {
            return (compressed, true);
        }
    }
    (data.to_vec(), false)
}

/// Check if data is gzip compressed (magic bytes check)
#[inline]
pub fn is_gzip(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_gzip() {
        let data = b"Hello, World! This is a test message that should compress well. \
                     Hello, World! This is a test message that should compress well. \
                     Hello, World! This is a test message that should compress well.";
        let compressed = compress_gzip(data);
        assert!(compressed.len() < data.len());
        assert!(is_gzip(&compressed));
    }

    #[test]
    fn test_should_compress() {
        let small = vec![0u8; 100];
        let large = vec![0u8; 1000];
        assert!(!should_compress(&small));
        assert!(should_compress(&large));
    }

    #[test]
    fn test_maybe_compress() {
        let small = b"small";
        let (data, compressed) = maybe_compress(small);
        assert!(!compressed);
        assert_eq!(data, small);

        let large = "x".repeat(1000);
        let (data, compressed) = maybe_compress(large.as_bytes());
        assert!(compressed);
        assert!(data.len() < large.len());
    }

    #[test]
    fn test_is_gzip() {
        assert!(is_gzip(&[0x1f, 0x8b, 0x08]));
        assert!(!is_gzip(&[0x00, 0x00]));
        assert!(!is_gzip(&[0x1f]));
    }
}
