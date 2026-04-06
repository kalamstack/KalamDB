//! WebSocket message decompression utilities
//!
//! Provides gzip decompression for WebSocket messages received from the server.
//! The server compresses messages over 512 bytes automatically.

/// Check if data is gzip compressed (magic bytes check)
#[inline]
pub fn is_gzip(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

/// Decompress gzip data using miniz_oxide (pure Rust, WASM-compatible)
///
/// Returns the decompressed bytes, or an error if decompression fails.
pub fn decompress_gzip(data: &[u8]) -> Result<Vec<u8>, DecompressError> {
    decompress_gzip_with_limit(data, usize::MAX)
}

/// Decompress gzip data with a maximum output size limit.
///
/// This prevents memory-exhaustion attacks from highly-compressed payloads.
pub fn decompress_gzip_with_limit(
    data: &[u8],
    max_output_bytes: usize,
) -> Result<Vec<u8>, DecompressError> {
    if data.len() < 18 {
        return Err(DecompressError::TooShort);
    }

    // Verify gzip magic bytes
    if !is_gzip(data) {
        return Err(DecompressError::NotGzip);
    }

    // Gzip format:
    // - 10 byte header (magic, method, flags, mtime, xfl, os)
    // - Optional extra fields based on flags
    // - Compressed data (DEFLATE)
    // - 8 byte trailer (CRC32, original size)

    let flags = data[3];
    let mut pos = 10;

    // Skip extra field if present (FEXTRA flag = 0x04)
    if flags & 0x04 != 0 {
        if pos + 2 > data.len() {
            return Err(DecompressError::InvalidHeader);
        }
        let xlen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2 + xlen;
    }

    // Skip original filename if present (FNAME flag = 0x08)
    if flags & 0x08 != 0 {
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        pos += 1; // Skip null terminator
    }

    // Skip comment if present (FCOMMENT flag = 0x10)
    if flags & 0x10 != 0 {
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        pos += 1; // Skip null terminator
    }

    // Skip CRC16 if present (FHCRC flag = 0x02)
    if flags & 0x02 != 0 {
        pos += 2;
    }

    if pos > data.len() - 8 {
        return Err(DecompressError::InvalidHeader);
    }

    // The deflate data is between header and 8-byte trailer.
    let deflate_data = &data[pos..data.len() - 8];

    // ISIZE (last 4 bytes of the gzip trailer) is the uncompressed size modulo
    // 2^32. We use it as an early bound check.
    let advertised_size = u32::from_le_bytes([
        data[data.len() - 4],
        data[data.len() - 3],
        data[data.len() - 2],
        data[data.len() - 1],
    ]) as usize;
    if advertised_size > max_output_bytes {
        return Err(DecompressError::OutputTooLarge {
            advertised: advertised_size,
            limit: max_output_bytes,
        });
    }

    // Decompress using miniz_oxide
    miniz_oxide::inflate::decompress_to_vec_zlib_with_limit(deflate_data, max_output_bytes)
        .or_else(|_| {
            // Try raw deflate (without zlib header)
            miniz_oxide::inflate::decompress_to_vec_with_limit(deflate_data, max_output_bytes)
        })
        .map_err(|_| DecompressError::DecompressFailed)
}

/// Decompress gzip data, or return the input unchanged.
///
/// Returns `Cow::Borrowed` when no decompression is needed, avoiding an
/// allocation on the hot path for uncompressed messages.
pub fn decompress_if_gzip(data: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    if is_gzip(data) {
        match decompress_gzip(data) {
            Ok(decompressed) => std::borrow::Cow::Owned(decompressed),
            Err(_) => std::borrow::Cow::Borrowed(data),
        }
    } else {
        std::borrow::Cow::Borrowed(data)
    }
}

/// Decompression error types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecompressError {
    /// Data is too short to be valid gzip
    TooShort,
    /// Data doesn't have gzip magic bytes
    NotGzip,
    /// Invalid gzip header
    InvalidHeader,
    /// Decompression failed
    DecompressFailed,
    /// Advertised decompressed payload exceeds configured limit
    OutputTooLarge { advertised: usize, limit: usize },
}

impl std::fmt::Display for DecompressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => write!(f, "Data too short for gzip"),
            Self::NotGzip => write!(f, "Not gzip compressed"),
            Self::InvalidHeader => write!(f, "Invalid gzip header"),
            Self::DecompressFailed => write!(f, "Decompression failed"),
            Self::OutputTooLarge { advertised, limit } => {
                write!(
                    f,
                    "Decompressed payload too large (advertised {} bytes > limit {} bytes)",
                    advertised, limit
                )
            },
        }
    }
}

impl std::error::Error for DecompressError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_gzip() {
        assert!(is_gzip(&[0x1f, 0x8b, 0x08]));
        assert!(!is_gzip(&[0x00, 0x00]));
        assert!(!is_gzip(&[0x1f]));
        assert!(!is_gzip(&[]));
    }

    #[test]
    fn test_decompress_if_gzip_plain() {
        let plain = b"Hello, World!";
        let result = decompress_if_gzip(plain);
        assert_eq!(&*result, plain);
    }

    #[test]
    fn test_decompress_if_gzip_plain_returns_borrowed() {
        // Non-gzip data should return a Cow::Borrowed (zero-copy).
        let plain = b"not compressed";
        let result = decompress_if_gzip(plain);
        assert!(matches!(result, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn test_decompress_if_gzip_empty() {
        let result = decompress_if_gzip(&[]);
        assert!(result.is_empty());
        assert!(matches!(result, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn test_decompress_gzip_with_limit_rejects_large_advertised_size() {
        // Minimal gzip payload with empty deflate body and a forged trailer
        // advertising a large uncompressed size.
        let mut payload = vec![0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0, 0x03];
        payload.extend_from_slice(&[0, 0, 0, 0]); // CRC32
        payload.extend_from_slice(&(1024u32).to_le_bytes()); // ISIZE

        let err =
            decompress_gzip_with_limit(&payload, 16).expect_err("must reject oversize payload");
        assert!(matches!(
            err,
            DecompressError::OutputTooLarge {
                advertised: 1024,
                limit: 16
            }
        ));
    }
}
