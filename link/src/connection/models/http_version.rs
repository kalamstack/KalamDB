use serde::{Deserialize, Serialize};

/// HTTP protocol version to use for connections.
///
/// HTTP/2 provides benefits like multiplexing multiple requests over a single
/// connection, header compression, and improved performance for multiple
/// concurrent requests.
///
/// # Example
///
/// ```rust
/// use kalam_link::{ConnectionOptions, HttpVersion};
///
/// let options = ConnectionOptions::new()
///     .with_http_version(HttpVersion::Http2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
#[serde(rename_all = "lowercase")]
pub enum HttpVersion {
    /// HTTP/1.1 (default) - widely compatible, one request per connection
    #[default]
    #[serde(rename = "http1", alias = "http/1.1", alias = "1.1")]
    Http1,

    /// HTTP/2 - multiplexed requests, header compression, better performance
    #[serde(rename = "http2", alias = "http/2", alias = "2")]
    Http2,

    /// Automatic - let the client negotiate the best version with the server
    #[serde(rename = "auto")]
    Auto,
}
