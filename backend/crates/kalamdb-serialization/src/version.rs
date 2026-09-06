//! Protocol version constants for the Kalam object envelope.

/// Four-byte magic identifying a Kalam persisted object.
pub const MAGIC: [u8; 4] = *b"KOBJ";

/// Current Kalam Object Protocol version.
///
/// Bump only when the common envelope/global decoding contract changes.
pub const PROTOCOL_VERSION: u16 = 1;
