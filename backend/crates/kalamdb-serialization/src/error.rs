//! Typed errors for the Kalam object protocol.

use thiserror::Error;

use crate::object::ObjectKind;

/// Errors returned by encode/decode of persisted objects.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SerializationError {
    /// Envelope did not start with the `KOBJ` magic.
    #[error("invalid object magic")]
    InvalidMagic,
    /// Bytes ended before a complete envelope or value.
    #[error("truncated persisted object")]
    Truncated,
    /// Envelope protocol version is newer than this crate understands.
    #[error("unsupported protocol version {found} (supported {supported})")]
    UnsupportedProtocolVersion { found: u16, supported: u16 },
    /// Caller asked for a different semantic object kind than the envelope.
    #[error("wrong object kind: expected {expected:?}, found {found:?}")]
    WrongObjectKind {
        expected: ObjectKind,
        found:    ObjectKind,
    },
    /// Payload could not be encoded.
    #[error("encode failed: {0}")]
    Encode(String),
    /// Payload could not be decoded.
    #[error("decode failed: {0}")]
    Decode(String),
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, SerializationError>;
