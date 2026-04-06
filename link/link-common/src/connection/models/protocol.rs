use serde::{Deserialize, Serialize};

/// Wire-format serialization type negotiated during authentication.
///
/// Mirrors `SerializationType` from `kalamdb-commons`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SerializationType {
    /// JSON text frames (default, backward-compatible).
    #[default]
    Json,
    /// MessagePack binary frames.
    #[serde(rename = "msgpack")]
    MessagePack,
}

/// Wire-format compression negotiated during authentication.
///
/// Mirrors `CompressionType` from `kalamdb-commons`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    /// No compression.
    None,
    /// Gzip compression for payloads above threshold (default).
    #[default]
    Gzip,
}

/// Protocol options negotiated once per connection during authentication.
///
/// Mirrors `ProtocolOptions` from `kalamdb-commons`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolOptions {
    /// Serialization format for messages after auth.
    pub serialization: SerializationType,
    /// Compression policy.
    pub compression: CompressionType,
}

impl Default for ProtocolOptions {
    fn default() -> Self {
        Self {
            serialization: SerializationType::Json,
            compression: CompressionType::Gzip,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_type_json_roundtrip() {
        let ser = SerializationType::Json;
        let json = serde_json::to_string(&ser).unwrap();
        assert_eq!(json, "\"json\"");
        let parsed: SerializationType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, SerializationType::Json);
    }

    #[test]
    fn test_serialization_type_msgpack_roundtrip() {
        let ser = SerializationType::MessagePack;
        let json = serde_json::to_string(&ser).unwrap();
        assert_eq!(json, "\"msgpack\"");
        let parsed: SerializationType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, SerializationType::MessagePack);
    }

    #[test]
    fn test_protocol_options_default() {
        let opts = ProtocolOptions::default();
        assert_eq!(opts.serialization, SerializationType::Json);
        assert_eq!(opts.compression, CompressionType::Gzip);
    }

    #[test]
    fn test_protocol_options_roundtrip() {
        let opts = ProtocolOptions {
            serialization: SerializationType::MessagePack,
            compression: CompressionType::None,
        };
        let json = serde_json::to_string(&opts).unwrap();
        let parsed: ProtocolOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, opts);
    }
}
