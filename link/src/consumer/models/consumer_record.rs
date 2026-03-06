use serde::{Deserialize, Serialize};

use super::{PayloadMode, TopicOp};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerRecord {
    pub topic_id: String,
    pub topic_name: String,
    pub partition_id: u32,
    pub offset: u64,
    pub message_id: Option<String>,
    pub source_table: String,
    pub op: TopicOp,
    pub timestamp_ms: u64,
    pub payload_mode: PayloadMode,
    pub payload: Vec<u8>,
}

#[cfg(feature = "tokio-runtime")]
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ConsumerRecordWire {
    pub topic_id: String,
    pub partition_id: u32,
    pub offset: u64,
    #[serde(default, alias = "key")]
    pub message_id: Option<String>,
    #[serde(default)]
    pub source_table: Option<String>,
    #[serde(default)]
    pub op: Option<TopicOp>,
    #[serde(default, rename = "timestamp_ms", alias = "ts")]
    pub timestamp_ms: u64,
    #[serde(default)]
    pub payload_mode: Option<PayloadMode>,
    #[serde(default, with = "base64_bytes")]
    pub payload: Vec<u8>,
}

#[cfg(feature = "tokio-runtime")]
impl ConsumerRecordWire {
    pub fn into_record(self, topic_name: String) -> ConsumerRecord {
        ConsumerRecord {
            topic_id: self.topic_id,
            topic_name,
            partition_id: self.partition_id,
            offset: self.offset,
            message_id: self.message_id,
            source_table: self.source_table.unwrap_or_default(),
            op: self.op.unwrap_or(TopicOp::Insert),
            timestamp_ms: self.timestamp_ms,
            payload_mode: self.payload_mode.unwrap_or(PayloadMode::Full),
            payload: self.payload,
        }
    }
}

#[cfg(feature = "tokio-runtime")]
mod base64_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    #[allow(dead_code)]
    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = STANDARD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded.as_bytes()).map_err(D::Error::custom)
    }
}
