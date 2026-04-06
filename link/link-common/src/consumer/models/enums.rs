use serde::{de::Visitor, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
    Offset(u64),
}

impl Serialize for AutoOffsetReset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            AutoOffsetReset::Earliest => serializer.serialize_str("Earliest"),
            AutoOffsetReset::Latest => serializer.serialize_str("Latest"),
            AutoOffsetReset::Offset(value) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("Offset", value)?;
                map.end()
            },
        }
    }
}

impl<'de> Deserialize<'de> for AutoOffsetReset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AutoOffsetVisitor;

        impl<'de> Visitor<'de> for AutoOffsetVisitor {
            type Value = AutoOffsetReset;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Earliest, Latest, or { Offset: <u64> }")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value.to_lowercase().as_str() {
                    "earliest" => Ok(AutoOffsetReset::Earliest),
                    "latest" => Ok(AutoOffsetReset::Latest),
                    _ => Err(E::custom(format!("Invalid auto offset reset: {}", value))),
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&value)
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                if let Some((key, value)) = map.next_entry::<String, u64>()? {
                    if key.to_lowercase() == "offset" {
                        return Ok(AutoOffsetReset::Offset(value));
                    }
                    return Err(serde::de::Error::custom(format!("Invalid offset key: {}", key)));
                }
                Err(serde::de::Error::custom("Expected offset map"))
            }
        }

        deserializer.deserialize_any(AutoOffsetVisitor)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CommitMode {
    Auto,
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum PayloadMode {
    Key,
    Full,
    Diff,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum TopicOp {
    Insert,
    Update,
    Delete,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_offset_reset_serialize_earliest() {
        let value = AutoOffsetReset::Earliest;
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"Earliest\"");
    }

    #[test]
    fn test_auto_offset_reset_serialize_latest() {
        let value = AutoOffsetReset::Latest;
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "\"Latest\"");
    }

    #[test]
    fn test_auto_offset_reset_serialize_offset() {
        let value = AutoOffsetReset::Offset(12345);
        let json = serde_json::to_string(&value).unwrap();
        assert_eq!(json, "{\"Offset\":12345}");
    }

    #[test]
    fn test_auto_offset_reset_deserialize_earliest() {
        let value: AutoOffsetReset = serde_json::from_str("\"earliest\"").unwrap();
        assert_eq!(value, AutoOffsetReset::Earliest);

        let value: AutoOffsetReset = serde_json::from_str("\"Earliest\"").unwrap();
        assert_eq!(value, AutoOffsetReset::Earliest);
    }

    #[test]
    fn test_auto_offset_reset_deserialize_latest() {
        let value: AutoOffsetReset = serde_json::from_str("\"latest\"").unwrap();
        assert_eq!(value, AutoOffsetReset::Latest);

        let value: AutoOffsetReset = serde_json::from_str("\"Latest\"").unwrap();
        assert_eq!(value, AutoOffsetReset::Latest);
    }

    #[test]
    fn test_auto_offset_reset_deserialize_offset() {
        let value: AutoOffsetReset = serde_json::from_str("{\"Offset\": 999}").unwrap();
        assert_eq!(value, AutoOffsetReset::Offset(999));

        let value: AutoOffsetReset = serde_json::from_str("{\"offset\": 123}").unwrap();
        assert_eq!(value, AutoOffsetReset::Offset(123));
    }

    #[test]
    fn test_commit_mode_serialize() {
        assert_eq!(serde_json::to_string(&CommitMode::Auto).unwrap(), "\"auto\"");
        assert_eq!(serde_json::to_string(&CommitMode::Manual).unwrap(), "\"manual\"");
    }

    #[test]
    fn test_payload_mode_serialize() {
        assert_eq!(serde_json::to_string(&PayloadMode::Key).unwrap(), "\"Key\"");
        assert_eq!(serde_json::to_string(&PayloadMode::Full).unwrap(), "\"Full\"");
        assert_eq!(serde_json::to_string(&PayloadMode::Diff).unwrap(), "\"Diff\"");
    }

    #[test]
    fn test_topic_op_serialize() {
        assert_eq!(serde_json::to_string(&TopicOp::Insert).unwrap(), "\"Insert\"");
        assert_eq!(serde_json::to_string(&TopicOp::Update).unwrap(), "\"Update\"");
        assert_eq!(serde_json::to_string(&TopicOp::Delete).unwrap(), "\"Delete\"");
    }

    #[test]
    fn test_topic_op_deserialize() {
        let insert: TopicOp = serde_json::from_str("\"Insert\"").unwrap();
        assert_eq!(insert, TopicOp::Insert);

        let update: TopicOp = serde_json::from_str("\"Update\"").unwrap();
        assert_eq!(update, TopicOp::Update);

        let delete: TopicOp = serde_json::from_str("\"Delete\"").unwrap();
        assert_eq!(delete, TopicOp::Delete);
    }

    #[test]
    fn test_payload_mode_deserialize() {
        let key: PayloadMode = serde_json::from_str("\"Key\"").unwrap();
        assert_eq!(key, PayloadMode::Key);

        let full: PayloadMode = serde_json::from_str("\"Full\"").unwrap();
        assert_eq!(full, PayloadMode::Full);

        let diff: PayloadMode = serde_json::from_str("\"Diff\"").unwrap();
        assert_eq!(diff, PayloadMode::Diff);
    }
}
