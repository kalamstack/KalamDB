//! Starting position for consumption

use serde::Deserialize;

/// Starting position for consumption
#[derive(Debug, Default)]
pub enum StartPosition {
    /// Start from latest message
    #[default]
    Latest,
    /// Start from earliest message
    Earliest,
    /// Start from specific offset
    Offset { offset: u64 },
}

impl StartPosition {
    fn variant(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "latest" => Some(Self::Latest),
            "earliest" => Some(Self::Earliest),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for StartPosition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        use serde_json::Value;

        let v = Value::deserialize(deserializer)?;
        match v {
            Value::String(s) => Self::variant(&s)
                .ok_or_else(|| D::Error::custom(format!("Invalid start position: {}", s))),
            Value::Object(map) => {
                if let Some(Value::Number(n)) = map.get("Offset").or(map.get("offset")) {
                    n.as_u64()
                        .map(|o| StartPosition::Offset { offset: o })
                        .ok_or_else(|| D::Error::custom("Offset must be a positive integer"))
                } else {
                    Err(D::Error::custom("Expected 'Offset' key with numeric value"))
                }
            },
            _ => Err(D::Error::custom(
                "Expected string ('Latest', 'Earliest') or object {'Offset': n}",
            )),
        }
    }
}
