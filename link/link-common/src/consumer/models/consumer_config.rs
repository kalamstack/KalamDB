use std::{collections::HashMap, time::Duration};

use crate::error::{KalamLinkError, Result};

use super::AutoOffsetReset;

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub group_id: String,
    pub client_id: Option<String>,
    pub topic: String,
    pub auto_offset_reset: AutoOffsetReset,
    pub enable_auto_commit: bool,
    pub auto_commit_interval: Duration,
    pub max_poll_records: u32,
    pub poll_timeout: Duration,
    pub partition_id: u32,
    pub request_timeout: Duration,
    pub retry_backoff: Duration,
}

impl ConsumerConfig {
    pub fn new(group_id: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            client_id: None,
            topic: topic.into(),
            auto_offset_reset: AutoOffsetReset::Latest,
            enable_auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            max_poll_records: 100,
            poll_timeout: Duration::from_secs(30),
            partition_id: 0,
            request_timeout: Duration::from_secs(30),
            retry_backoff: Duration::from_millis(500),
        }
    }

    pub fn from_map(map: &HashMap<String, String>) -> Result<Self> {
        let group_id = get_str(map, "group_id", "group.id")
            .ok_or_else(|| KalamLinkError::ConfigurationError("group_id is required".into()))?;
        let topic = get_str(map, "topic", "topic")
            .ok_or_else(|| KalamLinkError::ConfigurationError("topic is required".into()))?;

        let mut config = Self::new(group_id, topic);

        config.client_id = get_str(map, "client_id", "client.id");
        if let Some(value) = get_str(map, "auto_offset_reset", "auto.offset.reset") {
            config.auto_offset_reset = parse_auto_offset(&value)?;
        }
        if let Some(value) = get_str(map, "enable_auto_commit", "enable.auto.commit") {
            config.enable_auto_commit = parse_bool(&value)?;
        }
        if let Some(value) = get_str(map, "auto_commit_interval", "auto.commit.interval.ms") {
            config.auto_commit_interval = parse_duration_ms(&value)?;
        }
        if let Some(value) = get_str(map, "max_poll_records", "max.poll.records") {
            config.max_poll_records = parse_u32(&value, "max_poll_records")?;
        }
        if let Some(value) = get_str(map, "poll_timeout", "fetch.max.wait.ms") {
            config.poll_timeout = parse_duration_ms(&value)?;
        }
        if let Some(value) = get_str(map, "partition_id", "partition.id") {
            config.partition_id = parse_u32(&value, "partition_id")?;
        }
        if let Some(value) = get_str(map, "request_timeout", "request.timeout.ms") {
            config.request_timeout = parse_duration_ms(&value)?;
        }
        if let Some(value) = get_str(map, "retry_backoff", "retry.backoff.ms") {
            config.retry_backoff = parse_duration_ms(&value)?;
        }

        Ok(config)
    }
}

fn get_str(map: &HashMap<String, String>, key: &str, alias: &str) -> Option<String> {
    map.get(key).cloned().or_else(|| map.get(alias).cloned())
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.to_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" => Ok(false),
        _ => Err(KalamLinkError::ConfigurationError(format!("Invalid boolean value: {}", value))),
    }
}

fn parse_u32(value: &str, field: &str) -> Result<u32> {
    value.parse::<u32>().map_err(|_| {
        KalamLinkError::ConfigurationError(format!("Invalid {} value: {}", field, value))
    })
}

fn parse_duration_ms(value: &str) -> Result<Duration> {
    let millis = value.parse::<u64>().map_err(|_| {
        KalamLinkError::ConfigurationError(format!("Invalid duration (ms) value: {}", value))
    })?;
    Ok(Duration::from_millis(millis))
}

fn parse_auto_offset(value: &str) -> Result<AutoOffsetReset> {
    let lower = value.to_lowercase();
    if lower == "earliest" {
        return Ok(AutoOffsetReset::Earliest);
    }
    if lower == "latest" {
        return Ok(AutoOffsetReset::Latest);
    }
    if let Some(offset_str) = lower.strip_prefix("offset") {
        let trimmed = offset_str
            .trim_start_matches(':')
            .trim_start_matches('=')
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim();
        let offset = trimmed.parse::<u64>().map_err(|_| {
            KalamLinkError::ConfigurationError(format!(
                "Invalid auto.offset.reset offset: {}",
                value
            ))
        })?;
        return Ok(AutoOffsetReset::Offset(offset));
    }

    Err(KalamLinkError::ConfigurationError(format!(
        "Invalid auto.offset.reset value: {}",
        value
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn test_new_config_defaults() {
        let config = ConsumerConfig::new("my-group", "my-topic");
        assert_eq!(config.group_id, "my-group");
        assert_eq!(config.topic, "my-topic");
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Latest);
        assert!(config.enable_auto_commit);
        assert_eq!(config.auto_commit_interval, Duration::from_secs(5));
        assert_eq!(config.max_poll_records, 100);
        assert_eq!(config.poll_timeout, Duration::from_secs(30));
        assert_eq!(config.partition_id, 0);
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.retry_backoff, Duration::from_millis(500));
    }

    #[test]
    fn test_from_map_snake_case() {
        let map = make_map(&[
            ("group_id", "test-group"),
            ("topic", "test-topic"),
            ("auto_offset_reset", "earliest"),
            ("enable_auto_commit", "false"),
            ("max_poll_records", "50"),
        ]);

        let config = ConsumerConfig::from_map(&map).unwrap();
        assert_eq!(config.group_id, "test-group");
        assert_eq!(config.topic, "test-topic");
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!config.enable_auto_commit);
        assert_eq!(config.max_poll_records, 50);
    }

    #[test]
    fn test_from_map_kafka_aliases() {
        let map = make_map(&[
            ("group.id", "kafka-group"),
            ("topic", "kafka-topic"),
            ("auto.offset.reset", "latest"),
            ("enable.auto.commit", "true"),
            ("auto.commit.interval.ms", "3000"),
            ("max.poll.records", "200"),
            ("fetch.max.wait.ms", "15000"),
            ("partition.id", "2"),
            ("request.timeout.ms", "10000"),
            ("retry.backoff.ms", "250"),
        ]);

        let config = ConsumerConfig::from_map(&map).unwrap();
        assert_eq!(config.group_id, "kafka-group");
        assert_eq!(config.topic, "kafka-topic");
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Latest);
        assert!(config.enable_auto_commit);
        assert_eq!(config.auto_commit_interval, Duration::from_millis(3000));
        assert_eq!(config.max_poll_records, 200);
        assert_eq!(config.poll_timeout, Duration::from_millis(15000));
        assert_eq!(config.partition_id, 2);
        assert_eq!(config.request_timeout, Duration::from_millis(10000));
        assert_eq!(config.retry_backoff, Duration::from_millis(250));
    }

    #[test]
    fn test_snake_case_wins_over_kafka_alias() {
        let map = make_map(&[
            ("group_id", "snake-group"),
            ("group.id", "kafka-group"),
            ("topic", "test-topic"),
        ]);

        let config = ConsumerConfig::from_map(&map).unwrap();
        assert_eq!(config.group_id, "snake-group");
    }

    #[test]
    fn test_missing_group_id_error() {
        let map = make_map(&[("topic", "test-topic")]);
        let result = ConsumerConfig::from_map(&map);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("group_id"));
    }

    #[test]
    fn test_missing_topic_error() {
        let map = make_map(&[("group_id", "test-group")]);
        let result = ConsumerConfig::from_map(&map);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("topic"));
    }

    #[test]
    fn test_invalid_bool_error() {
        let map = make_map(&[
            ("group_id", "test-group"),
            ("topic", "test-topic"),
            ("enable_auto_commit", "maybe"),
        ]);
        let result = ConsumerConfig::from_map(&map);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Invalid boolean"));
    }

    #[test]
    fn test_invalid_duration_error() {
        let map = make_map(&[
            ("group_id", "test-group"),
            ("topic", "test-topic"),
            ("auto_commit_interval", "not-a-number"),
        ]);
        let result = ConsumerConfig::from_map(&map);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Invalid duration"));
    }

    #[test]
    fn test_parse_auto_offset_earliest() {
        assert_eq!(parse_auto_offset("earliest").unwrap(), AutoOffsetReset::Earliest);
        assert_eq!(parse_auto_offset("EARLIEST").unwrap(), AutoOffsetReset::Earliest);
    }

    #[test]
    fn test_parse_auto_offset_latest() {
        assert_eq!(parse_auto_offset("latest").unwrap(), AutoOffsetReset::Latest);
        assert_eq!(parse_auto_offset("LATEST").unwrap(), AutoOffsetReset::Latest);
    }

    #[test]
    fn test_parse_auto_offset_explicit() {
        assert_eq!(parse_auto_offset("offset:123").unwrap(), AutoOffsetReset::Offset(123));
        assert_eq!(parse_auto_offset("offset=456").unwrap(), AutoOffsetReset::Offset(456));
        assert_eq!(parse_auto_offset("offset(789)").unwrap(), AutoOffsetReset::Offset(789));
        assert_eq!(parse_auto_offset("OFFSET:999").unwrap(), AutoOffsetReset::Offset(999));
    }

    #[test]
    fn test_parse_auto_offset_invalid() {
        assert!(parse_auto_offset("none").is_err());
        assert!(parse_auto_offset("random").is_err());
        assert!(parse_auto_offset("offset:abc").is_err());
    }

    #[test]
    fn test_parse_bool_variants() {
        assert!(parse_bool("true").unwrap());
        assert!(parse_bool("TRUE").unwrap());
        assert!(parse_bool("1").unwrap());
        assert!(parse_bool("yes").unwrap());
        assert!(parse_bool("y").unwrap());

        assert!(!parse_bool("false").unwrap());
        assert!(!parse_bool("FALSE").unwrap());
        assert!(!parse_bool("0").unwrap());
        assert!(!parse_bool("no").unwrap());
        assert!(!parse_bool("n").unwrap());

        assert!(parse_bool("invalid").is_err());
    }

    #[test]
    fn test_client_id_optional() {
        let map = make_map(&[
            ("group_id", "test-group"),
            ("topic", "test-topic"),
            ("client_id", "my-client"),
        ]);
        let config = ConsumerConfig::from_map(&map).unwrap();
        assert_eq!(config.client_id, Some("my-client".to_string()));

        let map_without = make_map(&[("group_id", "test-group"), ("topic", "test-topic")]);
        let config_without = ConsumerConfig::from_map(&map_without).unwrap();
        assert!(config_without.client_id.is_none());
    }
}
