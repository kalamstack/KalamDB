//! Topic pub/sub SQL command parsers.
//!
//! This module handles parsing of topic-related SQL commands:
//! - CREATE TOPIC: Define a new pub/sub topic
//! - DROP TOPIC: Remove a topic
//! - ALTER TOPIC ADD SOURCE: Add a table route to a topic
//! - CONSUME FROM: Consume messages from a topic

use kalamdb_commons::models::{PayloadMode, TableId, TopicOp};

use crate::{
    parser::utils::{extract_identifier, extract_keyword_value, normalize_sql},
    DdlAst,
};

/// CREATE TOPIC statement
///
/// Syntax:
/// ```sql
/// CREATE TOPIC <topic_name> [PARTITIONS <count>];
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTopicStatement {
    pub topic_name: String,
    pub partitions: Option<u32>,
}

/// DROP TOPIC statement
///
/// Syntax:
/// ```sql
/// DROP TOPIC <topic_name>;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct DropTopicStatement {
    pub topic_name: String,
}

/// CLEAR TOPIC statement
///
/// Syntax:
/// ```sql
/// CLEAR TOPIC <topic_name>;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ClearTopicStatement {
    pub topic_id: kalamdb_commons::models::TopicId,
}

/// ALTER TOPIC ADD SOURCE statement
///
/// Syntax:
/// ```sql
/// ALTER TOPIC <topic_name>
/// ADD SOURCE <table_name>
/// ON <operation>
/// [WHERE <filter>]
/// [WITH (payload = '<mode>')];
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct AddTopicSourceStatement {
    pub topic_name: String,
    pub table_id: TableId,
    pub operation: TopicOp,
    pub filter_expr: Option<String>,
    pub payload_mode: PayloadMode,
}

/// CONSUME FROM statement
///
/// Syntax:
/// ```sql
/// CONSUME FROM <topic_name>
/// [GROUP '<group_id>']
/// [FROM <position>]
/// [LIMIT <count>];
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumeStatement {
    pub topic_name: String,
    pub group_id: Option<String>,
    pub position: ConsumePosition,
    pub limit: Option<u64>,
}

/// Position to start consuming from
#[derive(Debug, Clone, PartialEq)]
pub enum ConsumePosition {
    /// Start from earliest available message
    Earliest,
    /// Start from latest message (only new messages)
    Latest,
    /// Start from specific offset
    Offset(u64),
}

/// ACK statement for committing consumer group offsets
///
/// Syntax:
/// ```sql
/// ACK <topic_name>
/// GROUP '<group_id>'
/// [PARTITION <partition_id>]
/// UPTO OFFSET <offset>;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct AckStatement {
    pub topic_name: String,
    pub group_id: String,
    pub partition_id: u32,
    pub upto_offset: u64,
}

// Implement DdlAst trait for all topic statement types
impl DdlAst for CreateTopicStatement {}
impl DdlAst for DropTopicStatement {}
impl DdlAst for ClearTopicStatement {}
impl DdlAst for AddTopicSourceStatement {}
impl DdlAst for ConsumeStatement {}
impl DdlAst for AckStatement {}

/// Parse CREATE TOPIC statement
///
/// Syntax: CREATE TOPIC <name> [PARTITIONS <count>]
pub fn parse_create_topic(sql: &str) -> Result<CreateTopicStatement, String> {
    let normalized = normalize_sql(sql);
    let sql_upper = normalized.to_uppercase();

    if !sql_upper.starts_with("CREATE TOPIC") {
        return Err("Expected CREATE TOPIC".to_string());
    }

    // Extract topic name (3rd token: CREATE TOPIC <name>)
    let topic_name = extract_identifier(&normalized, 2)?;

    // Optional: PARTITIONS <count>
    let partitions = if sql_upper.contains("PARTITIONS") {
        let count_str = extract_keyword_value(&normalized, "PARTITIONS")?;
        Some(
            count_str
                .parse::<u32>()
                .map_err(|_| "Partition count must be a positive integer".to_string())?,
        )
    } else {
        None
    };

    Ok(CreateTopicStatement {
        topic_name,
        partitions,
    })
}

/// Parse DROP TOPIC statement
///
/// Syntax: DROP TOPIC <name>
pub fn parse_drop_topic(sql: &str) -> Result<DropTopicStatement, String> {
    let normalized = normalize_sql(sql);
    let sql_upper = normalized.to_uppercase();

    if !sql_upper.starts_with("DROP TOPIC") {
        return Err("Expected DROP TOPIC".to_string());
    }

    // Extract topic name (3rd token: DROP TOPIC <name>)
    let topic_name = extract_identifier(&normalized, 2)?;

    Ok(DropTopicStatement { topic_name })
}

/// Parse CLEAR TOPIC statement
///
/// Syntax: CLEAR TOPIC <name>
pub fn parse_clear_topic(sql: &str) -> Result<ClearTopicStatement, String> {
    let normalized = normalize_sql(sql);
    let sql_upper = normalized.to_uppercase();

    if !sql_upper.starts_with("CLEAR TOPIC") {
        return Err("Expected CLEAR TOPIC".to_string());
    }

    // Extract topic name (3rd token: CLEAR TOPIC <name>)
    let topic_name = extract_identifier(&normalized, 2)?;

    Ok(ClearTopicStatement {
        topic_id: kalamdb_commons::models::TopicId::new(topic_name),
    })
}

/// Parse ALTER TOPIC ADD SOURCE statement
///
/// Syntax: ALTER TOPIC <name> ADD SOURCE <table> ON <op> [WHERE ...] [WITH (...)]
pub fn parse_alter_topic_add_source(sql: &str) -> Result<AddTopicSourceStatement, String> {
    let normalized = normalize_sql(sql);
    let sql_upper = normalized.to_uppercase();

    if !sql_upper.starts_with("ALTER TOPIC") {
        return Err("Expected ALTER TOPIC".to_string());
    }

    // Extract topic name (3rd token)
    let topic_name = extract_identifier(&normalized, 2)?;

    // Extract table name after SOURCE
    let table_str = extract_keyword_value(&normalized, "SOURCE")?;
    let table_id = parse_table_id(&table_str)?;

    // Extract operation after ON
    let operation = parse_topic_operation(&extract_keyword_value(&normalized, "ON")?)?;

    // Optional: WHERE clause (extract everything between WHERE and WITH/end)
    let filter_expr = if sql_upper.contains(" WHERE ") {
        Some(extract_where_clause(&normalized)?)
    } else {
        None
    };

    // Optional: WITH clause for payload mode
    let payload_mode = if sql_upper.contains(" WITH ") {
        parse_payload_mode_from_sql(&normalized)?
    } else {
        PayloadMode::Full // Default
    };

    Ok(AddTopicSourceStatement {
        topic_name,
        table_id,
        operation,
        filter_expr,
        payload_mode,
    })
}

/// Parse CONSUME FROM statement
///
/// Syntax: CONSUME FROM <topic> [GROUP '<id>'] [FROM <pos>] [LIMIT <n>]
pub fn parse_consume(sql: &str) -> Result<ConsumeStatement, String> {
    let normalized = normalize_sql(sql);
    let sql_upper = normalized.to_uppercase();

    if !sql_upper.starts_with("CONSUME FROM") {
        return Err("Expected CONSUME FROM".to_string());
    }

    // Extract topic name (3rd token: CONSUME FROM <topic>)
    let topic_name = extract_identifier(&normalized, 2)?;

    // Optional: GROUP '<group_id>'
    let group_id = if sql_upper.contains(" GROUP ") {
        Some(extract_group_id(&normalized)?)
    } else {
        None
    };

    // Optional: FROM <position>
    let position = if sql_upper.contains(" FROM ") && sql_upper.split(" FROM ").count() > 2 {
        // Second FROM is for position
        parse_consume_position_from_sql(&normalized)?
    } else {
        ConsumePosition::Latest // Default
    };

    // Optional: LIMIT <count>
    let limit = if sql_upper.contains(" LIMIT ") {
        let limit_str = extract_keyword_value(&normalized, "LIMIT")?;
        Some(
            limit_str
                .parse::<u64>()
                .map_err(|_| "LIMIT must be a positive integer".to_string())?,
        )
    } else {
        None
    };

    Ok(ConsumeStatement {
        topic_name,
        group_id,
        position,
        limit,
    })
}

/// Parse ACK statement
///
/// Syntax: ACK <topic> GROUP '<group>' [PARTITION <n>] UPTO OFFSET <offset>
pub fn parse_ack(sql: &str) -> Result<AckStatement, String> {
    let normalized = normalize_sql(sql);
    let sql_upper = normalized.to_uppercase();

    if !sql_upper.starts_with("ACK ") {
        return Err("Expected ACK statement".to_string());
    }

    // Extract topic name (2nd token: ACK <topic>)
    let topic_name = extract_identifier(&normalized, 1)?;

    // Required: GROUP '<group_id>'
    if !sql_upper.contains(" GROUP ") {
        return Err("ACK requires GROUP clause".to_string());
    }
    let group_id = extract_group_id(&normalized)?;

    // Optional: PARTITION <n> (default 0)
    let partition_id = if sql_upper.contains(" PARTITION ") {
        let partition_str = extract_keyword_value(&normalized, "PARTITION")?;
        partition_str
            .parse::<u32>()
            .map_err(|_| "PARTITION must be a non-negative integer".to_string())?
    } else {
        0 // Default partition
    };

    // Required: UPTO OFFSET <offset>
    if !sql_upper.contains(" UPTO ") || !sql_upper.contains(" OFFSET ") {
        return Err("ACK requires UPTO OFFSET clause".to_string());
    }

    // Extract offset value after "OFFSET"
    let offset_pos = sql_upper.find(" OFFSET ").ok_or_else(|| "OFFSET not found".to_string())?;
    let after_offset = normalized[offset_pos + 8..].trim();
    let offset_str = after_offset
        .split_whitespace()
        .next()
        .ok_or_else(|| "Missing offset value".to_string())?;
    let upto_offset = offset_str
        .trim_end_matches(';')
        .parse::<u64>()
        .map_err(|_| format!("Invalid offset '{}'. Must be a non-negative integer", offset_str))?;

    Ok(AckStatement {
        topic_name,
        group_id,
        partition_id,
        upto_offset,
    })
}

// Helper functions

// TODO: We aready have a method inside tableId for this. Refactor to use that.
fn parse_table_id(table_str: &str) -> Result<TableId, String> {
    // Support both "table" and "namespace.table" formats
    if table_str.contains('.') {
        let parts: Vec<&str> = table_str.split('.').collect();
        if parts.len() == 2 {
            Ok(TableId::from_strings(parts[0], parts[1]))
        } else {
            Err("Table name must be 'namespace.table' format".to_string())
        }
    } else {
        // Default namespace
        Ok(TableId::from_strings("default", table_str))
    }
}

fn parse_topic_operation(op_str: &str) -> Result<TopicOp, String> {
    match op_str.to_uppercase().as_str() {
        "INSERT" => Ok(TopicOp::Insert),
        "UPDATE" => Ok(TopicOp::Update),
        "DELETE" => Ok(TopicOp::Delete),
        op => Err(format!("Invalid operation '{}'. Expected INSERT, UPDATE, or DELETE", op)),
    }
}

fn extract_where_clause(sql: &str) -> Result<String, String> {
    let sql_upper = sql.to_uppercase();
    let where_pos =
        sql_upper.find(" WHERE ").ok_or_else(|| "WHERE clause not found".to_string())?;

    let after_where = &sql[where_pos + 7..]; // Skip " WHERE "

    // Find end of WHERE clause (WITH keyword or end of string)
    let end_pos = after_where.to_uppercase().find(" WITH ").unwrap_or(after_where.len());

    Ok(after_where[..end_pos].trim().to_string())
}

fn parse_payload_mode_from_sql(sql: &str) -> Result<PayloadMode, String> {
    let sql_upper = sql.to_uppercase();
    let with_pos = sql_upper.find(" WITH ").ok_or_else(|| "WITH clause not found".to_string())?;

    let after_with = &sql[with_pos + 6..]; // Skip " WITH "

    // Extract payload mode from WITH (payload = 'mode') or WITH (payload='mode')
    if !after_with.contains("PAYLOAD") && !after_with.to_uppercase().contains("PAYLOAD") {
        return Err("Expected 'payload' parameter in WITH clause".to_string());
    }

    // Find the value after =
    let eq_pos = after_with.find('=').ok_or_else(|| "Expected '=' after 'payload'".to_string())?;

    let after_eq = &after_with[eq_pos + 1..].trim();

    // Extract quoted or unquoted value
    let mode_str = if after_eq.starts_with('\'') {
        let end_quote = after_eq[1..]
            .find('\'')
            .ok_or_else(|| "Unclosed quote in payload mode".to_string())?;
        &after_eq[1..end_quote + 1]
    } else if after_eq.starts_with('"') {
        let end_quote = after_eq[1..]
            .find('"')
            .ok_or_else(|| "Unclosed quote in payload mode".to_string())?;
        &after_eq[1..end_quote + 1]
    } else {
        // Unquoted - take until space or )
        let end_pos =
            after_eq.find(|c: char| c.is_whitespace() || c == ')').unwrap_or(after_eq.len());
        &after_eq[..end_pos]
    };

    match mode_str.to_lowercase().as_str() {
        "key" => Ok(PayloadMode::Key),
        "full" => Ok(PayloadMode::Full),
        "diff" => Ok(PayloadMode::Diff),
        m => Err(format!("Invalid payload mode '{}'. Expected 'key', 'full', or 'diff'", m)),
    }
}

fn extract_group_id(sql: &str) -> Result<String, String> {
    let sql_upper = sql.to_uppercase();
    let group_pos =
        sql_upper.find(" GROUP ").ok_or_else(|| "GROUP keyword not found".to_string())?;

    let after_group = &sql[group_pos + 7..]; // Skip " GROUP "

    // Extract quoted or unquoted identifier
    let group_id = if after_group.starts_with('\'') {
        let end_quote = after_group[1..]
            .find('\'')
            .ok_or_else(|| "Unclosed quote in group ID".to_string())?;
        after_group[1..end_quote + 1].to_string()
    } else if after_group.starts_with('"') {
        let end_quote = after_group[1..]
            .find('"')
            .ok_or_else(|| "Unclosed quote in group ID".to_string())?;
        after_group[1..end_quote + 1].to_string()
    } else {
        // Unquoted - take until space
        let end_pos = after_group.find(char::is_whitespace).unwrap_or(after_group.len());
        after_group[..end_pos].to_string()
    };

    Ok(group_id)
}

fn parse_consume_position_from_sql(sql: &str) -> Result<ConsumePosition, String> {
    let sql_upper = sql.to_uppercase();

    // Find the second FROM (first is "CONSUME FROM")
    let first_from_pos = sql_upper.find(" FROM ").ok_or_else(|| "FROM not found".to_string())?;
    let remaining = &sql_upper[first_from_pos + 6..];
    let second_from_pos = remaining.find(" FROM ");

    if let Some(pos) = second_from_pos {
        let after_from = remaining[pos + 6..].trim();
        let position_word = after_from
            .split_whitespace()
            .next()
            .ok_or_else(|| "Missing position after FROM".to_string())?;

        match position_word {
            "EARLIEST" => Ok(ConsumePosition::Earliest),
            "LATEST" => Ok(ConsumePosition::Latest),
            num => {
                let offset = num
                    .parse::<u64>()
                    .map_err(|_| format!("Invalid offset '{}'. Must be a number", num))?;
                Ok(ConsumePosition::Offset(offset))
            },
        }
    } else {
        Ok(ConsumePosition::Latest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_topic() {
        let stmt = parse_create_topic("CREATE TOPIC app.notifications").unwrap();
        assert_eq!(stmt.topic_name, "app.notifications");
        assert_eq!(stmt.partitions, None);
    }

    #[test]
    fn test_parse_create_topic_with_partitions() {
        let stmt = parse_create_topic("CREATE TOPIC app.events PARTITIONS 4").unwrap();
        assert_eq!(stmt.topic_name, "app.events");
        assert_eq!(stmt.partitions, Some(4));
    }

    #[test]
    fn test_parse_drop_topic() {
        let stmt = parse_drop_topic("DROP TOPIC app.old_topic").unwrap();
        assert_eq!(stmt.topic_name, "app.old_topic");
    }

    #[test]
    fn test_parse_consume_basic() {
        let stmt = parse_consume("CONSUME FROM app.messages").unwrap();
        assert_eq!(stmt.topic_name, "app.messages");
        assert_eq!(stmt.group_id, None);
        assert_eq!(stmt.position, ConsumePosition::Latest);
        assert_eq!(stmt.limit, None);
    }

    #[test]
    fn test_parse_consume_with_group() {
        let stmt = parse_consume("CONSUME FROM app.messages GROUP 'ai-service'").unwrap();
        assert_eq!(stmt.topic_name, "app.messages");
        assert_eq!(stmt.group_id, Some("ai-service".to_string()));
    }

    #[test]
    fn test_parse_consume_with_position() {
        let stmt = parse_consume("CONSUME FROM app.messages FROM EARLIEST LIMIT 100").unwrap();
        assert_eq!(stmt.position, ConsumePosition::Earliest);
        assert_eq!(stmt.limit, Some(100));
    }

    #[test]
    fn test_parse_ack_basic() {
        let stmt = parse_ack("ACK app.messages GROUP 'ai-service' UPTO OFFSET 100").unwrap();
        assert_eq!(stmt.topic_name, "app.messages");
        assert_eq!(stmt.group_id, "ai-service");
        assert_eq!(stmt.partition_id, 0);
        assert_eq!(stmt.upto_offset, 100);
    }

    #[test]
    fn test_parse_ack_with_partition() {
        let stmt =
            parse_ack("ACK app.messages GROUP 'ai-service' PARTITION 2 UPTO OFFSET 500").unwrap();
        assert_eq!(stmt.topic_name, "app.messages");
        assert_eq!(stmt.group_id, "ai-service");
        assert_eq!(stmt.partition_id, 2);
        assert_eq!(stmt.upto_offset, 500);
    }
}
