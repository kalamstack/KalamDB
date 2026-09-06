//! CREATE / DROP / ALTER TRIGGER parsers for durable topic delivery.

use kalamdb_commons::models::{NamespaceId, RoutineId, TopicId, TriggerId};

use crate::ddl::{create_type::split_qualified_ident, DdlResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTriggerStatement {
    pub trigger_id:       TriggerId,
    pub namespace_id:     NamespaceId,
    pub name:             String,
    pub topic_id:         TopicId,
    pub routine_id:       RoutineId,
    pub principal:        String,
    pub start_from:       String,
    pub retries:          i32,
    pub retry_backoff_ms: i64,
    pub concurrency:      i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTriggerStatement {
    pub trigger_id: TriggerId,
    pub if_exists:  bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTriggerStatement {
    pub trigger_id: TriggerId,
    pub enabled:    bool,
}

impl CreateTriggerStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("CREATE TRIGGER") {
            return Err("Expected CREATE TRIGGER statement".to_string());
        }
        let rest = trimmed["CREATE TRIGGER".len()..].trim_start();
        let (qual, after) = split_qualified_ident(rest)?;
        let namespace_id = qual.namespace_or(default_namespace);
        let name = qual.name.to_ascii_lowercase();
        let trigger_id = TriggerId::from_parts(Some(&namespace_id), &name);

        let after = after.trim_start();
        let after_upper = after.to_ascii_uppercase();
        if !after_upper.starts_with("ON TOPIC") {
            return Err("Expected ON TOPIC after trigger name".to_string());
        }
        let after = after["ON TOPIC".len()..].trim_start();
        let (topic_qual, after) = split_qualified_ident(after)?;
        let topic_schema = topic_qual.namespace_or(&namespace_id);
        let topic_id = TopicId::new(format!(
            "{}.{}",
            topic_schema.as_str(),
            topic_qual.name.to_ascii_lowercase()
        ));

        let after = after.trim_start();
        let after_upper = after.to_ascii_uppercase();
        if !after_upper.starts_with("EXECUTE PROCEDURE") {
            return Err("Expected EXECUTE PROCEDURE after ON TOPIC".to_string());
        }
        let after = after["EXECUTE PROCEDURE".len()..].trim_start();
        let (routine_qual, after) = split_qualified_ident(after)?;
        let routine_namespace = routine_qual.namespace_or(&namespace_id);
        let routine_id = RoutineId::from_parts(Some(&routine_namespace), &routine_qual.name);
        let after = skip_payload_binding(after)?;

        let mut principal = String::new();
        let mut start_from = "latest".to_string();
        let mut retries = 5;
        let mut retry_backoff_ms = 1000;
        let mut concurrency = 1;
        let after = after.trim_start();
        if after.to_ascii_uppercase().starts_with("WITH") {
            let with_body = parse_with_options(after)?;
            for (key, value) in with_body {
                match key.as_str() {
                    "principal" => principal = value,
                    "start" => start_from = value.to_ascii_lowercase(),
                    "retries" => {
                        retries = value.parse().map_err(|_| "retries must be an integer")?
                    },
                    "retry_backoff" => retry_backoff_ms = parse_duration_ms(&value)?,
                    "concurrency" => {
                        concurrency = value.parse().map_err(|_| "concurrency must be an integer")?
                    },
                    _ => return Err(format!("unknown trigger option '{key}'")),
                }
            }
        }
        if start_from != "latest" && start_from != "earliest" {
            return Err("start must be 'latest' or 'earliest'".to_string());
        }
        if retries < 0 {
            return Err("retries cannot be negative".to_string());
        }
        if concurrency < 1 {
            return Err("concurrency must be at least 1".to_string());
        }

        Ok(Self {
            trigger_id,
            namespace_id,
            name,
            topic_id,
            routine_id,
            principal,
            start_from,
            retries,
            retry_backoff_ms,
            concurrency,
        })
    }
}

impl DropTriggerStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("DROP TRIGGER") {
            return Err("Expected DROP TRIGGER statement".to_string());
        }
        let mut rest = trimmed["DROP TRIGGER".len()..].trim_start();
        let if_exists = rest.len() >= 9 && rest[..9].eq_ignore_ascii_case("IF EXISTS");
        if if_exists {
            rest = rest["IF EXISTS".len()..].trim_start();
        }
        let (qual, _) = split_qualified_ident(rest)?;
        let namespace_id = qual.namespace_or(default_namespace);
        Ok(Self {
            trigger_id: TriggerId::from_parts(Some(&namespace_id), &qual.name),
            if_exists,
        })
    }
}

impl AlterTriggerStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("ALTER TRIGGER") {
            return Err("Expected ALTER TRIGGER statement".to_string());
        }
        let rest = trimmed["ALTER TRIGGER".len()..].trim_start();
        let (qual, after) = split_qualified_ident(rest)?;
        let namespace_id = qual.namespace_or(default_namespace);
        let after_upper = after.trim_start().to_ascii_uppercase();
        let enabled = if after_upper.starts_with("ENABLE") {
            true
        } else if after_upper.starts_with("DISABLE") {
            false
        } else {
            return Err("Expected ENABLE or DISABLE after ALTER TRIGGER name".to_string());
        };
        Ok(Self {
            trigger_id: TriggerId::from_parts(Some(&namespace_id), &qual.name),
            enabled,
        })
    }
}

fn skip_payload_binding(input: &str) -> DdlResult<&str> {
    let after = input.trim_start();
    if !after.starts_with('(') {
        return Ok(after);
    }
    let close = crate::ddl::create_type::matching_paren(after)
        .ok_or_else(|| "Unterminated trigger payload binding".to_string())?;
    let binding = after[1..close].trim();
    if !binding.is_empty() && !binding.eq_ignore_ascii_case("PAYLOAD") {
        return Err("V1 trigger binding must be PAYLOAD".to_string());
    }
    Ok(after[close + 1..].trim_start())
}

fn parse_with_options(input: &str) -> DdlResult<Vec<(String, String)>> {
    let rest = input.trim_start();
    let rest_upper = rest.to_ascii_uppercase();
    if !rest_upper.starts_with("WITH") {
        return Ok(Vec::new());
    }
    let rest = rest["WITH".len()..].trim_start();
    if !rest.starts_with('(') {
        return Err("Expected WITH (...) after CREATE TRIGGER".to_string());
    }
    let close = crate::ddl::create_type::matching_paren(rest)
        .ok_or_else(|| "Unterminated WITH options".to_string())?;
    let body = &rest[1..close];
    let mut options = Vec::new();
    for part in crate::ddl::create_type::split_top_level(body, ',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, value) =
            part.split_once('=').ok_or_else(|| format!("invalid trigger option '{part}'"))?;
        options.push((key.trim().to_ascii_lowercase(), unquote(value.trim())));
    }
    Ok(options)
}

fn unquote(value: &str) -> String {
    let trimmed = value.trim();
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

fn parse_duration_ms(value: &str) -> DdlResult<i64> {
    let trimmed = value.trim();
    if let Some(seconds) = trimmed.strip_suffix('s').or_else(|| trimmed.strip_suffix('S')) {
        if let Some(millis) = seconds.strip_suffix("m").or_else(|| seconds.strip_suffix("M")) {
            return millis
                .parse::<i64>()
                .map_err(|_| "retry_backoff must be a duration like 1s or 250ms".to_string());
        }
        let parsed = seconds
            .parse::<i64>()
            .map_err(|_| "retry_backoff must be a duration like 1s or 250ms".to_string())?;
        return Ok(parsed.saturating_mul(1000));
    }
    if let Some(millis) = trimmed.strip_suffix("ms").or_else(|| trimmed.strip_suffix("MS")) {
        return millis
            .parse::<i64>()
            .map_err(|_| "retry_backoff must be a duration like 1s or 250ms".to_string());
    }
    trimmed
        .parse::<i64>()
        .map_err(|_| "retry_backoff must be a duration like 1s or 250ms".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_trigger_with_options() {
        let ns = NamespaceId::new("chat");
        let stmt = CreateTriggerStatement::parse(
            "CREATE TRIGGER chat.process_message ON TOPIC chat.message_created EXECUTE PROCEDURE \
             chat.on_message_created(PAYLOAD) WITH (principal = 'function-worker', start = \
             'latest', retries = 5, retry_backoff = '1s', concurrency = 4)",
            &ns,
        )
        .unwrap();
        assert_eq!(stmt.trigger_id.as_str(), "chat.process_message");
        assert_eq!(stmt.topic_id.as_str(), "chat.message_created");
        assert_eq!(stmt.routine_id.as_str(), "chat.on_message_created");
        assert_eq!(stmt.principal, "function-worker");
        assert_eq!(stmt.start_from, "latest");
        assert_eq!(stmt.retries, 5);
        assert_eq!(stmt.retry_backoff_ms, 1000);
        assert_eq!(stmt.concurrency, 4);
    }

    #[test]
    fn parse_create_trigger_defaults_principal_to_session() {
        let ns = NamespaceId::new("chat");
        let stmt = CreateTriggerStatement::parse(
            "CREATE TRIGGER chat.process_message ON TOPIC chat.message_created EXECUTE PROCEDURE \
             chat.on_message_created(PAYLOAD)",
            &ns,
        )
        .unwrap();
        assert!(
            stmt.principal.is_empty(),
            "omitted principal is filled from the creating session"
        );
    }

    #[test]
    fn parse_alter_and_drop_trigger() {
        let ns = NamespaceId::new("chat");
        let disable =
            AlterTriggerStatement::parse("ALTER TRIGGER chat.process_message DISABLE", &ns)
                .unwrap();
        assert!(!disable.enabled);
        let drop =
            DropTriggerStatement::parse("DROP TRIGGER IF EXISTS process_message", &ns).unwrap();
        assert_eq!(drop.trigger_id.as_str(), "chat.process_message");
        assert!(drop.if_exists);
    }
}
