//! Shared `CALL` / column-default invocation: routine identity plus arguments.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::{call_argument::CallArgument, ids::RoutineId};

/// Built-in DEFAULT functions. These are ScalarUDFs, not catalog procedures.
const BUILTIN_DEFAULT_NAMES: &[&str] = &[
    "now",
    "current_timestamp",
    "snowflake_id",
    "auto_increment",
    "uuid_v7",
    "ulid",
    "current_user",
];

/// Same shape as SQL `CALL schema.name(args...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RoutineCall {
    pub routine_id: RoutineId,
    pub arguments:  Vec<CallArgument>,
}

impl RoutineCall {
    pub fn new(routine_id: RoutineId, arguments: Vec<CallArgument>) -> Self {
        Self {
            routine_id,
            arguments,
        }
    }

    pub fn builtin(name: &str) -> Self {
        Self::new(RoutineId::new(normalize_builtin_name(name)), Vec::new())
    }

    pub fn unqualified_name(&self) -> &str {
        self.routine_id
            .as_str()
            .rsplit_once('.')
            .map(|(_, name)| name)
            .unwrap_or(self.routine_id.as_str())
    }

    pub fn is_builtin_default(&self) -> bool {
        !self.routine_id.as_str().contains('.') && is_builtin_default_name(self.unqualified_name())
    }

    pub fn is_id_generator_default(&self) -> bool {
        self.is_builtin_default()
            && matches!(self.unqualified_name(), "snowflake_id" | "auto_increment")
    }

    pub fn has_placeholder(&self) -> bool {
        self.arguments.iter().any(CallArgument::is_placeholder)
    }

    pub fn scalar_udf_name(&self) -> Option<&'static str> {
        if !self.is_builtin_default() {
            return None;
        }
        Some(match self.unqualified_name() {
            "now" | "current_timestamp" => "now",
            "snowflake_id" | "auto_increment" => "snowflake_id",
            "uuid_v7" => "uuid_v7",
            "ulid" => "ulid",
            "current_user" => "kdb_current_user",
            _ => return None,
        })
    }

    pub fn to_sql(&self) -> String {
        let name = if self.is_builtin_default() {
            self.unqualified_name().to_ascii_uppercase()
        } else {
            self.routine_id.as_str().to_string()
        };
        if self.arguments.is_empty() {
            format!("{name}()")
        } else {
            let args =
                self.arguments.iter().map(CallArgument::to_sql).collect::<Vec<_>>().join(", ");
            format!("{name}({args})")
        }
    }
}

pub fn is_builtin_default_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    BUILTIN_DEFAULT_NAMES.contains(&lower.as_str())
}

pub fn normalize_builtin_name(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower == "current_timestamp" || lower == "now" {
        "now".to_string()
    } else if lower == "auto_increment" {
        "snowflake_id".to_string()
    } else {
        lower
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_now_is_unqualified() {
        let call = RoutineCall::builtin("NOW");
        assert_eq!(call.routine_id.as_str(), "now");
        assert!(call.is_builtin_default());
        assert_eq!(call.to_sql(), "NOW()");
        assert_eq!(call.scalar_udf_name(), Some("now"));
    }

    #[test]
    fn qualified_procedure_is_not_builtin() {
        let call = RoutineCall::new(
            RoutineId::from_parts(Some(&crate::models::NamespaceId::new("chat")), "next_id"),
            Vec::new(),
        );
        assert!(!call.is_builtin_default());
        assert_eq!(call.to_sql(), "chat.next_id()");
    }

    #[test]
    fn qualified_snowflake_id_is_user_procedure() {
        let call = RoutineCall::new(
            RoutineId::from_parts(Some(&crate::models::NamespaceId::new("app")), "snowflake_id"),
            Vec::new(),
        );
        assert!(!call.is_builtin_default());
        assert!(!call.is_id_generator_default());
    }
}
