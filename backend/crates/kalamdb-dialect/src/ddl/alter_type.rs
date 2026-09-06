//! ALTER TYPE composite operations.

use kalamdb_commons::models::{NamespaceId, TypeId};

use crate::ddl::{
    create_type::{parse_type_reference, split_qualified_ident, take_ident, TypeReference},
    DdlResult,
};

/// ALTER TYPE operations supported in V1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTypeOperation {
    AddAttribute {
        field:    String,
        type_ref: TypeReference,
    },
    DropAttribute {
        field:   String,
        cascade: bool,
    },
    RenameAttribute {
        from: String,
        to:   String,
    },
    AlterAttributeType {
        field:    String,
        type_ref: TypeReference,
    },
    SetSchema {
        schema: NamespaceId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTypeStatement {
    pub type_id:   TypeId,
    pub operation: AlterTypeOperation,
}

impl AlterTypeStatement {
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> DdlResult<Self> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("ALTER TYPE") {
            return Err("Expected ALTER TYPE statement".to_string());
        }
        let rest = trimmed["ALTER TYPE".len()..].trim_start();
        let (qual, after) = split_qualified_ident(rest)?;
        let namespace_id = qual.namespace_or(default_namespace);
        let type_id = TypeId::from_parts(Some(&namespace_id), &qual.name);
        let after = after.trim_start();
        let after_upper = after.to_ascii_uppercase();

        let operation = if after_upper.starts_with("ADD ATTRIBUTE") {
            let rest = after["ADD ATTRIBUTE".len()..].trim_start();
            let (field, rest) = take_ident(rest)?;
            let (type_ref, leftover) = parse_type_reference(rest.trim_start())?;
            let leftover_upper = leftover.trim().to_ascii_uppercase();
            let mut type_ref = type_ref;
            if leftover_upper.contains("NOT NULL") {
                type_ref.not_null = true;
            }
            AlterTypeOperation::AddAttribute { field, type_ref }
        } else if after_upper.starts_with("DROP ATTRIBUTE") {
            let rest = after["DROP ATTRIBUTE".len()..].trim_start();
            let (field, rest) = take_ident(rest)?;
            let cascade = rest.trim().eq_ignore_ascii_case("CASCADE");
            AlterTypeOperation::DropAttribute { field, cascade }
        } else if after_upper.starts_with("RENAME ATTRIBUTE") {
            let rest = after["RENAME ATTRIBUTE".len()..].trim_start();
            let (from, rest) = take_ident(rest)?;
            let rest = rest.trim_start();
            if !rest.to_ascii_uppercase().starts_with("TO ") {
                return Err("Expected TO in RENAME ATTRIBUTE".to_string());
            }
            let (to, _) = take_ident(rest[3..].trim_start())?;
            AlterTypeOperation::RenameAttribute { from, to }
        } else if after_upper.starts_with("ALTER ATTRIBUTE") {
            let rest = after["ALTER ATTRIBUTE".len()..].trim_start();
            let (field, rest) = take_ident(rest)?;
            let rest = rest.trim_start();
            if !rest.to_ascii_uppercase().starts_with("TYPE ") {
                return Err("Expected TYPE in ALTER ATTRIBUTE".to_string());
            }
            let (type_ref, _) = parse_type_reference(rest[5..].trim_start())?;
            AlterTypeOperation::AlterAttributeType { field, type_ref }
        } else if after_upper.starts_with("SET SCHEMA") {
            let rest = after["SET SCHEMA".len()..].trim_start();
            let (schema_name, _) = take_ident(rest)?;
            AlterTypeOperation::SetSchema {
                schema: NamespaceId::try_parse_reference(schema_name)
                    .map_err(|error| error.to_string())?,
            }
        } else {
            return Err("Expected ADD/DROP/RENAME/ALTER ATTRIBUTE or SET SCHEMA".to_string());
        };

        Ok(Self { type_id, operation })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns() -> NamespaceId {
        NamespaceId::new("app")
    }

    #[test]
    fn parse_add_attribute() {
        let stmt = AlterTypeStatement::parse(
            "ALTER TYPE app.address ADD ATTRIBUTE postal_code TEXT",
            &ns(),
        )
        .unwrap();
        match stmt.operation {
            AlterTypeOperation::AddAttribute { field, .. } => {
                assert_eq!(field, "postal_code");
            },
            _ => panic!("expected add"),
        }
    }
}
