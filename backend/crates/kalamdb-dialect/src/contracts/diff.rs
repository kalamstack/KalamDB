//! Diff two contract snapshots into deterministic DDL.

use super::snapshot::{
    ContractField, ContractRoutine, ContractSnapshot, ContractType, ContractTypeKind,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContractDiff {
    pub statements: Vec<String>,
}

pub fn diff_contracts(current: &ContractSnapshot, target: &ContractSnapshot) -> ContractDiff {
    let mut statements = Vec::new();

    for (id, ty) in &target.types {
        if matches!(ty.kind, ContractTypeKind::ImplicitTableRow { .. }) {
            continue;
        }
        match current.types.get(id) {
            None => statements.push(emit_create_type(ty)),
            Some(prev) => statements.extend(diff_type(prev, ty)),
        }
    }

    for (id, ty) in &current.types {
        if matches!(ty.kind, ContractTypeKind::ImplicitTableRow { .. }) {
            continue;
        }
        if !target.types.contains_key(id) {
            statements.push(format!("DROP TYPE {};", ty.type_id));
        }
    }

    for (id, routine) in &target.routines {
        match current.routines.get(id) {
            None => {
                statements.push(emit_create_procedure(routine));
                for grant in &routine.grants {
                    statements.push(format!(
                        "GRANT EXECUTE ON PROCEDURE {} TO {};",
                        routine.routine_id,
                        grant.as_sql()
                    ));
                }
            },
            Some(prev) => statements.extend(diff_routine(prev, routine)),
        }
    }

    for (id, routine) in &current.routines {
        if !target.routines.contains_key(id) {
            statements.push(format!("DROP PROCEDURE {};", routine.routine_id));
        }
    }

    ContractDiff { statements }
}

fn diff_type(current: &ContractType, target: &ContractType) -> Vec<String> {
    let mut out = Vec::new();
    match (&current.kind, &target.kind) {
        (
            ContractTypeKind::Composite { fields: before },
            ContractTypeKind::Composite { fields: after },
        ) => {
            let before_by_name: std::collections::BTreeMap<_, _> =
                before.iter().map(|f| (f.name.as_str(), f)).collect();
            let after_by_name: std::collections::BTreeMap<_, _> =
                after.iter().map(|f| (f.name.as_str(), f)).collect();
            for field in after {
                match before_by_name.get(field.name.as_str()) {
                    None => out.push(format!(
                        "ALTER TYPE {} ADD ATTRIBUTE {};",
                        target.type_id,
                        emit_field(field)
                    )),
                    Some(prev) if field_signature(prev) != field_signature(field) => {
                        out.push(format!(
                            "ALTER TYPE {} ALTER ATTRIBUTE {} TYPE {};",
                            target.type_id,
                            field.name,
                            emit_type_sql(field)
                        ))
                    },
                    _ => {},
                }
            }
            for field in before {
                if !after_by_name.contains_key(field.name.as_str()) {
                    out.push(format!(
                        "ALTER TYPE {} DROP ATTRIBUTE {};",
                        target.type_id, field.name
                    ));
                }
            }
        },
        (ContractTypeKind::Enum { labels: before }, ContractTypeKind::Enum { labels: after })
            if before != after =>
        {
            out.push(format!("DROP TYPE {};", current.type_id));
            out.push(emit_create_type(target));
        },
        (left, right) if left != right => {
            out.push(format!("DROP TYPE {};", current.type_id));
            out.push(emit_create_type(target));
        },
        _ => {},
    }
    out
}

fn diff_routine(current: &ContractRoutine, target: &ContractRoutine) -> Vec<String> {
    let mut out = Vec::new();
    let signature_changed = current.parameters != target.parameters
        || current.return_type != target.return_type
        || current.security != target.security
        || current.language != target.language
        || current.body.as_deref().map(str::trim) != target.body.as_deref().map(str::trim);
    if signature_changed {
        out.push(emit_create_procedure(target));
    }
    for grant in target.grants.difference(&current.grants) {
        out.push(format!(
            "GRANT EXECUTE ON PROCEDURE {} TO {};",
            target.routine_id,
            grant.as_sql()
        ));
    }
    for grant in current.grants.difference(&target.grants) {
        out.push(format!(
            "REVOKE EXECUTE ON PROCEDURE {} FROM {};",
            target.routine_id,
            grant.as_sql()
        ));
    }
    out
}

fn emit_create_type(ty: &ContractType) -> String {
    match &ty.kind {
        ContractTypeKind::Composite { fields } => {
            let body = fields.iter().map(emit_field).collect::<Vec<_>>().join(",\n  ");
            format!("CREATE TYPE {} AS (\n  {}\n);", ty.type_id, body)
        },
        ContractTypeKind::Enum { labels } => {
            let labels = labels
                .iter()
                .map(|label| format!("'{}'", label.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            format!("CREATE TYPE {} AS ENUM ({});", ty.type_id, labels)
        },
        ContractTypeKind::RowAlias { source } => {
            format!("CREATE TYPE {} FROM TABLE {};", ty.type_id, source)
        },
        ContractTypeKind::ImplicitTableRow { .. } => String::new(),
    }
}

fn emit_create_procedure(routine: &ContractRoutine) -> String {
    let params = routine.parameters.iter().map(emit_field).collect::<Vec<_>>().join(", ");
    let mut sql = format!("CREATE OR REPLACE PROCEDURE {}({})", routine.routine_id, params);
    if let Some(ret) = &routine.return_type {
        sql.push_str("\nRETURNS ");
        sql.push_str(&emit_type_sql(ret));
    }
    if let Some(language) = &routine.language {
        sql.push_str("\nLANGUAGE ");
        sql.push_str(language);
    }
    sql.push_str("\nSECURITY ");
    sql.push_str(routine.security.as_str());
    if let Some(body) = &routine.body {
        sql.push_str("\nAS $$");
        sql.push_str(body);
        sql.push_str("$$");
    }
    sql.push(';');
    sql
}

fn emit_field(field: &ContractField) -> String {
    format!("{} {}", field.name, emit_type_sql(field))
}

fn emit_type_sql(field: &ContractField) -> String {
    let mut sql = field.type_name.clone();
    if field.is_array {
        sql.push_str("[]");
    }
    if field.not_null {
        sql.push_str(" NOT NULL");
    }
    if field.nonempty {
        sql.push_str(" NONEMPTY");
    }
    sql
}

fn field_signature(field: &ContractField) -> String {
    format!("{}|{}|{}|{}", field.type_name, field.is_array, field.not_null, field.nonempty)
}
