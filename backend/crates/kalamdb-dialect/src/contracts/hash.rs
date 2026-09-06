//! Canonical contract hashing. Formatting and comments are stripped before compile.

use sha2::{Digest, Sha256};

use super::snapshot::{ContractField, ContractSnapshot, ContractTypeKind};

/// SHA-256 hex digest of the resolved logical contract. Independent of SQL whitespace/comments.
pub fn canonical_contract_hash(snapshot: &ContractSnapshot) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_bytes(snapshot));
    hasher.finalize().iter().fold(String::with_capacity(64), |mut out, byte| {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn canonical_bytes(snapshot: &ContractSnapshot) -> Vec<u8> {
    let mut out = String::new();
    for schema in &snapshot.schemas {
        out.push_str("schema\t");
        out.push_str(schema);
        out.push('\n');
    }
    for (id, ty) in &snapshot.types {
        out.push_str("type\t");
        out.push_str(id);
        out.push('\t');
        match &ty.kind {
            ContractTypeKind::ImplicitTableRow { table_id, fields } => {
                out.push_str("implicit_table_row\t");
                out.push_str(table_id);
                out.push('\n');
                write_fields(&mut out, fields);
            },
            ContractTypeKind::RowAlias { source } => {
                out.push_str("row_alias\t");
                out.push_str(source.as_str());
                out.push('\n');
            },
            ContractTypeKind::Composite { fields } => {
                out.push_str("composite\n");
                write_fields(&mut out, fields);
            },
            ContractTypeKind::Enum { labels } => {
                out.push_str("enum\t");
                out.push_str(&labels.join(","));
                out.push('\n');
            },
        }
    }
    for (id, table) in &snapshot.tables {
        out.push_str("table\t");
        out.push_str(id);
        out.push('\t');
        out.push_str(table.kind.as_str());
        if let Some(alias) = &table.row_alias_id {
            out.push_str("\talias\t");
            out.push_str(alias.as_str());
        }
        out.push('\n');
        write_fields(&mut out, &table.fields);
    }
    for (id, routine) in &snapshot.routines {
        out.push_str("routine\t");
        out.push_str(id);
        out.push('\t');
        out.push_str(routine.security.as_str());
        out.push('\t');
        out.push_str(routine.language.as_deref().unwrap_or(""));
        out.push('\n');
        write_fields(&mut out, &routine.parameters);
        if let Some(ret) = &routine.return_type {
            out.push_str("returns\t");
            write_field(&mut out, ret);
        }
        if let Some(body) = &routine.body {
            out.push_str("body\t");
            out.push_str(body.trim());
            out.push('\n');
        }
        for grant in &routine.grants {
            out.push_str("grant\t");
            out.push_str(&grant.as_sql());
            out.push('\n');
        }
    }
    out.into_bytes()
}

fn write_fields(out: &mut String, fields: &[ContractField]) {
    for field in fields {
        write_field(out, field);
    }
}

fn write_field(out: &mut String, field: &ContractField) {
    out.push_str("field\t");
    out.push_str(&field.name);
    out.push('\t');
    out.push_str(&field.type_name);
    out.push('\t');
    out.push_str(if field.is_array { "1" } else { "0" });
    out.push('\t');
    out.push_str(if field.not_null { "1" } else { "0" });
    out.push('\t');
    out.push_str(if field.nonempty { "1" } else { "0" });
    out.push('\n');
}
