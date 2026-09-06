use crate::model::{Table, TableIndex, TableIndexKind};

pub(super) fn emit_create_index(table: &Table, index: &TableIndex) -> String {
    match &index.kind {
        TableIndexKind::Vector { metric } => {
            format!("ALTER TABLE {} CREATE INDEX {} USING {};", table.name_sql, index.name, metric)
        },
        TableIndexKind::Scalar => {
            let unique = if index.unique { "UNIQUE " } else { "" };
            format!(
                "ALTER TABLE {} CREATE {}INDEX {} ({});",
                table.name_sql,
                unique,
                index.name,
                index.columns.join(", ")
            )
        },
    }
}

pub(super) fn emit_indexes_after_create_table(table: &Table, out: &mut Vec<String>) {
    for index in table.indexes.values() {
        out.push(emit_create_index(table, index));
    }
}
