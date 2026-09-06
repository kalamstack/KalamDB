use crate::model::{Table, TableIndex, TableIndexKind};

pub(super) fn emit_drop_index(
    table: &Table,
    index: &TableIndex,
    allow_drop: bool,
    out: &mut Vec<String>,
) {
    let statement = match index.kind {
        TableIndexKind::Vector { .. } => {
            format!("ALTER TABLE {} DROP VECTOR INDEX {};", table.name_sql, index.name)
        },
        TableIndexKind::Scalar => {
            format!("ALTER TABLE {} DROP INDEX {};", table.name_sql, index.name)
        },
    };

    if allow_drop {
        out.push(statement);
        return;
    }

    out.push(format!(
        "-- destructive change skipped: index {}.{} exists in current schema but not in target \
         schema",
        table.name_sql, index.name
    ));
    out.push(format!("-- rerun with destructive changes enabled to emit: {statement}"));
}
