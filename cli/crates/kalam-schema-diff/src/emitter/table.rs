use std::collections::BTreeSet;

use crate::{
    emitter::{
        add_column::emit_add_column,
        create_index::emit_create_index,
        drop_column::emit_drop_column,
        drop_index::emit_drop_index,
        flush_policy::emit_flush_policy_change,
        modify_column::emit_modify_column,
        set_tblproperties::{emit_removed_option_comments, emit_set_tblproperties},
    },
    model::Table,
};

pub(super) fn diff_existing_table(
    current: &Table,
    target: &Table,
    allow_drop: bool,
    out: &mut Vec<String>,
) {
    let start_len = out.len();

    if current.kind != target.kind {
        out.push(format!(
            "-- manual review required: table {} changed kind from {:?} to {:?}",
            target.name_sql, current.kind, target.kind
        ));
    }

    if let Some(statement) = emit_set_tblproperties(current, target) {
        out.push(statement);
    }

    if let Some(statement) = emit_flush_policy_change(current, target) {
        out.push(statement);
    }

    emit_removed_option_comments(current, target, out);

    let current_constraints = current.constraints.iter().cloned().collect::<BTreeSet<_>>();
    let target_constraints = target.constraints.iter().cloned().collect::<BTreeSet<_>>();

    if current_constraints != target_constraints {
        out.push(format!(
            "-- manual review required: constraints changed on table {}",
            target.name_sql
        ));
        out.push(format!("-- current constraints: {current_constraints:?}"));
        out.push(format!("-- target constraints: {target_constraints:?}"));
    }

    for column_key in &target.column_order {
        let target_column = target.columns.get(column_key).expect("target column exists");

        match current.columns.get(column_key) {
            Some(current_column) => {
                if current_column.semantic_signature() != target_column.semantic_signature() {
                    if current_column.primary_key != target_column.primary_key {
                        out.push(format!(
                            "-- manual review required: primary key changed for {}.{}",
                            target.name_sql, target_column.name_sql
                        ));
                        continue;
                    }

                    out.push(emit_modify_column(target, target_column));
                }
            },
            None => {
                out.push(emit_add_column(target, target_column));
            },
        }
    }

    for column_key in &current.column_order {
        if !target.columns.contains_key(column_key) {
            let current_column = current.columns.get(column_key).expect("current column exists");
            emit_drop_column(target, current_column, allow_drop, out);
        }
    }

    for (index_key, target_index) in &target.indexes {
        match current.indexes.get(index_key) {
            Some(current_index) if current_index.signature() == target_index.signature() => {},
            Some(current_index) => {
                emit_drop_index(target, current_index, allow_drop, out);
                out.push(emit_create_index(target, target_index));
            },
            None => {
                out.push(emit_create_index(target, target_index));
            },
        }
    }

    for (index_key, current_index) in &current.indexes {
        if !target.indexes.contains_key(index_key) {
            emit_drop_index(target, current_index, allow_drop, out);
        }
    }

    if out.len() > start_len {
        out.push(String::new());
    }
}
