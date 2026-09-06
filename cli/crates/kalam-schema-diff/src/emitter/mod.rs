mod add_column;
mod add_topic_source;
mod alter_policy;
mod clear_topic_retention;
mod create_index;
mod create_namespace;
mod create_policy;
mod create_table;
mod create_topic;
mod drop_column;
mod drop_index;
mod drop_policy;
mod drop_table;
mod drop_topic;
mod flush_policy;
mod modify_column;
mod set_tblproperties;
mod set_topic_retention;
mod table;
mod topic;

use crate::{
    emitter::{
        add_topic_source::emit_add_topic_source,
        alter_policy::emit_alter_policy,
        create_namespace::emit_create_namespace,
        create_policy::{emit_create_policy, emit_starter_shared_table_policy, policies_for_table},
        create_table::emit_create_table,
        create_topic::emit_create_topic,
        drop_policy::emit_drop_policy,
        drop_table::emit_drop_table,
        drop_topic::emit_drop_topic,
        table::diff_existing_table,
        topic::diff_existing_topic,
    },
    model::{Policy, Schema},
};

pub(crate) fn diff_schema(current: &Schema, target: &Schema, allow_drop: bool) -> Vec<String> {
    let mut out = vec![
        "-- Generated KalamDB schema evolution".to_string(),
        "-- Review before applying in production.".to_string(),
        String::new(),
    ];

    for namespace in target.namespaces.difference(&current.namespaces) {
        out.push(emit_create_namespace(namespace));
    }

    if !target.namespaces.is_empty() && out.last().map(String::as_str) != Some("") {
        out.push(String::new());
    }

    for (table_key, target_table) in &target.tables {
        match current.tables.get(table_key) {
            Some(current_table) => {
                diff_existing_table(current_table, target_table, allow_drop, &mut out);
                diff_existing_table_policies(current, target, table_key, allow_drop, &mut out);
            },
            None => {
                out.push(emit_create_table(target_table));
                crate::emitter::create_index::emit_indexes_after_create_table(
                    target_table,
                    &mut out,
                );

                let table_policies = policies_for_table(&target.policies, table_key);
                if table_policies.is_empty() && target_table.is_shared() {
                    emit_starter_shared_table_policy(target_table, &mut out);
                } else {
                    for policy in table_policies {
                        out.push(emit_create_policy(policy));
                    }
                }

                out.push(String::new());
            },
        }
    }

    for (policy_key, current_policy) in &current.policies {
        if target.policies.contains_key(policy_key) {
            continue;
        }

        if !target.tables.contains_key(&current_policy.table_key) {
            continue;
        }

        emit_drop_policy(current_policy, allow_drop, &mut out);
        out.push(String::new());
    }

    for (topic_key, target_topic) in &target.topics {
        match current.topics.get(topic_key) {
            Some(current_topic) => {
                diff_existing_topic(current_topic, target_topic, &mut out);
            },
            None => {
                out.push(emit_create_topic(target_topic));

                for source in target_topic.sources.values() {
                    out.push(emit_add_topic_source(target_topic, source));
                }

                out.push(String::new());
            },
        }
    }

    for (topic_key, current_topic) in &current.topics {
        if !target.topics.contains_key(topic_key) {
            emit_drop_topic(current_topic, allow_drop, &mut out);
        }
    }

    for (table_key, current_table) in &current.tables {
        if !target.tables.contains_key(table_key) {
            emit_drop_table(current_table, allow_drop, &mut out);
        }
    }

    if out.iter().all(|line| line.starts_with("--") || line.trim().is_empty()) {
        out.push("-- No schema changes.".to_string());
    }

    out
}

fn diff_existing_table_policies(
    current: &Schema,
    target: &Schema,
    table_key: &str,
    allow_drop: bool,
    out: &mut Vec<String>,
) {
    let start_len = out.len();
    let current_policies = policies_for_table(&current.policies, table_key);
    let target_policies = policies_for_table(&target.policies, table_key);
    let current_by_key = current_policies
        .iter()
        .map(|policy| (policy.key.as_str(), *policy))
        .collect::<std::collections::BTreeMap<_, _>>();

    for policy in target_policies {
        match current_by_key.get(policy.key.as_str()) {
            Some(current_policy) => {
                emit_changed_policy(current_policy, policy, allow_drop, out);
            },
            None => {
                out.push(emit_create_policy(policy));
            },
        }
    }

    if out.len() > start_len && out.last().map(String::as_str) != Some("") {
        out.push(String::new());
    }
}

fn emit_changed_policy(current: &Policy, target: &Policy, allow_drop: bool, out: &mut Vec<String>) {
    if current.same_authorization(target) {
        return;
    }

    match emit_alter_policy(current, target) {
        Some(statement) if !statement.is_empty() => out.push(statement),
        Some(_) => {},
        None => {
            emit_drop_policy(current, allow_drop, out);
            if allow_drop {
                out.push(emit_create_policy(target));
            } else {
                out.push(format!(
                    "-- recommended replacement: {}",
                    emit_create_policy(target).trim_end_matches(';')
                ));
            }
        },
    }
}
