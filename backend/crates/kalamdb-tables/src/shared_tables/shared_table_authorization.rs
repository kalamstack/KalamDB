//! Shared-table RLS orchestration: policy binding, membership loading, and scan authorization.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{col, lit, Expr},
};
use kalamdb_commons::{
    ids::SharedTableRowId,
    models::{rows::Row, UserId},
    AuthorizationRelation, PolicyCommand, PolicyProgram, Role, TableId,
};
use kalamdb_rls::{
    extract_authorization_constraint, AuthorizationCache, AuthorizationCacheMetrics,
    AuthorizationDependencyGuard, AuthorizationMutationGuard, AuthorizationPolicyGuard,
    AuthorizationSet, BoundAuthorization, BoundLiveAuthorization, BoundTablePolicies,
};

use super::{SharedScanContext, SharedTableProvider};
use crate::{
    error::KalamDbError,
    rls::TablePoliciesEpoch,
    shared_tables::SharedTableRow,
    utils::base::{self, BaseTableProvider, TableProviderCore},
};

#[derive(Clone)]
pub(crate) struct SharedTableAuthorization {
    cache:            Arc<AuthorizationCache>,
    data_generation:  Arc<AtomicU64>,
    active_mutations: Arc<AtomicU64>,
}

impl SharedTableAuthorization {
    pub fn new() -> Self {
        Self {
            cache:            Arc::new(AuthorizationCache::default()),
            data_generation:  Arc::new(AtomicU64::new(0)),
            active_mutations: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn authorization_generation(&self) -> u64 {
        self.data_generation.load(Ordering::Acquire)
    }

    pub fn has_active_authorization_mutations(&self) -> bool {
        self.active_mutations.load(Ordering::Acquire) != 0
    }

    pub fn begin_mutation(&self) -> AuthorizationMutationGuard {
        AuthorizationMutationGuard::begin(
            Arc::clone(&self.data_generation),
            Arc::clone(&self.active_mutations),
        )
    }

    pub fn cache_metrics(&self) -> AuthorizationCacheMetrics {
        self.cache.metrics()
    }

    pub fn bind_policies(
        &self,
        core: &TableProviderCore,
        user_id: &UserId,
        role: Role,
        command: PolicyCommand,
        check: bool,
    ) -> Result<BoundTablePolicies, KalamDbError> {
        if matches!(role, Role::System | Role::Dba) {
            return Ok(BoundTablePolicies::admin_bypass(user_id.clone()));
        }

        let Some(provider) = core.services.table_policies.as_ref() else {
            return Ok(BoundTablePolicies::default_deny(user_id.clone()));
        };

        let compiled = provider
            .compiled_for_table(core.table_id(), u64::from(core.table_def().schema_version))
            .map_err(|error| {
                KalamDbError::InvalidOperation(format!(
                    "failed to bind RLS policies for {}: {}",
                    core.table_id(),
                    error
                ))
            })?;

        if compiled.policies.is_empty() {
            return Ok(BoundTablePolicies::default_deny(user_id.clone()));
        }

        Ok(if check {
            BoundTablePolicies::bind_check(
                compiled.policies.as_ref(),
                user_id.clone(),
                role,
                command,
            )
        } else {
            BoundTablePolicies::bind(compiled.policies.as_ref(), user_id.clone(), role, command)
        })
    }

    pub async fn bind_authorization(
        &self,
        host: &SharedTableProvider,
        policies: &BoundTablePolicies,
        snapshot_commit_seq: Option<u64>,
    ) -> Result<BoundAuthorization, KalamDbError> {
        let mut sets = HashMap::new();
        let mut dependencies = HashMap::new();
        for policy in policies.policies() {
            let PolicyProgram::AuthorizationRelation(relation) = &policy.program else {
                continue;
            };
            let relation_provider = resolve_relation_provider(host, &relation.relation_table)?;
            let dependency =
                dependencies.entry(relation.relation_table.clone()).or_insert_with(|| {
                    AuthorizationDependencyGuard::capture(
                        relation.relation_table.clone(),
                        Arc::new(relation_provider.clone()),
                    )
                });
            let relation_generation = dependency.expected_generation();
            if !dependency.is_current() {
                return Err(KalamDbError::InvalidOperation(format!(
                    "RLS relation {} is changing; authorization fails closed",
                    relation.relation_table
                )));
            }
            let cache_key = policies.authorization_cache_key(
                policy,
                policies.principal(),
                &relation.relation_table,
                relation_generation,
                snapshot_commit_seq,
            );
            if let Some(set) = self.cache.get(&cache_key) {
                if relation_provider.authorization.authorization_generation() == relation_generation
                    && !relation_provider.authorization.has_active_authorization_mutations()
                {
                    sets.insert(policy.policy_id.clone(), set);
                    continue;
                }
            }

            let set = Arc::new(
                self.load_membership_authorization_set(
                    &relation_provider,
                    relation_provider.core.table_def(),
                    relation,
                    policies.principal(),
                    snapshot_commit_seq,
                )
                .await?,
            );
            if relation_provider.authorization.authorization_generation() != relation_generation
                || relation_provider.authorization.has_active_authorization_mutations()
            {
                return Err(KalamDbError::InvalidOperation(format!(
                    "RLS relation {} changed while authorization was built; authorization fails \
                     closed",
                    relation.relation_table
                )));
            }
            self.cache.insert(cache_key, Arc::clone(&set));
            sets.insert(policy.policy_id.clone(), set);
        }
        let dependencies = dependencies.into_values().collect::<Vec<_>>();
        if dependencies.iter().any(|dependency| !dependency.is_current()) {
            return Err(KalamDbError::InvalidOperation(
                "RLS dependency changed while authorization was bound".to_string(),
            ));
        }
        Ok(BoundAuthorization::new(
            Arc::clone(host.core.table_def()),
            policies.clone(),
            sets,
            dependencies,
        ))
    }

    async fn load_membership_authorization_set(
        &self,
        relation_provider: &SharedTableProvider,
        relation_table: &kalamdb_commons::schemas::TableDefinition,
        relation: &AuthorizationRelation,
        principal: &UserId,
        snapshot_commit_seq: Option<u64>,
    ) -> Result<AuthorizationSet, KalamDbError> {
        let principal_filter =
            indexed_principal_filter(relation_provider, relation_table, relation, principal);

        let mut required_column_ids = relation.relation_keys.clone();
        required_column_ids.push(relation.principal_column);
        required_column_ids
            .extend(relation.static_predicates.iter().map(|predicate| predicate.column_id));
        required_column_ids.sort_unstable();
        required_column_ids.dedup();
        let mut cold_columns = required_column_ids
            .into_iter()
            .filter_map(|column_id| {
                relation_table
                    .columns
                    .iter()
                    .find(|column| column.column_id == column_id)
                    .map(|column| column.column_name.clone())
            })
            .collect::<Vec<_>>();
        let primary_key = relation_provider.primary_key_field_name().to_string();
        if !cold_columns.iter().any(|column| column == &primary_key) {
            cold_columns.push(primary_key);
        }
        let relation_rows = relation_provider
            .scan_with_version_resolution_to_kvs_async(
                base::system_user_id(),
                principal_filter.as_ref(),
                None,
                None,
                false,
                Some(&cold_columns),
                snapshot_commit_seq,
            )
            .await?;
        if principal_filter.is_some() {
            // Prefix scan can return stale versions after the principal column
            // changes. Resolve each PK to the live winner before binding.
            let pk_name = relation_provider.primary_key_field_name();
            let mut seen = HashSet::new();
            let mut winners = Vec::new();
            for (_, row) in &relation_rows {
                let Some(pk) = row.fields.get(pk_name) else {
                    continue;
                };
                if !seen.insert(pk.clone()) {
                    continue;
                }
                if let Some((_, winner)) = relation_provider.find_by_pk(pk).await? {
                    winners.push(winner.fields);
                }
            }
            return Ok(AuthorizationSet::from_relation_rows(
                relation.clone(),
                relation_table,
                principal,
                winners.iter(),
            ));
        }
        Ok(AuthorizationSet::from_relation_rows(
            relation.clone(),
            relation_table,
            principal,
            relation_rows.iter().map(|(_, row)| &row.fields),
        ))
    }

    pub async fn ensure_rows_authorized(
        &self,
        host: &SharedTableProvider,
        policies: &BoundTablePolicies,
        rows: &[Row],
        snapshot_commit_seq: Option<u64>,
        operation: &str,
    ) -> DataFusionResult<()> {
        if policies.bypasses_rls() {
            return Ok(());
        }
        let authorization = self
            .bind_authorization(host, policies, snapshot_commit_seq)
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        if authorization.authorizes_all(rows) {
            Ok(())
        } else {
            Err(DataFusionError::Plan(format!(
                "row-level security {} policy denied row on {}",
                operation,
                host.core.table_id()
            )))
        }
    }

    pub async fn check_rows_authorized(
        &self,
        host: &SharedTableProvider,
        user_id: &UserId,
        role: Role,
        command: PolicyCommand,
        check: bool,
        rows: &[Row],
        snapshot_commit_seq: Option<u64>,
    ) -> Result<(), KalamDbError> {
        let policies = self.bind_policies(host.core.as_ref(), user_id, role, command, check)?;
        self.ensure_rows_authorized(
            host,
            &policies,
            rows,
            snapshot_commit_seq,
            if check { "WITH CHECK" } else { "USING" },
        )
        .await
        .map_err(|error| KalamDbError::InvalidOperation(error.to_string()))
    }

    pub async fn bind_live_authorization(
        &self,
        host: &SharedTableProvider,
        user_id: &UserId,
        role: Role,
    ) -> Result<BoundLiveAuthorization, KalamDbError> {
        let policy_guard = host
            .core
            .services
            .table_policies
            .as_ref()
            .map(|provider| {
                AuthorizationPolicyGuard::capture(
                    host.core.table_id().clone(),
                    Arc::new(TablePoliciesEpoch(Arc::clone(provider))),
                )
            })
            .transpose()
            .map_err(KalamDbError::InvalidOperation)?;
        let policies =
            self.bind_policies(host.core.as_ref(), user_id, role, PolicyCommand::Select, false)?;
        let authorization = self.bind_authorization(host, &policies, None).await?;
        if policy_guard.as_ref().is_some_and(|guard| !guard.is_current()) {
            return Err(KalamDbError::InvalidOperation(
                "RLS policy catalog changed while binding live authorization".to_string(),
            ));
        }
        Ok(BoundLiveAuthorization::new(authorization, policy_guard))
    }

    pub fn authorization_cold_columns(
        &self,
        host: &SharedTableProvider,
        scan_context: &SharedScanContext,
        cold_columns: Option<&[String]>,
    ) -> Option<Vec<String>> {
        let mut columns = cold_columns?.to_vec();
        for column_id in scan_context.policies.required_column_ids() {
            if let Some(column) = host
                .core
                .table_def()
                .columns
                .iter()
                .find(|column| column.column_id == column_id)
            {
                if !columns.iter().any(|name| name == &column.column_name) {
                    columns.push(column.column_name.clone());
                }
            }
        }
        Some(columns)
    }

    pub fn authorization_plan_details(
        &self,
        host: &SharedTableProvider,
        scan_context: &SharedScanContext,
        filter: Option<&Expr>,
    ) -> Option<String> {
        if scan_context.policies.bypasses_rls() {
            return Some(append_policy_explain(
                "RlsAuthorization bypass=admin",
                &scan_context.policies,
            ));
        }
        if scan_context.policies.is_default_deny() {
            return Some(append_policy_explain(
                "RlsAuthorization strategy=DefaultDeny",
                &scan_context.policies,
            ));
        }
        if let Some((policy, relation)) = scan_context.policies.single_membership_policy() {
            let constraint = relation
                .protected_keys
                .first()
                .and_then(|column_id| {
                    host.core
                        .table_def()
                        .columns
                        .iter()
                        .find(|column| column.column_id == *column_id)
                })
                .and_then(|column| extract_authorization_constraint(filter, &column.column_name));
            let cache_hit = resolve_relation_provider(host, &relation.relation_table)
                .ok()
                .is_some_and(|relation_provider| {
                    let generation = relation_provider.authorization.authorization_generation();
                    !relation_provider.authorization.has_active_authorization_mutations()
                        && self
                            .cache
                            .peek(&scan_context.policies.authorization_cache_key(
                                policy,
                                scan_context.policies.principal(),
                                &relation.relation_table,
                                generation,
                                scan_context.snapshot_commit_seq,
                            ))
                            .is_some()
                });
            let strategy = constraint
                .map(|constraint| format!("{:?}", constraint.strategy))
                .unwrap_or_else(|| "CachedAuthorizationSet".to_string());
            return Some(append_policy_explain(
                &format!(
                    "RlsAuthorization strategy={strategy}, auth_cache={}",
                    if cache_hit { "hit" } else { "miss" }
                ),
                &scan_context.policies,
            ));
        }
        Some(append_policy_explain(
            "RlsAuthorization strategy=RowLocal",
            &scan_context.policies,
        ))
    }

    pub async fn pre_authorize_scan(
        &self,
        host: &SharedTableProvider,
        scan_context: &SharedScanContext,
        filter: Option<&Expr>,
    ) -> Result<bool, KalamDbError> {
        if scan_context.policies.bypasses_rls() {
            return Ok(true);
        }
        if scan_context.policies.is_default_deny() {
            return Ok(false);
        }
        let Some((policy, relation)) = scan_context.policies.single_membership_policy() else {
            return Ok(true);
        };
        let [protected_column_id] = relation.protected_keys.as_slice() else {
            return Ok(true);
        };
        let Some(protected_column) = host
            .core
            .table_def()
            .columns
            .iter()
            .find(|column| column.column_id == *protected_column_id)
        else {
            return Ok(false);
        };
        let Some(constraint) =
            extract_authorization_constraint(filter, &protected_column.column_name)
        else {
            return Ok(true);
        };

        let relation_provider = match resolve_relation_provider(host, &relation.relation_table) {
            Ok(provider) => provider,
            Err(_) => return Ok(false),
        };
        let relation_generation = relation_provider.authorization.authorization_generation();
        if relation_provider.authorization.has_active_authorization_mutations() {
            return Ok(false);
        }
        let cache_key = scan_context.policies.authorization_cache_key(
            policy,
            scan_context.policies.principal(),
            &relation.relation_table,
            relation_generation,
            scan_context.snapshot_commit_seq,
        );
        let Some(set) = self.cache.get(&cache_key) else {
            if relation_provider.core.primary_key_column_id() != relation.principal_column {
                return Ok(true);
            }
            let set = Arc::new(
                self.load_membership_authorization_set(
                    &relation_provider,
                    relation_provider.core.table_def(),
                    relation,
                    scan_context.policies.principal(),
                    scan_context.snapshot_commit_seq,
                )
                .await?,
            );
            if relation_provider.authorization.authorization_generation() != relation_generation
                || relation_provider.authorization.has_active_authorization_mutations()
            {
                return Ok(false);
            }
            self.cache.insert(cache_key, Arc::clone(&set));
            return Ok(constraint
                .values
                .iter()
                .any(|value| set.contains_key(std::slice::from_ref(value))));
        };
        if relation_provider.authorization.authorization_generation() != relation_generation
            || relation_provider.authorization.has_active_authorization_mutations()
        {
            return Ok(false);
        }
        Ok(constraint
            .values
            .iter()
            .any(|value| set.contains_key(std::slice::from_ref(value))))
    }

    pub async fn authorize_resolved_rows(
        &self,
        host: &SharedTableProvider,
        scan_context: &SharedScanContext,
        rows: Vec<(SharedTableRowId, SharedTableRow)>,
    ) -> Result<Vec<(SharedTableRowId, SharedTableRow)>, KalamDbError> {
        if scan_context.policies.bypasses_rls() {
            return Ok(rows);
        }
        if scan_context.policies.is_default_deny() {
            return Ok(Vec::new());
        }
        let authorization = self
            .bind_authorization(host, &scan_context.policies, scan_context.snapshot_commit_seq)
            .await?;
        authorization.filter_authorized(rows, |(_, row)| &row.fields).ok_or_else(|| {
            KalamDbError::InvalidOperation(
                "RLS dependency changed while rows were authorized".to_string(),
            )
        })
    }
}

pub(crate) fn resolve_relation_provider(
    host: &SharedTableProvider,
    relation_table: &TableId,
) -> Result<SharedTableProvider, KalamDbError> {
    let provider = host
        .core
        .services
        .schema_registry
        .get_table_provider(relation_table)
        .ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "RLS relation provider {relation_table} is unavailable"
            ))
        })?;
    (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<SharedTableProvider>()
        .cloned()
        .ok_or_else(|| {
            KalamDbError::InvalidOperation(format!(
                "RLS relation {relation_table} must be a shared table"
            ))
        })
}

fn append_policy_explain(details: &str, policies: &BoundTablePolicies) -> String {
    match policies.explain_policies() {
        Some(fragment) => format!("{details}, {fragment}"),
        None => details.to_string(),
    }
}

fn indexed_principal_filter(
    relation_provider: &SharedTableProvider,
    relation_table: &kalamdb_commons::schemas::TableDefinition,
    relation: &AuthorizationRelation,
    principal: &UserId,
) -> Option<Expr> {
    let indexed = relation_table.scalar_indexes.iter().any(|index| {
        index
            .columns
            .first()
            .is_some_and(|column_id| column_id.as_u64() == relation.principal_column)
    });
    if !indexed {
        return None;
    }
    let column = relation_table
        .columns
        .iter()
        .find(|column| column.column_id == relation.principal_column)?;
    let data_type = relation_provider
        .schema_ref()
        .field_with_name(&column.column_name)
        .ok()?
        .data_type()
        .clone();
    let scalar =
        kalamdb_commons::conversions::parse_string_as_scalar(principal.as_str(), &data_type)
            .ok()?;
    Some(col(column.column_name.as_str()).eq(lit(scalar)))
}
