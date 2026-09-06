//! Shared workflow SQL execution helpers.

use kalam_client::{HttpVersion, KalamLinkClient, QueryResponse};
use kalamdb_commons::NamespaceId;

use crate::{
    error::{CLIError, Result},
    output::WorkflowOutput,
    sql_batch,
    workflow::{
        auth::resolve_workflow_auth_provider, project::resolve::ResolvedEnvironment,
        WorkflowContext,
    },
};

pub(crate) fn build_workflow_client(
    ctx: &WorkflowContext,
    environment: &ResolvedEnvironment,
) -> Result<KalamLinkClient> {
    let auth = resolve_workflow_auth_provider(ctx, environment)?;
    let mut connection_options = ctx.cli_config.to_connection_options();
    if environment.url.starts_with("http://")
        && connection_options.http_version == HttpVersion::Http2
    {
        connection_options = connection_options.with_http_version(HttpVersion::Auto);
    }

    KalamLinkClient::builder()
        .base_url(&environment.url)
        .timeout(std::time::Duration::from_secs(ctx.cli_config.resolved_server().timeout))
        .auth(auth)
        .connection_options(connection_options)
        .build()
        .map_err(CLIError::from)
}

pub(crate) async fn ensure_namespace_exists(
    client: &KalamLinkClient,
    namespace: &NamespaceId,
    output: &WorkflowOutput,
) -> Result<()> {
    let sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace.as_str());
    {
        let _spinner = output.status_spinner(format!("ensuring namespace {}", namespace.as_str()));
        execute_single_statement(client, &sql, None, "namespace bootstrap").await?;
    }
    output.status(format!("ensured namespace {}", namespace.as_str()));
    Ok(())
}

pub(crate) async fn drop_namespace_if_exists(
    client: &KalamLinkClient,
    namespace: &NamespaceId,
) -> Result<()> {
    let sql = format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace.as_str());
    execute_single_statement(client, &sql, None, "namespace reset").await
}

pub(crate) async fn execute_sql_batch(
    client: &KalamLinkClient,
    sql: &str,
    namespace: Option<&str>,
    output: &WorkflowOutput,
    source_label: &str,
) -> Result<usize> {
    let statements = sql_batch::parse_execution_batch(sql)?;
    if statements.is_empty() {
        output.detail(format!("no executable SQL statements found in {source_label}"));
        return Ok(0);
    }

    let total = statements.len();
    for (index, statement) in statements.iter().enumerate() {
        let summary = summarize_sql(statement);
        output.detail(format!(
            "executing statement {}/{} from {}: {}",
            index + 1,
            total,
            source_label,
            summary
        ));
        execute_single_statement(client, statement, namespace, &format!("Statement {}", index + 1))
            .await?;
    }

    Ok(total)
}

pub(crate) async fn execute_single_statement(
    client: &KalamLinkClient,
    sql: &str,
    namespace: Option<&str>,
    failure_prefix: &str,
) -> Result<()> {
    let response = client.execute_query(sql, None, None, namespace).await?;
    if response.success() {
        return Ok(());
    }

    Err(CLIError::ConfigurationError(format!(
        "{failure_prefix} failed: {}",
        query_failure_message(&response, "query failed")
    )))
}

pub(crate) fn query_failure_message(response: &QueryResponse, fallback: &str) -> String {
    response
        .error
        .as_ref()
        .map(|error| error.message.clone())
        .or_else(|| response.results.first().and_then(|result| result.message.clone()))
        .unwrap_or_else(|| fallback.to_string())
}

fn summarize_sql(statement: &str) -> String {
    const MAX_LEN: usize = 96;
    let normalized = statement.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.len() <= MAX_LEN {
        normalized
    } else {
        format!("{}...", &normalized[..MAX_LEN])
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs};

    use kalam_client::{
        credentials::{CredentialStore, Credentials},
        AuthProvider,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::CLIConfiguration,
        workflow::{
            auth::resolve_workflow_auth_provider,
            project::config::{
                ConnectionEnv, DevSection, KalamProjectConfig, LoggingSection, MigrationsSection,
                ProjectSection, SchemaMode, SchemaSection, SchemaTarget,
            },
        },
        FileCredentialStore,
    };

    #[test]
    fn workflow_auth_prefers_project_profile_over_local_password() {
        let temp = TempDir::new().unwrap();
        let home = temp.path().join("home");
        fs::create_dir_all(&home).unwrap();

        let original_home = std::env::var_os("HOME");
        let original_userprofile = std::env::var_os("USERPROFILE");
        std::env::set_var("HOME", &home);
        std::env::set_var("USERPROFILE", &home);

        let project_root = temp.path().join("project");
        fs::create_dir_all(project_root.join("kalam/server")).unwrap();
        fs::write(project_root.join(".env"), "KALAM_PROFILE=kalam-dev\n").unwrap();
        fs::write(
            project_root.join("kalam/server/server.toml"),
            "[auth]\nroot_password = \"mypass\"\n",
        )
        .unwrap();

        let mut store = FileCredentialStore::new().unwrap();
        store
            .set_credentials(&Credentials::new("kalam-dev".into(), "jwt-from-profile".into()))
            .unwrap();

        let ctx = WorkflowContext {
            project_root:       project_root.clone(),
            config:             KalamProjectConfig {
                project:    ProjectSection {
                    name:            "demo".into(),
                    default_env:     "dev".into(),
                    package_manager: None,
                    kalam_dir:       "kalam".into(),
                },
                connection: HashMap::from([(
                    "dev".into(),
                    ConnectionEnv {
                        url:       "http://localhost:2900".into(),
                        namespace: kalamdb_commons::NamespaceId::new("demo"),
                    },
                )]),
                schema:     SchemaSection {
                    mode:      SchemaMode::Sql,
                    path:      Some("schema.sql".into()),
                    watch:     false,
                    languages: vec!["typescript".into()],
                    targets:   HashMap::from([(
                        "typescript".into(),
                        SchemaTarget {
                            output:            "src/generated/kalam.ts".into(),
                            unqualified_names: false,
                        },
                    )]),
                },
                migrations: MigrationsSection::default(),
                dev:        DevSection::default(),
                logging:    LoggingSection::default(),
            },
            cli_config:         CLIConfiguration::default(),
            use_color:          false,
            animations:         true,
            agent:              false,
            json:               false,
            project_dir:        None,
            env_override:       None,
            namespace_override: None,
            url_override:       None,
        };
        let environment = ResolvedEnvironment {
            name:             "dev".into(),
            url:              "http://localhost:2900".into(),
            namespace:        kalamdb_commons::NamespaceId::new("demo"),
            env_source:       crate::workflow::project::resolve::ResolutionSource::ProjectConfig,
            url_source:       crate::workflow::project::resolve::ResolutionSource::ProjectConfig,
            namespace_source: crate::workflow::project::resolve::ResolutionSource::ProjectConfig,
        };

        let auth = resolve_workflow_auth_provider(&ctx, &environment).unwrap();

        match auth {
            AuthProvider::JwtToken(token) => assert_eq!(token, "jwt-from-profile"),
            other => panic!("expected jwt auth, got {other:?}"),
        }

        match original_home {
            Some(value) => std::env::set_var("HOME", value),
            None => std::env::remove_var("HOME"),
        }
        match original_userprofile {
            Some(value) => std::env::set_var("USERPROFILE", value),
            None => std::env::remove_var("USERPROFILE"),
        }
    }
}
