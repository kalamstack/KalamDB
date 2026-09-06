//! Persist environment connection settings in `kalam.toml` (no secrets).

use std::path::Path;

use crate::{
    error::{CLIError, Result},
    output::WorkflowOutput,
    workflow::{
        project::{
            config::{ConnectionEnv, KalamProjectConfig, KALAM_TOML},
            identifiers::parse_namespace_id,
        },
        WorkflowContext,
    },
};

pub struct LinkOptions {
    pub env:       Option<String>,
    pub url:       Option<String>,
    pub namespace: Option<String>,
}

pub fn link_environment(
    ctx: &WorkflowContext,
    options: &LinkOptions,
    output: &WorkflowOutput,
) -> Result<()> {
    let env_name = options
        .env
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| ctx.config.project.default_env.clone());

    let url = options.url.as_deref().map(str::trim).filter(|v| !v.is_empty());
    let namespace = options.namespace.as_deref().map(str::trim).filter(|v| !v.is_empty());

    let (url, namespace) = match (url, namespace) {
        (Some(url), Some(namespace)) => (url.to_string(), namespace.to_string()),
        (None, None) => {
            return Err(CLIError::ConfigurationError(
                "link requires --url and --namespace (secrets stay outside kalam.toml)".into(),
            ));
        },
        _ => {
            return Err(CLIError::ConfigurationError(
                "link requires both --url and --namespace".into(),
            ));
        },
    };

    let mut config = ctx.config.clone();
    config.connection.insert(
        env_name.clone(),
        ConnectionEnv {
            url,
            namespace: parse_namespace_id(&namespace)?,
        },
    );

    let config_path = ctx.project_root.join(KALAM_TOML);
    save_project_config(&config_path, &config)?;

    output.status(format!("linked environment '{env_name}' in kalam.toml"));
    Ok(())
}

fn save_project_config(path: &Path, config: &KalamProjectConfig) -> Result<()> {
    config.validate()?;
    config.save_to_path(path)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::{CLIConfiguration, WorkflowLoggingPolicy},
        workflow::project::config::{
            DevSection, LoggingSection, MigrationsSection, ProjectSection, SchemaMode,
            SchemaSection, SchemaTarget,
        },
    };

    #[test]
    fn link_persists_connection_without_secrets() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        let config = KalamProjectConfig {
            project:    ProjectSection {
                name:            "demo".into(),
                default_env:     "dev".into(),
                package_manager: None,
                kalam_dir:       "kalam".into(),
            },
            connection: HashMap::new(),
            schema:     SchemaSection {
                mode:      SchemaMode::Sql,
                path:      Some("schema.sql".into()),
                watch:     true,
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
        };
        config.save_to_path(&root.join(KALAM_TOML)).unwrap();

        let ctx = WorkflowContext {
            project_root: root.to_path_buf(),
            config,
            cli_config: CLIConfiguration::default(),
            use_color: false,
            animations: true,
            agent: false,
            json: false,
            project_dir: None,
            env_override: None,
            namespace_override: None,
            url_override: None,
        };
        let output = WorkflowOutput::new(false, WorkflowLoggingPolicy::disabled());

        link_environment(
            &ctx,
            &LinkOptions {
                env:       Some("prod".into()),
                url:       Some("https://db.example.com".into()),
                namespace: Some("app".into()),
            },
            &output,
        )
        .unwrap();

        let loaded = KalamProjectConfig::load_from_path(&root.join(KALAM_TOML)).unwrap();
        let prod = loaded.connection.get("prod").expect("prod link");
        assert_eq!(prod.url, "https://db.example.com");
        assert_eq!(prod.namespace, kalamdb_commons::NamespaceId::new("app"));
    }
}
