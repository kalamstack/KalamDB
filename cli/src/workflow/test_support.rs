//! Shared test fixtures for workflow unit tests.

use std::{collections::HashMap, path::Path};

use kalamdb_commons::NamespaceId;

use crate::{
    config::CLIConfiguration,
    workflow::{
        project::{
            config::{
                ConnectionEnv, DevSection, KalamProjectConfig, LoggingSection, MigrationsSection,
                ProjectSection, SchemaMode, SchemaSection, SchemaTarget,
            },
            resolve::{ResolutionSource, ResolvedEnvironment},
        },
        WorkflowContext,
    },
};

pub fn test_workflow_context(project_root: &Path) -> WorkflowContext {
    WorkflowContext {
        project_root:       project_root.to_path_buf(),
        config:             minimal_sql_project_config(),
        cli_config:         CLIConfiguration::default(),
        use_color:          false,
        animations:         true,
        agent:              false,
        json:               false,
        project_dir:        None,
        env_override:       None,
        namespace_override: None,
        url_override:       None,
    }
}

pub fn test_environment(name: &str) -> ResolvedEnvironment {
    ResolvedEnvironment {
        name:             name.into(),
        url:              "http://localhost:2900".into(),
        namespace:        NamespaceId::new("demo"),
        env_source:       ResolutionSource::ProjectConfig,
        url_source:       ResolutionSource::ProjectConfig,
        namespace_source: ResolutionSource::ProjectConfig,
    }
}

pub fn minimal_sql_project_config() -> KalamProjectConfig {
    KalamProjectConfig {
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
                namespace: NamespaceId::new("demo"),
            },
        )]),
        schema:     SchemaSection {
            mode:      SchemaMode::Sql,
            path:      Some("schema.sql".into()),
            watch:     false,
            languages: Vec::new(),
            targets:   HashMap::new(),
        },
        migrations: MigrationsSection::default(),
        dev:        DevSection::default(),
        logging:    LoggingSection::default(),
    }
}

pub fn sql_project_config_with_typescript_target() -> KalamProjectConfig {
    let mut config = minimal_sql_project_config();
    config.schema.languages = vec!["typescript".into()];
    config.schema.targets = HashMap::from([(
        "typescript".into(),
        SchemaTarget {
            output:            "src/generated/kalam.ts".into(),
            unqualified_names: false,
        },
    )]);
    config
}

pub fn watch_test_config() -> KalamProjectConfig {
    let mut config = sql_project_config_with_typescript_target();
    config.schema.watch = true;
    config.dev = DevSection {
        apply_schema: true,
        generate_types: false,
        watch: true,
        ..DevSection::default()
    };
    config
}

pub fn prod_deploy_test_config() -> KalamProjectConfig {
    let mut config = sql_project_config_with_typescript_target();
    config.connection = HashMap::from([(
        "prod".into(),
        ConnectionEnv {
            url:       "https://db.example.com".into(),
            namespace: NamespaceId::new("app"),
        },
    )]);
    config.migrations.auto_create = true;
    config
}

pub fn multi_env_resolve_test_config() -> KalamProjectConfig {
    let mut config = sql_project_config_with_typescript_target();
    config.schema.watch = true;
    config.connection = HashMap::from([
        (
            "dev".into(),
            ConnectionEnv {
                url:       "http://localhost:2900".into(),
                namespace: NamespaceId::new("app"),
            },
        ),
        (
            "prod".into(),
            ConnectionEnv {
                url:       "https://db.example.com".into(),
                namespace: NamespaceId::new("app"),
            },
        ),
    ]);
    config
}

pub fn parse_minimal_project_config() -> KalamProjectConfig {
    KalamProjectConfig::parse(
        r#"
[project]
name = "demo"

[schema]
mode = "sql"
path = "schema.sql"

[schema.targets.typescript]
output = "src/generated/kalam.ts"
"#,
    )
    .expect("valid minimal test config")
}

#[cfg(test)]
mod env_lock {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn mutex() -> &'static Mutex<()> {
        static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_MUTEX.get_or_init(|| Mutex::new(()))
    }

    pub fn lock() -> MutexGuard<'static, ()> {
        mutex().lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn with_var<F, R>(key: &str, value: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _guard = lock();
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        let result = f();
        restore_var(key, previous);
        result
    }

    pub fn without_var<F, R>(key: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _guard = lock();
        let previous = std::env::var_os(key);
        std::env::remove_var(key);
        let result = f();
        restore_var(key, previous);
        result
    }

    fn restore_var(key: &str, previous: Option<std::ffi::OsString>) {
        match previous {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
}

#[cfg(test)]
pub use env_lock::{
    lock as test_env_lock, with_var as with_test_env_var, without_var as without_test_env_var,
};
