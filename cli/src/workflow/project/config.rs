//! Project configuration types and discovery for `kalam.toml`.

use std::{
    collections::HashMap,
    fs,
    path::{Component, Path, PathBuf},
};

use kalamdb_commons::NamespaceId;
use serde::{Deserialize, Serialize};

use crate::{
    error::{CLIError, Result},
    workflow::project::{
        connection_url,
        identifiers::serde_namespace,
        templates::{find_template_file, resolve_scaffold_template},
    },
};

pub const KALAM_TOML: &str = "kalam.toml";

/// User-facing message when a workflow command runs outside an initialized project.
pub fn missing_kalam_toml_message(location: &str) -> String {
    format!(
        "kalam.toml is missing in '{}'. Run `kalam init` to scaffold a new KalamDB project.",
        location
    )
}

fn invalid_kalam_toml_message(path: &Path, err: &toml::de::Error) -> String {
    format!(
        "failed to parse kalam.toml at '{}': {}\nRun `kalam init` to create a valid project \
         configuration, or fix the existing kalam.toml.",
        path.display(),
        err
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KalamProjectConfig {
    pub project:    ProjectSection,
    #[serde(default)]
    pub connection: HashMap<String, ConnectionEnv>,
    pub schema:     SchemaSection,
    #[serde(default)]
    pub migrations: MigrationsSection,
    #[serde(default)]
    pub dev:        DevSection,
    #[serde(default)]
    pub logging:    LoggingSection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectSection {
    pub name:            String,
    #[serde(default = "default_env_name")]
    pub default_env:     String,
    /// JavaScript package manager for TypeScript tooling (`npm`, `pnpm`, `yarn`, `bun`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_manager: Option<String>,
    #[serde(
        default = "default_kalam_dir",
        skip_serializing_if = "is_default_kalam_dir"
    )]
    pub kalam_dir:       String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectionEnv {
    pub url:       String,
    #[serde(with = "serde_namespace")]
    pub namespace: NamespaceId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaSection {
    pub mode:      SchemaMode,
    #[serde(default)]
    pub path:      Option<String>,
    #[serde(default = "default_true")]
    pub watch:     bool,
    #[serde(default = "default_languages")]
    pub languages: Vec<String>,
    #[serde(default)]
    pub targets:   HashMap<String, SchemaTarget>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaMode {
    Sql,
    Remote,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaTarget {
    pub output:            String,
    /// Drop schema prefixes from generated type names (`User` instead of `ChatUser`).
    /// Call paths stay nested. Generate fails if short names collide.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unqualified_names: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationsSection {
    #[serde(default = "default_migrations_dir", skip_serializing)]
    pub dir:         String,
    #[serde(default = "default_true")]
    pub auto_create: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DevSection {
    #[serde(default = "default_true")]
    pub auto_start_db:  bool,
    #[serde(default = "default_true")]
    pub apply_schema:   bool,
    #[serde(default = "default_true")]
    pub generate_types: bool,
    #[serde(default = "default_true")]
    pub watch:          bool,
    #[serde(default)]
    pub processes:      HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoggingSection {
    #[serde(default = "default_true")]
    pub file:                   bool,
    #[serde(default = "default_log_path", skip_serializing)]
    pub path:                   String,
    #[serde(default = "default_true")]
    pub capture_process_output: bool,
}

fn default_env_name() -> String {
    "dev".to_string()
}

fn default_kalam_dir() -> String {
    "kalam".to_string()
}

fn is_default_kalam_dir(value: &str) -> bool {
    value == default_kalam_dir()
}

fn default_true() -> bool {
    true
}

fn default_languages() -> Vec<String> {
    vec!["typescript".to_string()]
}

fn default_migrations_dir() -> String {
    "kalam/migrations".to_string()
}

fn default_log_path() -> String {
    "kalam/cli/logs/kalam.log".to_string()
}

impl KalamProjectConfig {
    pub fn discover(start: &Path, explicit_project_dir: Option<&Path>) -> Result<(PathBuf, Self)> {
        let root = if let Some(dir) = explicit_project_dir {
            dir.canonicalize().map_err(|e| {
                CLIError::ConfigurationError(format!(
                    "invalid project directory '{}': {}",
                    dir.display(),
                    e
                ))
            })?
        } else {
            discover_project_root(start)?
        };

        let config_path = root.join(KALAM_TOML);
        if !config_path.is_file() {
            return Err(CLIError::ConfigurationError(missing_kalam_toml_message(
                &root.display().to_string(),
            )));
        }

        let config = Self::load_from_path(&config_path)?;
        Ok((root, config))
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return CLIError::ConfigurationError(missing_kalam_toml_message(
                    &path.display().to_string(),
                ));
            }
            CLIError::ConfigurationError(format!("failed to read '{}': {}", path.display(), e))
        })?;
        let config: Self = toml::from_str(&contents)
            .map_err(|err| CLIError::ConfigurationError(invalid_kalam_toml_message(path, &err)))?;
        config.validate()?;
        Ok(config)
    }

    pub fn parse(contents: &str) -> Result<Self> {
        let config: Self = toml::from_str(contents).map_err(|err| {
            CLIError::ConfigurationError(format!(
                "failed to parse kalam.toml: {}\nRun `kalam init` to create a valid project \
                 configuration, or fix the existing kalam.toml.",
                err
            ))
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn save_to_path(&self, path: &Path) -> Result<()> {
        let contents = toml::to_string_pretty(self).map_err(|e| {
            CLIError::ConfigurationError(format!("failed to serialize kalam.toml: {}", e))
        })?;
        fs::write(path, contents)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.project.name.trim().is_empty() {
            return Err(CLIError::ConfigurationError("project.name must not be empty".into()));
        }

        validate_relative_project_path("project.kalam_dir", &self.project.kalam_dir)?;

        if let Some(manager) = self.project.package_manager.as_deref() {
            crate::workflow::project::ts::PackageManager::parse(manager)?;
        }

        for (env_name, connection) in &self.connection {
            connection_url::validate_http_server_url(&connection.url).map_err(|error| {
                CLIError::ConfigurationError(format!(
                    "connection.{env_name}.url is invalid: {error}"
                ))
            })?;
        }

        match self.schema.mode {
            SchemaMode::Sql => {
                let path = self.schema.path.as_deref().unwrap_or("").trim();
                if path.is_empty() {
                    return Err(CLIError::ConfigurationError(
                        "schema.path is required when schema.mode is 'sql'".into(),
                    ));
                }
            },
            SchemaMode::Remote => {},
        }

        let mut outputs = std::collections::HashSet::new();
        for language in &self.schema.languages {
            let Some(target) = self.schema.targets.get(language) else {
                return Err(CLIError::ConfigurationError(format!(
                    "schema.targets.{language} is required for language '{language}'"
                )));
            };
            if !outputs.insert(target.output.clone()) {
                return Err(CLIError::ConfigurationError(format!(
                    "duplicate schema target output path '{}'",
                    target.output
                )));
            }
        }

        Ok(())
    }

    pub fn schema_source_path(&self, project_root: &Path) -> Option<PathBuf> {
        self.schema.path.as_ref().map(|p| project_root.join(p))
    }

    pub fn kalam_dir(&self, project_root: &Path) -> PathBuf {
        project_root.join(&self.project.kalam_dir)
    }

    pub fn cli_dir(&self, project_root: &Path) -> PathBuf {
        self.kalam_dir(project_root).join("cli")
    }

    pub fn workflow_log_path(&self, project_root: &Path) -> PathBuf {
        self.cli_dir(project_root).join("logs").join("kalam.log")
    }

    pub fn dev_session_path(&self, project_root: &Path) -> PathBuf {
        self.cli_dir(project_root).join("dev.session.json")
    }

    pub fn ensure_cli_log_dir(&self, project_root: &Path) -> Result<PathBuf> {
        let log_dir = self.cli_dir(project_root).join("logs");
        fs::create_dir_all(&log_dir).map_err(|error| {
            CLIError::FileError(format!(
                "failed to create KalamDB CLI log directory '{}': {error}",
                log_dir.display()
            ))
        })?;
        Ok(log_dir)
    }

    pub fn ensure_kalam_gitignore(&self, project_root: &Path) -> Result<PathBuf> {
        let kalam_dir = self.kalam_dir(project_root);
        fs::create_dir_all(&kalam_dir).map_err(|error| {
            CLIError::FileError(format!(
                "failed to create KalamDB project directory '{}': {error}",
                kalam_dir.display()
            ))
        })?;

        let gitignore_path = kalam_dir.join(".gitignore");
        if !gitignore_path.exists() {
            let template = resolve_scaffold_template().map_err(|error| {
                CLIError::ConfigurationError(format!(
                    "failed to load scaffold template for kalam/.gitignore: {error}"
                ))
            })?;
            let contents = find_template_file(template, "kalam/.gitignore").ok_or_else(|| {
                CLIError::ConfigurationError(
                    "missing scaffold template file 'kalam/.gitignore'".into(),
                )
            })?;
            fs::write(&gitignore_path, contents).map_err(|error| {
                CLIError::FileError(format!(
                    "failed to write '{}': {error}",
                    gitignore_path.display()
                ))
            })?;
        }
        Ok(gitignore_path)
    }

    pub fn local_server_dir(&self, project_root: &Path) -> PathBuf {
        self.kalam_dir(project_root).join("server")
    }

    pub fn local_server_config_path(&self, project_root: &Path) -> PathBuf {
        self.local_server_dir(project_root).join("server.toml")
    }

    pub fn relative_local_server_data_path(&self) -> String {
        format!("{}/server/data", normalize_project_dir_for_config(&self.project.kalam_dir))
    }

    pub fn relative_local_server_logs_path(&self) -> String {
        format!("{}/server/logs", normalize_project_dir_for_config(&self.project.kalam_dir))
    }

    pub fn migrations_dir(&self, project_root: &Path) -> PathBuf {
        self.kalam_dir(project_root).join("migrations")
    }

    /// Baseline schema snapshot used as the "before" side of migration diffs.
    pub fn schema_baseline_path(&self, project_root: &Path) -> PathBuf {
        self.kalam_dir(project_root).join(".schema-baseline.sql")
    }
}

fn validate_relative_project_path(field: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CLIError::ConfigurationError(format!("{field} must not be empty")));
    }

    let path = Path::new(trimmed);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_))
        })
    {
        return Err(CLIError::ConfigurationError(format!(
            "{field} must be a relative path inside the project"
        )));
    }

    Ok(())
}

fn normalize_project_dir_for_config(value: &str) -> String {
    value.trim().trim_matches('/').to_string()
}

fn discover_project_root(start: &Path) -> Result<PathBuf> {
    let start = if start.is_file() {
        start
            .parent()
            .ok_or_else(|| CLIError::ConfigurationError("invalid start path".into()))?
    } else {
        start
    };

    let mut current = start.canonicalize().unwrap_or_else(|_| start.to_path_buf());

    loop {
        if current.join(KALAM_TOML).is_file() {
            return Ok(current);
        }
        if !current.pop() {
            break;
        }
    }

    Err(CLIError::ConfigurationError(missing_kalam_toml_message(
        &start.display().to_string(),
    )))
}

pub use super::identifiers::{
    normalize_namespace_name, parse_namespace_id, parse_table_id, parse_table_name,
    parse_table_ref, parse_user_id, preferred_user_label,
};

#[cfg(test)]
mod tests {
    use kalamdb_commons::NamespaceId;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn parse_config_with_package_manager_field() {
        let toml = r#"
[project]
name = "demo"
default_env = "dev"
package_manager = "pnpm"

[connection.dev]
url = "http://localhost:2900"
namespace = "app"

[schema]
mode = "sql"
path = "schema.sql"
languages = ["typescript"]

[schema.targets.typescript]
output = "src/generated/kalam.ts"
"#;
        let config = KalamProjectConfig::parse(toml).expect("parse");
        assert_eq!(config.project.package_manager.as_deref(), Some("pnpm"));
    }

    #[test]
    fn validate_rejects_non_http_connection_urls() {
        let toml = r#"
[project]
name = "demo"

[connection.dev]
url = "javascript:alert(1)"
namespace = "app"

[schema]
mode = "sql"
path = "schema.sql"
languages = ["typescript"]

[schema.targets.typescript]
output = "src/generated/kalam.ts"
"#;
        let err = KalamProjectConfig::parse(toml).unwrap_err().to_string();
        assert!(err.contains("http or https"));
    }

    #[test]
    fn validate_rejects_unknown_package_manager() {
        let toml = r#"
[project]
name = "demo"
package_manager = "deno"

[connection.dev]
url = "http://localhost:2900"
namespace = "app"

[schema]
mode = "sql"
path = "schema.sql"
languages = ["typescript"]

[schema.targets.typescript]
output = "src/generated/kalam.ts"
"#;
        let err = KalamProjectConfig::parse(toml).unwrap_err().to_string();
        assert!(err.contains("unsupported package manager 'deno'"));
    }

    #[test]
    fn validate_accepts_supported_package_manager_values() {
        for manager in ["npm", "pnpm", "yarn", "bun"] {
            let toml = format!(
                r#"
[project]
name = "demo"
package_manager = "{manager}"

[connection.dev]
url = "http://localhost:2900"
namespace = "app"

[schema]
mode = "sql"
path = "schema.sql"
languages = ["typescript"]

[schema.targets.typescript]
output = "src/generated/kalam.ts"
"#
            );
            let config = KalamProjectConfig::parse(&toml).expect("parse supported manager");
            assert_eq!(config.project.package_manager.as_deref(), Some(manager));
        }
    }

    #[test]
    fn parse_minimal_config() {
        let toml = r#"
[project]
name = "demo"
default_env = "dev"

[connection.dev]
url = "http://localhost:2900"
namespace = "app"

[schema]
mode = "sql"
path = "schema.sql"
languages = ["typescript"]

[schema.targets.typescript]
output = "src/generated/kalam.ts"
"#;
        let config = KalamProjectConfig::parse(toml).expect("parse");
        assert_eq!(config.project.name, "demo");
        assert_eq!(config.project.kalam_dir, "kalam");
        assert_eq!(config.schema.mode, SchemaMode::Sql);
    }

    #[test]
    fn project_paths_use_configured_kalam_dir() {
        let toml = r#"
[project]
name = "demo"
kalam_dir = "db"

[schema]
mode = "sql"
path = "schema.sql"

[schema.targets.typescript]
output = "src/generated/kalam.ts"
"#;
        let config = KalamProjectConfig::parse(toml).expect("parse");
        let root = Path::new("/tmp/demo");

        assert_eq!(config.kalam_dir(root), root.join("db"));
        assert_eq!(config.migrations_dir(root), root.join("db/migrations"));
        assert_eq!(config.workflow_log_path(root), root.join("db/cli/logs/kalam.log"));
        assert_eq!(config.dev_session_path(root), root.join("db/cli/dev.session.json"));
        assert_eq!(config.schema_baseline_path(root), root.join("db/.schema-baseline.sql"));
        assert_eq!(config.local_server_config_path(root), root.join("db/server/server.toml"));
    }

    #[test]
    fn missing_kalam_toml_message_includes_init_hint() {
        let message = missing_kalam_toml_message("/tmp/demo");
        assert!(message.contains("kalam.toml is missing"));
        assert!(message.contains("kalam init"));
    }

    #[test]
    fn parse_reports_invalid_kalam_toml_with_init_hint() {
        let toml = r#"
[project]
name = "demo"

[schema]
path = "schema.sql"
"#;
        let err = KalamProjectConfig::parse(toml).unwrap_err().to_string();
        assert!(err.contains("failed to parse kalam.toml"));
        assert!(err.contains("kalam init"));
        assert!(err.contains("missing field `mode`"));
    }

    #[test]
    fn discover_reports_missing_kalam_toml_with_init_hint() {
        let temp = TempDir::new().unwrap();
        let err = KalamProjectConfig::discover(temp.path(), None).unwrap_err().to_string();
        assert!(err.contains("kalam.toml is missing"));
        assert!(err.contains("kalam init"));
    }

    #[test]
    fn normalize_namespace_name_replaces_invalid_characters() {
        assert_eq!(normalize_namespace_name("dev-test1"), "dev_test1");
        assert_eq!(normalize_namespace_name("demo.app"), "demo_app");
        assert_eq!(normalize_namespace_name("  my app  "), "my_app");
    }

    #[test]
    fn parse_rejects_invalid_namespace_in_connection() {
        let toml = r#"
[project]
name = "demo"

[connection.dev]
url = "http://localhost:2900"
namespace = "dev-test1"

[schema]
mode = "sql"
path = "schema.sql"
languages = ["typescript"]

[schema.targets.typescript]
output = "src/generated/kalam.ts"
"#;
        let err = KalamProjectConfig::parse(toml).unwrap_err().to_string();
        assert!(err.contains("Invalid namespace ID 'dev-test1'"));
        assert!(err.contains("dev_test1"));
    }

    #[test]
    fn discover_walks_up_directories() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("proj");
        let nested = root.join("src").join("app");
        fs::create_dir_all(&nested).unwrap();
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
                    namespace: NamespaceId::new("app"),
                },
            )]),
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
        }
        .save_to_path(&root.join(KALAM_TOML))
        .unwrap();

        let (found_root, _) =
            KalamProjectConfig::discover(&nested, None).expect("discover from nested dir");
        assert_eq!(found_root, root.canonicalize().unwrap());
    }
}

impl Default for MigrationsSection {
    fn default() -> Self {
        Self {
            dir:         default_migrations_dir(),
            auto_create: true,
        }
    }
}

impl Default for DevSection {
    fn default() -> Self {
        Self {
            auto_start_db:  true,
            apply_schema:   true,
            generate_types: true,
            watch:          true,
            processes:      HashMap::new(),
        }
    }
}

impl Default for LoggingSection {
    fn default() -> Self {
        Self {
            file:                   true,
            path:                   default_log_path(),
            capture_process_output: true,
        }
    }
}
