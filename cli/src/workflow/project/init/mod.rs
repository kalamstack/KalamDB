//! `kalam init` project scaffolding.

mod prompts;
mod write;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::{
    error::{CLIError, Result},
    output::WorkflowOutput,
    workflow::project::{
        config::{
            ConnectionEnv, DevSection, KalamProjectConfig, LoggingSection, MigrationsSection,
            ProjectSection, SchemaMode, SchemaSection, SchemaTarget, KALAM_TOML,
        },
        dart::{
            self, DEFAULT_DEV_COMMAND as DART_DEV_COMMAND,
            SCHEMA_TARGET_OUTPUT as DART_SCHEMA_TARGET_OUTPUT,
        },
        guidance::{
            init_config_validation_failed, init_project_already_exists, init_stage_context,
        },
        identifiers::{normalize_namespace_name, parse_namespace_id},
        prompts::print_workflow_banner,
        repository_examples, templates,
        ts::{
            execute_package_install, install_dependencies, resolve_package_manager,
            resolve_starter, PackageManager, ProjectStarter, SCHEMA_TARGET_OUTPUT,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerMode {
    Local,
    Remote,
}

#[derive(Debug, Clone)]
pub struct InitOptions {
    pub name:            Option<String>,
    pub schema_mode:     Option<SchemaMode>,
    pub languages:       Option<Vec<String>>,
    pub template:        Option<String>,
    pub package_manager: Option<PackageManager>,
    pub server_mode:     Option<ServerMode>,
    pub server_url:      Option<String>,
    pub yes:             bool,
    pub cwd:             PathBuf,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct InitTemplateInfo {
    pub id:          String,
    pub kind:        String,
    pub language:    String,
    pub description: String,
}

pub fn list_init_templates() -> Vec<InitTemplateInfo> {
    let mut templates = Vec::new();
    for language in ["typescript", "dart"] {
        for template in templates::templates_for_language(language) {
            templates.push(InitTemplateInfo {
                id:          template.id.to_string(),
                kind:        "embedded".to_string(),
                language:    language.to_string(),
                description: template.description.to_string(),
            });
        }
    }
    for example in repository_examples::available() {
        templates.push(InitTemplateInfo {
            id:          example.id.to_string(),
            kind:        "repository".to_string(),
            language:    "typescript".to_string(),
            description: example.description.to_string(),
        });
    }
    templates
}

pub async fn run_init(options: InitOptions, output: &WorkflowOutput) -> Result<()> {
    run_init_with_installer(options, output, execute_package_install).await
}

pub(crate) async fn run_init_with_installer<F>(
    options: InitOptions,
    output: &WorkflowOutput,
    mut installer: F,
) -> Result<()>
where
    F: FnMut(&Path, PackageManager) -> Result<()>,
{
    if options.cwd.join(KALAM_TOML).exists() {
        return Err(CLIError::ConfigurationError(init_project_already_exists(&options.cwd)));
    }

    prompts::ensure_interactive_or_yes(&options)?;

    if !options.yes {
        print_workflow_banner(
            "KalamDB project setup",
            "Create a project configuration for development and deployment.",
            output.use_color,
        );
    }

    let name = prompts::resolve_project_name(&options, output.use_color)?;
    let schema_mode = prompts::resolve_schema_mode(&options, output.use_color)?;
    let languages = prompts::resolve_languages(&options, output.use_color)?;
    let starter =
        resolve_starter(&languages, options.template.as_deref(), options.yes, output.use_color)?;
    let dart_template = if crate::workflow::project::ts::is_enabled(&languages) {
        dart::resolve_starter(&languages, None, true, output.use_color)?
    } else {
        dart::resolve_starter(
            &languages,
            options.template.as_deref(),
            options.yes,
            output.use_color,
        )?
    };
    let package_manager =
        resolve_package_manager(&languages, options.package_manager, options.yes, output)?;
    if let Some(ProjectStarter::Repository(example)) = starter {
        let download = {
            let _spinner =
                output.status_spinner(format!("downloading KalamDB example '{}'", example.id));
            repository_examples::download_repository_example(&options.cwd, example, false).await
        };
        download.map_err(|error| map_init_stage_error("downloading repository example", error))?;
        install_dependencies(&options.cwd, package_manager, output, &mut installer)
            .map_err(|error| map_init_stage_error("installing JavaScript dependencies", error))?;
        finish_init(output, example.id, &options.cwd, options.yes)?;
        return Ok(());
    }

    let typescript_template = match starter {
        Some(ProjectStarter::Embedded(template)) => Some(template),
        Some(ProjectStarter::Repository(_)) => unreachable!("repository examples return earlier"),
        None => None,
    };
    let server_mode = prompts::resolve_server_mode(&options, output.use_color)?;
    let server_url = prompts::resolve_server_url(&options, server_mode, output.use_color)?;

    let config =
        build_config(&name, schema_mode, &languages, server_mode, &server_url, package_manager)?;
    config.validate().map_err(|error| map_init_config_validation_error(error))?;
    if let Some(connection) = config.connection.get("dev") {
        if connection.namespace.as_str() != name {
            output.detail(format!(
                "using namespace '{}' for project name '{name}'",
                connection.namespace.as_str()
            ));
        }
    }

    write::write_project_scaffold(
        &options.cwd,
        &config,
        schema_mode,
        server_mode,
        &server_url,
        typescript_template,
        dart_template,
        output,
    )
    .map_err(|error| map_init_stage_error("writing project files", error))?;
    install_dependencies(&options.cwd, package_manager, output, &mut installer)
        .map_err(|error| map_init_stage_error("installing JavaScript dependencies", error))?;
    if dart::is_enabled(&languages) {
        let package_name = config
            .connection
            .get("dev")
            .map(|connection| connection.namespace.as_str().to_string())
            .unwrap_or_else(|| normalize_namespace_name(&name));
        dart::maybe_bootstrap_flutter_project(&options.cwd, &package_name, output)
            .map_err(|error| map_init_stage_error("bootstrapping Flutter project", error))?;
    }
    finish_init(output, &name, &options.cwd, options.yes)?;
    Ok(())
}

fn finish_init(output: &WorkflowOutput, name: &str, root: &Path, yes: bool) -> Result<()> {
    output.status(format!("initialized KalamDB project '{name}'"));
    if output.json {
        output.emit_json(&serde_json::json!({
            "ok": true,
            "project": name,
            "created": created_init_files(root),
            "next": "kalam dev --agent",
        }));
        return Ok(());
    }
    if yes {
        output.status("next: kalam dev --agent");
    }
    Ok(())
}

fn created_init_files(root: &Path) -> Vec<String> {
    const CANDIDATES: &[&str] = &[
        "kalam.toml",
        "schema.sql",
        ".env",
        ".env.example",
        "src/index.ts",
        "package.json",
        "lib/main.dart",
        "pubspec.yaml",
    ];
    CANDIDATES
        .iter()
        .copied()
        .filter(|path| root.join(path).is_file())
        .map(ToString::to_string)
        .collect()
}

fn map_init_stage_error(stage: &str, error: CLIError) -> CLIError {
    match error {
        CLIError::ConfigurationError(message) => {
            CLIError::ConfigurationError(init_stage_context(stage, message))
        },
        CLIError::FileError(message) => {
            CLIError::ConfigurationError(init_stage_context(stage, message))
        },
        other => other,
    }
}

fn map_init_config_validation_error(error: CLIError) -> CLIError {
    match error {
        CLIError::ConfigurationError(message) => CLIError::ConfigurationError(init_stage_context(
            "validating project configuration",
            init_config_validation_failed(&message),
        )),
        other => map_init_stage_error("validating project configuration", other),
    }
}

fn build_config(
    name: &str,
    schema_mode: SchemaMode,
    languages: &[String],
    server_mode: ServerMode,
    server_url: &str,
    package_manager: Option<PackageManager>,
) -> Result<KalamProjectConfig> {
    let mut targets = HashMap::new();
    for language in languages {
        let output = match language.as_str() {
            "typescript" => SCHEMA_TARGET_OUTPUT,
            "dart" => DART_SCHEMA_TARGET_OUTPUT,
            _ => continue,
        };
        targets.insert(
            language.clone(),
            SchemaTarget {
                output:            output.into(),
                unqualified_names: false,
            },
        );
    }

    Ok(KalamProjectConfig {
        project:    ProjectSection {
            name:            name.to_string(),
            default_env:     "dev".into(),
            package_manager: package_manager.map(PackageManager::as_str).map(str::to_string),
            kalam_dir:       "kalam".into(),
        },
        connection: HashMap::from([(
            "dev".into(),
            ConnectionEnv {
                url:       server_url.to_string(),
                namespace: parse_namespace_id(&normalize_namespace_name(name))?,
            },
        )]),
        schema:     SchemaSection {
            mode: schema_mode,
            path: Some("schema.sql".into()),
            watch: true,
            languages: languages.to_vec(),
            targets,
        },
        migrations: MigrationsSection {
            dir:         "kalam/migrations".into(),
            auto_create: true,
        },
        dev:        DevSection {
            auto_start_db: matches!(server_mode, ServerMode::Local),
            processes: if let Some(manager) = package_manager {
                HashMap::from([("app".into(), manager.dev_run_command().into())])
            } else if languages.iter().any(|language| language == "dart") {
                HashMap::from([("app".into(), DART_DEV_COMMAND.into())])
            } else {
                HashMap::new()
            },
            ..DevSection::default()
        },
        logging:    LoggingSection::default(),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::WorkflowLoggingPolicy,
        workflow::{project::ts::SKIP_PACKAGE_INSTALL_ENV, test_support::with_test_env_var},
    };

    fn block_on_init<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Runtime::new().expect("tokio runtime").block_on(future)
    }

    #[test]
    fn init_scaffolds_project_files() {
        with_test_env_var(SKIP_PACKAGE_INSTALL_ENV, "1", || {
            let temp = TempDir::new().unwrap();
            let output = WorkflowOutput::new(false, WorkflowLoggingPolicy::disabled());
            block_on_init(run_init(
                InitOptions {
                    name:            Some("demo-app".into()),
                    schema_mode:     Some(SchemaMode::Sql),
                    languages:       Some(vec!["typescript".into()]),
                    template:        Some("simple-live".into()),
                    package_manager: None,
                    server_mode:     Some(ServerMode::Local),
                    server_url:      None,
                    yes:             true,
                    cwd:             temp.path().to_path_buf(),
                },
                &output,
            ))
            .unwrap();

            assert!(temp.path().join(KALAM_TOML).is_file());
            assert!(temp.path().join("schema.sql").is_file());
            assert!(temp.path().join("package.json").is_file());
            assert!(temp.path().join("kalam/migrations/.gitkeep").is_file());
            assert!(temp.path().join("src/generated").is_dir());
            assert!(temp.path().join("kalam/server/server.toml").is_file());
            let kalam_toml = fs::read_to_string(temp.path().join(KALAM_TOML)).unwrap();
            assert!(kalam_toml.contains("[dev.processes]"));
            let config = KalamProjectConfig::load_from_path(&temp.path().join(KALAM_TOML)).unwrap();
            assert!(config.dev.processes.get("app").is_some_and(|command| command.contains("dev")));
        });
    }

    #[test]
    fn list_init_templates_includes_embedded_and_repository() {
        let templates = list_init_templates();
        assert!(templates.iter().any(|template| {
            template.id == "simple-live"
                && template.kind == "embedded"
                && template.language == "typescript"
        }));
        assert!(templates.iter().any(|template| {
            template.id == "chat-with-ai"
                && template.kind == "repository"
                && template.description.contains("SHARED")
        }));
        assert!(templates.iter().any(|template| template.id == "react-ai-chat"));
    }

    #[test]
    fn init_creates_project_env_file_and_ignores_it() {
        with_test_env_var(SKIP_PACKAGE_INSTALL_ENV, "1", || {
            let temp = TempDir::new().unwrap();
            let output = WorkflowOutput::new(false, WorkflowLoggingPolicy::disabled());
            block_on_init(run_init(
                InitOptions {
                    name:            Some("demo-app".into()),
                    schema_mode:     Some(SchemaMode::Sql),
                    languages:       Some(vec!["typescript".into()]),
                    template:        None,
                    package_manager: None,
                    server_mode:     Some(ServerMode::Local),
                    server_url:      None,
                    yes:             true,
                    cwd:             temp.path().to_path_buf(),
                },
                &output,
            ))
            .unwrap();

            let env_contents = fs::read_to_string(temp.path().join(".env")).unwrap();
            assert!(env_contents.contains("KALAM_PROFILE=kalam-dev"));
            assert!(env_contents.contains("KALAM_NAMESPACE=demo_app"));
            assert!(env_contents.contains("KALAM_USER=root"));
            assert!(env_contents.contains("KALAM_PASSWORD=kalamdb123"));

            let gitignore = fs::read_to_string(temp.path().join(".gitignore")).unwrap();
            assert!(gitignore.lines().any(|line| line.trim() == ".env"));
        });
    }

    #[test]
    fn init_dart_project_omits_package_manager_from_kalam_toml() {
        with_test_env_var(SKIP_PACKAGE_INSTALL_ENV, "1", || {
            let temp = TempDir::new().unwrap();
            let output = WorkflowOutput::new(false, WorkflowLoggingPolicy::disabled());

            block_on_init(run_init(
                InitOptions {
                    name:            Some("demo-dart".into()),
                    schema_mode:     Some(SchemaMode::Sql),
                    languages:       Some(vec!["dart".into()]),
                    template:        None,
                    package_manager: None,
                    server_mode:     Some(ServerMode::Local),
                    server_url:      None,
                    yes:             true,
                    cwd:             temp.path().to_path_buf(),
                },
                &output,
            ))
            .unwrap();

            let kalam_toml = fs::read_to_string(temp.path().join(KALAM_TOML)).unwrap();
            assert!(!kalam_toml.contains("package_manager"));
            assert!(kalam_toml.contains("[dev.processes]"));
            assert!(kalam_toml.contains("app = \"flutter run\""));

            let config = KalamProjectConfig::load_from_path(&temp.path().join(KALAM_TOML)).unwrap();
            assert!(config.project.package_manager.is_none());
            assert_eq!(config.dev.processes.get("app").map(String::as_str), Some("flutter run"));

            assert!(temp.path().join("pubspec.yaml").is_file());
            assert!(temp.path().join("lib/main.dart").is_file());
            assert!(temp.path().join("schema.sql").is_file());
            let generated =
                fs::read_to_string(temp.path().join("lib/generated/kalam.dart")).unwrap();
            assert!(generated.contains("KalamTableSpec<Users>"));
            assert!(generated.contains("tableId: 'users'"));
            assert!(!generated.to_lowercase().contains("placeholder"));
        });
    }

    #[test]
    fn init_dart_only_project_skips_npm_install() {
        let temp = TempDir::new().unwrap();
        let output = WorkflowOutput::new(false, WorkflowLoggingPolicy::disabled());
        let mut called = false;

        block_on_init(run_init_with_installer(
            InitOptions {
                name:            Some("demo-dart".into()),
                schema_mode:     Some(SchemaMode::Sql),
                languages:       Some(vec!["dart".into()]),
                template:        None,
                package_manager: None,
                server_mode:     Some(ServerMode::Local),
                server_url:      None,
                yes:             true,
                cwd:             temp.path().to_path_buf(),
            },
            &output,
            |_root, _command| {
                called = true;
                Ok(())
            },
        ))
        .unwrap();

        assert!(!called, "dart-only init should not request npm install");
    }

    #[test]
    fn init_normalizes_namespace_for_hyphenated_project_name() {
        with_test_env_var(SKIP_PACKAGE_INSTALL_ENV, "1", || {
            let temp = TempDir::new().unwrap();
            let output = WorkflowOutput::new(false, WorkflowLoggingPolicy::disabled());
            block_on_init(run_init(
                InitOptions {
                    name:            Some("dev-test1".into()),
                    schema_mode:     Some(SchemaMode::Sql),
                    languages:       Some(vec!["typescript".into()]),
                    template:        None,
                    package_manager: None,
                    server_mode:     Some(ServerMode::Local),
                    server_url:      None,
                    yes:             true,
                    cwd:             temp.path().to_path_buf(),
                },
                &output,
            ))
            .unwrap();

            let config = KalamProjectConfig::load_from_path(&temp.path().join(KALAM_TOML)).unwrap();
            assert_eq!(config.project.name, "dev-test1");
            assert_eq!(
                config.connection.get("dev").expect("dev env").namespace,
                kalamdb_commons::NamespaceId::new("dev_test1")
            );

            let env_contents = fs::read_to_string(temp.path().join(".env")).unwrap();
            assert!(env_contents.contains("KALAM_NAMESPACE=dev_test1"));
        });
    }

    #[test]
    fn remote_server_mode_disables_auto_start_db() {
        with_test_env_var(SKIP_PACKAGE_INSTALL_ENV, "1", || {
            let temp = TempDir::new().unwrap();
            let output = WorkflowOutput::new(false, WorkflowLoggingPolicy::disabled());
            block_on_init(run_init(
                InitOptions {
                    name:            Some("remote-app".into()),
                    schema_mode:     Some(SchemaMode::Sql),
                    languages:       Some(vec!["typescript".into()]),
                    template:        None,
                    package_manager: None,
                    server_mode:     Some(ServerMode::Remote),
                    server_url:      Some("http://localhost:2900".into()),
                    yes:             true,
                    cwd:             temp.path().to_path_buf(),
                },
                &output,
            ))
            .unwrap();

            let config = KalamProjectConfig::load_from_path(&temp.path().join(KALAM_TOML)).unwrap();
            assert!(!config.dev.auto_start_db);
            assert!(!temp.path().join("kalam/server/server.toml").exists());
        });
    }
}
