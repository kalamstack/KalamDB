//! Schema generation handlers.

use std::path::Path;

use crate::{
    error::{CLIError, Result},
    output::WorkflowOutput,
    workflow::{
        project::config::KalamProjectConfig,
        schema::{
            dart, load,
            model::{parse_language_list, LanguageTarget},
            naming::{assign_names, NamingOptions},
            rust, typescript,
        },
        WorkflowContext,
    },
};

pub struct GenerateOptions {
    pub languages: Option<Vec<String>>,
}

pub fn generate_schema_artifacts(
    ctx: &WorkflowContext,
    options: &GenerateOptions,
    output: &WorkflowOutput,
) -> Result<()> {
    let languages = resolve_languages(&ctx.config, options.languages.as_ref())?;
    generate_languages(&ctx.project_root, &ctx.config, &languages, Some(output))
}

pub fn generate_languages(
    project_root: &Path,
    config: &KalamProjectConfig,
    languages: &[LanguageTarget],
    output: Option<&WorkflowOutput>,
) -> Result<()> {
    if languages.is_empty() {
        return Err(CLIError::ConfigurationError(
            "no language targets selected for generation".into(),
        ));
    }

    let (snapshot, hash) = load::compile_project_contract(project_root, config)?;

    for language in languages {
        let key = language.as_str();
        let Some(target) = config.schema.targets.get(key) else {
            return Err(CLIError::ConfigurationError(format!(
                "missing schema.targets.{key} in kalam.toml"
            )));
        };
        let names = assign_names(
            &snapshot,
            NamingOptions {
                unqualified_names: target.unqualified_names,
            },
        )?;
        let output_path = project_root.join(&target.output);
        {
            let _spinner = output
                .map(|out| out.status_spinner(format!("generating {} -> {}", key, target.output)));
            match language {
                LanguageTarget::TypeScript => {
                    typescript::write_typescript(
                        &output_path,
                        &snapshot,
                        &hash,
                        &names,
                        project_root,
                    )?;
                },
                LanguageTarget::Dart => {
                    dart::write_dart_schema(&output_path, &snapshot, &hash, &names)?;
                },
                LanguageTarget::Rust => {
                    rust::write_rust_schema(&output_path, &snapshot, &hash, &names)?;
                },
            }
        }
        if let Some(out) = output {
            out.status(format!("generated {} -> {}", key, target.output));
        }
    }

    Ok(())
}

fn resolve_languages(
    config: &KalamProjectConfig,
    requested: Option<&Vec<String>>,
) -> Result<Vec<LanguageTarget>> {
    let languages = requested
        .map(|values| parse_language_list(values))
        .unwrap_or_else(|| parse_language_list(&config.schema.languages));
    if languages.is_empty() {
        return Err(CLIError::ConfigurationError(
            "no language targets selected for generation".into(),
        ));
    }
    Ok(languages)
}

pub fn validate_language_filter(
    requested: &[String],
    configured: &[String],
) -> Result<Vec<String>> {
    let mut normalized = Vec::new();
    for value in requested {
        let Some(parsed) = LanguageTarget::parse(value) else {
            return Err(CLIError::ConfigurationError(format!(
                "unsupported language target '{value}'; supported: typescript, dart, rust"
            )));
        };
        let canonical = parsed.as_str().to_string();
        if !configured.iter().any(|lang| LanguageTarget::parse(lang) == Some(parsed)) {
            return Err(CLIError::ConfigurationError(format!(
                "language '{canonical}' is not enabled in kalam.toml schema.languages"
            )));
        }
        if !normalized.iter().any(|lang| lang == &canonical) {
            normalized.push(canonical);
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_language_filter_accepts_flutter_alias() {
        let filtered =
            validate_language_filter(&["flutter".into()], &["dart".into()]).expect("filter");
        assert_eq!(filtered, vec!["dart"]);
    }

    #[test]
    fn validate_language_filter_accepts_rust() {
        let filtered =
            validate_language_filter(&["rust".into()], &["rust".into()]).expect("filter");
        assert_eq!(filtered, vec!["rust"]);
    }
}
