use std::{
    env, fs,
    io::{self, IsTerminal, Write},
    path::{Component, Path, PathBuf},
};

use kalam_cli::{CLIError, Result};

use crate::args::Cli;

const DEFAULT_TABLE_ID: &str = "blog.blogs";
const DEFAULT_ID_COLUMN: &str = "blog_id";
const DEFAULT_INPUT_COLUMN: &str = "content";
const DEFAULT_OUTPUT_COLUMN: &str = "summary";
const DEFAULT_SYSTEM_PROMPT: &str = "Write one concise sentence summarizing the content. Preserve \
                                     key facts and avoid hallucinations.";
const FALLBACK_LOCAL_SDK_PATH: &str = "../../link/sdks/typescript/client";

#[derive(Debug, Clone)]
struct AgentScaffoldConfig {
    project_name: String,
    output_root: PathBuf,
    table_id: String,
    topic_id: String,
    group_id: String,
    id_column: String,
    input_column: String,
    output_column: String,
    system_prompt: String,
}

#[derive(Debug, Clone)]
struct ParsedTableId {
    namespace: String,
    table: String,
}

pub fn handle_init_agent(cli: &Cli) -> Result<bool> {
    if !cli.init_agent {
        return Ok(false);
    }

    let config = if cli.init_agent_non_interactive {
        build_non_interactive_config(cli)?
    } else {
        build_interactive_config(cli)?
    };

    let parsed_table = parse_table_id(&config.table_id)?;
    let project_dir = resolve_project_dir(&config.output_root, &config.project_name)?;
    let normalized_project_dir = normalize_project_dir_for_resolution(&project_dir)?;
    let sdk_dependency = detect_sdk_dependency(&normalized_project_dir)?;

    generate_agent_project(&config, &parsed_table, &project_dir, &sdk_dependency)?;

    println!("Created agent project at {}", project_dir.display());
    println!("\nNext steps:");
    println!("  1. cd {}", project_dir.display());
    println!("  2. ./setup.sh");
    println!("  3. npm install");
    println!("  4. npm run start");

    Ok(true)
}

fn build_non_interactive_config(cli: &Cli) -> Result<AgentScaffoldConfig> {
    build_config(cli, false)
}

fn build_interactive_config(cli: &Cli) -> Result<AgentScaffoldConfig> {
    if !io::stdin().is_terminal() {
        return Err(CLIError::ConfigurationError(
            "Interactive init requires a terminal. Use --init-agent-non-interactive to run in \
             scripts."
                .into(),
        ));
    }

    build_config(cli, true)
}

fn build_config(cli: &Cli, interactive: bool) -> Result<AgentScaffoldConfig> {
    let base_table = cli.agent_table.clone().unwrap_or_else(|| DEFAULT_TABLE_ID.to_string());
    let parsed_table = parse_table_id(&base_table)?;

    let default_project_name = format!("{}-agent", parsed_table.table);
    let default_topic = format!("{}.{}_agent", parsed_table.namespace, parsed_table.table);
    let default_group = format!("{}-group", slugify(&default_project_name));

    let project_name = choose_value(
        interactive,
        "Project name",
        cli.agent_name.as_deref(),
        &default_project_name,
    )?;
    let output_root =
        choose_path(interactive, "Output directory", cli.agent_output.as_deref(), Path::new("."))?;
    let table_id = choose_value(
        interactive,
        "Table id (namespace.table)",
        cli.agent_table.as_deref(),
        &base_table,
    )?;
    let topic_id =
        choose_value(interactive, "Topic id", cli.agent_topic.as_deref(), &default_topic)?;
    let group_id =
        choose_value(interactive, "Consumer group id", cli.agent_group.as_deref(), &default_group)?;
    let id_column = choose_value(
        interactive,
        "Primary key column",
        cli.agent_id_column.as_deref(),
        DEFAULT_ID_COLUMN,
    )?;
    let input_column = choose_value(
        interactive,
        "Input text column",
        cli.agent_input_column.as_deref(),
        DEFAULT_INPUT_COLUMN,
    )?;
    let output_column = choose_value(
        interactive,
        "Output summary column",
        cli.agent_output_column.as_deref(),
        DEFAULT_OUTPUT_COLUMN,
    )?;
    let system_prompt = choose_value(
        interactive,
        "System prompt",
        cli.agent_system_prompt.as_deref(),
        DEFAULT_SYSTEM_PROMPT,
    )?;

    validate_identifier(&id_column, "agent-id-column")?;
    validate_identifier(&input_column, "agent-input-column")?;
    validate_identifier(&output_column, "agent-output-column")?;
    parse_table_id(&table_id)?;
    validate_topic_id(&topic_id)?;
    validate_group_id(&group_id)?;

    Ok(AgentScaffoldConfig {
        project_name,
        output_root,
        table_id,
        topic_id,
        group_id,
        id_column,
        input_column,
        output_column,
        system_prompt,
    })
}

fn choose_value(
    interactive: bool,
    prompt: &str,
    provided: Option<&str>,
    default: &str,
) -> Result<String> {
    if let Some(value) = provided {
        return Ok(value.trim().to_string());
    }

    if !interactive {
        return Ok(default.to_string());
    }

    print!("{} [{}]: ", prompt, default);
    io::stdout()
        .flush()
        .map_err(|e| CLIError::FileError(format!("Failed to flush stdout: {}", e)))?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| CLIError::FileError(format!("Failed to read input: {}", e)))?;

    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn choose_path(
    interactive: bool,
    prompt: &str,
    provided: Option<&Path>,
    default: &Path,
) -> Result<PathBuf> {
    if let Some(path) = provided {
        return Ok(path.to_path_buf());
    }

    if !interactive {
        return Ok(default.to_path_buf());
    }

    let default_str = default.to_string_lossy();
    print!("{} [{}]: ", prompt, default_str);
    io::stdout()
        .flush()
        .map_err(|e| CLIError::FileError(format!("Failed to flush stdout: {}", e)))?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| CLIError::FileError(format!("Failed to read input: {}", e)))?;

    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(default.to_path_buf())
    } else {
        Ok(PathBuf::from(trimmed))
    }
}

fn validate_identifier(value: &str, field_name: &str) -> Result<()> {
    if value.is_empty() {
        return Err(CLIError::ConfigurationError(format!("{} cannot be empty", field_name)));
    }

    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(CLIError::ConfigurationError(format!("{} cannot be empty", field_name)));
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(CLIError::ConfigurationError(format!(
            "{} must start with a letter or underscore",
            field_name
        )));
    }

    for ch in chars {
        if !(ch == '_' || ch.is_ascii_alphanumeric()) {
            return Err(CLIError::ConfigurationError(format!(
                "{} contains invalid character '{}'",
                field_name, ch
            )));
        }
    }

    Ok(())
}

fn validate_topic_id(topic_id: &str) -> Result<()> {
    if !topic_id.contains('.') {
        return Err(CLIError::ConfigurationError(
            "agent-topic must include namespace.topic".into(),
        ));
    }

    for part in topic_id.split('.') {
        if part.is_empty() {
            return Err(CLIError::ConfigurationError(
                "agent-topic cannot contain empty namespace/topic segments".into(),
            ));
        }
        for ch in part.chars() {
            if !(ch == '_' || ch == '-' || ch.is_ascii_alphanumeric()) {
                return Err(CLIError::ConfigurationError(format!(
                    "agent-topic contains invalid character '{}'",
                    ch
                )));
            }
        }
    }

    Ok(())
}

fn validate_group_id(group_id: &str) -> Result<()> {
    if group_id.trim().is_empty() {
        return Err(CLIError::ConfigurationError("agent-group cannot be empty".into()));
    }

    Ok(())
}

fn parse_table_id(table_id: &str) -> Result<ParsedTableId> {
    let mut segments = table_id.split('.');
    let namespace = segments
        .next()
        .ok_or_else(|| CLIError::ConfigurationError("agent-table is required".into()))?;
    let table = segments.next().ok_or_else(|| {
        CLIError::ConfigurationError("agent-table must be namespace.table".into())
    })?;

    if segments.next().is_some() {
        return Err(CLIError::ConfigurationError("agent-table must be namespace.table".into()));
    }

    validate_identifier(namespace, "table namespace")?;
    validate_identifier(table, "table name")?;

    Ok(ParsedTableId {
        namespace: namespace.to_string(),
        table: table.to_string(),
    })
}

fn slugify(value: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;

    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            Some(ch.to_ascii_lowercase())
        } else if ch == '-' || ch == '_' || ch.is_whitespace() {
            Some('-')
        } else {
            None
        };

        let Some(mapped) = mapped else {
            continue;
        };

        if mapped == '-' {
            if !last_dash && !out.is_empty() {
                out.push('-');
                last_dash = true;
            }
        } else {
            out.push(mapped);
            last_dash = false;
        }
    }

    let normalized = out.trim_matches('-').to_string();
    if normalized.is_empty() {
        "agent".to_string()
    } else {
        normalized
    }
}

fn resolve_project_dir(output_root: &Path, project_name: &str) -> Result<PathBuf> {
    if project_name.trim().is_empty() {
        return Err(CLIError::ConfigurationError("agent-name cannot be empty".into()));
    }

    let cwd = env::current_dir()
        .map_err(|e| CLIError::FileError(format!("Failed to read current directory: {}", e)))?;

    let root = if output_root.is_absolute() {
        output_root.to_path_buf()
    } else {
        cwd.join(output_root)
    };

    Ok(root.join(project_name))
}

fn detect_sdk_dependency(project_dir: &Path) -> Result<String> {
    let cwd = env::current_dir()
        .map_err(|e| CLIError::FileError(format!("Failed to read current directory: {}", e)))?;

    for ancestor in cwd.ancestors() {
        let candidate = ancestor.join("link/sdks/typescript/client");
        if candidate.join("package.json").exists() {
            let relative = relative_path(project_dir, &candidate);
            let relative_str = relative.to_string_lossy().replace('\\', "/").trim().to_string();

            if relative_str.is_empty() {
                break;
            }

            return Ok(format!("file:{}", relative_str));
        }
    }

    Ok(format!("file:{}", FALLBACK_LOCAL_SDK_PATH))
}

fn normalize_project_dir_for_resolution(project_dir: &Path) -> Result<PathBuf> {
    if project_dir.exists() {
        return fs::canonicalize(project_dir).map_err(|e| {
            CLIError::FileError(format!(
                "Failed to canonicalize project directory {}: {}",
                project_dir.display(),
                e
            ))
        });
    }

    let Some(parent) = project_dir.parent() else {
        return Ok(project_dir.to_path_buf());
    };

    let canonical_parent = fs::canonicalize(parent).map_err(|e| {
        CLIError::FileError(format!(
            "Failed to canonicalize output directory {}: {}",
            parent.display(),
            e
        ))
    })?;

    let name = project_dir
        .file_name()
        .ok_or_else(|| CLIError::ConfigurationError("agent-name cannot be empty".into()))?;

    Ok(canonical_parent.join(name))
}

fn relative_path(from: &Path, to: &Path) -> PathBuf {
    let from_components: Vec<Component<'_>> = from.components().collect();
    let to_components: Vec<Component<'_>> = to.components().collect();

    let mut idx = 0;
    while idx < from_components.len()
        && idx < to_components.len()
        && from_components[idx] == to_components[idx]
    {
        idx += 1;
    }

    let mut out = PathBuf::new();

    for _ in idx..from_components.len() {
        out.push("..");
    }

    for component in &to_components[idx..] {
        out.push(component.as_os_str());
    }

    if out.as_os_str().is_empty() {
        out.push(".");
    }

    out
}

fn generate_agent_project(
    config: &AgentScaffoldConfig,
    table: &ParsedTableId,
    project_dir: &Path,
    sdk_dependency: &str,
) -> Result<()> {
    if project_dir.exists() {
        let mut entries = project_dir.read_dir().map_err(|e| {
            CLIError::FileError(format!("Failed to read {}: {}", project_dir.display(), e))
        })?;
        if entries.next().is_some() {
            return Err(CLIError::FileError(format!(
                "Target directory {} already exists and is not empty",
                project_dir.display()
            )));
        }
    }

    fs::create_dir_all(project_dir.join("src"))
        .map_err(|e| CLIError::FileError(format!("Failed to create src directory: {}", e)))?;
    fs::create_dir_all(project_dir.join("scripts"))
        .map_err(|e| CLIError::FileError(format!("Failed to create scripts directory: {}", e)))?;

    let package_name = format!("kalamdb-{}", slugify(&config.project_name));
    let failure_table = format!("{}.{}_agent_failures", table.namespace, table.table);
    let sdk_relative_for_script = sdk_dependency
        .strip_prefix("file:")
        .unwrap_or(FALLBACK_LOCAL_SDK_PATH)
        .to_string();

    write_file(
        &project_dir.join("package.json"),
        &render_package_json(&package_name, sdk_dependency),
    )?;
    write_file(&project_dir.join("tsconfig.json"), TSCONFIG_TEMPLATE)?;
    write_file(&project_dir.join(".env.example"), &render_env_example(config))?;
    write_file(&project_dir.join(".gitignore"), GITIGNORE_TEMPLATE)?;
    write_file(&project_dir.join("setup.sql"), &render_setup_sql(config, &failure_table))?;
    write_file(&project_dir.join("setup.sh"), &render_setup_sh(config))?;
    write_file(
        &project_dir.join("scripts/ensure-sdk.sh"),
        &render_ensure_sdk_sh(&sdk_relative_for_script),
    )?;
    write_file(&project_dir.join("src/agent.ts"), &render_agent_ts(config, &failure_table))?;
    write_file(
        &project_dir.join("src/langchain-openai.d.ts"),
        "declare module '@langchain/openai';\n",
    )?;
    write_file(&project_dir.join("README.md"), &render_readme(config))?;

    set_executable(&project_dir.join("setup.sh"))?;
    set_executable(&project_dir.join("scripts/ensure-sdk.sh"))?;

    Ok(())
}

fn write_file(path: &Path, content: &str) -> Result<()> {
    fs::write(path, content)
        .map_err(|e| CLIError::FileError(format!("Failed to write {}: {}", path.display(), e)))
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut perms = fs::metadata(path)
        .map_err(|e| CLIError::FileError(format!("Failed to stat {}: {}", path.display(), e)))?
        .permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms)
        .map_err(|e| CLIError::FileError(format!("Failed to chmod {}: {}", path.display(), e)))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<()> {
    Ok(())
}

fn render_package_json(package_name: &str, sdk_dependency: &str) -> String {
    format!(
        r#"{{
  "name": "{package_name}",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "description": "KalamDB agent scaffold generated by kalam --init-agent",
  "scripts": {{
    "ensure-sdk": "bash scripts/ensure-sdk.sh",
    "precheck": "npm run ensure-sdk",
    "prestart": "npm run ensure-sdk",
    "start": "tsx src/agent.ts",
    "check": "tsc --noEmit",
    "setup": "bash ./setup.sh"
  }},
  "dependencies": {{
    "@langchain/openai": "^0.3.17",
    "dotenv": "^17.3.1",
    "@kalamdb/client": "{sdk_dependency}"
  }},
  "devDependencies": {{
    "@types/node": "^24.3.0",
    "tsx": "^4.20.6",
    "typescript": "^5.9.2"
  }}
}}
"#,
    )
}

fn render_env_example(config: &AgentScaffoldConfig) -> String {
    format!(
        "# KalamDB connection\nKALAMDB_URL=http://localhost:8080\nKALAMDB_USER=root\nKALAMDB_PASSWORD=kalamdb123\n\n# Agent routing\nKALAMDB_TOPIC={}\nKALAMDB_GROUP={}\n\n# Table mapping\nKALAMDB_TABLE_ID={}\nKALAMDB_ID_COLUMN={}\nKALAMDB_INPUT_COLUMN={}\nKALAMDB_OUTPUT_COLUMN={}\n\n# LLM settings\nKALAMDB_SYSTEM_PROMPT={}\nOPENAI_API_KEY=\nOPENAI_MODEL=gpt-4o-mini\n",
        config.topic_id,
        config.group_id,
        config.table_id,
        config.id_column,
        config.input_column,
        config.output_column,
        config.system_prompt,
    )
}

fn render_setup_sql(config: &AgentScaffoldConfig, failure_table: &str) -> String {
    let table = parse_table_id(&config.table_id).expect("validated table id");
    let failure = parse_table_id(failure_table).expect("validated failure table id");

    format!(
        "CREATE NAMESPACE IF NOT EXISTS {};\n\nCREATE SHARED TABLE IF NOT EXISTS {} (\n    {} \
         BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),\n    {} TEXT NOT NULL,\n    {} TEXT,\n    \
         created TIMESTAMP NOT NULL DEFAULT NOW(),\n    updated TIMESTAMP NOT NULL DEFAULT \
         NOW()\n);\n\nCREATE SHARED TABLE IF NOT EXISTS {} (\n    run_key TEXT PRIMARY KEY,\n    \
         row_id TEXT NOT NULL,\n    error TEXT NOT NULL,\n    created TIMESTAMP NOT NULL DEFAULT \
         NOW(),\n    updated TIMESTAMP NOT NULL DEFAULT NOW()\n);\n\nCREATE TOPIC {};\nALTER \
         TOPIC {} ADD SOURCE {} ON INSERT WITH (payload = 'full');\nALTER TOPIC {} ADD SOURCE {} \
         ON UPDATE WITH (payload = 'full');\n\nINSERT INTO {} ({}, {})\nVALUES (\n    'KalamDB \
         topics let tiny agents consume table changes and write enriched data back with minimal \
         boilerplate.',\n    NULL\n);\n",
        table.namespace,
        config.table_id,
        config.id_column,
        config.input_column,
        config.output_column,
        format!("{}.{}", failure.namespace, failure.table),
        config.topic_id,
        config.topic_id,
        config.table_id,
        config.topic_id,
        config.table_id,
        config.table_id,
        config.input_column,
        config.output_column,
    )
}

fn render_setup_sh(config: &AgentScaffoldConfig) -> String {
    format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
KALAMDB_URL="${{KALAMDB_URL:-http://localhost:8080}}"
ROOT_PASSWORD="${{KALAMDB_ROOT_PASSWORD:-kalamdb123}}"
SQL_FILE="$SCRIPT_DIR/setup.sql"
ENV_FILE="$SCRIPT_DIR/.env.local"
ACCESS_TOKEN=""
SAMPLE_ROW_ID=""
FORCE_ENV_WRITE=0

log_info() {{
  echo "[setup] $*"
}}

log_warn() {{
  echo "[setup][warn] $*"
}}

log_error() {{
  echo "[setup][error] $*" >&2
}}

require_cmd() {{
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "Missing required command: $1"
    exit 1
  fi
}}

check_server() {{
  log_info "Checking server: $KALAMDB_URL"
  if ! curl -fsS "$KALAMDB_URL/health" >/dev/null; then
    log_error "KalamDB is not reachable at $KALAMDB_URL"
    log_error "Start it first: cd backend && cargo run"
    exit 1
  fi
}}

login_root() {{
  log_info "Logging in as root"
  local response
  response="$(curl -fsS -X POST "$KALAMDB_URL/v1/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{{\"user\":\"root\",\"password\":\"$ROOT_PASSWORD\"}}")"

  ACCESS_TOKEN="$(echo "$response" | jq -r '.access_token // empty')"
  if [[ -z "$ACCESS_TOKEN" ]]; then
    log_error "Could not get access token. Check KALAMDB_ROOT_PASSWORD."
    exit 1
  fi
}}

execute_sql_raw() {{
  local sql="$1"
  local response

  response="$(curl -sS -w "\n%{{http_code}}" -X POST "$KALAMDB_URL/v1/api/sql" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -d "{{\"sql\": $(jq -Rs . <<<"$sql")}}")"

  local http_code
  http_code="$(echo "$response" | tail -1)"
  local body
  body="$(echo "$response" | sed '$d')"

  if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
    echo "$body"
    return 0
  fi

  echo "$body"
  return 1
}}

execute_sql_allow_exists() {{
  local sql="$1"
  local output
  if output="$(execute_sql_raw "$sql")"; then
    return 0
  fi

  if echo "$output" | grep -Eiq "already exists|duplicate|conflict|idempotent"; then
    log_info "Ignoring idempotent error for: $sql"
    return 0
  fi

  log_error "SQL failed: $sql"
  log_error "$output"
  return 1
}}

execute_sql_file() {{
  log_info "Applying schema + topic routes from $(basename "$SQL_FILE")"

  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]] && continue

    SQL_BUFFER+="${{line}} "
    if [[ "$line" =~ \;[[:space:]]*$ ]]; then
      local stmt
      stmt="${{SQL_BUFFER%;*}}"
      stmt="$(echo "$stmt" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
      SQL_BUFFER=""

      [[ -z "$stmt" ]] && continue
      execute_sql_allow_exists "$stmt"
    fi
  done < "$SQL_FILE"
}}

verify() {{
  log_info "Verifying sample row"
  local result
  result="$(execute_sql_raw "SELECT {id_column}, {input_column}, {output_column} FROM {table_id} ORDER BY created DESC LIMIT 1")"

  local row_count
  row_count="$(echo "$result" | jq -r '.results[0].row_count // 0')"
  if [[ "${{row_count}}" -lt 1 ]]; then
    log_error "Expected at least one row in {table_id}, but none were found."
    exit 1
  fi

  SAMPLE_ROW_ID="$(echo "$result" | jq -r '(try .results[0].rows[0].{id_column} catch empty) // (try .results[0].rows[0][0] catch empty)')"
  if [[ -z "$SAMPLE_ROW_ID" || "$SAMPLE_ROW_ID" == "null" ]]; then
    log_error "Failed to extract sample id from verification query."
    exit 1
  fi

  log_info "Sample row id: $SAMPLE_ROW_ID"
}}

generate_env_file() {{
  if [[ -f "$ENV_FILE" && "$FORCE_ENV_WRITE" -ne 1 ]]; then
    log_warn ".env.local already exists - keeping current file"
    log_info "Use --force-env to overwrite"
    return 0
  fi

  log_info "Writing .env.local"
  cat > "$ENV_FILE" <<EOF_ENV
# KalamDB connection
KALAMDB_URL=$KALAMDB_URL
KALAMDB_USER=root
KALAMDB_PASSWORD=$ROOT_PASSWORD

# Agent routing
KALAMDB_TOPIC={topic_id}
KALAMDB_GROUP={group_id}

# Table mapping
KALAMDB_TABLE_ID={table_id}
KALAMDB_ID_COLUMN={id_column}
KALAMDB_INPUT_COLUMN={input_column}
KALAMDB_OUTPUT_COLUMN={output_column}

# LLM settings
KALAMDB_SYSTEM_PROMPT={system_prompt}
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o-mini
EOF_ENV
}}

show_help() {{
  cat <<HELP
Setup generated agent project

Usage:
  ./setup.sh [--server URL] [--password ROOT_PASSWORD] [--force-env]

Options:
  --server     KalamDB server URL (default: http://localhost:8080)
  --password   Root password (default: kalamdb123)
  --force-env  Overwrite existing .env.local
HELP
}}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server)
      KALAMDB_URL="$2"
      shift 2
      ;;
    --password)
      ROOT_PASSWORD="$2"
      shift 2
      ;;
    --force-env)
      FORCE_ENV_WRITE=1
      shift 1
      ;;
    --help)
      show_help
      exit 0
      ;;
    *)
      log_error "Unknown option: $1"
      show_help
      exit 1
      ;;
  esac
done

require_cmd curl
require_cmd jq

check_server
login_root
SQL_BUFFER=""
execute_sql_file
verify
generate_env_file

cat <<DONE

Setup complete.

Next:
  1. npm install
  2. npm run start
  3. Update content to trigger processing for row id=$SAMPLE_ROW_ID:
     curl -sS -X POST "$KALAMDB_URL/v1/api/sql" \\
       -H "Content-Type: application/json" \\
       -H "Authorization: Bearer <token>" \\
       -d '{{"sql":"UPDATE {table_id} SET {input_column} = ''KalamDB topics make event-driven agents simple'' WHERE {id_column} = '$SAMPLE_ROW_ID"}}'

DONE
"#,
        table_id = config.table_id,
        topic_id = config.topic_id,
        group_id = config.group_id,
        id_column = config.id_column,
        input_column = config.input_column,
        output_column = config.output_column,
        system_prompt = config.system_prompt,
    )
}

fn render_ensure_sdk_sh(sdk_relative_for_script: &str) -> String {
    format!(
        r#"#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SDK_DIR="$(cd "$PROJECT_DIR/{sdk_path}" && pwd)"
WASM_FILE="$SDK_DIR/dist/wasm/kalam_client_bg.wasm"
SDK_ENTRY="$SDK_DIR/dist/src/index.js"

needs_build=0
if [[ ! -f "$WASM_FILE" || ! -f "$SDK_ENTRY" ]]; then
  needs_build=1
fi

if [[ "$needs_build" -eq 0 ]]; then
  if find "$SDK_DIR/src" "$SDK_DIR/../../../kalam-client/src" "$SDK_DIR/../../../link-common/src" -type f -newer "$SDK_ENTRY" | head -n 1 | grep -q .; then
    needs_build=1
  fi
fi

if [[ "$needs_build" -eq 1 ]]; then
  echo "[ensure-sdk] Building local @kalamdb/client SDK..."
  (
    cd "$SDK_DIR"
    bash ./build.sh
  )
  echo "[ensure-sdk] SDK build complete"
else
  echo "[ensure-sdk] Local @kalamdb/client SDK is up to date"
fi
"#,
        sdk_path = sdk_relative_for_script,
    )
}

fn render_agent_ts(config: &AgentScaffoldConfig, failure_table: &str) -> String {
    format!(
        r#"import {{ config as loadEnv }} from 'dotenv';
import {{ ChatOpenAI }} from '@langchain/openai';
import {{
  Auth,
  createClient,
  createLangChainAdapter,
  runAgent,
  type AgentContext,
  type AgentLLMAdapter,
}} from '@kalamdb/client';

loadEnv({{ path: '.env.local', quiet: true }});
loadEnv({{ quiet: true }});

const KALAMDB_URL = process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080';
const KALAMDB_USER = process.env.KALAMDB_USER ?? 'root';
const KALAMDB_PASSWORD = process.env.KALAMDB_PASSWORD ?? 'kalamdb123';
const TOPIC = process.env.KALAMDB_TOPIC ?? '{topic_id}';
const GROUP = process.env.KALAMDB_GROUP ?? '{group_id}';
const TABLE_ID = process.env.KALAMDB_TABLE_ID ?? '{table_id}';
const ID_COLUMN = process.env.KALAMDB_ID_COLUMN ?? '{id_column}';
const INPUT_COLUMN = process.env.KALAMDB_INPUT_COLUMN ?? '{input_column}';
const OUTPUT_COLUMN = process.env.KALAMDB_OUTPUT_COLUMN ?? '{output_column}';
const SYSTEM_PROMPT = process.env.KALAMDB_SYSTEM_PROMPT
  ?? '{system_prompt}';
const OPENAI_API_KEY = process.env.OPENAI_API_KEY?.trim();
const OPENAI_MODEL = process.env.OPENAI_MODEL?.trim() || 'gpt-4o-mini';
const FAILURE_TABLE_ID = '{failure_table}';

function normalizeUrlForNode(url: string): string {{
  const parsed = new URL(url);
  if (parsed.hostname === 'localhost') {{
    parsed.hostname = '127.0.0.1';
  }}
  return parsed.toString().replace(/\/$/, '');
}}

function assertIdentifier(value: string, label: string): string {{
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {{
    throw new Error(`Invalid ${{label}}: "${{value}}"`);
  }}
  return value;
}}

function parseTableId(value: string): [string, string] {{
  const [namespaceId, tableName, ...rest] = value.split('.');
  if (!namespaceId || !tableName || rest.length > 0) {{
    throw new Error(`Invalid table id "${{value}}". Expected namespace.table`);
  }}
  return [
    assertIdentifier(namespaceId, 'table namespace'),
    assertIdentifier(tableName, 'table name'),
  ];
}}

const [TABLE_NAMESPACE, TABLE_NAME] = parseTableId(TABLE_ID);
assertIdentifier(ID_COLUMN, 'id column');
assertIdentifier(INPUT_COLUMN, 'input column');
assertIdentifier(OUTPUT_COLUMN, 'output column');

const client = createClient({{
  url: normalizeUrlForNode(KALAMDB_URL),
  auth: Auth.basic(KALAMDB_USER, KALAMDB_PASSWORD),
}});

function fallbackSummary(content: string): string {{
  const compact = content.replace(/\s+/g, ' ').trim();
  if (!compact) return '';
  const words = compact.split(' ');
  const excerpt = words.slice(0, 24).join(' ');
  return words.length > 24 ? `${{excerpt}}...` : excerpt;
}}

function toRowId(value: unknown): string | null {{
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value === 'string' && value.trim() !== '') return value.trim();
  return null;
}}

function buildLlmAdapter(): AgentLLMAdapter | undefined {{
  if (!OPENAI_API_KEY) return undefined;

  const model = new ChatOpenAI({{
    apiKey: OPENAI_API_KEY,
    model: OPENAI_MODEL,
    temperature: 0.1,
  }});

  return createLangChainAdapter(model);
}}

async function summarizeContent(ctx: AgentContext<Record<string, unknown>>, content: string): Promise<string> {{
  const compact = content.replace(/\s+/g, ' ').trim();
  if (!compact) return '';

  if (!ctx.llm) {{
    return fallbackSummary(compact);
  }}

  const prompt = `Summarize this text in one short sentence:\n\n${{compact}}`;
  const summary = (await ctx.llm.complete(prompt)).trim();
  return summary || fallbackSummary(compact);
}}

async function syncSummary(
  ctx: AgentContext<Record<string, unknown>>,
  rowId: string,
): Promise<void> {{
  const sql = `SELECT ${{ID_COLUMN}}, ${{INPUT_COLUMN}}, ${{OUTPUT_COLUMN}} FROM ${{TABLE_NAMESPACE}}.${{TABLE_NAME}} WHERE ${{ID_COLUMN}} = $1`;
  const row = await ctx.queryOne<Record<string, unknown>>(sql, [rowId]);
  if (!row) return;

  const content = String(row[INPUT_COLUMN] ?? '').trim();
  if (!content) return;

  const currentSummary = String(row[OUTPUT_COLUMN] ?? '').trim();
  const nextSummary = await summarizeContent(ctx, content);
  if (!nextSummary || nextSummary === currentSummary) return;

  const updateSql = `UPDATE ${{TABLE_NAMESPACE}}.${{TABLE_NAME}} SET ${{OUTPUT_COLUMN}} = $1, updated = NOW() WHERE ${{ID_COLUMN}} = $2`;
  await ctx.sql(updateSql, [nextSummary, rowId]);
  console.log(`[processed] ${{ID_COLUMN}}=${{rowId}}`);
}}

async function main(): Promise<void> {{
  const llmAdapter = buildLlmAdapter();
  const abortController = new AbortController();

  const shutdown = (): void => abortController.abort();
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  console.log(`agent starting (topic=${{TOPIC}}, group=${{GROUP}}, table=${{TABLE_ID}})`);
  if (llmAdapter) {{
    console.log(`agent using LangChain model: ${{OPENAI_MODEL}}`);
  }} else {{
    console.log('agent using fallback summarizer (set OPENAI_API_KEY for LangChain)');
  }}

  try {{
    await runAgent<Record<string, unknown>>({{
      client,
      name: '{project_name}',
      topic: TOPIC,
      groupId: GROUP,
      start: 'earliest',
      batchSize: 20,
      timeoutSeconds: 30,
      systemPrompt: SYSTEM_PROMPT,
      llm: llmAdapter,
      stopSignal: abortController.signal,
      retry: {{
        maxAttempts: 3,
        initialBackoffMs: 250,
        maxBackoffMs: 1500,
        multiplier: 2,
      }},
      onRow: async (ctx, row): Promise<void> => {{
        const rowId = toRowId(row[ID_COLUMN]);
        if (!rowId) return;
        await syncSummary(ctx, rowId);
      }},
      onFailed: async (ctx): Promise<void> => {{
        const rowId = toRowId(ctx.row[ID_COLUMN]) ?? 'unknown';
        const errorText = String(ctx.error instanceof Error ? ctx.error.message : ctx.error ?? 'unknown');
        await ctx.sql(
          `INSERT INTO ${{FAILURE_TABLE_ID}} (run_key, row_id, error, created, updated)
           VALUES ($1, $2, $3, NOW(), NOW())
           ON CONFLICT (run_key)
           DO UPDATE SET error = EXCLUDED.error, updated = NOW()`,
          [ctx.runKey, rowId, errorText.slice(0, 4000)],
        );
      }},
      ackOnFailed: true,
      onRetry: (event) => {{
        console.warn(`[retry] run_key=${{event.runKey}} attempt=${{event.attempt}}/${{event.maxAttempts}}`);
      }},
      onError: (event) => {{
        console.error(`[agent-error] run_key=${{event.runKey}} error=${{String(event.error)}}`);
      }},
    }});
  }} finally {{
    process.off('SIGINT', shutdown);
    process.off('SIGTERM', shutdown);
    await client.disconnect();
  }}
}}

main().catch((error) => {{
  console.error('agent failed:', error);
  process.exit(1);
}});
"#,
        project_name = config.project_name,
        topic_id = config.topic_id,
        group_id = config.group_id,
        table_id = config.table_id,
        id_column = config.id_column,
        input_column = config.input_column,
        output_column = config.output_column,
        system_prompt = config.system_prompt.replace('\'', "\\'"),
        failure_table = failure_table,
    )
}

fn render_readme(config: &AgentScaffoldConfig) -> String {
    format!(
        "# {}\n\nGenerated by `kalam --init-agent`.\n\n## What it does\n\n1. Consumes topic \
         events from `{}`.\n2. Reads rows from `{}`.\n3. Writes summarized output back to \
         `{}`.\n4. Retries failures and records exhausted runs in a failure table.\n\n## \
         Run\n\n1. Start KalamDB server:\n   - `cd backend && cargo run`\n2. Bootstrap \
         schema/topic/sample row:\n   - `./setup.sh`\n3. Install dependencies:\n   - `npm \
         install`\n4. Start the agent:\n   - `npm run start`\n\n## Environment\n\nCopy \
         `.env.example` to `.env.local` and adjust values as needed.\nSet `OPENAI_API_KEY` to \
         enable LangChain-backed summarization.\n",
        config.project_name, config.topic_id, config.table_id, config.output_column,
    )
}

const TSCONFIG_TEMPLATE: &str = r#"{
  "compilerOptions": {
    "target": "ES2020",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "strict": true,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "types": ["node"]
  },
  "include": ["src/**/*.ts", "src/**/*.d.ts"]
}
"#;

const GITIGNORE_TEMPLATE: &str = "node_modules\n.env\n.env.local\n";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_id_valid() {
        let parsed = parse_table_id("blog.posts").expect("table id should parse");
        assert_eq!(parsed.namespace, "blog");
        assert_eq!(parsed.table, "posts");
    }

    #[test]
    fn test_parse_table_id_invalid() {
        assert!(parse_table_id("invalid").is_err());
        assert!(parse_table_id("a.b.c").is_err());
    }

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("My Agent"), "my-agent");
        assert_eq!(slugify("___"), "agent");
    }
}
