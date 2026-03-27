//! Command history persistence
//!
//! **Implements T089**: CommandHistory with persistence to ~/.kalam/history
//!
//! Maintains command history across sessions for better user experience.

use std::env;
use std::path::{Path, PathBuf};

use crate::error::{CLIError, Result};

fn is_usable_home_dir(path: &Path) -> bool {
    !path.as_os_str().is_empty() && path != Path::new("/")
}

fn resolve_cli_home_dir_with<F>(
    mut env_var: F,
    detected_home: Option<PathBuf>,
    current_dir: Option<PathBuf>,
) -> PathBuf
where
    F: FnMut(&str) -> Option<String>,
{
    for key in ["HOME", "USERPROFILE"] {
        if let Some(value) = env_var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                let candidate = PathBuf::from(trimmed);
                if is_usable_home_dir(&candidate) {
                    return candidate;
                }
            }
        }
    }

    if let Some(home_dir) = detected_home.filter(|path| is_usable_home_dir(path)) {
        return home_dir;
    }

    current_dir.unwrap_or_else(|| PathBuf::from("."))
}

/// Resolve the CLI home directory.
///
/// In containerized runtimes a numeric user can have no passwd entry or a broken
/// home lookup that resolves to `/`. In that case, fall back to the current
/// working directory so config, credentials, and history still land in a
/// writable `.kalam` directory.
pub fn get_cli_home_dir() -> PathBuf {
    resolve_cli_home_dir_with(
        |key| env::var(key).ok(),
        dirs::home_dir(),
        env::current_dir().ok(),
    )
}

/// Get the KalamDB CLI configuration directory path.
/// - Windows: `~/.kalam`
/// - Linux/macOS: `~/.kalam`
pub fn get_kalam_config_dir() -> PathBuf {
    get_cli_home_dir().join(".kalam")
}

/// Command history manager
pub struct CommandHistory {
    /// History file path
    path: PathBuf,

    /// Maximum history size
    max_size: usize,
}

/// Return whether a command is safe to persist in local history.
///
/// This intentionally errs on the side of skipping persistence to avoid
/// storing secrets (passwords/tokens/authorization material) on disk.
pub fn should_persist_command(command: &str) -> bool {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return false;
    }

    let lower = trimmed.to_ascii_lowercase();

    // Never persist credential-management meta commands.
    let blocked_meta_prefixes = [
        "\\history",
        "\\h",
        "\\quit",
        "\\q",
        "\\update-credentials",
        "\\show-credentials",
        "\\credentials",
        "\\delete-credentials",
    ];
    if blocked_meta_prefixes
        .iter()
        .any(|prefix| lower == *prefix || lower.starts_with(&format!("{} ", prefix)))
    {
        return false;
    }

    // Skip potentially sensitive SQL/commands that include secret material.
    let sensitive_markers = [
        "password",
        "authorization",
        "bearer ",
        "refresh_token",
        "jwt_token",
        "api_key",
        "secret",
        "client_secret",
    ];
    !sensitive_markers.iter().any(|marker| lower.contains(marker))
}

#[cfg(test)]
mod home_dir_tests {
    use super::*;

    #[test]
    fn prefers_home_env_when_available() {
        let resolved = resolve_cli_home_dir_with(
            |key| match key {
                "HOME" => Some("/tmp/kalam-home".to_string()),
                _ => None,
            },
            Some(PathBuf::from("/ignored")),
            Some(PathBuf::from("/cwd")),
        );

        assert_eq!(resolved, PathBuf::from("/tmp/kalam-home"));
    }

    #[test]
    fn falls_back_to_current_dir_when_home_is_root() {
        let resolved = resolve_cli_home_dir_with(
            |_key| Some("/".to_string()),
            Some(PathBuf::from("/")),
            Some(PathBuf::from("/data")),
        );

        assert_eq!(resolved, PathBuf::from("/data"));
    }

    #[test]
    fn uses_detected_home_when_env_is_missing() {
        let resolved = resolve_cli_home_dir_with(
            |_key| None,
            Some(PathBuf::from("/Users/tester")),
            Some(PathBuf::from("/cwd")),
        );

        assert_eq!(resolved, PathBuf::from("/Users/tester"));
    }
}

impl CommandHistory {
    /// Create a new history manager
    pub fn new(max_size: usize) -> Self {
        // Default history path using shared config directory
        let path = get_kalam_config_dir().join("history");

        Self { path, max_size }
    }

    /// Create with custom path
    pub fn with_path<P: AsRef<Path>>(path: P, max_size: usize) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            max_size,
        }
    }

    /// Load history from file
    pub fn load(&self) -> Result<Vec<String>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let contents = std::fs::read_to_string(&self.path)
            .map_err(|e| CLIError::HistoryError(format!("Failed to read history file: {}", e)))?;

        let entries = parse_history_entries(&contents);

        // Take last max_size entries
        let start_idx = if entries.len() > self.max_size {
            entries.len() - self.max_size
        } else {
            0
        };

        Ok(entries[start_idx..].to_vec())
    }

    /// Count entries in the history file (without truncation)
    pub fn entry_count(&self) -> Result<usize> {
        if !self.path.exists() {
            return Ok(0);
        }

        let contents = std::fs::read_to_string(&self.path)
            .map_err(|e| CLIError::HistoryError(format!("Failed to read history file: {}", e)))?;

        Ok(parse_history_entries(&contents).len())
    }

    /// Save history to file
    pub fn save(&self, history: &[String]) -> Result<()> {
        // Ensure directory exists
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Take last max_size entries
        let entries: Vec<&String> = history.iter().rev().take(self.max_size).collect();
        let entries: Vec<&String> = entries.into_iter().rev().collect();

        // Encode each entry in base64 to preserve newlines and special characters
        // Use a sentinel delimiter between entries
        let contents = entries
            .iter()
            .map(|s| {
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, s.as_bytes())
            })
            .collect::<Vec<_>>()
            .join("\n---ENTRY---\n");

        std::fs::write(&self.path, contents)
            .map_err(|e| CLIError::HistoryError(format!("Failed to write history file: {}", e)))?;

        // Restrict history permissions on Unix to avoid accidental disclosure.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&self.path, permissions).map_err(|e| {
                CLIError::HistoryError(format!("Failed to secure history file permissions: {}", e))
            })?;
        }

        Ok(())
    }

    /// Append a command to history
    pub fn append(&self, command: &str) -> Result<()> {
        let mut history = self.load()?;

        // Don't add empty or duplicate consecutive commands
        if command.trim().is_empty() {
            return Ok(());
        }
        if !should_persist_command(command) {
            return Ok(());
        }
        if history.last().map(|s| s.as_str()) == Some(command) {
            return Ok(());
        }

        history.push(command.to_string());
        self.save(&history)?;
        Ok(())
    }

    /// Clear history
    pub fn clear(&self) -> Result<()> {
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    /// Get history file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Move selected command to end and remove all older occurrences
    /// This ensures the most recent selection is at the end without duplicates
    pub fn deduplicate_and_move_to_end(&self, command: &str) -> Result<()> {
        let mut history = self.load()?;

        // Remove all occurrences of this command
        history.retain(|entry| entry != command);

        // Add it at the end
        history.push(command.to_string());

        self.save(&history)?;
        Ok(())
    }
}

fn parse_history_entries(contents: &str) -> Vec<String> {
    // Use a sentinel delimiter that won't appear in SQL
    // Each entry is stored as: base64_encoded_command\n---ENTRY---\n
    contents
        .split("\n---ENTRY---\n")
        .filter(|s| !s.trim().is_empty())
        .filter_map(|encoded| {
            // Decode from base64 to preserve newlines and special characters
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded.trim())
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn test_history_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        // Save some history
        let commands = vec!["SELECT 1".to_string(), "SELECT 2".to_string()];
        history.save(&commands).unwrap();

        // Load and verify
        let loaded = history.load().unwrap();
        assert_eq!(loaded, commands);
    }

    #[test]
    fn test_history_max_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 2);

        // Save 3 commands (should keep last 2)
        let commands = vec![
            "SELECT 1".to_string(),
            "SELECT 2".to_string(),
            "SELECT 3".to_string(),
        ];
        history.save(&commands).unwrap();

        let loaded = history.load().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0], "SELECT 2");
        assert_eq!(loaded[1], "SELECT 3");
    }

    #[test]
    fn test_append() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        history.append("SELECT 1").unwrap();
        history.append("SELECT 2").unwrap();

        let loaded = history.load().unwrap();
        assert_eq!(loaded.len(), 2);
    }

    #[test]
    fn test_clear() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        history.append("SELECT 1").unwrap();
        assert!(path.exists());

        history.clear().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_multiline_commands() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        // Test multiline SQL command with newlines
        let multiline_cmd =
            "SELECT id,\n       name,\n       email\nFROM users\nWHERE active = true;";

        history.append(multiline_cmd).unwrap();
        history.append("SELECT * FROM jobs;").unwrap();

        let loaded = history.load().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0], multiline_cmd);
        assert_eq!(loaded[1], "SELECT * FROM jobs;");

        // Verify the multiline command preserved all newlines
        assert!(loaded[0].contains("\n"));
        assert_eq!(loaded[0].matches('\n').count(), 4);
    }

    #[test]
    fn test_multiline_with_special_chars() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        // Test command with special characters and newlines
        let special_cmd = "INSERT INTO messages (content)\nVALUES ('Hello\nWorld!'),\n       ('Test\tmessage\nwith\\special chars');";

        history.append(special_cmd).unwrap();

        let loaded = history.load().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], special_cmd);
    }

    #[test]
    fn test_deduplicate_and_move_to_end() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        // Create history with duplicates
        history.append("SELECT 1").unwrap();
        history.append("SELECT 2").unwrap();
        history.append("SELECT 1").unwrap(); // Duplicate
        history.append("SELECT 3").unwrap();
        history.append("SELECT 1").unwrap(); // Another duplicate

        // Select "SELECT 1" - should remove all older occurrences and move to end
        history.deduplicate_and_move_to_end("SELECT 1").unwrap();

        let loaded = history.load().unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0], "SELECT 2");
        assert_eq!(loaded[1], "SELECT 3");
        assert_eq!(loaded[2], "SELECT 1"); // Moved to end, no duplicates
    }

    #[test]
    fn test_deduplicate_nonexistent_command() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        // Create history
        history.append("SELECT 1").unwrap();
        history.append("SELECT 2").unwrap();

        // Try to deduplicate a command that doesn't exist - should just add it
        history.deduplicate_and_move_to_end("SELECT 3").unwrap();

        let loaded = history.load().unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[2], "SELECT 3");
    }

    #[test]
    fn test_should_persist_command_blocks_sensitive_inputs() {
        assert!(!should_persist_command("\\update-credentials admin supersecret"));
        assert!(!should_persist_command("CREATE USER alice WITH PASSWORD 'secret123'"));
        assert!(!should_persist_command("SELECT * FROM t WHERE authorization = 'Bearer abc'"));
        assert!(should_persist_command("SELECT * FROM users"));
    }

    #[test]
    fn test_append_skips_sensitive_commands() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        history.append("\\update-credentials admin supersecret").unwrap();
        history.append("CREATE USER alice WITH PASSWORD 'secret123'").unwrap();
        history.append("SELECT 1").unwrap();

        let loaded = history.load().unwrap();
        assert_eq!(loaded, vec!["SELECT 1".to_string()]);
    }

    #[cfg(unix)]
    #[test]
    fn test_history_permissions_are_restricted() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let path = dir.path().join("history");
        let history = CommandHistory::with_path(&path, 100);

        history.append("SELECT 1").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
