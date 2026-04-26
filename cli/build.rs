// Build script to capture Git commit hash and build timestamp
// Sets environment variables for use in the binary at compile time
// Falls back to version.toml if git is not available (e.g., Docker builds)

use std::{fs, process::Command};

fn main() {
    // Try to read from version.toml first (for Docker/CI builds)
    let version_toml_path = "../version.toml";
    let fallback = read_version_toml(version_toml_path);

    // Capture Git commit hash (short version)
    let commit_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| fallback.0.clone());

    // Capture build date/time in ISO 8601 format
    let build_date = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

    // Capture Git branch name
    let branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| fallback.1.clone());

    // Set environment variables for use in the binary
    println!("cargo:rustc-env=GIT_COMMIT_HASH={}", commit_hash);
    println!("cargo:rustc-env=BUILD_DATE={}", build_date);
    println!("cargo:rustc-env=GIT_BRANCH={}", branch);

    // Re-run build script if .git/HEAD changes (new commits) or version.toml changes
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs/heads/");
    println!("cargo:rerun-if-changed=../version.toml");
}

/// Read fallback values from version.toml
/// Returns (commit_hash, branch, build_date) with defaults if file doesn't exist
fn read_version_toml(path: &str) -> (String, String, String) {
    let default = ("unknown".to_string(), "unknown".to_string(), "unknown".to_string());

    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return default,
    };

    let mut commit = "unknown".to_string();
    let mut branch = "unknown".to_string();
    let mut build_date = "unknown".to_string();

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with("git_commit_hash") {
            if let Some(val) = extract_toml_value(line) {
                commit = val;
            }
        } else if line.starts_with("git_branch") {
            if let Some(val) = extract_toml_value(line) {
                branch = val;
            }
        } else if line.starts_with("build_date") {
            if let Some(val) = extract_toml_value(line) {
                build_date = val;
            }
        }
    }

    (commit, branch, build_date)
}

/// Extract value from a TOML line like: key = "value"
fn extract_toml_value(line: &str) -> Option<String> {
    let parts: Vec<&str> = line.splitn(2, '=').collect();
    if parts.len() == 2 {
        let val = parts[1].trim().trim_matches('"');
        if !val.is_empty() && val != "unknown" {
            return Some(val.to_string());
        }
    }
    None
}
