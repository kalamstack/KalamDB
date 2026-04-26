// Shared build script used by multiple crates.
// - Captures Git commit hash and build timestamp
// - Falls back to version.toml if git is not available (e.g., Docker builds)
// - Builds the Admin UI for kalamdb-api release builds (must run before rust-embed)

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let package_name = std::env::var("CARGO_PKG_NAME").unwrap_or_default();
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default());
    let repo_root = find_repo_root(&manifest_dir).unwrap_or_else(|| manifest_dir.clone());

    build_isoc23_glibc_shim_if_needed(&repo_root);

    // Build UI for release builds FIRST (before rust-embed macro runs).
    // Only when the embedded-ui feature is enabled.
    if package_name == "kalamdb-api" && std::env::var("CARGO_FEATURE_EMBEDDED_UI").is_ok() {
        build_ui_if_release(&repo_root);
    }

    // Try to read from version.toml first (for Docker/CI builds)
    let fallback = read_version_toml(&repo_root.join("version.toml"));

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
    let git_head = repo_root.join(".git").join("HEAD");
    let git_heads_dir = repo_root.join(".git").join("refs").join("heads");
    if git_head.exists() {
        println!("cargo:rerun-if-changed={}", git_head.display());
    }
    if git_heads_dir.exists() {
        println!("cargo:rerun-if-changed={}", git_heads_dir.display());
    }
    let version_toml = repo_root.join("version.toml");
    if version_toml.exists() {
        println!("cargo:rerun-if-changed={}", version_toml.display());
    }
}

fn build_isoc23_glibc_shim_if_needed(repo_root: &Path) {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();

    // Only relevant for linux-gnu builds.
    if target_os != "linux" || target_env != "gnu" {
        return;
    }

    // This build script is shared across crates (e.g. kalamdb-api uses ../../build.rs),
    // so resolve the shim from the repository root.
    let shim_path = repo_root.join("backend").join("build").join("isoc23_shim.c");
    if !shim_path.exists() {
        panic!("Expected glibc shim file at {} but it was not found", shim_path.display());
    }

    println!("cargo:rerun-if-changed={}", shim_path.display());

    cc::Build::new().file(&shim_path).compile("kalamdb_isoc23_shim");
}

fn find_repo_root(start: &Path) -> Option<PathBuf> {
    // Look for version.toml or .git to identify repository root.
    for ancestor in start.ancestors() {
        if ancestor.join("version.toml").exists() || ancestor.join(".git").exists() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

/// Build the UI for release builds
/// This ensures the UI is always up-to-date when building for release
fn build_ui_if_release(repo_root: &Path) {
    // Only build UI for release-like builds (release, release-dist, docker profiles)
    let profile = std::env::var("PROFILE").unwrap_or_default();
    let is_release_build = profile == "release" || profile == "release-dist" || profile == "docker";
    if !is_release_build {
        // For debug builds, just ensure the dist folder exists with a placeholder
        ensure_ui_dist_exists(repo_root);
        return;
    }

    // Check if we should skip UI build (for CI or when UI is pre-built)
    if std::env::var("SKIP_UI_BUILD").is_ok() {
        println!("cargo:warning=Skipping UI build (SKIP_UI_BUILD is set)");
        // Verify UI dist exists when skipping build
        let dist_dir = repo_root.join("ui").join("dist");
        let index_file = dist_dir.join("index.html");
        if !index_file.exists() {
            panic!(
                "SKIP_UI_BUILD is set but ui/dist/index.html does not exist! Build UI first or \
                 unset SKIP_UI_BUILD."
            );
        }
        return;
    }

    let ui_dir = repo_root.join("ui");
    if !ui_dir.exists() {
        panic!(
            "UI directory not found at {}/ui - UI is required for release builds!",
            repo_root.display()
        );
    }

    // Check if npm is available
    let npm_check = if cfg!(target_os = "windows") {
        Command::new("cmd").args(["/C", "npm", "--version"]).output()
    } else {
        Command::new("npm").arg("--version").output()
    };

    if npm_check.is_err() || !npm_check.as_ref().unwrap().status.success() {
        panic!("npm not found - npm is required for building the UI for release builds!");
    }

    println!("cargo:warning=Building UI for release...");

    // Use isolated cargo dirs for nested wasm-pack to avoid lock contention with the outer cargo
    // build.
    let isolated_target = repo_root.join("ui").join(".cargo-target");
    let isolated_home = repo_root.join("ui").join(".cargo-home");

    // Run npm install if node_modules doesn't exist
    let node_modules = ui_dir.join("node_modules");
    if !node_modules.exists() {
        println!("cargo:warning=Installing UI dependencies...");
        let install_status = if cfg!(target_os = "windows") {
            let mut cmd = Command::new("cmd");
            cmd.args(["/C", "npm", "install"])
                .current_dir(&ui_dir)
                .env("CARGO_TARGET_DIR", &isolated_target)
                .env("CARGO_HOME", &isolated_home);
            cmd.status()
        } else {
            let mut cmd = Command::new("npm");
            cmd.arg("install")
                .current_dir(&ui_dir)
                .env("CARGO_TARGET_DIR", &isolated_target)
                .env("CARGO_HOME", &isolated_home);
            cmd.status()
        };

        match install_status {
            Ok(status) if status.success() => {},
            Ok(status) => {
                panic!(
                    "npm install failed with status: {} - UI dependencies are required for \
                     release builds!",
                    status
                );
            },
            Err(e) => {
                panic!(
                    "Failed to run npm install: {} - UI dependencies are required for release \
                     builds!",
                    e
                );
            },
        }
    }

    // Run npm run build
    // IMPORTANT: use `.status()` (stream output) instead of `.output()`.
    // Capturing large stdout/stderr can make builds appear "stuck".
    eprintln!("Running UI build (npm run build) — this can take a few minutes...");
    let build_status = if cfg!(target_os = "windows") {
        let mut cmd = Command::new("cmd");
        cmd.args(["/C", "npm", "run", "build"])
            .current_dir(&ui_dir)
            .env("CARGO_TARGET_DIR", &isolated_target)
            .env("CARGO_HOME", &isolated_home);
        cmd.status()
    } else {
        let mut cmd = Command::new("npm");
        cmd.args(["run", "build"])
            .current_dir(&ui_dir)
            .env("CARGO_TARGET_DIR", &isolated_target)
            .env("CARGO_HOME", &isolated_home);
        cmd.status()
    };

    match build_status {
        Ok(status) if status.success() => {
            eprintln!("UI build completed successfully");
        },
        Ok(status) => {
            panic!("UI build failed with status: {}\n\nUI is required for release builds!", status);
        },
        Err(e) => {
            panic!("Failed to run npm build: {} - UI is required for release builds!", e);
        },
    }

    // Verify dist was created
    let dist_dir = ui_dir.join("dist");
    let index_file = dist_dir.join("index.html");
    if !index_file.exists() {
        panic!("UI build completed but ui/dist/index.html not found - UI build may have failed!");
    }

    // Rerun if UI inputs change.
    // NOTE: do NOT watch link/sdks/typescript/client/src. The SDK build creates/removes wasm and
    // can self-trigger rebuild loops.
    println!("cargo:rerun-if-changed={}", repo_root.join("ui").join("src").display());
    println!("cargo:rerun-if-changed={}", repo_root.join("ui").join("index.html").display());
    println!("cargo:rerun-if-changed={}", repo_root.join("ui").join("public").display());
    println!("cargo:rerun-if-changed={}", repo_root.join("ui").join("package.json").display());
    println!(
        "cargo:rerun-if-changed={}",
        repo_root.join("ui").join("vite.config.ts").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        repo_root
            .join("link")
            .join("sdks")
            .join("typescript")
            .join("client")
            .join("package.json")
            .display()
    );
}

/// Ensure ui/dist exists with at least a placeholder file
/// This prevents rust-embed from failing when the UI hasn't been built
fn ensure_ui_dist_exists(repo_root: &Path) {
    let dist_dir = repo_root.join("ui").join("dist");
    if !dist_dir.exists() {
        println!("cargo:warning=Creating placeholder ui/dist directory");
        if let Err(e) = std::fs::create_dir_all(&dist_dir) {
            println!("cargo:warning=Failed to create ui/dist: {}", e);
            return;
        }
        let placeholder = dist_dir.join("index.html");
        let content = r#"<!DOCTYPE html>
<html>
<head><title>KalamDB Admin UI</title></head>
<body>
<h1>UI Not Built</h1>
<p>Run <code>npm run build</code> in the ui/ directory to build the admin UI.</p>
</body>
</html>"#;
        if let Err(e) = std::fs::write(&placeholder, content) {
            println!("cargo:warning=Failed to create placeholder index.html: {}", e);
        }
    }
}

/// Read fallback values from version.toml
/// Returns (commit_hash, branch, build_date) with defaults if file doesn't exist
fn read_version_toml(path: &Path) -> (String, String, String) {
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
