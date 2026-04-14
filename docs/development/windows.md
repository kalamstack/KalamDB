# Windows Development Setup - KalamDB

Complete guide for setting up KalamDB development environment on Windows.

## System Requirements

- **Windows**: 10 (version 1809+) or Windows 11
- **RAM**: 8GB minimum, 16GB recommended
- **Disk Space**: 10GB for Visual Studio Build Tools + 3GB for dependencies
- **Visual Studio Build Tools**: Required for C++ compilation
- **LLVM/Clang**: Required for RocksDB bindings
- **Administrator Access**: Required for initial installation

---

## Quick Setup

```powershell
# 1. Install Visual Studio Build Tools (if not already installed)
# Download from: https://visualstudio.microsoft.com/downloads/
# Select: "Desktop development with C++" workload

# 2. Install LLVM (if not already installed)
# Download from: https://github.com/llvm/llvm-project/releases
# During install: Check "Add LLVM to system PATH"

# 3. Install Rust (if not already installed)
# Download from: https://rustup.rs/
# Run rustup-init.exe and follow prompts

# 4. Verify prerequisites (in PowerShell)
clang --version
cargo --version

# 5. Clone and build
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB\backend
cargo build

# 6. Run
cargo run
```

---

## Detailed Setup Instructions

### Step 1: Install Visual Studio Build Tools

KalamDB requires Microsoft Visual C++ (MSVC) build tools for compiling native dependencies like RocksDB.

#### Option A: Visual Studio 2022 (Recommended)

1. Download [Visual Studio 2022 Community Edition](https://visualstudio.microsoft.com/downloads/) (Free)
2. Run the installer (`vs_community.exe`)
3. In the installer, select **"Desktop development with C++"** workload
4. Ensure these components are checked:
   - ✅ MSVC v143 - VS 2022 C++ x64/x86 build tools (Latest)
   - ✅ Windows 11 SDK (or Windows 10 SDK)
   - ✅ C++ CMake tools for Windows
   - ✅ C++ ATL for latest v143 build tools (optional but recommended)
5. Click "Install" (requires ~7GB disk space)
6. **Reboot** after installation completes

#### Option B: Build Tools Only (Minimal Install)

For a lighter installation without the full Visual Studio IDE:

1. Download [Build Tools for Visual Studio 2022](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022)
2. Run the installer (`vs_BuildTools.exe`)
3. Select **"C++ build tools"** workload
4. Click "Install"
5. **Reboot** after installation completes

**Verify Installation:**

```powershell
# Open a new PowerShell window (important: after reboot)
# Check for cl.exe (MSVC compiler)
where.exe cl

# Should output something like:
# C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\14.38.33130\bin\Hostx64\x64\cl.exe
```

If `cl.exe` is not found, you may need to run the **"Developer Command Prompt for VS 2022"** or add MSVC to PATH manually.

### Step 2: Install LLVM (Clang)

LLVM/Clang is required for `bindgen`, which generates Rust bindings for RocksDB.

1. Download the latest [LLVM for Windows](https://github.com/llvm/llvm-project/releases)
   - Look for: `LLVM-<version>-win64.exe` (e.g., `LLVM-17.0.6-win64.exe`)
   - File size: ~400MB
2. Run the installer
3. **Important:** During installation, check **"Add LLVM to the system PATH for all users"** (or current user)
4. Complete the installation
5. **Restart PowerShell** (or reboot) for PATH changes to take effect

**Verify Installation:**

```powershell
# Check LLVM installation
clang --version

# Should output:
# clang version 17.0.6 (or later)
# Target: x86_64-pc-windows-msvc

# Check if LLVM is in PATH
where.exe clang

# Should output:
# C:\Program Files\LLVM\bin\clang.exe
```

### Step 3: Set Environment Variables

Some Rust crates need to locate LLVM libraries. Set the `LIBCLANG_PATH` environment variable:

#### Using PowerShell (Session Only)

```powershell
# Set for current session
$env:LIBCLANG_PATH = "C:\Program Files\LLVM\bin"

# Verify
$env:LIBCLANG_PATH
```

#### Using System Environment Variables (Permanent)

1. Press `Win + X`, select **"System"**
2. Click **"Advanced system settings"** → **"Environment Variables"**
3. Under **"User variables"** (or **"System variables"**), click **"New"**
4. Set:
   - **Variable name:** `LIBCLANG_PATH`
   - **Variable value:** `C:\Program Files\LLVM\bin`
5. Click **OK** to save
6. **Restart PowerShell** for changes to take effect

**Or use PowerShell to set permanently:**

```powershell
# Set user environment variable (permanent)
[System.Environment]::SetEnvironmentVariable('LIBCLANG_PATH', 'C:\Program Files\LLVM\bin', 'User')

# Verify
[System.Environment]::GetEnvironmentVariable('LIBCLANG_PATH', 'User')
```

### Step 4: Install Rust

1. Download [rustup-init.exe](https://rustup.rs/)
2. Run the installer
3. When prompted, select:
   - **Default installation** (option 1)
   - **MSVC toolchain** (should be automatic on Windows)
4. The installer will download and configure Rust
5. **Close and reopen PowerShell** after installation

**Verify Installation:**

```powershell
# Check Rust version (should be 1.92 or later)
rustc --version

# Check Cargo version
cargo --version

# Check installed toolchain (should show MSVC)
rustup show

# Should output something like:
# Default host: x86_64-pc-windows-msvc
# ...
# active toolchain: stable-x86_64-pc-windows-msvc (default)
```

**Important:** Ensure you're using the **MSVC** toolchain, not GNU. If you see `gnu` in the output:

```powershell
# Switch to MSVC toolchain
rustup default stable-x86_64-pc-windows-msvc
```

### Step 5: Install Git (if not already installed)

```powershell
# Check if Git is installed
git --version

# If not installed, download from: https://git-scm.com/download/win
# Or install via winget:
winget install --id Git.Git -e --source winget
```

### Step 6: Clone and Build KalamDB

```powershell
# Clone the repository
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB\backend

# First build (takes 15-30 minutes on Windows - compiles RocksDB, Arrow, Parquet)
cargo build

# Run tests to verify everything works
cargo test

# Run the server
cargo run
```

**Expected Output:**

```
INFO  kalamdb_server > KalamDB Server starting...
INFO  kalamdb_server > Storage path: ./data
INFO  kalamdb_server > Server listening on http://127.0.0.1:8080
```

---

## Troubleshooting

### Issue: "error: linker `link.exe` not found"

**Cause:** Visual Studio Build Tools not installed or not in PATH.

**Solution 1:** Install Visual Studio Build Tools (see Step 1)

**Solution 2:** Run from Developer Command Prompt

1. Open **"Developer Command Prompt for VS 2022"** from Start Menu
2. Navigate to project: `cd C:\path\to\KalamDB\backend`
3. Build: `cargo build`

**Solution 3:** Manually add MSVC to PATH

```powershell
# Find your MSVC installation
$msvcPath = "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\14.38.33130\bin\Hostx64\x64"

# Add to PATH for current session
$env:Path += ";$msvcPath"

# Verify
where.exe link
```

### Issue: "Unable to find libclang" / "could not find `libclang.dll`"

**Cause:** LLVM not installed or `LIBCLANG_PATH` not set.

**Solution:**

```powershell
# Check if LLVM is installed
clang --version

# If LLVM is installed but error persists, set LIBCLANG_PATH
$env:LIBCLANG_PATH = "C:\Program Files\LLVM\bin"

# Make it permanent (requires admin or restart PowerShell after)
[System.Environment]::SetEnvironmentVariable('LIBCLANG_PATH', 'C:\Program Files\LLVM\bin', 'User')

# Clean and rebuild
cargo clean
cargo build
```

### Issue: "failed to run custom build command for `rocksdb`"

**Cause:** Missing CMake or C++ build tools.

**Solution:** Ensure you installed the full "Desktop development with C++" workload in Visual Studio Build Tools, including CMake.

**Or install CMake separately:**

1. Download [CMake for Windows](https://cmake.org/download/)
2. During install, select "Add CMake to system PATH"
3. Restart PowerShell
4. Verify: `cmake --version`

### Issue: "error: could not compile `ring`"

**Cause:** Using GNU toolchain instead of MSVC.

**Solution:**

```powershell
# Check current toolchain
rustup show

# If it shows "gnu", switch to MSVC
rustup default stable-x86_64-pc-windows-msvc

# Clean and rebuild
cargo clean
cargo build
```

### Issue: Slow Compilation Times

**First Build:** 15-30 minutes is normal on Windows (compiling large C++ libraries)

**Speed up subsequent builds:**

```powershell
# Use cargo check for faster error checking (doesn't produce binaries)
cargo check

# Limit parallel jobs to reduce memory usage
cargo build -j 2

# Use faster linker (create or edit ~\.cargo\config.toml)
# Note: Requires installing LLVM lld
mkdir -p ~\.cargo
@"
[target.x86_64-pc-windows-msvc]
rustflags = ["-C", "link-arg=/DEBUG:NONE"]
"@ | Out-File -FilePath ~\.cargo\config.toml -Encoding utf8
```

### Issue: Out of Memory During Compilation

**Solution:** Limit parallel compilation jobs

```powershell
# Build with fewer parallel jobs
cargo build -j 2

# Or set globally in ~\.cargo\config.toml
mkdir -p ~\.cargo
@"
[build]
jobs = 2
"@ | Out-File -FilePath ~\.cargo\config.toml -Encoding utf8
```

### Issue: "Access Denied" or Permission Errors

**Cause:** Antivirus or Windows Defender blocking compilation.

**Solution:**

1. Add exclusion for project folder:
   - Open **Windows Security** → **Virus & threat protection**
   - Click **Manage settings** → **Add or remove exclusions**
   - Add folder: `C:\Users\<YourName>\KalamDB`
2. Add exclusion for Rust toolchain:
   - Add folder: `C:\Users\<YourName>\.cargo`
   - Add folder: `C:\Users\<YourName>\.rustup`

### Issue: "SSL error" or "certificate verify failed"

**Solution:** Update Windows root certificates

```powershell
# Update Windows
# Go to: Settings → Update & Security → Windows Update

# Or manually download root certificates
# From: https://curl.se/docs/caextract.html
```

### Issue: Long File Paths Error

**Error:** `"error: <filename>: The filename or extension is too long"`

**Solution:** Enable long path support in Windows

```powershell
# Run PowerShell as Administrator
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -Value 1

# Reboot required
```

---

## Windows-Specific Notes

### Using Windows Terminal (Recommended)

For a better terminal experience:

1. Install [Windows Terminal](https://aka.ms/terminal) from Microsoft Store
2. Set PowerShell as default profile
3. Customize appearance for better readability

### Using WSL2 (Alternative Approach)

If you prefer a Linux-like environment:

1. Install WSL2: `wsl --install`
2. Install Ubuntu from Microsoft Store
3. Follow the [Linux Setup Guide](linux.md) inside WSL2

**Pros:** Linux tooling, potentially faster builds  
**Cons:** Separate environment, file system performance overhead

### Visual Studio Integration

To use KalamDB with Visual Studio Code:

1. Install [VS Code](https://code.visualstudio.com/)
2. Install extensions:
   - **rust-analyzer** (Rust language support)
   - **CodeLLDB** (Debugging support)
3. Open KalamDB folder in VS Code
4. Use integrated terminal for cargo commands

---

## Verification Checklist

After setup, verify everything works:

- [ ] Visual Studio Build Tools installed: `where.exe cl`
- [ ] LLVM installed: `clang --version`
- [ ] Rust installed (MSVC): `cargo --version` and `rustup show`
- [ ] LIBCLANG_PATH set: `$env:LIBCLANG_PATH`
- [ ] Git installed: `git --version`
- [ ] KalamDB builds: `cargo build`
- [ ] Tests pass: `cargo test`
- [ ] Server runs: `cargo run`

---

## Environment Variables Reference

Set these in Windows Environment Variables or PowerShell profile:

```powershell
# LIBCLANG_PATH (required for RocksDB bindings)
LIBCLANG_PATH=C:\Program Files\LLVM\bin

# Optional: Rust logging level
RUST_LOG=debug

# Optional: Cargo parallel jobs (reduce if low RAM)
CARGO_BUILD_JOBS=4
```

**To edit PowerShell profile:**

```powershell
# Open PowerShell profile
notepad $PROFILE

# Add environment variables:
$env:LIBCLANG_PATH = "C:\Program Files\LLVM\bin"
$env:RUST_LOG = "info"
```

---

## Development Workflow

```powershell
# Navigate to backend
cd KalamDB\backend

# Check for errors (faster than build)
cargo check

# Build
cargo build

# Run tests
cargo test

# Run server with logging
$env:RUST_LOG="debug"; cargo run

# Build release version
cargo build --release

# Run release build
.\target\release\kalamdb-server.exe

# Format code
cargo fmt

# Run linter
cargo clippy
```

---

## Performance Tips

### Enable Incremental Compilation (Default)

Rust uses incremental compilation by default, but ensure it's enabled:

```powershell
# Check Cargo.toml has incremental = true
# Or set in ~\.cargo\config.toml:
@"
[build]
incremental = true
"@ | Out-File -Append -FilePath ~\.cargo\config.toml -Encoding utf8
```

### Use Release Builds for Testing

Debug builds are much slower. For performance testing:

```powershell
cargo build --release
.\target\release\kalamdb-server.exe
```

### Clean Build Artifacts

If disk space is limited:

```powershell
# Clean build artifacts (safe, can rebuild)
cargo clean

# Clean dependencies cache (re-downloads dependencies)
Remove-Item -Recurse -Force ~\.cargo\registry
Remove-Item -Recurse -Force ~\.cargo\git
```

---

## Next Steps

- Review [Backend README](../README.md) for project structure
- Read [Architecture Documentation](../../specs/001-build-a-rust/)
- Check [API Documentation](../README.md#api-endpoints)
- See [Testing Strategy](testing-strategy.md)

---

## Common PowerShell Commands

```powershell
# Navigate directories
cd path\to\folder
cd ..                    # Go up one directory
cd ~                     # Go to user home

# List files
ls                       # or dir

# Create directory
mkdir folder_name

# Remove directory
Remove-Item -Recurse folder_name

# View environment variable
$env:VARIABLE_NAME

# Set environment variable (session)
$env:VARIABLE_NAME = "value"

# Run as administrator
Start-Process powershell -Verb runAs
```

---

**Windows Versions Tested:**
- ✅ Windows 11 23H2 (x64)
- ✅ Windows 10 22H2 (x64)
- ✅ Windows Server 2022

**Build Tools Tested:**
- ✅ Visual Studio 2022 (v17.8+)
- ✅ Visual Studio Build Tools 2022

**Last Updated:** October 14, 2025  
**KalamDB Version:** 0.1.3
