# KalamDB Development Setup Guide

This guide provides detailed instructions for setting up KalamDB for development on Windows, macOS, and Linux.

## Table of Contents

- [System Requirements](#system-requirements)
- [Platform-Specific Setup Guides](#platform-specific-setup-guides)
- [Windows Setup](#windows-setup)
- [macOS Setup](#macos-setup)
- [Linux Setup](#linux-setup)
- [Verify Installation](#verify-installation)
- [Build and Run](#build-and-run)
- [Troubleshooting](#troubleshooting)

---

## Platform-Specific Setup Guides

For detailed, platform-specific setup instructions with comprehensive troubleshooting:

- 📖 **[macOS Setup Guide](macos.md)** - Complete guide for macOS (Intel & Apple Silicon)
  - Includes fix for `libclang.dylib` loading issues
  - Xcode Command Line Tools setup
  - Homebrew and LLVM configuration
  
- 📖 **[Linux Setup Guide](linux.md)** - Covers Ubuntu, Debian, Fedora, Arch, and more
  - Distribution-specific package installation
  - LLVM and build tools setup
  - Performance optimization tips
  
- 📖 **[Windows Setup Guide](windows.md)** - Complete Windows setup with Visual Studio
  - Visual Studio Build Tools installation
  - LLVM/Clang configuration
  - PowerShell commands and troubleshooting

---

## System Requirements

### All Platforms

- **Rust**: 1.92 or later
- **LLVM/Clang**: Required for RocksDB compilation
- **C++ Compiler**: Required for native dependencies
- **Git**: For cloning the repository
- **Minimum RAM**: 4GB (8GB recommended)
- **Disk Space**: 2GB for dependencies and build artifacts

### Why LLVM/C++ is Required

KalamDB depends on several native libraries that require C/C++ compilation:

- **RocksDB**: High-performance embedded database (C++)
- **Arrow/Parquet**: Columnar data format libraries (C++)
- **DataFusion**: SQL query engine with native components

Without LLVM/Clang and a C++ compiler, the Rust build process will fail when compiling these dependencies.

---

## Windows Setup

### Step 1: Install Visual Studio Build Tools

KalamDB requires the Microsoft Visual C++ (MSVC) build tools for compiling native dependencies.

#### Option A: Visual Studio 2022 (Recommended)

1. Download [Visual Studio 2022 Community Edition](https://visualstudio.microsoft.com/downloads/)
2. Run the installer
3. Select **"Desktop development with C++"** workload
4. Ensure these components are checked:
   - MSVC v143 - VS 2022 C++ x64/x86 build tools
   - Windows 10/11 SDK
   - C++ CMake tools for Windows
5. Click "Install" (requires ~7GB disk space)

#### Option B: Build Tools Only (Minimal Install)

1. Download [Visual Studio Build Tools 2022](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022)
2. Run the installer
3. Select **"C++ build tools"** workload
4. Click "Install"

### Step 2: Install LLVM (Clang)

LLVM/Clang is required for bindgen (used by RocksDB bindings).

1. Download [LLVM for Windows](https://github.com/llvm/llvm-project/releases)
   - Get the latest stable release (e.g., `LLVM-17.0.6-win64.exe`)
2. Run the installer
3. **Important**: Check "Add LLVM to the system PATH" during installation
4. Complete the installation

### Step 3: Install Rust

1. Download [rustup-init.exe](https://rustup.rs/)
2. Run the installer
3. Follow the prompts (default options are recommended)
4. Choose the **MSVC** toolchain when prompted
5. Restart your terminal/PowerShell after installation

### Step 4: Verify LLVM Installation

Open PowerShell and verify:

```powershell
# Check LLVM installation
clang --version

# Should output something like:
# clang version 17.0.6
# Target: x86_64-pc-windows-msvc
```

If `clang` is not found, add LLVM to your PATH manually:

```powershell
# Add to system PATH (adjust version number as needed)
$env:Path += ";C:\Program Files\LLVM\bin"
```

To make this permanent:
1. Open "System Properties" → "Environment Variables"
2. Edit "Path" under "System variables"
3. Add `C:\Program Files\LLVM\bin`

### Step 5: Set Environment Variables

Some Rust crates need to locate LLVM:

```powershell
# Set LIBCLANG_PATH (adjust path if needed)
[System.Environment]::SetEnvironmentVariable('LIBCLANG_PATH', 'C:\Program Files\LLVM\bin', 'User')

# Verify
$env:LIBCLANG_PATH
```

### Step 6: Clone and Build

```powershell
# Clone the repository
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB\backend

# Build the project (first build will take 10-20 minutes)
cargo build

# Run tests
cargo test
```

---

## macOS Setup

### Step 1: Install Xcode Command Line Tools

The Xcode Command Line Tools provide the C++ compiler and build tools.

```bash
# Install Xcode Command Line Tools
xcode-select --install

# Verify installation
clang --version
# Should show: Apple clang version 14.0.0 (or later)
```

### Step 2: Install LLVM (Optional but Recommended)

While macOS includes clang, installing LLVM via Homebrew ensures compatibility:

```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install LLVM
brew install llvm

# Add LLVM to PATH (add to ~/.zshrc or ~/.bash_profile)
echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
echo 'export LDFLAGS="-L/opt/homebrew/opt/llvm/lib"' >> ~/.zshrc
echo 'export CPPFLAGS="-I/opt/homebrew/opt/llvm/include"' >> ~/.zshrc

# Configure dynamic linker for RocksDB (CRITICAL for macOS)
echo 'export DYLD_FALLBACK_LIBRARY_PATH="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib:$DYLD_FALLBACK_LIBRARY_PATH"' >> ~/.zshrc

# Reload shell configuration
source ~/.zshrc
```

**Note for Intel Macs**: Replace `/opt/homebrew` with `/usr/local` in the paths above.

**Why DYLD_FALLBACK_LIBRARY_PATH?** This environment variable is critical for RocksDB compilation on macOS. Without it, you may encounter `libclang.dylib` loading errors. See the [macOS Setup Guide](macos.md#step-5-configure-dynamic-linker-for-rocksdb) for details.

### Step 3: Install Rust

```bash
# Download and install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Follow the prompts (default options recommended)

# Reload PATH
source $HOME/.cargo/env

# Verify installation
rustc --version
cargo --version
```

### Step 4: Install Additional Dependencies

```bash
# Install CMake (required for some dependencies)
brew install cmake

# Install pkg-config (helps Rust find native libraries)
brew install pkg-config
```

### Step 5: Clone and Build

```bash
# Clone the repository
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend

# Build the project (first build will take 10-20 minutes)
cargo build

# Run tests
cargo test
```

---

## Linux Setup

### Ubuntu/Debian

#### Step 1: Install Build Tools and LLVM

```bash
# Update package list
sudo apt update

# Install essential build tools
sudo apt install -y build-essential

# Install LLVM, Clang, and development libraries
sudo apt install -y llvm clang libclang-dev

# Install additional dependencies
sudo apt install -y cmake pkg-config libssl-dev git

# Verify installation
clang --version
# Should show: Ubuntu clang version 14.0.0 (or later)
```

#### Step 2: Install Rust

```bash
# Download and install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Follow the prompts (default options recommended)

# Reload PATH
source $HOME/.cargo/env

# Verify installation
rustc --version
cargo --version
```

#### Step 3: Clone and Build

```bash
# Clone the repository
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend

# Build the project (first build will take 10-20 minutes)
cargo build

# Run tests
cargo test
```

### Fedora/RHEL/CentOS

#### Step 1: Install Build Tools and LLVM

```bash
# Install development tools
sudo dnf groupinstall -y "Development Tools"

# Install LLVM and Clang
sudo dnf install -y llvm clang clang-devel

# Install additional dependencies
sudo dnf install -y cmake pkgconfig openssl-devel git

# Verify installation
clang --version
```

#### Step 2: Install Rust

```bash
# Download and install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Follow the prompts (default options recommended)

# Reload PATH
source $HOME/.cargo/env

# Verify installation
rustc --version
cargo --version
```

#### Step 3: Clone and Build

```bash
# Clone the repository
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend

# Build the project
cargo build

# Run tests
cargo test
```

### Arch Linux

#### Step 1: Install Build Tools and LLVM

```bash
# Install base development tools
sudo pacman -S base-devel

# Install LLVM, Clang, and dependencies
sudo pacman -S llvm clang cmake pkg-config openssl git

# Verify installation
clang --version
```

#### Step 2: Install Rust

```bash
# Option 1: Via rustup (recommended)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Option 2: Via pacman
sudo pacman -S rust

# Verify installation
rustc --version
cargo --version
```

#### Step 3: Clone and Build

```bash
# Clone the repository
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend

# Build the project
cargo build

# Run tests
cargo test
```

---

## Verify Installation

After completing the setup for your platform, verify everything is working:

### 1. Check Rust Toolchain

```bash
# Check Rust version (should be 1.92 or later)
rustc --version

# Check cargo version
cargo --version

# Check installed toolchain
rustup show
```

### 2. Check C/C++ Compiler

```bash
# Windows (PowerShell)
cl.exe

# macOS/Linux
clang --version
gcc --version
```

### 3. Check LLVM/Clang

```bash
clang --version

# Should output version information
# Example: clang version 14.0.0 or later
```

### 4. Test Build

```bash
# Navigate to backend directory
cd KalamDB/backend

# Clean build (removes previous artifacts)
cargo clean

# Build in debug mode
cargo build

# Build in release mode (optimized, slower to compile)
cargo build --release
```

---

## Build and Run

### Development Build

```bash
cd backend

# Build all crates
cargo build

# Build specific crate
cargo build -p kalamdb-core
cargo build -p kalamdb-api
cargo build -p kalamdb-server

# Run the server
cargo run

# Run with logging
RUST_LOG=debug cargo run
```

### Release Build

```bash
# Build optimized release version
cargo build --release

# Run release build
./target/release/kalamdb-server
```

### Configuration

```bash
# Copy example configuration
cp config.example.toml config.toml

# Edit configuration with your settings
# Default server: http://127.0.0.1:8080
```

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_message_storage

# Run integration tests only
cargo test --test '*'

# Run with output
cargo test -- --nocapture

# Run with logging
RUST_LOG=debug cargo test
```

### Code Quality

```bash
# Check for errors (faster than full build)
cargo check

# Run linter
cargo clippy

# Format code
cargo fmt

# Check formatting without changing files
cargo fmt -- --check
```

---

## Troubleshooting

### Common Issues

#### 1. "error: linker `link.exe` not found" (Windows)

**Cause**: Visual Studio Build Tools not installed or not in PATH.

**Solution**:
- Install Visual Studio Build Tools (see Windows Setup Step 1)
- Restart your terminal after installation
- Verify with: `where cl.exe`

#### 2. "Unable to find libclang" (All Platforms)

**Cause**: LLVM/Clang not installed or not in PATH.

**Solution**:

**Windows**:
```powershell
# Set LIBCLANG_PATH
$env:LIBCLANG_PATH = "C:\Program Files\LLVM\bin"
```

**macOS**:
```bash
# Set dynamic library fallback path (CRITICAL for RocksDB)
export DYLD_FALLBACK_LIBRARY_PATH="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib:$DYLD_FALLBACK_LIBRARY_PATH"

# Make permanent (add to ~/.zshrc)
echo 'export DYLD_FALLBACK_LIBRARY_PATH="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib:$DYLD_FALLBACK_LIBRARY_PATH"' >> ~/.zshrc
```

**Linux**:
```bash
sudo apt install libclang-dev  # Ubuntu/Debian
sudo dnf install clang-devel   # Fedora/RHEL
```

**See also:** [macOS libclang troubleshooting](macos.md#issue-library-not-loaded-rpathlibclangdylib)

#### 3. "failed to run custom build command for `rocksdb`"

**Cause**: Missing C++ compiler or build tools.

**Solution**:
- **Windows**: Install Visual Studio Build Tools
- **macOS**: Install Xcode Command Line Tools (`xcode-select --install`)
- **Linux**: Install build-essential (`sudo apt install build-essential`)

#### 4. Slow Compilation Times

**Cause**: Rust compiles dependencies from source, including large C++ libraries.

**Solution**:
- **First build**: Will take 10-20 minutes (normal)
- **Subsequent builds**: Should be much faster (incremental compilation)
- **Use `cargo check`**: Faster than `cargo build` for catching errors
- **Enable faster linker**:
  
  Create or edit `~/.cargo/config.toml`:
  
  ```toml
  # macOS/Linux
  [target.x86_64-unknown-linux-gnu]
  linker = "clang"
  rustflags = ["-C", "link-arg=-fuse-ld=lld"]
  
  # Windows
  [target.x86_64-pc-windows-msvc]
  rustflags = ["-C", "link-arg=/DEBUG:NONE"]
  ```

#### 5. Out of Memory During Compilation

**Cause**: Insufficient RAM for parallel compilation.

**Solution**:

Create or edit `~/.cargo/config.toml`:

```toml
[build]
# Reduce parallel compilation jobs
jobs = 2
```

Or use environment variable:

```bash
# Limit to 2 parallel jobs
cargo build -j 2
```

#### 6. "error: RocksDB version mismatch"

**Cause**: Cached dependencies with mismatched versions.

**Solution**:
```bash
# Clean build artifacts
cargo clean

# Remove dependency cache
rm -rf ~/.cargo/registry
rm -rf ~/.cargo/git

# Rebuild
cargo build
```

#### 7. SSL/TLS Errors (Linux)

**Cause**: Missing OpenSSL development libraries.

**Solution**:
```bash
# Ubuntu/Debian
sudo apt install libssl-dev pkg-config

# Fedora/RHEL
sudo dnf install openssl-devel pkgconfig

# Arch
sudo pacman -S openssl pkg-config
```

### Platform-Specific Issues

#### Windows: "error: could not compile `ring`"

**Solution**: Ensure you're using the MSVC toolchain (not GNU):

```powershell
# Check current toolchain
rustup default

# Should show: stable-x86_64-pc-windows-msvc
# If not, switch to MSVC:
rustup default stable-x86_64-pc-windows-msvc
```

#### macOS: "ld: library not found for -lSystem"

**Solution**: Reinstall Xcode Command Line Tools:

```bash
sudo rm -rf /Library/Developer/CommandLineTools
xcode-select --install
```

#### Linux: "error: failed to run custom build command for `arrow`"

**Solution**: Install additional development libraries:

```bash
# Ubuntu/Debian
sudo apt install libssl-dev libclang-dev cmake

# Fedora/RHEL
sudo dnf install openssl-devel clang-devel cmake
```

### Getting Help

If you encounter issues not covered here:

1. **Platform-Specific Guides**: See detailed troubleshooting in:
  - [macOS Setup Guide](macos.md#troubleshooting)
  - [Linux Setup Guide](linux.md#troubleshooting)
  - [Windows Setup Guide](windows.md#troubleshooting)
2. **Check GitHub Issues**: [KalamDB Issues](https://github.com/kalamstack/KalamDB/issues)
3. **Search Rust Forums**: Many dependency issues are common across Rust projects
4. **Enable Verbose Output**: `cargo build -vv` for detailed error messages
5. **Check Dependencies**: Ensure all system dependencies are installed

---

## Next Steps

Once your development environment is set up:

1. Read the [Backend README](../../backend/README.md) for project structure and development workflow
2. Review the [Architecture Documentation](../architecture/manifest.md) to understand KalamDB's storage design
3. Check out the [API Documentation](../api/api-reference.md) for REST endpoints
4. Review the [Constitution](../../.specify/memory/constitution.md) for development principles

---

## Quick Reference

### Essential Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build
cargo check                    # Fast error checking

# Run
cargo run                      # Run server
RUST_LOG=debug cargo run      # Run with logging

# Test
cargo test                     # All tests
cargo test test_name          # Specific test
cargo test -- --nocapture     # Show output

# Code Quality
cargo clippy                   # Linter
cargo fmt                      # Format code
cargo doc --open              # Generate and open docs

# Clean
cargo clean                    # Remove build artifacts
```

### Environment Variables

```bash
# Logging level
RUST_LOG=debug,info,warn,error,trace

# LLVM (if not in PATH)
LIBCLANG_PATH=/path/to/llvm/bin

# Parallel jobs
CARGO_BUILD_JOBS=2
```

---

**Last Updated**: December 31, 2025  
**KalamDB Version**: 0.1.3  
**Minimum Rust Version**: 1.92
