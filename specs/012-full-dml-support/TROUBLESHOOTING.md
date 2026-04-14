# Build Troubleshooting Checklist

Quick reference for resolving common KalamDB build issues.

## ✅ Pre-Build Checklist

Before running `cargo build`, verify:

### Windows

```powershell
# ✓ Visual Studio Build Tools installed
cl.exe
# Should show: Microsoft (R) C/C++ Optimizing Compiler Version...

# ✓ LLVM installed and in PATH
clang --version
# Should show: clang version 14.0.0 or later

# ✓ Rust MSVC toolchain
rustup default
# Should show: stable-x86_64-pc-windows-msvc

# ✓ LIBCLANG_PATH set (if needed)
echo $env:LIBCLANG_PATH
# Should show: C:\Program Files\LLVM\bin (or similar)
```

### macOS

```bash
# ✓ Xcode Command Line Tools installed
xcode-select -p
# Should show: /Library/Developer/CommandLineTools

# ✓ Clang available
clang --version
# Should show: Apple clang version 14.0.0 or later

# ✓ Rust installed
rustc --version
# Should show: rustc 1.75.0 or later

# ✓ LLVM in PATH (if using Homebrew LLVM)
echo $PATH | grep llvm
# Should include: /opt/homebrew/opt/llvm/bin
```

### Linux

```bash
# ✓ Build tools installed
gcc --version
clang --version
cmake --version

# ✓ Development libraries
dpkg -l | grep libclang  # Ubuntu/Debian
rpm -qa | grep clang     # Fedora/RHEL

# ✓ Rust installed
rustc --version
cargo --version
```

---

## 🔍 Quick Diagnostics

### Test Compilation Environment

Create a minimal test to verify your environment:

```bash
# Create test project
cargo new --bin test_build
cd test_build

# Try building
cargo build

# If this fails, your Rust/C++ toolchain has issues
# If this succeeds, the problem is KalamDB-specific
```

### Check Dependency Resolution

```bash
cd KalamDB/backend

# See what cargo is trying to build
cargo tree

# Check for dependency conflicts
cargo tree --duplicates

# Update dependencies
cargo update
```

---

## 🐛 Error → Solution Quick Reference

### "linker `link.exe` not found" (Windows)

**Missing**: Visual Studio Build Tools

```powershell
# Verify MSVC is in PATH
where cl.exe

# If not found, install Visual Studio Build Tools
# Download: https://visualstudio.microsoft.com/downloads/
```

### "Unable to find libclang"

**Missing**: LLVM/Clang or PATH not set

**Windows**:
```powershell
$env:LIBCLANG_PATH = "C:\Program Files\LLVM\bin"
```

**macOS/Linux**:
```bash
# Ubuntu/Debian
sudo apt install libclang-dev

# macOS
brew install llvm
export PATH="/opt/homebrew/opt/llvm/bin:$PATH"
```

### "failed to run custom build command for `rocksdb`"

**Missing**: C++ compiler or CMake

**Windows**: Install Visual Studio Build Tools

**macOS**:
```bash
xcode-select --install
brew install cmake
```

**Linux**:
```bash
# Ubuntu/Debian
sudo apt install build-essential cmake

# Fedora/RHEL
sudo dnf install gcc-c++ cmake
```

### "error: could not compile `ring`"

**Windows Only**: Using GNU toolchain instead of MSVC

```powershell
# Switch to MSVC toolchain
rustup default stable-x86_64-pc-windows-msvc

# Verify
rustup default
```

### "ld: library not found for -lSystem" (macOS)

**Fix**: Reinstall Xcode Command Line Tools

```bash
sudo rm -rf /Library/Developer/CommandLineTools
xcode-select --install
```

### "error: linking with `cc` failed" (Linux)

**Missing**: Development libraries

```bash
# Ubuntu/Debian
sudo apt install libssl-dev pkg-config

# Fedora/RHEL
sudo dnf install openssl-devel pkgconfig
```

### Out of Memory During Build

**Solution**: Reduce parallel jobs

```bash
# Limit to 2 parallel jobs
cargo build -j 2

# Or set in ~/.cargo/config.toml:
# [build]
# jobs = 2
```

### "error: RocksDB version mismatch"

**Solution**: Clean and rebuild

```bash
cargo clean
rm -rf ~/.cargo/registry
rm -rf ~/.cargo/git
cargo build
```

---

## 🔧 Environment Cleanup

If you're experiencing persistent issues:

```bash
# 1. Clean project
cd KalamDB/backend
cargo clean

# 2. Update Rust
rustup update

# 3. Clear cargo cache (nuclear option)
rm -rf ~/.cargo/registry
rm -rf ~/.cargo/git

# 4. Rebuild
cargo build
```

---

## 🚀 Speed Up Builds

### Use `cargo check` for Fast Feedback

```bash
# Check for errors without building (much faster)
cargo check

# Only do full builds when needed
cargo build
```

### Enable Faster Linker

Create or edit `~/.cargo/config.toml`:

**Linux**:
```toml
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

**macOS**:
```toml
[target.x86_64-apple-darwin]
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

**Windows**:
```toml
[target.x86_64-pc-windows-msvc]
rustflags = ["-C", "link-arg=/DEBUG:NONE"]
```

### Install `lld` (LLVM Linker)

```bash
# Ubuntu/Debian
sudo apt install lld

# macOS
brew install llvm

# Fedora/RHEL
sudo dnf install lld
```

---

## 📊 Build Performance Expectations

### First Build (Clean)

- **Windows**: 15-25 minutes
- **macOS**: 10-20 minutes
- **Linux**: 10-20 minutes

Compiles RocksDB, Arrow, Parquet, and all dependencies from source.

### Incremental Builds

- **Small changes**: 5-30 seconds
- **Module changes**: 1-3 minutes
- **Dependency updates**: 5-15 minutes

### `cargo check` vs `cargo build`

- **cargo check**: 2-5x faster (no code generation)
- **cargo build**: Full compilation and linking

---

## 🆘 Still Having Issues?

### Verbose Output

Get detailed error information:

```bash
# Maximum verbosity
cargo build -vv

# With environment info
RUST_BACKTRACE=1 cargo build -vv
```

### Platform-Specific Help

- **Windows**: Check the [Windows Setup](DEVELOPMENT_SETUP.md#windows-setup) section
- **macOS**: Check the [macOS Setup](DEVELOPMENT_SETUP.md#macos-setup) section  
- **Linux**: Check the [Linux Setup](DEVELOPMENT_SETUP.md#linux-setup) section

### Get Help

1. Check [Full Troubleshooting Guide](DEVELOPMENT_SETUP.md#troubleshooting)
2. Search [GitHub Issues](https://github.com/kalamstack/KalamDB/issues)
3. Open a new issue with:
   - Your OS and version
   - Rust version (`rustc --version`)
   - Full error output (`cargo build -vv`)
   - Steps you've already tried

---

## 📋 Environment Info Script

Run this to collect diagnostic information:

### Windows (PowerShell)

```powershell
Write-Host "=== System Info ===" -ForegroundColor Cyan
systeminfo | Select-String "OS Name","OS Version"

Write-Host "`n=== Rust Toolchain ===" -ForegroundColor Cyan
rustc --version
cargo --version
rustup default

Write-Host "`n=== Compilers ===" -ForegroundColor Cyan
cl.exe 2>&1 | Select-String "Version"
clang --version

Write-Host "`n=== Environment ===" -ForegroundColor Cyan
Write-Host "LIBCLANG_PATH: $env:LIBCLANG_PATH"
Write-Host "PATH (LLVM):" ($env:Path -split ';' | Select-String "LLVM")
```

### macOS/Linux (Bash)

```bash
#!/bin/bash

echo "=== System Info ==="
uname -a

echo -e "\n=== Rust Toolchain ==="
rustc --version
cargo --version
rustup default

echo -e "\n=== Compilers ==="
clang --version
gcc --version 2>/dev/null || echo "gcc not found"

echo -e "\n=== Libraries ==="
ldconfig -p 2>/dev/null | grep -E "libclang|libstdc" || echo "ldconfig not available"

echo -e "\n=== Environment ==="
echo "LIBCLANG_PATH: $LIBCLANG_PATH"
echo "LLVM_CONFIG_PATH: $LLVM_CONFIG_PATH"
echo "PATH (LLVM): $(echo $PATH | grep -o '[^:]*llvm[^:]*')"
```

Save output and include it when asking for help.

---

**Last Updated**: October 14, 2025  
**For detailed solutions**: See [DEVELOPMENT_SETUP.md](DEVELOPMENT_SETUP.md)
