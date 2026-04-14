# macOS Development Setup - KalamDB

Complete guide for setting up KalamDB development environment on macOS.

## System Requirements

- **macOS**: 11 (Big Sur) or later
- **Architecture**: Apple Silicon (M1/M2/M3) or Intel
- **RAM**: 8GB minimum, 16GB recommended
- **Disk Space**: 3GB for dependencies and build artifacts
- **Xcode Command Line Tools**: Required
- **Homebrew**: Recommended for package management

---

## Quick Setup

```bash
# 1. Install Xcode Command Line Tools
xcode-select --install

# 2. Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 3. Install LLVM and Rust
brew install llvm rust

# 4. Set up library paths for RocksDB compilation
echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
echo 'export DYLD_FALLBACK_LIBRARY_PATH="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib:$DYLD_FALLBACK_LIBRARY_PATH"' >> ~/.zshrc
source ~/.zshrc

# 5. Clone and build
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend
cargo build
```

---

## Detailed Setup Instructions

### Step 1: Install Xcode Command Line Tools

The Xcode Command Line Tools provide essential build tools including clang, git, and make.

```bash
# Install Command Line Tools
xcode-select --install
```

A dialog will appear asking you to install the tools. Click "Install" and accept the license agreement.

**Verify Installation:**

```bash
# Check clang version
clang --version
# Should output: Apple clang version 14.0.0 or later

# Check git
git --version

# Verify Xcode tools path
xcode-select -p
# Should output: /Applications/Xcode.app/Contents/Developer
# or: /Library/Developer/CommandLineTools
```

### Step 2: Install Homebrew

Homebrew is the recommended package manager for macOS.

```bash
# Install Homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

**For Apple Silicon Macs**, add Homebrew to your PATH:

```bash
# Add to ~/.zshrc
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zshrc
source ~/.zshrc
```

**For Intel Macs**, Homebrew installs to `/usr/local` and should already be in your PATH.

**Verify Installation:**

```bash
brew --version
# Should output: Homebrew 4.x.x or later
```

### Step 3: Install LLVM

LLVM/Clang is required for compiling RocksDB and other native dependencies.

```bash
# Install LLVM via Homebrew
brew install llvm

# Add LLVM to PATH
echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

**For Intel Macs**, use `/usr/local` instead:

```bash
echo 'export PATH="/usr/local/opt/llvm/bin:$PATH"' >> ~/.zshrc
```

**Verify Installation:**

```bash
# Check LLVM installation
llvm-config --version
clang --version
```

### Step 4: Install Rust

```bash
# Install Rust via Homebrew (recommended)
brew install rust

# OR install via rustup (alternative method)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**Verify Installation:**

```bash
rustc --version
# Should output: rustc 1.92.0 or later

cargo --version
# Should output: cargo 1.92.0 or later

rustup show
# Shows installed toolchains
```

### Step 5: Configure Dynamic Linker for RocksDB

**Critical for macOS**: The RocksDB build process requires `libclang.dylib`, which may not be found automatically. Set the fallback library path:

```bash
# Add to ~/.zshrc
echo 'export DYLD_FALLBACK_LIBRARY_PATH="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib:$DYLD_FALLBACK_LIBRARY_PATH"' >> ~/.zshrc

# Reload configuration
source ~/.zshrc

# Verify the path is set
echo $DYLD_FALLBACK_LIBRARY_PATH
```

**Why is this needed?**

The `librocksdb-sys` crate compiles native C++ code and uses bindgen to generate Rust bindings. During compilation, the build script needs to find `libclang.dylib` to parse C++ headers. Without this environment variable, you'll see errors like:

```
dyld[xxxxx]: Library not loaded: @rpath/libclang.dylib
Reason: tried: ... (no such file)
```

### Step 6: Install Additional Dependencies (Optional)

Some development tasks may require additional tools:

```bash
# CMake (for building some native dependencies)
brew install cmake

# pkg-config (helps Rust find native libraries)
brew install pkg-config

# OpenSSL (usually pre-installed, but just in case)
brew install openssl@3
```

### Step 7: Clone and Build KalamDB

```bash
# Clone the repository
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend

# First build (takes 10-20 minutes - compiles RocksDB, Arrow, Parquet)
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

### Issue: "Library not loaded: @rpath/libclang.dylib"

**Error:**
```
dyld[xxxxx]: Library not loaded: @rpath/libclang.dylib
  Referenced from: .../build-script-build
  Reason: tried: ... (no such file)
```

**Solution:**

```bash
# Clean previous build attempts
cd backend
cargo clean

# Set the dynamic library fallback path
export DYLD_FALLBACK_LIBRARY_PATH="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib:$DYLD_FALLBACK_LIBRARY_PATH"

# Make it permanent (add to ~/.zshrc)
echo 'export DYLD_FALLBACK_LIBRARY_PATH="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib:$DYLD_FALLBACK_LIBRARY_PATH"' >> ~/.zshrc
source ~/.zshrc

# Rebuild
cargo build
```

### Issue: "xcrun: error: unable to find utility 'clang'"

**Solution:** Reinstall Xcode Command Line Tools

```bash
# Remove existing tools
sudo rm -rf /Library/Developer/CommandLineTools

# Reinstall
xcode-select --install

# Verify
xcode-select -p
```

### Issue: "ld: library not found for -lSystem"

**Solution:** Reset Xcode Command Line Tools path

```bash
# Point to the correct tools
sudo xcode-select --switch /Library/Developer/CommandLineTools

# Or if you have Xcode installed
sudo xcode-select --switch /Applications/Xcode.app/Contents/Developer

# Verify
xcode-select -p
```

### Issue: Homebrew paths not found (Apple Silicon)

**Solution:** Ensure Homebrew is in your PATH

```bash
# For Apple Silicon Macs
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zshrc
source ~/.zshrc

# Verify
which brew
# Should output: /opt/homebrew/bin/brew
```

### Issue: "Unable to find libclang"

**Solution:** Set LLVM environment variables

```bash
# For Apple Silicon
export LLVM_CONFIG_PATH="/opt/homebrew/opt/llvm/bin/llvm-config"

# For Intel
export LLVM_CONFIG_PATH="/usr/local/opt/llvm/bin/llvm-config"

# Add to ~/.zshrc to make permanent
echo 'export LLVM_CONFIG_PATH="/opt/homebrew/opt/llvm/bin/llvm-config"' >> ~/.zshrc
```

### Issue: Slow compilation times

**First Build:** 10-20 minutes is normal (compiling RocksDB, Arrow, Parquet from source)

**Speed up subsequent builds:**

```bash
# Use cargo check for faster error checking
cargo check

# Enable faster linker (create ~/.cargo/config.toml)
mkdir -p ~/.cargo
cat > ~/.cargo/config.toml << EOF
[target.aarch64-apple-darwin]
rustflags = ["-C", "link-arg=-fuse-ld=/opt/homebrew/opt/llvm/bin/ld64.lld"]

[target.x86_64-apple-darwin]
rustflags = ["-C", "link-arg=-fuse-ld=/usr/local/opt/llvm/bin/ld64.lld"]
EOF
```

### Issue: Out of memory during compilation

**Solution:** Limit parallel compilation jobs

```bash
# Build with fewer parallel jobs
cargo build -j 2

# Or set globally in ~/.cargo/config.toml
[build]
jobs = 2
```

---

## Architecture-Specific Notes

### Apple Silicon (M1/M2/M3)

- Default toolchain: `aarch64-apple-darwin`
- Homebrew prefix: `/opt/homebrew`
- Rosetta 2 not required for KalamDB
- Native ARM64 performance is excellent

### Intel Macs

- Default toolchain: `x86_64-apple-darwin`
- Homebrew prefix: `/usr/local`
- Adjust all `/opt/homebrew` paths to `/usr/local` in the commands above

**Check your architecture:**

```bash
uname -m
# Apple Silicon: arm64
# Intel: x86_64
```

---

## Verification Checklist

After setup, verify everything works:

- [ ] Xcode Command Line Tools installed: `xcode-select -p`
- [ ] Homebrew installed: `brew --version`
- [ ] LLVM installed: `llvm-config --version`
- [ ] Rust installed: `cargo --version`
- [ ] Dynamic linker configured: `echo $DYLD_FALLBACK_LIBRARY_PATH`
- [ ] KalamDB builds: `cargo build`
- [ ] Tests pass: `cargo test`
- [ ] Server runs: `cargo run`

---

## Environment Variables Reference

Add these to your `~/.zshrc` (or `~/.bash_profile` if using bash):

```bash
# Homebrew (Apple Silicon)
eval "$(/opt/homebrew/bin/brew shellenv)"

# LLVM
export PATH="/opt/homebrew/opt/llvm/bin:$PATH"

# Dynamic linker for RocksDB compilation
export DYLD_FALLBACK_LIBRARY_PATH="/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib:$DYLD_FALLBACK_LIBRARY_PATH"

# Optional: Rust logging
export RUST_LOG=debug

# Optional: LLVM config (if issues persist)
export LLVM_CONFIG_PATH="/opt/homebrew/opt/llvm/bin/llvm-config"
```

**For Intel Macs**, replace `/opt/homebrew` with `/usr/local`.

---

## Development Workflow

```bash
# Navigate to backend
cd KalamDB/backend

# Check for errors (faster than build)
cargo check

# Build
cargo build

# Run tests
cargo test

# Run server with logging
RUST_LOG=debug cargo run

# Build release version
cargo build --release

# Format code
cargo fmt

# Run linter
cargo clippy
```

---

## Next Steps

- Review [Backend README](../README.md) for project structure
- Read [Architecture Documentation](../../specs/001-build-a-rust/)
- Check [API Documentation](../README.md#api-endpoints)
- See [Testing Strategy](testing-strategy.md)

---

**macOS Versions Tested:**
- ✅ macOS 15 Sequoia (Apple Silicon)
- ✅ macOS 14 Sonoma (Apple Silicon & Intel)
- ✅ macOS 13 Ventura (Apple Silicon & Intel)
- ✅ macOS 12 Monterey (Intel)

**Last Updated:** October 14, 2025  
**KalamDB Version:** 0.1.3
