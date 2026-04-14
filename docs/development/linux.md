# Linux Development Setup - KalamDB

Complete guide for setting up KalamDB development environment on Linux distributions.

## System Requirements

- **Linux Distribution**: Ubuntu 20.04+, Fedora 36+, Debian 11+, Arch Linux, or compatible
- **RAM**: 8GB minimum, 16GB recommended
- **Disk Space**: 3GB for dependencies and build artifacts
- **Build Tools**: GCC/Clang, Make, CMake
- **Package Manager**: apt, dnf, pacman, or compatible

---

## Quick Setup by Distribution

### Ubuntu / Debian / Pop!_OS / Linux Mint

```bash
# 1. Update package list and install build tools
sudo apt update
sudo apt install -y build-essential llvm clang libclang-dev cmake pkg-config libssl-dev git curl

# 2. Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# 3. Clone and build
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend
cargo build
```

### Fedora / RHEL / CentOS Stream / Rocky Linux

```bash
# 1. Install development tools and dependencies
sudo dnf groupinstall -y "Development Tools"
sudo dnf install -y llvm clang clang-devel cmake pkgconfig openssl-devel git

# 2. Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# 3. Clone and build
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend
cargo build
```

### Arch Linux / Manjaro / EndeavourOS

```bash
# 1. Install build tools and dependencies
sudo pacman -S base-devel llvm clang cmake pkg-config openssl git

# 2. Install Rust (via rustup or pacman)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# OR: sudo pacman -S rust
source $HOME/.cargo/env

# 3. Clone and build
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend
cargo build
```

### openSUSE Leap / Tumbleweed

```bash
# 1. Install development tools and dependencies
sudo zypper install -t pattern devel_basis
sudo zypper install llvm clang clang-devel cmake pkg-config libopenssl-devel git

# 2. Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# 3. Clone and build
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend
cargo build
```

---

## Detailed Setup Instructions

### Step 1: Install Build Tools

Build tools are required for compiling RocksDB, Arrow, and other native dependencies.

#### Ubuntu / Debian

```bash
# Update package list
sudo apt update

# Install essential build tools
sudo apt install -y build-essential

# Verify installation
gcc --version
g++ --version
make --version
```

#### Fedora / RHEL

```bash
# Install development tools group
sudo dnf groupinstall -y "Development Tools"

# Verify installation
gcc --version
g++ --version
make --version
```

#### Arch Linux

```bash
# Install base development packages
sudo pacman -S base-devel

# Verify installation
gcc --version
make --version
```

### Step 2: Install LLVM and Clang

LLVM/Clang is required for RocksDB bindings generation.

#### Ubuntu / Debian

```bash
# Install LLVM, Clang, and development headers
sudo apt install -y llvm clang libclang-dev

# Verify installation
clang --version
llvm-config --version

# Should output: Ubuntu clang version 14.0.0 or later
```

#### Fedora / RHEL

```bash
# Install LLVM and Clang
sudo dnf install -y llvm clang clang-devel

# Verify installation
clang --version
llvm-config --version
```

#### Arch Linux

```bash
# Install LLVM and Clang
sudo pacman -S llvm clang

# Verify installation
clang --version
llvm-config --version
```

### Step 3: Install Additional Dependencies

#### Ubuntu / Debian

```bash
# CMake (build system)
sudo apt install -y cmake

# pkg-config (helps find libraries)
sudo apt install -y pkg-config

# OpenSSL development libraries
sudo apt install -y libssl-dev

# Git (if not already installed)
sudo apt install -y git curl
```

#### Fedora / RHEL

```bash
# CMake and pkg-config
sudo dnf install -y cmake pkgconfig

# OpenSSL development libraries
sudo dnf install -y openssl-devel

# Git and curl
sudo dnf install -y git curl
```

#### Arch Linux

```bash
# CMake and pkg-config
sudo pacman -S cmake pkg-config

# OpenSSL
sudo pacman -S openssl

# Git and curl
sudo pacman -S git curl
```

### Step 4: Install Rust

We recommend using `rustup` for managing Rust installations.

```bash
# Download and install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Follow the interactive prompts
# Select: 1) Proceed with installation (default)

# Add Rust to your current shell session
source $HOME/.cargo/env

# Verify installation
rustc --version
# Should output: rustc 1.92.0 or later

cargo --version
# Should output: cargo 1.92.0 or later

# Check installed toolchain
rustup show
```

**Make Rust permanent:** The installer automatically adds Rust to your `~/.bashrc` or `~/.zshrc`.

**Alternative (Arch Linux only):**

```bash
# Install Rust via pacman (may not be latest version)
sudo pacman -S rust

# Verify
cargo --version
```

### Step 5: Configure Environment (Optional)

Some configurations can improve build performance:

```bash
# Add to ~/.bashrc or ~/.zshrc

# Rust logging level
export RUST_LOG=info

# Parallel build jobs (adjust based on CPU cores)
export CARGO_BUILD_JOBS=4

# Use faster linker (mold or lld)
# Install mold: sudo apt install mold (Ubuntu 22.04+)
# Then add to ~/.cargo/config.toml:
# [target.x86_64-unknown-linux-gnu]
# linker = "clang"
# rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

### Step 6: Clone and Build KalamDB

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

### Issue: "Unable to find libclang"

**Error:**
```
error: failed to run custom build command for `bindgen`
Unable to find libclang
```

**Solution:**

#### Ubuntu / Debian
```bash
sudo apt install -y libclang-dev

# If issue persists, set LIBCLANG_PATH
export LIBCLANG_PATH=/usr/lib/llvm-14/lib
# Or find it: find /usr -name "libclang.so*" 2>/dev/null
```

#### Fedora / RHEL
```bash
sudo dnf install -y clang-devel

export LIBCLANG_PATH=/usr/lib64
```

#### Arch Linux
```bash
sudo pacman -S clang

export LIBCLANG_PATH=/usr/lib
```

### Issue: "error: linker `cc` not found"

**Solution:** Install build tools

```bash
# Ubuntu/Debian
sudo apt install -y build-essential

# Fedora/RHEL
sudo dnf groupinstall -y "Development Tools"

# Arch
sudo pacman -S base-devel
```

### Issue: "failed to run custom build command for `rocksdb`"

**Error:**
```
error: failed to run custom build command for `librocksdb-sys`
```

**Solution:** Install missing C++ build dependencies

```bash
# Ubuntu/Debian
sudo apt install -y g++ cmake libssl-dev

# Fedora/RHEL
sudo dnf install -y gcc-c++ cmake openssl-devel

# Arch
sudo pacman -S gcc cmake openssl
```

### Issue: SSL/TLS errors

**Error:**
```
error: failed to fetch `https://...`
```

**Solution:** Install OpenSSL development libraries

```bash
# Ubuntu/Debian
sudo apt install -y libssl-dev pkg-config

# Fedora/RHEL
sudo dnf install -y openssl-devel pkgconfig

# Arch
sudo pacman -S openssl pkg-config
```

### Issue: "error: failed to run custom build command for `arrow`"

**Solution:** Install additional build dependencies

```bash
# Ubuntu/Debian
sudo apt install -y cmake libssl-dev libclang-dev

# Fedora/RHEL
sudo dnf install -y cmake openssl-devel clang-devel

# Arch
sudo pacman -S cmake openssl clang
```

### Issue: Slow compilation times

**First Build:** 10-20 minutes is normal (compiling large C++ libraries from source)

**Speed up subsequent builds:**

```bash
# Use cargo check for faster error checking
cargo check

# Use faster linker (mold or lld)
# Ubuntu 22.04+:
sudo apt install mold

# Fedora:
sudo dnf install mold

# Arch:
sudo pacman -S mold

# Configure in ~/.cargo/config.toml:
mkdir -p ~/.cargo
cat > ~/.cargo/config.toml << EOF
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
EOF
```

### Issue: Out of memory during compilation

**Solution:** Limit parallel jobs

```bash
# Build with fewer parallel jobs
cargo build -j 2

# Or set globally in ~/.cargo/config.toml
mkdir -p ~/.cargo
cat > ~/.cargo/config.toml << EOF
[build]
jobs = 2
EOF
```

### Issue: "permission denied" errors

**Solution:** Fix file permissions

```bash
# If you cloned with sudo, fix ownership
sudo chown -R $USER:$USER ~/KalamDB

# Or reclone without sudo
cd ~
rm -rf KalamDB
git clone https://github.com/kalamstack/KalamDB.git
```

---

## Distribution-Specific Notes

### Ubuntu / Debian

- **LLVM Version:** Ubuntu 20.04 ships with LLVM 10, Ubuntu 22.04 with LLVM 14
- **Upgrade LLVM:** For latest version, use [LLVM APT repository](https://apt.llvm.org/)
- **WSL:** Works great on WSL2 (Windows Subsystem for Linux)

### Fedora / RHEL

- **SELinux:** May need to configure if enabled (`sudo setenforce 0` for testing)
- **Firewall:** Open port 8080 if testing from other machines: `sudo firewall-cmd --add-port=8080/tcp`

### Arch Linux

- **Rolling Release:** Usually has latest versions of all dependencies
- **AUR:** Consider `mold` from AUR for faster linking

### Pop!_OS / System76

- Fully compatible with Ubuntu instructions
- CUDA drivers pre-installed (not needed for KalamDB)

---

## Verification Checklist

After setup, verify everything works:

- [ ] Build tools installed: `gcc --version`
- [ ] LLVM installed: `clang --version`
- [ ] Rust installed: `cargo --version`
- [ ] CMake installed: `cmake --version`
- [ ] OpenSSL dev libs installed: `pkg-config --libs openssl`
- [ ] KalamDB builds: `cargo build`
- [ ] Tests pass: `cargo test`
- [ ] Server runs: `cargo run`

---

## Environment Variables Reference

Add these to your `~/.bashrc` or `~/.zshrc`:

```bash
# Rust environment (automatically added by rustup)
source $HOME/.cargo/env

# Optional: Rust logging level
export RUST_LOG=debug

# Optional: LIBCLANG_PATH (if build fails)
# Ubuntu/Debian:
export LIBCLANG_PATH=/usr/lib/llvm-14/lib
# Fedora/RHEL:
export LIBCLANG_PATH=/usr/lib64
# Arch:
export LIBCLANG_PATH=/usr/lib
```

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

## Performance Optimization

### Use System Libraries (Optional)

By default, KalamDB builds everything from source. You can use system libraries to speed up builds:

```bash
# Install system libraries
# Ubuntu/Debian:
sudo apt install -y librocksdb-dev

# Fedora:
sudo dnf install -y rocksdb-devel

# Note: May cause version compatibility issues
```

### Enable LTO (Link-Time Optimization)

For release builds, enable LTO in `Cargo.toml`:

```toml
[profile.release]
lto = true
codegen-units = 1
```

---

## Next Steps

- Review [Backend README](../README.md) for project structure
- Read [Architecture Documentation](../../specs/001-build-a-rust/)
- Check [API Documentation](../README.md#api-endpoints)
- See [Testing Strategy](testing-strategy.md)

---

**Linux Distributions Tested:**
- ✅ Ubuntu 22.04 LTS
- ✅ Ubuntu 24.04 LTS
- ✅ Fedora 38, 39, 40
- ✅ Debian 12 (Bookworm)
- ✅ Arch Linux (2024.10)
- ✅ Pop!_OS 22.04
- ✅ Rocky Linux 9

**Last Updated:** October 14, 2025  
**KalamDB Version:** 0.1.3
