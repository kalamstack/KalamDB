#!/usr/bin/env bash
# build_native_libs.sh
# Cross-compile the kalam-link-dart Rust crate for every Flutter platform and
# place the resulting artefacts in the correct platform directories so they
# are bundled with the published package.
#
# Usage:
#   ./build_native_libs.sh                 # build all platforms detectable on this host
#   ./build_native_libs.sh android         # Android only (arm64 + x86_64)
#   ./build_native_libs.sh android --all-abis  # Android all four ABIs
#   ./build_native_libs.sh ios             # iOS only (aarch64, fat .a via lipo)
#   ./build_native_libs.sh macos           # macOS only (universal binary)
#   ./build_native_libs.sh linux           # Linux x86_64
#   ./build_native_libs.sh windows         # Windows x86_64 (cross-compile)
#   ./build_native_libs.sh web             # WASM via wasm-pack
#   ./build_native_libs.sh android ios web # multiple specific platforms
#
# Prerequisites:
#   Rust stable, cargo-ndk (Android), wasm-pack (Web), Xcode (iOS/macOS)
#   See DEV.md for full details.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/../../kalam-link-dart" && pwd)"
LINK_CRATE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Resolve the Cargo target directory (workspace root target dir).
TARGET_DIR="$(cd "$CRATE_DIR" && cargo metadata --format-version=1 --no-deps 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['target_directory'])" 2>/dev/null \
  || echo "$CRATE_DIR/target")"

# ── Colour helpers ────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}ℹ️  $*${NC}"; }
ok()    { echo -e "${GREEN}✅ $*${NC}"; }
warn()  { echo -e "${YELLOW}⚠️  $*${NC}"; }
fail()  { echo -e "${RED}❌ $*${NC}" >&2; }

# ── Parse arguments ───────────────────────────────────────────────────────
PLATFORMS=()
ANDROID_ALL_ABIS=false

for arg in "$@"; do
  case "$arg" in
    --all-abis) ANDROID_ALL_ABIS=true ;;
    android|ios|macos|linux|windows|web) PLATFORMS+=("$arg") ;;
    all) PLATFORMS=(android ios macos linux windows web) ;;
    *) fail "Unknown argument: $arg"; exit 1 ;;
  esac
done

# If no platforms specified, auto-detect from the current OS.
if [[ ${#PLATFORMS[@]} -eq 0 ]]; then
  case "$(uname -s)" in
    Darwin)
      PLATFORMS=(android ios web)
      info "macOS detected — building android, ios, web"
      ;;
    Linux)
      PLATFORMS=(android linux web)
      info "Linux detected — building android, linux, web"
      ;;
    MINGW*|MSYS*|CYGWIN*)
      PLATFORMS=(windows web)
      info "Windows detected — building windows, web"
      ;;
    *)
      fail "Unknown OS: $(uname -s). Specify platforms explicitly."
      exit 1
      ;;
  esac
fi

# ── Helper: ensure Rust targets are installed ─────────────────────────────
ensure_target() {
  local target="$1"
  if ! rustup target list --installed | grep -q "^${target}$"; then
    info "Adding Rust target $target..."
    rustup target add "$target"
  fi
}

# ═══════════════════════════════════════════════════════════════════════════
# ANDROID
# ═══════════════════════════════════════════════════════════════════════════
build_android() {
  info "Building Android native libraries..."

  if ! cargo ndk --version >/dev/null 2>&1; then
    fail "cargo-ndk not found. Install with: cargo install cargo-ndk"
    return 1
  fi

  local JNILIBS_DIR="$SCRIPT_DIR/android/src/main/jniLibs"
  mkdir -p "$JNILIBS_DIR"

  # Map Rust target triple → Android ABI directory name
  abi_for() {
    case "$1" in
      aarch64-linux-android)    echo "arm64-v8a" ;;
      armv7-linux-androideabi)  echo "armeabi-v7a" ;;
      x86_64-linux-android)    echo "x86_64" ;;
      i686-linux-android)      echo "x86" ;;
    esac
  }

  local TARGETS=(
    "aarch64-linux-android"
    "x86_64-linux-android"
  )
  if [[ "$ANDROID_ALL_ABIS" == "true" ]]; then
    TARGETS+=(
      "armv7-linux-androideabi"
      "i686-linux-android"
    )
  fi

  local NDK_FLAGS=()
  for TARGET in "${TARGETS[@]}"; do
    ensure_target "$TARGET"
    NDK_FLAGS+=(-t "$(abi_for "$TARGET")")
  done

  # API level 28+ required for getentropy() used by aws-lc-sys
  # --profile release-dist: opt-level=z, LTO, strip, panic=abort (smallest binary)
  (cd "$CRATE_DIR" && RUSTC_WRAPPER="" cargo ndk "${NDK_FLAGS[@]}" -o "$JNILIBS_DIR" -P 28 build --profile release-dist) || {
    fail "Android build failed"; return 1
  }

  ok "Android: $(find "$JNILIBS_DIR" -name '*.so' | wc -l | tr -d ' ') .so files"
}

# ═══════════════════════════════════════════════════════════════════════════
# iOS
# ═══════════════════════════════════════════════════════════════════════════
build_ios() {
  info "Building iOS static library..."

  ensure_target "aarch64-apple-ios"

  # aws-lc-sys requires a minimum deployment target of 16.0 for the
  # ___chkstk_darwin symbol to be available.
  export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-16.0}"
  info "IPHONEOS_DEPLOYMENT_TARGET=$IPHONEOS_DEPLOYMENT_TARGET"

  (cd "$CRATE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target aarch64-apple-ios) || {
    fail "iOS aarch64 build failed"; return 1
  }

  local IOS_LIB_DIR="$SCRIPT_DIR/ios/Frameworks"
  mkdir -p "$IOS_LIB_DIR"

  # For simulator we also need aarch64-apple-ios-sim
  if rustup target list --installed | grep -q "aarch64-apple-ios-sim"; then
    (cd "$CRATE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target aarch64-apple-ios-sim) || {
      warn "iOS simulator build failed — shipping device-only .a"
    }
    if [[ -f "$TARGET_DIR/aarch64-apple-ios-sim/release-dist/libkalam_link_dart.a" ]]; then
      # Create a fat/universal static library via lipo
      lipo -create \
        "$TARGET_DIR/aarch64-apple-ios/release-dist/libkalam_link_dart.a" \
        "$TARGET_DIR/aarch64-apple-ios-sim/release-dist/libkalam_link_dart.a" \
        -output "$IOS_LIB_DIR/libkalam_link_dart.a" 2>/dev/null || \
      cp "$TARGET_DIR/aarch64-apple-ios/release-dist/libkalam_link_dart.a" \
         "$IOS_LIB_DIR/libkalam_link_dart.a"
    else
      cp "$TARGET_DIR/aarch64-apple-ios/release-dist/libkalam_link_dart.a" \
         "$IOS_LIB_DIR/libkalam_link_dart.a"
    fi
  else
    cp "$TARGET_DIR/aarch64-apple-ios/release-dist/libkalam_link_dart.a" \
       "$IOS_LIB_DIR/libkalam_link_dart.a"
    warn "iOS simulator target (aarch64-apple-ios-sim) not installed."
    warn "Add it with: rustup target add aarch64-apple-ios-sim"
  fi

  ok "iOS: $(du -sh "$IOS_LIB_DIR/libkalam_link_dart.a" | cut -f1) static lib"
}

# ═══════════════════════════════════════════════════════════════════════════
# macOS
# ═══════════════════════════════════════════════════════════════════════════
build_macos() {
  info "Building macOS dynamic library..."

  ensure_target "aarch64-apple-darwin"
  ensure_target "x86_64-apple-darwin"

  (cd "$CRATE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target aarch64-apple-darwin) || {
    fail "macOS aarch64 build failed"; return 1
  }
  (cd "$CRATE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target x86_64-apple-darwin) || {
    fail "macOS x86_64 build failed"; return 1
  }

  local MACOS_LIB_DIR="$SCRIPT_DIR/macos/Libs"
  mkdir -p "$MACOS_LIB_DIR"

  # Universal binary via lipo
  lipo -create \
    "$TARGET_DIR/aarch64-apple-darwin/release-dist/libkalam_link_dart.dylib" \
    "$TARGET_DIR/x86_64-apple-darwin/release-dist/libkalam_link_dart.dylib" \
    -output "$MACOS_LIB_DIR/libkalam_link_dart.dylib"

  # Fix install name for macOS dylib
  install_name_tool -id "@rpath/libkalam_link_dart.dylib" \
    "$MACOS_LIB_DIR/libkalam_link_dart.dylib" 2>/dev/null || true

  ok "macOS: $(du -sh "$MACOS_LIB_DIR/libkalam_link_dart.dylib" | cut -f1) universal dylib"
}

# ═══════════════════════════════════════════════════════════════════════════
# Linux
# ═══════════════════════════════════════════════════════════════════════════
build_linux() {
  info "Building Linux shared library..."

  local LINUX_TARGET="x86_64-unknown-linux-gnu"
  ensure_target "$LINUX_TARGET"

  (cd "$CRATE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target "$LINUX_TARGET") || {
    fail "Linux build failed"; return 1
  }

  local LINUX_LIB_DIR="$SCRIPT_DIR/linux/lib"
  mkdir -p "$LINUX_LIB_DIR"
  cp "$TARGET_DIR/$LINUX_TARGET/release-dist/libkalam_link_dart.so" \
     "$LINUX_LIB_DIR/libkalam_link_dart.so"

  ok "Linux: $(du -sh "$LINUX_LIB_DIR/libkalam_link_dart.so" | cut -f1) .so"
}

# ═══════════════════════════════════════════════════════════════════════════
# Windows
# ═══════════════════════════════════════════════════════════════════════════
build_windows() {
  info "Building Windows DLL..."

  local WIN_TARGET="x86_64-pc-windows-gnu"
  ensure_target "$WIN_TARGET"

  (cd "$CRATE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target "$WIN_TARGET") || {
    fail "Windows build failed"; return 1
  }

  local WIN_LIB_DIR="$SCRIPT_DIR/windows/lib"
  mkdir -p "$WIN_LIB_DIR"
  cp "$TARGET_DIR/$WIN_TARGET/release-dist/kalam_link_dart.dll" \
     "$WIN_LIB_DIR/kalam_link_dart.dll"

  ok "Windows: $(du -sh "$WIN_LIB_DIR/kalam_link_dart.dll" | cut -f1) .dll"
}

# ═══════════════════════════════════════════════════════════════════════════
# Web (WASM)
# ═══════════════════════════════════════════════════════════════════════════
build_web() {
  info "Building Web WASM module..."

  if ! command -v wasm-pack >/dev/null 2>&1; then
    fail "wasm-pack not found. Install with: cargo install wasm-pack"
    return 1
  fi

  ensure_target "wasm32-unknown-unknown"

  # For web, we build the kalam-link crate (not the bridge crate) with the wasm
  # feature. The bridge crate pulls in tokio which is incompatible with WASM.
  # flutter_rust_bridge handles the web FFI layer; this WASM build is used by
  # its web runtime as the backing module.
  local WEB_OUT_DIR="$SCRIPT_DIR/web/pkg"
  mkdir -p "$WEB_OUT_DIR"

  (cd "$LINK_CRATE_DIR" && RUSTC_WRAPPER="" wasm-pack build \
    --profile release-dist \
    --no-opt \
    --target web \
    --out-dir "$WEB_OUT_DIR" \
    --out-name kalam_link_dart \
    --features wasm \
    --no-default-features) || {
    fail "Web WASM build failed"; return 1
  }

  if command -v wasm-opt >/dev/null 2>&1; then
    info "Optimizing web WASM with wasm-opt..."
    wasm-opt -Oz --all-features -o "$WEB_OUT_DIR/kalam_link_dart_bg.wasm" "$WEB_OUT_DIR/kalam_link_dart_bg.wasm" || {
      fail "Web WASM optimization failed"; return 1
    }
  else
    warn "wasm-opt not found; skipping post-build size optimization"
  fi

  ok "Web: $(find "$WEB_OUT_DIR" -name '*.wasm' -exec du -sh {} \; | cut -f1) WASM module"
}

# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════════════════════════════"
echo " KalamDB Dart SDK — Native Library Builder"
echo " Crate: $CRATE_DIR"
echo " Platforms: ${PLATFORMS[*]}"
echo "════════════════════════════════════════════════════════════════"
echo ""

FAILED=()

for PLATFORM in "${PLATFORMS[@]}"; do
  case "$PLATFORM" in
    android) build_android  || FAILED+=("android") ;;
    ios)     build_ios      || FAILED+=("ios") ;;
    macos)   build_macos    || FAILED+=("macos") ;;
    linux)   build_linux    || FAILED+=("linux") ;;
    windows) build_windows  || FAILED+=("windows") ;;
    web)     build_web      || FAILED+=("web") ;;
  esac
  echo ""
done

echo "════════════════════════════════════════════════════════════════"
if [[ ${#FAILED[@]} -gt 0 ]]; then
  fail "Some platforms failed: ${FAILED[*]}"
  echo ""
  echo "Common fixes:"
  echo "  Android:  cargo install cargo-ndk && export ANDROID_NDK_HOME=..."
  echo "  iOS:      rustup target add aarch64-apple-ios aarch64-apple-ios-sim"
  echo "  macOS:    rustup target add aarch64-apple-darwin x86_64-apple-darwin"
  echo "  Linux:    rustup target add x86_64-unknown-linux-gnu"
  echo "  Windows:  rustup target add x86_64-pc-windows-gnu"
  echo "  Web:      cargo install wasm-pack"
  echo ""
  exit 1
else
  ok "All platforms built successfully!"
  echo ""
  echo "Next steps:"
  echo "  1. git add android/ ios/ macos/ linux/ windows/ web/"
  echo "  2. git commit -m 'chore(dart): rebuild native libs for all platforms'"
  echo "  3. ./publish.sh --dry-run"
fi
