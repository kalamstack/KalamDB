#!/usr/bin/env bash
set -euo pipefail

echo "🔨 Building KalamDB Dart SDK..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BRIDGE_DIR="$(cd "$SCRIPT_DIR/../../kalam-link-dart" && pwd)"
LINK_WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$SCRIPT_DIR"

TARGET_DIR="$(cargo metadata --manifest-path "$BRIDGE_DIR/Cargo.toml" --format-version=1 --no-deps 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['target_directory'])" 2>/dev/null \
  || echo "$BRIDGE_DIR/target")"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}ℹ️  $*${NC}"; }
ok()    { echo -e "${GREEN}✅ $*${NC}"; }
warn()  { echo -e "${YELLOW}⚠️  $*${NC}"; }
fail()  { echo -e "${RED}❌ $*${NC}" >&2; }

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

if [[ ${#PLATFORMS[@]} -eq 0 && -n "${BUILD_PLATFORMS:-}" ]]; then
  # shellcheck disable=SC2206
  PLATFORMS=(${BUILD_PLATFORMS})
fi

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

ensure_target() {
  local target="$1"
  if ! rustup target list --installed | grep -q "^${target}$"; then
    info "Adding Rust target $target..."
    rustup target add "$target"
  fi
}

build_android() {
  info "Building Android native libraries..."

  if ! cargo ndk --version >/dev/null 2>&1; then
    fail "cargo-ndk not found. Install with: cargo install cargo-ndk"
    return 1
  fi

  local jnilibs_dir="$SCRIPT_DIR/android/src/main/jniLibs"
  mkdir -p "$jnilibs_dir"

  abi_for() {
    case "$1" in
      aarch64-linux-android) echo "arm64-v8a" ;;
      armv7-linux-androideabi) echo "armeabi-v7a" ;;
      x86_64-linux-android) echo "x86_64" ;;
      i686-linux-android) echo "x86" ;;
    esac
  }

  local targets=(
    "aarch64-linux-android"
    "x86_64-linux-android"
  )
  if [[ "$ANDROID_ALL_ABIS" == "true" ]]; then
    targets+=(
      "armv7-linux-androideabi"
      "i686-linux-android"
    )
  fi

  local ndk_flags=()
  local target
  for target in "${targets[@]}"; do
    ensure_target "$target"
    ndk_flags+=( -t "$(abi_for "$target")" )
  done

  (cd "$BRIDGE_DIR" && RUSTC_WRAPPER="" cargo ndk "${ndk_flags[@]}" -o "$jnilibs_dir" -P 28 build --profile release-dist) || {
    fail "Android build failed"
    return 1
  }

  ok "Android: $(find "$jnilibs_dir" -name '*.so' | wc -l | tr -d ' ') .so files"
}

build_ios() {
  info "Building iOS static library..."

  ensure_target "aarch64-apple-ios"

  export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-16.0}"
  info "IPHONEOS_DEPLOYMENT_TARGET=$IPHONEOS_DEPLOYMENT_TARGET"

  (cd "$BRIDGE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target aarch64-apple-ios) || {
    fail "iOS aarch64 build failed"
    return 1
  }

  local ios_lib_dir="$SCRIPT_DIR/ios/Frameworks"
  mkdir -p "$ios_lib_dir"

  if rustup target list --installed | grep -q "aarch64-apple-ios-sim"; then
    (cd "$BRIDGE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target aarch64-apple-ios-sim) || {
      warn "iOS simulator build failed — shipping device-only .a"
    }
    if [[ -f "$TARGET_DIR/aarch64-apple-ios-sim/release-dist/libkalam_link_dart.a" ]]; then
      lipo -create \
        "$TARGET_DIR/aarch64-apple-ios/release-dist/libkalam_link_dart.a" \
        "$TARGET_DIR/aarch64-apple-ios-sim/release-dist/libkalam_link_dart.a" \
        -output "$ios_lib_dir/libkalam_link_dart.a" 2>/dev/null || \
      cp "$TARGET_DIR/aarch64-apple-ios/release-dist/libkalam_link_dart.a" "$ios_lib_dir/libkalam_link_dart.a"
    else
      cp "$TARGET_DIR/aarch64-apple-ios/release-dist/libkalam_link_dart.a" "$ios_lib_dir/libkalam_link_dart.a"
    fi
  else
    cp "$TARGET_DIR/aarch64-apple-ios/release-dist/libkalam_link_dart.a" "$ios_lib_dir/libkalam_link_dart.a"
    warn "iOS simulator target (aarch64-apple-ios-sim) not installed."
    warn "Add it with: rustup target add aarch64-apple-ios-sim"
  fi

  ok "iOS: $(du -sh "$ios_lib_dir/libkalam_link_dart.a" | cut -f1) static lib"
}

build_macos() {
  info "Building macOS dynamic library..."

  ensure_target "aarch64-apple-darwin"
  ensure_target "x86_64-apple-darwin"

  (cd "$BRIDGE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target aarch64-apple-darwin) || {
    fail "macOS aarch64 build failed"
    return 1
  }
  (cd "$BRIDGE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target x86_64-apple-darwin) || {
    fail "macOS x86_64 build failed"
    return 1
  }

  local macos_lib_dir="$SCRIPT_DIR/macos/Libs"
  mkdir -p "$macos_lib_dir"

  lipo -create \
    "$TARGET_DIR/aarch64-apple-darwin/release-dist/libkalam_link_dart.dylib" \
    "$TARGET_DIR/x86_64-apple-darwin/release-dist/libkalam_link_dart.dylib" \
    -output "$macos_lib_dir/libkalam_link_dart.dylib"

  install_name_tool -id "@rpath/libkalam_link_dart.dylib" "$macos_lib_dir/libkalam_link_dart.dylib" 2>/dev/null || true

  ok "macOS: $(du -sh "$macos_lib_dir/libkalam_link_dart.dylib" | cut -f1) universal dylib"
}

build_linux() {
  info "Building Linux shared library..."

  local linux_target="x86_64-unknown-linux-gnu"
  ensure_target "$linux_target"

  (cd "$BRIDGE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target "$linux_target") || {
    fail "Linux build failed"
    return 1
  }

  local linux_lib_dir="$SCRIPT_DIR/linux/lib"
  mkdir -p "$linux_lib_dir"
  cp "$TARGET_DIR/$linux_target/release-dist/libkalam_link_dart.so" "$linux_lib_dir/libkalam_link_dart.so"

  ok "Linux: $(du -sh "$linux_lib_dir/libkalam_link_dart.so" | cut -f1) .so"
}

build_windows() {
  info "Building Windows DLL..."

  local win_target="x86_64-pc-windows-gnu"
  ensure_target "$win_target"

  (cd "$BRIDGE_DIR" && RUSTC_WRAPPER="" cargo build --profile release-dist --target "$win_target") || {
    fail "Windows build failed"
    return 1
  }

  local win_lib_dir="$SCRIPT_DIR/windows/lib"
  mkdir -p "$win_lib_dir"
  cp "$TARGET_DIR/$win_target/release-dist/kalam_link_dart.dll" "$win_lib_dir/kalam_link_dart.dll"

  ok "Windows: $(du -sh "$win_lib_dir/kalam_link_dart.dll" | cut -f1) .dll"
}

build_web() {
  info "Building Web WASM module..."

  if ! command -v wasm-pack >/dev/null 2>&1; then
    fail "wasm-pack not found. Install with: cargo install wasm-pack"
    return 1
  fi

  ensure_target "wasm32-unknown-unknown"

  local web_out_dir="$SCRIPT_DIR/web/pkg"
  mkdir -p "$web_out_dir"

  (
    cd "$LINK_WORKSPACE_DIR"
    RUSTC_WRAPPER="" wasm-pack build kalam-client \
      --target web \
      --out-dir "$web_out_dir" \
      --out-name kalam_link_dart \
      --no-opt \
      --profile release-dist \
      --features wasm \
      --no-default-features
  ) || {
    fail "Web WASM build failed"
    return 1
  }

  if command -v wasm-opt >/dev/null 2>&1; then
    info "Optimizing web WASM with wasm-opt..."
    wasm-opt -Oz --all-features -o "$web_out_dir/kalam_link_dart_bg.wasm" "$web_out_dir/kalam_link_dart_bg.wasm" || {
      fail "Web WASM optimization failed"
      return 1
    }
  else
    warn "wasm-opt not found; skipping post-build size optimization"
  fi

  ok "Web: $(find "$web_out_dir" -name '*.wasm' -exec du -sh {} \; | cut -f1) WASM module"
}

echo "📦 Fetching Dart/Flutter dependencies..."
flutter pub get

if [[ -f "$BRIDGE_DIR/flutter_rust_bridge.yaml" ]]; then
  if command -v flutter_rust_bridge_codegen >/dev/null 2>&1; then
    GENERATED_DIR="$SCRIPT_DIR/lib/src/generated"
    STAMP_FILE="$GENERATED_DIR/.frb_codegen.stamp"
    GENERATE_MODE="${FRB_GENERATE:-never}"

    should_generate=false
    if [[ "$GENERATE_MODE" == "always" ]]; then
      should_generate=true
    elif [[ "$GENERATE_MODE" == "never" ]]; then
      should_generate=false
    else
      if [[ ! -f "$STAMP_FILE" ]]; then
        should_generate=true
      elif [[ "$BRIDGE_DIR/flutter_rust_bridge.yaml" -nt "$STAMP_FILE" ]]; then
        should_generate=true
      elif find "$BRIDGE_DIR/src" -type f -newer "$STAMP_FILE" | grep -q .; then
        should_generate=true
      fi
    fi

    if [[ "$should_generate" == "true" ]]; then
      echo "🧬 Generating flutter_rust_bridge bindings..."
      (
        cd "$BRIDGE_DIR"
        flutter_rust_bridge_codegen generate
      )
      touch "$STAMP_FILE"
    else
      echo "🧬 Skipping flutter_rust_bridge generation (disabled or up-to-date)."
      echo "   Override with FRB_GENERATE=always to force regeneration."
    fi
  else
    echo "⚠️ flutter_rust_bridge_codegen not found; skipping binding generation."
    echo "   Install with: dart pub global activate flutter_rust_bridge"
  fi
fi

echo ""
echo "════════════════════════════════════════════════════════════════"
echo " KalamDB Dart SDK — Build + Native Artifact Preparation"
echo " Bridge Crate: $BRIDGE_DIR"
echo " Platforms: ${PLATFORMS[*]}"
echo "════════════════════════════════════════════════════════════════"
echo ""

FAILED=()
for PLATFORM in "${PLATFORMS[@]}"; do
  case "$PLATFORM" in
    android) build_android || FAILED+=("android") ;;
    ios) build_ios || FAILED+=("ios") ;;
    macos) build_macos || FAILED+=("macos") ;;
    linux) build_linux || FAILED+=("linux") ;;
    windows) build_windows || FAILED+=("windows") ;;
    web) build_web || FAILED+=("web") ;;
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
fi

ok "Native artefacts prepared successfully!"

echo "🦀 Building host native library for Dart VM / Flutter desktop..."
(
  cd "$BRIDGE_DIR"
  CARGO_TARGET_DIR="$BRIDGE_DIR/target" cargo build --release
)

echo "🔍 Running static analysis..."
flutter analyze

echo "✅ Dart SDK build complete"
