#!/bin/bash
set -euo pipefail

echo "🔨 Building KalamDB Dart SDK..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# Native libraries (all platforms)
# ---------------------------------------------------------------------------
# The Flutter plugin ships pre-built native binaries for every platform:
#   android/ — .so (arm64-v8a, x86_64, optionally armeabi-v7a + x86)
#   ios/     — static .a (aarch64, optionally universal with sim target)
#   macos/   — universal .dylib (aarch64 + x86_64)
#   linux/   — .so (x86_64)
#   windows/ — .dll (x86_64)
#   web/     — .wasm via wasm-pack
#
# Run build_native_libs.sh whenever the Rust source changes or when
# preparing a new pub.dev release:
#
#   ./build_native_libs.sh                   # auto-detect platforms for this OS
#   ./build_native_libs.sh all               # all platforms
#   ./build_native_libs.sh android ios web   # specific platforms
#   ./build_native_libs.sh android --all-abis  # all four Android ABIs
#
# By default build.sh skips native compilation to avoid requiring cross-compile
# toolchains.  Set BUILD_NATIVE=1 (or BUILD_ANDROID=1 for back-compat) to
# trigger it:
#
#   BUILD_NATIVE=1 ./build.sh
#   BUILD_NATIVE=1 BUILD_PLATFORMS="android ios web" ./build.sh
#
if [[ "${BUILD_NATIVE:-${BUILD_ANDROID:-0}}" == "1" ]]; then
  PLATFORM_ARGS="${BUILD_PLATFORMS:-}"
  echo "🏗️  Building native libraries..."
  # shellcheck disable=SC2086
  "$SCRIPT_DIR/build_native_libs.sh" $PLATFORM_ARGS
fi

echo "📦 Fetching Dart/Flutter dependencies..."
flutter pub get

# Generate flutter_rust_bridge bindings when the bridge config exists.
BRIDGE_DIR="$SCRIPT_DIR/../../kalam-link-dart"
if [ -f "$BRIDGE_DIR/flutter_rust_bridge.yaml" ]; then
  if command -v flutter_rust_bridge_codegen >/dev/null 2>&1; then
    GENERATED_DIR="$SCRIPT_DIR/lib/src/generated"
    STAMP_FILE="$GENERATED_DIR/.frb_codegen.stamp"
    GENERATE_MODE="${FRB_GENERATE:-never}"

    should_generate=false
    if [ "$GENERATE_MODE" = "always" ]; then
      should_generate=true
    elif [ "$GENERATE_MODE" = "never" ]; then
      should_generate=false
    else
      if [ ! -f "$STAMP_FILE" ]; then
        should_generate=true
      elif [ "$BRIDGE_DIR/flutter_rust_bridge.yaml" -nt "$STAMP_FILE" ]; then
        should_generate=true
      elif find "$BRIDGE_DIR/src" -type f -newer "$STAMP_FILE" | grep -q .; then
        should_generate=true
      fi
    fi

    if [ "$should_generate" = true ]; then
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

echo "🦀 Building host native library for Dart VM / Flutter desktop..."
(
  cd "$BRIDGE_DIR"
  CARGO_TARGET_DIR="$BRIDGE_DIR/target" cargo build --release
)

echo "🔍 Running static analysis..."
flutter analyze

echo "✅ Dart SDK build complete"
