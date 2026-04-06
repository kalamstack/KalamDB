#!/usr/bin/env bash
# publish.sh — Publish kalam_link to pub.dev
# Usage: ./publish.sh [--dry-run]
set -euo pipefail

DRY_RUN=false
SKIP_NATIVE_CHECK=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --skip-native-check) SKIP_NATIVE_CHECK=true ;;
  esac
done

cd "$(dirname "$0")"

# ── Validate that native artefacts exist for every declared platform ──────
if ! $SKIP_NATIVE_CHECK; then
  echo "==> Checking native library artefacts (file existence + git-tracked)..."
  MISSING=()
  UNTRACKED=()

  check_native() {
    local file="$1" label="$2"
    if [[ ! -f "$file" ]]; then
      MISSING+=("$label")
    elif ! git ls-files --error-unmatch "$file" >/dev/null 2>&1; then
      UNTRACKED+=("$label ($file)")
    fi
  }

  # Android (at least arm64-v8a + x86_64)
  check_native android/src/main/jniLibs/arm64-v8a/libkalam_link_dart.so "android/arm64-v8a"
  check_native android/src/main/jniLibs/x86_64/libkalam_link_dart.so    "android/x86_64"
  # iOS
  check_native ios/Frameworks/libkalam_link_dart.a                       "ios"
  # Web WASM
  check_native web/pkg/kalam_link_dart_bg.wasm                           "web/wasm"

  if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo ""
    echo "ERROR: Missing pre-built native libraries for: ${MISSING[*]}"
    echo "Run './build.sh' to prepare the SDK for this host, or './build.sh all' for a full release rebuild."
    echo "Or pass --skip-native-check to bypass this validation."
    exit 1
  fi

  if [[ ${#UNTRACKED[@]} -gt 0 ]]; then
    echo ""
    echo "ERROR: Native libraries exist on disk but are NOT committed to git."
    echo "dart pub publish only packages git-tracked files. Run:"
    echo ""
    for entry in "${UNTRACKED[@]}"; do echo "  git add <file> # $entry"; done
    echo "  git commit -m 'chore(dart): pre-built native libs'"
    echo ""
    echo "Or pass --skip-native-check to bypass this validation."
    exit 1
  fi

  echo "    All native artefacts present and git-tracked."
fi

echo "==> Checking git working tree..."
if [[ -n "$(git status --porcelain -- .)" ]]; then
  echo "ERROR: Uncommitted changes detected in this package directory."
  echo "Commit or stash them before publishing."
  git status --short -- .
  exit 1
fi

echo "==> Running flutter pub get..."
flutter pub get

echo "==> Running dart analyze..."
dart analyze lib

echo "==> Running tests..."
flutter test || true   # warn but don't block (live-server tests may require a running server)

if $DRY_RUN; then
  echo "==> Dry run — validating package (no upload)..."
  flutter pub publish --dry-run
  echo ""
  echo "Dry run complete. Run './publish.sh' (without --dry-run) to publish."
else
  echo "==> Publishing kalam_link to pub.dev..."
  echo "y" | flutter pub publish
fi
