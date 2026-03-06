#!/usr/bin/env bash
# publish.sh - Publish the kalam-link TypeScript SDK to npm
#
# Usage:
#   ./publish.sh [OPTIONS]
#
# Options:
#   --force            Force republish: unpublish existing version first (only works within 72h)
#   --version VERSION  Override version (default: read from root Cargo.toml)
#   --skip-build       Skip the build step (use existing dist/)
#   --dry-run          Run everything except the actual npm publish
#   --otp CODE         One-time password for 2FA-protected accounts
#
# Environment:
#   NODE_AUTH_TOKEN    npm auth token (required for publishing; skipped in --dry-run)
#
# Example (local):
#   NPM_TOKEN=npm_xxx NODE_AUTH_TOKEN=$NPM_TOKEN ./publish.sh
#
# Example (force republish):
#   NODE_AUTH_TOKEN=$NPM_TOKEN ./publish.sh --force

set -euo pipefail

# ─── Resolve the script's own directory and repo root ────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SDK_DIR="$SCRIPT_DIR"

# ─── Load .env early (before everything else) ────────────────────────────────
if [[ -z "${NODE_AUTH_TOKEN:-}" ]]; then
  ENV_FILE="$SDK_DIR/.env"
  if [[ -f "$ENV_FILE" ]]; then
    NODE_AUTH_TOKEN="$(grep -E '^NODE_AUTH_TOKEN=' "$ENV_FILE" | head -n1 | cut -d'=' -f2- | tr -d '[:space:]')"
    [[ -n "$NODE_AUTH_TOKEN" ]] && echo "🔑 Loaded NODE_AUTH_TOKEN from .env"
  fi
fi
export NODE_AUTH_TOKEN

# ─── Defaults ────────────────────────────────────────────────────────────────
FORCE_PUBLISH=false
SKIP_BUILD=false
DRY_RUN=false
VERSION_OVERRIDE=""
OTP_CODE=""

# ─── Parse arguments ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE_PUBLISH=true
      shift
      ;;
    --version)
      VERSION_OVERRIDE="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --otp)
      OTP_CODE="$2"
      shift 2
      ;;
    *)
      echo "❌ Unknown option: $1"
      echo "Usage: $0 [--force] [--version VERSION] [--skip-build] [--dry-run]"
      exit 1
      ;;
  esac
done

# ─── Read version ────────────────────────────────────────────────────────────
if [[ -n "$VERSION_OVERRIDE" ]]; then
  VERSION="$VERSION_OVERRIDE"
  echo "📌 Using overridden version: $VERSION"
else
  CARGO_TOML="$REPO_ROOT/Cargo.toml"
  if [[ ! -f "$CARGO_TOML" ]]; then
    echo "❌ Could not find root Cargo.toml at: $CARGO_TOML"
    exit 1
  fi
  VERSION="$(awk -F'"' '/^\[workspace\.package\]/{found=1} found && /^version[[:space:]]*=/{print $2; exit}' "$CARGO_TOML")"
  if [[ -z "$VERSION" ]]; then
    echo "❌ Failed to read [workspace.package].version from $CARGO_TOML"
    exit 1
  fi
  echo "📋 Version read from Cargo.toml: $VERSION"
fi

echo ""
echo "══════════════════════════════════════════════════════"
echo "  kalam-link npm publish"
echo "  Version  : $VERSION"
echo "  Force    : $FORCE_PUBLISH"
echo "  Dry-run  : $DRY_RUN"
echo "  Skip-build: $SKIP_BUILD"
echo "══════════════════════════════════════════════════════"
echo ""

cd "$SDK_DIR"

# ─── Build ────────────────────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" == "false" ]]; then
  echo "📦 Installing npm dependencies..."
  npm ci 2>/dev/null || npm install

  echo "🔨 Building SDK..."
  npm run build

  echo "✅ Build complete"
  ls -la dist/
else
  echo "⏭️  Skipping build (--skip-build)"
  if [[ ! -d "$SDK_DIR/dist" ]]; then
    echo "❌ dist/ directory not found. Run without --skip-build first."
    exit 1
  fi
fi

# ─── Update package.json version ─────────────────────────────────────────────
echo ""
echo "📝 Updating package.json version to $VERSION..."
npm version "$VERSION" --no-git-tag-version --allow-same-version
echo "   $(grep '"version"' package.json | head -n1 | xargs)"

# ─── Determine npm dist-tag for pre-release versions ─────────────────────────
# npm requires --tag for pre-release versions (anything with a hyphen)
# e.g. 0.4.0-alpha3 → --tag alpha | 1.0.0-beta.1 → --tag beta | 1.0.0 → latest
NPM_TAG_FLAG=""
if [[ "$VERSION" == *"-"* ]]; then
  PRERELEASE_LABEL="$(echo "$VERSION" | sed 's/^[^-]*-//' | sed 's/[.0-9]*$//' | tr -d '[:digit:]')"
  # Fallback to "next" if label is empty (e.g. purely numeric pre-release)
  PRERELEASE_LABEL="${PRERELEASE_LABEL:-next}"
  NPM_TAG_FLAG="--tag $PRERELEASE_LABEL"
  echo "🏷️  Pre-release version detected — using dist-tag: $PRERELEASE_LABEL"
fi

# ─── Dry-run early exit ───────────────────────────────────────────────────────
if [[ "$DRY_RUN" == "true" ]]; then
  echo ""
  echo "🔍 Dry-run mode: skipping actual publish."
  echo "   Would publish: kalam-link@$VERSION${NPM_TAG_FLAG:+ ($NPM_TAG_FLAG)}"
  # shellcheck disable=SC2086
  npm publish --access public $NPM_TAG_FLAG --dry-run --ignore-scripts
  exit 0
fi

# ─── Validate auth token ─────────────────────────────────────────────────────
if [[ -z "${NODE_AUTH_TOKEN:-}" ]]; then
  echo ""
  echo "❌ NODE_AUTH_TOKEN is not set."
  echo "   Either export it or add it to $SDK_DIR/.env:"
  echo "     NODE_AUTH_TOKEN=npm_xxxxxxxx"
  exit 1
fi

# ─── Write a local .npmrc with the auth token (mirrors what CI setup-node does)
# Use a trap to always remove it on exit so the token isn't left on disk.
LOCAL_NPMRC="$SDK_DIR/.npmrc"
cleanup_npmrc() { rm -f "$LOCAL_NPMRC"; }
trap cleanup_npmrc EXIT
npm config set "//registry.npmjs.org/:_authToken" "${NODE_AUTH_TOKEN}" --location=project

# ─── Publish ──────────────────────────────────────────────────────────────────
echo ""
echo "🚀 Publishing kalam-link@$VERSION to npm..."
# --skip-build means the user has pre-built dist/; skip npm lifecycle scripts so
# prepublishOnly doesn't re-run the full build (including wasm-pack).
PUBLISH_SCRIPTS_FLAG=""
if [[ "$SKIP_BUILD" == "true" ]]; then
  PUBLISH_SCRIPTS_FLAG="--ignore-scripts"
fi

# OTP flag for 2FA-protected accounts
OTP_FLAG=""
if [[ -n "$OTP_CODE" ]]; then
  OTP_FLAG="--otp $OTP_CODE"
fi
# Check existence via the registry REST API (no auth needed for public packages)
HTTP_STATUS="$(curl -s -o /dev/null -w '%{http_code}' "https://registry.npmjs.org/kalam-link/$VERSION")"
if [[ "$HTTP_STATUS" == "200" ]]; then
  if [[ "$FORCE_PUBLISH" == "true" ]]; then
    echo "⚠️  Version $VERSION exists. Force publish enabled — attempting to unpublish..."
    if npm unpublish "kalam-link@$VERSION" --force 2>/dev/null; then
      echo "✅ Successfully unpublished kalam-link@$VERSION"
      # shellcheck disable=SC2086
      npm publish --access public $NPM_TAG_FLAG $PUBLISH_SCRIPTS_FLAG $OTP_FLAG
      echo "✅ Successfully republished kalam-link@$VERSION to npm!"
    else
      echo "❌ Failed to unpublish (version may be >72 hours old)"
      echo "💡 Tip: npm doesn't allow unpublishing after 72 hours."
      echo "    Use a different version number (e.g., patch bump in Cargo.toml)."
      exit 1
    fi
  else
    echo "⚠️  Version $VERSION already exists on npm — skipping publish."
    echo "💡 To force republish, run with: --force"
    exit 0
  fi
else
  # shellcheck disable=SC2086
  # shellcheck disable=SC2086
  if npm publish --access public $NPM_TAG_FLAG $PUBLISH_SCRIPTS_FLAG $OTP_FLAG 2>&1; then
    echo "✅ Successfully published kalam-link@$VERSION to npm!"
  else
    NPM_PUBLISH_EXIT=$?
    echo ""
    echo "❌ npm publish failed (exit $NPM_PUBLISH_EXIT)"
    echo "   Common causes:"
    echo "   • 2FA required: pass --otp <code> for your authenticator app."
    echo "     → For CI/CD, use an npm 'automation' token (bypasses 2FA)."
    echo "   • Version was previously published and unpublished — npm permanently blocks republishing the same version."
    echo "     → Bump the version in root Cargo.toml and try again."
    echo "   • Auth token expired or lacks publish permissions."
    echo "     → Regenerate your npm token and update .env"
    exit $NPM_PUBLISH_EXIT
  fi
fi
