#!/usr/bin/env bash
# publish.sh - Publish the @kalamdb/consumer TypeScript SDK to npm

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SDK_DIR="$SCRIPT_DIR"

if [[ -z "${NODE_AUTH_TOKEN:-}" ]]; then
  ENV_FILE="$SDK_DIR/.env"
  if [[ -f "$ENV_FILE" ]]; then
    NODE_AUTH_TOKEN="$(grep -E '^NODE_AUTH_TOKEN=' "$ENV_FILE" | head -n1 | cut -d'=' -f2- | tr -d '[:space:]')"
    [[ -n "$NODE_AUTH_TOKEN" ]] && echo "🔑 Loaded NODE_AUTH_TOKEN from .env"
  fi
fi
export NODE_AUTH_TOKEN

FORCE_PUBLISH=false
SKIP_BUILD=false
DRY_RUN=false
VERSION_OVERRIDE=""
OTP_CODE=""

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
      echo "Usage: $0 [--force] [--version VERSION] [--skip-build] [--dry-run] [--otp CODE]"
      exit 1
      ;;
  esac
done

PACKAGE_JSON="$SDK_DIR/package.json"
if [[ ! -f "$PACKAGE_JSON" ]]; then
  echo "❌ Could not find package.json at: $PACKAGE_JSON"
  exit 1
fi

PACKAGE_NAME="$(node -p "JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8')).name" "$PACKAGE_JSON")"
if [[ -n "$VERSION_OVERRIDE" ]]; then
  VERSION="$VERSION_OVERRIDE"
  echo "📌 Using overridden version: $VERSION"
else
  VERSION="$(node -p "JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8')).version" "$PACKAGE_JSON")"
  echo "📋 Version read from package.json: $VERSION"
fi
PACKAGE_REGISTRY_URL="https://registry.npmjs.org/${PACKAGE_NAME}"
PACKAGE_NPM_PAGE_URL="https://www.npmjs.com/package/${PACKAGE_NAME}"
CLIENT_PACKAGE_NAME="$(node -p "Object.keys(JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8')).peerDependencies ?? {})[0] || ''" "$PACKAGE_JSON")"
if [[ -z "$CLIENT_PACKAGE_NAME" ]]; then
  echo "❌ Could not determine the required app-facing client package from peerDependencies."
  exit 1
fi
CLIENT_REGISTRY_URL="https://registry.npmjs.org/${CLIENT_PACKAGE_NAME}"

echo ""
echo "══════════════════════════════════════════════════════"
echo "  $PACKAGE_NAME npm publish"
echo "  Version   : $VERSION"
echo "  Force     : $FORCE_PUBLISH"
echo "  Dry-run   : $DRY_RUN"
echo "  Skip-build: $SKIP_BUILD"
echo "══════════════════════════════════════════════════════"
echo ""

cd "$SDK_DIR"

if [[ "$SKIP_BUILD" == "false" ]]; then
  echo "📦 Installing npm dependencies..."
  npm ci --legacy-peer-deps 2>/dev/null || npm install --legacy-peer-deps

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

NPM_TAG_FLAG=""
if [[ "$VERSION" == *"-"* ]]; then
  PRERELEASE_LABEL="$(echo "$VERSION" | sed 's/^[^-]*-//' | sed 's/[.0-9]*$//' | tr -d '[:digit:]')"
  PRERELEASE_LABEL="${PRERELEASE_LABEL:-next}"
  NPM_TAG_FLAG="--tag $PRERELEASE_LABEL"
  echo "🏷️  Pre-release version detected — using dist-tag: $PRERELEASE_LABEL"
fi

if ! npm view "$CLIENT_PACKAGE_NAME@$VERSION" version --silent >/dev/null 2>&1; then
  echo "❌ ${CLIENT_PACKAGE_NAME}@$VERSION is not published yet. Publish the main package first."
  echo "   registry API: $CLIENT_REGISTRY_URL"
  exit 1
fi

if [[ "$DRY_RUN" == "true" ]]; then
  echo ""
  echo "🔍 Dry-run mode: skipping actual publish."
  echo "   Would publish: $PACKAGE_NAME@$VERSION${NPM_TAG_FLAG:+ ($NPM_TAG_FLAG)}"
  # shellcheck disable=SC2086
  npm publish --access public $NPM_TAG_FLAG --dry-run --ignore-scripts
  exit 0
fi

if [[ -z "${NODE_AUTH_TOKEN:-}" ]]; then
  echo ""
  echo "❌ NODE_AUTH_TOKEN is not set."
  echo "   Either export it or add it to $SDK_DIR/.env:"
  echo "     NODE_AUTH_TOKEN=npm_xxxxxxxx"
  exit 1
fi

LOCAL_NPMRC="$SDK_DIR/.npmrc"
cleanup_npmrc() { rm -f "$LOCAL_NPMRC"; }
trap cleanup_npmrc EXIT
npm config set "//registry.npmjs.org/:_authToken" "${NODE_AUTH_TOKEN}" --location=project

echo ""
echo "🚀 Publishing $PACKAGE_NAME@$VERSION to npm..."
PUBLISH_SCRIPTS_FLAG=""
if [[ "$SKIP_BUILD" == "true" ]]; then
  PUBLISH_SCRIPTS_FLAG="--ignore-scripts"
fi

OTP_FLAG=""
if [[ -n "$OTP_CODE" ]]; then
  OTP_FLAG="--otp $OTP_CODE"
fi

if npm view "$PACKAGE_NAME@$VERSION" version --silent >/dev/null 2>&1; then
  if [[ "$FORCE_PUBLISH" == "true" ]]; then
    echo "⚠️  Version $VERSION exists. Force publish enabled — attempting to unpublish..."
    if npm unpublish "$PACKAGE_NAME@$VERSION" --force 2>/dev/null; then
      echo "✅ Successfully unpublished $PACKAGE_NAME@$VERSION"
      # shellcheck disable=SC2086
      npm publish --access public $NPM_TAG_FLAG $PUBLISH_SCRIPTS_FLAG $OTP_FLAG
      echo "✅ Successfully republished $PACKAGE_NAME@$VERSION to npm!"
    else
      echo "❌ Failed to unpublish (version may be >72 hours old)"
      echo "💡 Tip: npm doesn't allow unpublishing after 72 hours."
      echo "    Use a different version number in package.json and try again."
      exit 1
    fi
  else
    echo "⚠️  Version $VERSION already exists on npm — skipping publish."
    echo "💡 To force republish, run with: --force"
    exit 0
  fi
else
  # shellcheck disable=SC2086
  if npm publish --access public $NPM_TAG_FLAG $PUBLISH_SCRIPTS_FLAG $OTP_FLAG 2>&1; then
    echo "✅ Successfully published $PACKAGE_NAME@$VERSION to npm!"
    echo "   npm page    : $PACKAGE_NPM_PAGE_URL"
    echo "   registry API: $PACKAGE_REGISTRY_URL"
  else
    NPM_PUBLISH_EXIT=$?
    echo ""
    echo "❌ npm publish failed (exit $NPM_PUBLISH_EXIT)"
    echo "   Common causes:"
    echo "   • 2FA required: pass --otp <code> for your authenticator app."
    echo "   • Version was previously published and unpublished."
    echo "   • Auth token expired or lacks publish permissions."
    exit $NPM_PUBLISH_EXIT
  fi
fi