#!/bin/bash
set -euo pipefail

echo "🧪 Testing KalamDB Dart SDK..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── Configuration ──────────────────────────────────────────────────────
export KALAMDB_URL="${KALAMDB_URL:-${KALAM_URL:-http://localhost:8080}}"
export KALAMDB_USER="${KALAMDB_USER:-${KALAM_USER:-admin}}"
export KALAMDB_PASSWORD="${KALAMDB_PASSWORD:-${KALAM_PASS:-kalamdb123}}"
export KALAM_URL="${KALAM_URL:-$KALAMDB_URL}"
export KALAM_USER="${KALAM_USER:-$KALAMDB_USER}"
export KALAM_PASS="${KALAM_PASS:-$KALAMDB_PASSWORD}"

# ── Dependencies ──────────────────────────────────────────────────────
echo "📦 Ensuring dependencies are installed..."
flutter pub get

BRIDGE_DIR="$SCRIPT_DIR/../../kalam-link-dart"
echo "🦀 Building host native library used by flutter test..."
(
  cd "$BRIDGE_DIR"
  CARGO_TARGET_DIR="$BRIDGE_DIR/target" cargo build --release
)

# ── Static analysis ──────────────────────────────────────────────────
echo ""
echo "🧭 Running analyzer checks..."
flutter analyze

# ── Unit tests (offline, no server required) ─────────────────────────
echo ""
echo "🔬 Running unit tests (no server)..."
flutter test test/models_test.dart

# ── E2E tests (require running KalamDB server) ──────────────────────
echo ""
echo "🔗 Checking server at $KALAMDB_URL ..."
if curl -sf "$KALAMDB_URL/health" > /dev/null 2>&1; then
  echo "✅ Server is reachable"
  echo ""
  echo "🧪 Running e2e tests..."
  export KALAM_INTEGRATION_TEST=1
  flutter test \
    test/e2e/auth/auth_test.dart \
    test/e2e/query/query_test.dart \
    test/e2e/ddl/ddl_test.dart \
    test/e2e/lifecycle/lifecycle_test.dart \
    test/e2e/health/health_test.dart \
    test/e2e/subscription/subscription_test.dart
  echo ""
  echo "✅ All Dart SDK tests passed!"
else
  echo "⚠️  Server not reachable at $KALAMDB_URL — skipping e2e tests."
  echo "   Start the server: cd backend && cargo run"
  echo "   Then re-run: ./test.sh"
  exit 1
fi
