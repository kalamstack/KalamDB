#!/bin/bash
set -euo pipefail

echo "🧪 Testing KalamDB TypeScript SDK..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── Configuration ──────────────────────────────────────────────────────
export KALAMDB_URL="${KALAMDB_URL:-http://localhost:8080}"
export KALAMDB_USER="${KALAMDB_USER:-admin}"
export KALAMDB_PASSWORD="${KALAMDB_PASSWORD:-kalamdb123}"

# ── Build SDK ──────────────────────────────────────────────────────────
if [[ ! -d node_modules ]]; then
  echo "📥 Installing npm dependencies..."
  npm install --no-audit --no-fund
fi

echo "📦 Building SDK..."
npm run build

# ── Unit tests (offline, no server required) ───────────────────────────
echo ""
echo "🔬 Running unit tests (no server)..."
NO_SERVER=true node --test \
  tests/basic.test.mjs \
  tests/normalize.test.mjs \
  tests/auth-provider-retry.test.mjs \
  tests/cell-value.test.mjs \
  tests/single-socket-subscriptions.test.mjs \
  tests/readme-examples.test.mjs \
  tests/sdk-runtime-coverage.test.mjs

# ── E2E tests (require running KalamDB server) ────────────────────────
echo ""
echo "🔗 Checking server at $KALAMDB_URL ..."
if curl -sf "$KALAMDB_URL/health" > /dev/null 2>&1 \
  || curl -sf "$KALAMDB_URL/v1/api/healthcheck" > /dev/null 2>&1; then
  echo "✅ Server is reachable"
  echo ""
  echo "🧪 Running e2e tests..."
  node --test --test-concurrency=1 \
    tests/e2e/auth/auth.test.mjs \
    tests/e2e/query/query.test.mjs \
    tests/e2e/query/dml-helpers.test.mjs \
    tests/e2e/ddl/ddl.test.mjs \
    tests/e2e/lifecycle/lifecycle.test.mjs \
    tests/e2e/subscription/subscription.test.mjs \
    tests/e2e/reconnect/reconnect.test.mjs \
    tests/e2e/reconnect/resume.test.mjs
  echo ""
  echo "✅ All TypeScript SDK tests passed!"
else
  echo "⚠️  Server not reachable at $KALAMDB_URL — skipping e2e tests."
  echo "   Start the server: cd backend && cargo run"
  echo "   Then re-run: ./test.sh"
  exit 1
fi
