#!/bin/bash
# Ensures the @kalamdb/client SDK is compiled before running the app.
# Called automatically by npm run dev / npm run service via predev/preservice hooks.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SDK_DIR="$(cd "$PROJECT_DIR/../../link/sdks/typescript/client" && pwd)"
WASM_FILE="$SDK_DIR/dist/wasm/kalam_client_bg.wasm"

# Check if SDK dist exists with WASM
if [ ! -f "$WASM_FILE" ]; then
    echo "⚠️  @kalamdb/client SDK not compiled. Building now..."
    echo ""
    cd "$SDK_DIR"
    bash build.sh
    echo ""
    echo "✅ SDK compiled successfully"
else
    echo "✅ @kalamdb/client SDK already compiled"
fi

# Copy WASM file to public/ so Next.js can serve it
PUBLIC_WASM_DIR="$PROJECT_DIR/public/wasm"
mkdir -p "$PUBLIC_WASM_DIR"

if [ ! -f "$PUBLIC_WASM_DIR/kalam_link_bg.wasm" ] || \
   [ "$WASM_FILE" -nt "$PUBLIC_WASM_DIR/kalam_client_bg.wasm" ]; then
    echo "📦 Copying WASM to public/wasm/ for browser access..."
    cp "$SDK_DIR/dist/wasm/kalam_client_bg.wasm" "$PUBLIC_WASM_DIR/"
    echo "✅ WASM file ready at /wasm/kalam_client_bg.wasm"
else
    echo "✅ WASM file in public/ is up to date"
fi
