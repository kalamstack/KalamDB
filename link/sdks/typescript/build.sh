#!/bin/bash
set -e

echo "🔨 Building KalamDB TypeScript SDK..."

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Clean previous build
echo "🧹 Cleaning previous build..."
rm -rf dist wasm

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

# Navigate to link crate root (parent of sdks/)
cd "$SCRIPT_DIR/../.."

# Build WASM using wasm-pack (output to wasm/)
echo "📦 Compiling Rust to WASM..."
wasm-pack build \
  --target web \
  --out-dir sdks/typescript/wasm \
  --features wasm \
  --no-default-features

# Return to SDK directory
cd "$SCRIPT_DIR"

# Compile TypeScript
echo "🔧 Compiling TypeScript..."
npx tsc

# Copy WASM files to dist/wasm for published output
echo "📁 Copying WASM files to dist..."
mkdir -p dist/wasm
for f in wasm/*; do
    filename=$(basename "$f")
    if [[ "$filename" != "package.json" && "$filename" != ".gitignore" ]]; then
        cp "$f" dist/wasm/
    fi
done

echo ""
echo "✅ Build complete!"
echo ""
echo "Output files in dist/:"
echo "  - src/index.js (TypeScript client)"
echo "  - src/index.d.ts (TypeScript types)"
echo "  - wasm/kalam_link.js (WASM bindings)"
echo "  - wasm/kalam_link.d.ts (WASM TypeScript definitions)"
echo "  - wasm/kalam_link_bg.wasm (WebAssembly module)"
echo ""
echo "To publish: npm publish"
