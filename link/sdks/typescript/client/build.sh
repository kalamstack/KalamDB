#!/bin/bash
set -e

echo "🔨 Building KalamDB TypeScript SDK..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -d "node_modules" ]; then
  echo "📦 Installing dependencies..."
  npm install
fi

echo "🧹 Running package build..."
npm run build
