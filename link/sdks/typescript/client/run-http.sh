#!/bin/bash
set -e

echo "�� KalamDB TypeScript SDK - Build & Run Example"
echo "================================================"
echo ""

# Navigate to typescript SDK directory
cd "$(dirname "$0")"

# Rebuild everything
echo "🔨 Rebuilding WASM and TypeScript SDK..."
./build.sh

echo ""
echo "🌐 Starting HTTP server for example app..."
echo "   URL: http://localhost:3000/example/"
echo ""
echo "   Press Ctrl+C to stop the server"
echo ""

# Run http-server from typescript folder so all paths work
npx http-server -p 3000 -c-1 -o /example/index.html
