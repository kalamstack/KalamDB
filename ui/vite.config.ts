import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";
import fs from "fs";

function normalizeOrigin(url: string): string {
  return url.replace(/\/+$/, "");
}

function toWebSocketOrigin(url: string): string {
  if (url.startsWith("https://")) {
    return `wss://${url.slice("https://".length)}`;
  }

  if (url.startsWith("http://")) {
    return `ws://${url.slice("http://".length)}`;
  }

  return url;
}

// Plugin to clean up duplicate kalam-link folder after build
const cleanupPlugin = () => ({
  name: 'cleanup-kalam-link',
  closeBundle() {
    const duplicatePath = path.resolve(__dirname, 'dist/kalam-link');
    if (fs.existsSync(duplicatePath)) {
      fs.rmSync(duplicatePath, { recursive: true, force: true });
      console.log('Cleaned up duplicate kalam-link folder');
    }
  }
});

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, "");
  const backendOrigin = normalizeOrigin(env.VITE_API_URL || "http://localhost:8080");
  const backendWebSocketOrigin = toWebSocketOrigin(backendOrigin);

  return {
    plugins: [react(), cleanupPlugin()],
    // Base path for production build (embedded in server at /ui/)
    base: "/ui/",
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      port: 5173,
      proxy: {
        "/v1": {
          target: backendOrigin,
          changeOrigin: true,
        },
        "/ws": {
          target: backendWebSocketOrigin,
          ws: true,
        },
      },
      fs: {
        // Allow serving files from the link SDK directory for WASM
        allow: [
          path.resolve(__dirname, "."),
          path.resolve(__dirname, "../link/sdks/typescript"),
        ],
      },
      // Disable caching for WASM and kalam-link files
      headers: {
        "Cache-Control": "no-store",
      },
    },
    build: {
      outDir: "dist",
      sourcemap: true,
      target: "esnext",
      rollupOptions: {
        output: {
          // Ensure WASM files have consistent naming
          assetFileNames: (assetInfo) => {
            if (assetInfo.name?.endsWith('.wasm')) {
              return 'assets/[name]-[hash][extname]';
            }
            return 'assets/[name]-[hash][extname]';
          },
        },
      },
    },
    optimizeDeps: {
      // Force re-bundling on every server start
      force: true,
      // Exclude kalam-link from pre-bundling so WASM files load correctly
      // When pre-bundled, import.meta.url points to .vite/deps which breaks WASM loading
      exclude: ["kalam-link"],
    },
    // Ensure WASM files are handled correctly
    assetsInclude: ["**/*.wasm"],
  };
});
