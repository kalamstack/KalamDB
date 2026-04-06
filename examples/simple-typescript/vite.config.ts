import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    host: true,
    fs: {
      // Allow serving files from the SDK directory (outside project root)
      allow: [
        // Search up for workspace root
        '..',
        // Allow access to the entire KalamDB repository for SDK files
        '../..'
      ]
    }
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    target: 'es2020'
  },
  optimizeDeps: {
    exclude: ['@kalamdb/client']
  }
})
