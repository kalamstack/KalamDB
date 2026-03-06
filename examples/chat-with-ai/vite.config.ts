import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5174,
    strictPort: true,
    fs: {
      allow: ['..', '../..'],
    },
  },
  optimizeDeps: {
    exclude: ['kalam-link'],
  },
});