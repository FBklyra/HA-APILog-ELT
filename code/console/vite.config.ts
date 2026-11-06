/// <reference types="vitest/config" />
import { defineConfig, loadEnv } from 'vite';

// The hub serves dist/ behind nginx; /api is proxied to hub-api in dev when VITE_ALELT_MOCK=0.
export default defineConfig(({ mode }) => ({
  esbuild: { jsx: 'automatic' },
  build: { outDir: 'dist', sourcemap: true, target: 'es2022' },
  server: {
    port: 5173,
    proxy: { '/api': { target: loadEnv(mode, '.', 'ALELT_').ALELT_HUB_URL ?? 'http://localhost:8080', changeOrigin: true } },
  },
  test: { include: ['src/**/*.test.ts'], environment: 'node' },
}));
