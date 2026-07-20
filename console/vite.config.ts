import { defineConfig } from 'vitest/config';
import { loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), 'TICKR_VITE_');
  const host = env.TICKR_VITE_HOST || '127.0.0.1';
  const allowedHosts = (env.TICKR_VITE_ALLOWED_HOSTS || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);

  const proxy = {
    '/api': {
      target: 'http://127.0.0.1:6000',
      changeOrigin: true,
    },
  };

  return {
    plugins: [react()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    test: {
      environment: 'jsdom',
      globals: true,
      setupFiles: './src/test/setup.ts',
      css: false,
    },
    server: {
      host,
      // strictPort so the dev UI is deterministically on 3000 and fails loudly
      // rather than silently bumping to another port.
      port: 3000,
      strictPort: true,
      allowedHosts,
      // The API component is the per-tenant UI gateway on :6000 for every
      // /api/* read and write.
      proxy,
    },
    // `vite preview` serves explicit production-build checks from its own
    // config block, so mirror the development server's port and API proxy.
    preview: {
      host,
      port: 3000,
      strictPort: true,
      allowedHosts,
      proxy,
    },
  };
});
