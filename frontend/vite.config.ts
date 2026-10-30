import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
export default defineConfig(({ mode }) => ({
  plugins: [react(), tailwindcss()],
  server: {
    host: '127.0.0.1', port: 5173, strictPort: true,
    proxy: { '/api/lab': `http://127.0.0.1:${loadEnv(mode, process.cwd(), '').OBSERVER_PORT ?? '3010'}` },
  },
}));
