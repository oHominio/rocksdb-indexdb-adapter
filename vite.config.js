import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  // Base public path when served
  base: './',
  
  // Development server config
  server: {
    port: 3000,
    open: '/tests/browser/index.html'
  },
  
  // Build configuration
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'tests/browser/index.html'),
      },
    },
  },
  
  // Resolve paths
  resolve: {
    alias: {
      '@': resolve(__dirname, '.'),
    },
  },
  
  // Plugins
  plugins: [],
}); 