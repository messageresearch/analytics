import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Set `base` so built assets load correctly when hosted under
// https://<user>.github.io/<repo>/ (GitHub Pages). Output will go to `docs/`.
export default defineConfig({
  base: '/wmbmentions.github.io/',
  plugins: [react()],
  server: { port: 5173 },
  build: { outDir: 'docs' }
})
