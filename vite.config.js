import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

function serveHashNamedPublicFiles() {
  const base = '/analytics/'
  return {
    name: 'serve-hash-named-public-files',
    apply: 'serve',
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        try {
          const rawUrl = req.url || ''
          const pathname = rawUrl.split('?')[0] || ''
          // Only handle requests under the configured base.
          if (!pathname.startsWith(base)) return next()
          // Only relevant when a URL-encoded hash is present.
          if (!pathname.includes('%23')) return next()

          // Strip base and map into public/.
          const rel = pathname.slice(base.length).replace(/^\/+/, '')
          const decodedRel = decodeURIComponent(rel)
          const fsPath = server.config.publicDir + '/' + decodedRel

          // eslint-disable-next-line no-undef
          const { promises: fs } = await import('node:fs')
          // eslint-disable-next-line no-undef
          const path = await import('node:path')

          const normalized = path.normalize(fsPath)
          // Basic safety: ensure we don't escape publicDir.
          if (!normalized.startsWith(path.normalize(server.config.publicDir))) return next()

          const stat = await fs.stat(normalized).catch(() => null)
          if (!stat || !stat.isFile()) return next()

          // Basic content-type mapping (we mainly need .txt).
          const lower = normalized.toLowerCase()
          if (lower.endsWith('.json')) res.setHeader('Content-Type', 'application/json')
          else if (lower.endsWith('.csv')) res.setHeader('Content-Type', 'text/csv')
          else res.setHeader('Content-Type', 'text/plain')

          const buf = await fs.readFile(normalized)
          res.statusCode = 200
          res.end(buf)
        } catch {
          next()
        }
      })
    }
  }
}

// Set `base` so built assets load correctly when hosted under
// https://<user>.github.io/<repo>/ (GitHub Pages). Output will go to `docs/`.
export default defineConfig({
  base: '/analytics/',
  plugins: [serveHashNamedPublicFiles(), react()],
  server: { port: 5173 },
  build: { outDir: 'docs', emptyOutDir: false }
})