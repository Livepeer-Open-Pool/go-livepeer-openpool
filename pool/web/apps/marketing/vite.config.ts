import { defineConfig, type Plugin } from "vite"

/**
 * In development, pages are rendered when asked for, by the same code that
 * renders them to files at build time, so a page looks the same either way.
 */
const pages = (): Plugin => ({
  name: "openpool:pages",
  apply: "serve",
  configureServer(server) {
    return () => {
      server.middlewares.use((req, res, next) => {
        const url = new URL(req.originalUrl ?? req.url ?? "/", "http://localhost")
        // A page is asked for by its path, which names no file: /connect/ and not /connect/x.js.
        if (req.method !== "GET" || /\.[a-z0-9]+$/i.test(url.pathname)) return next()
        server
          .ssrLoadModule("/src/render.ts")
          .then((module) => (module as typeof import("./src/render.js")).renderPage(url.pathname, { scripts: ["/src/entry-client.ts"], styles: ["/src/marketing.css"] }))
          .then((page) => server.transformIndexHtml(url.pathname, page.html).then((html) => ({ ...page, html })))
          .then((page) => {
            res.statusCode = page.found ? 200 : 404
            res.setHeader("content-type", "text/html; charset=utf-8")
            res.end(page.html)
          })
          .catch((error: unknown) => {
            if (error instanceof Error) server.ssrFixStacktrace(error)
            next(error)
          })
      })
    }
  },
})

// The public site is built in two steps: Vite bundles the scripts and styles
// of the parts that load live data, then scripts/prerender.ts writes every
// page as an HTML file that references them. The result is plain files.
export default defineConfig({
  appType: "custom",
  plugins: [pages()],
  server: { port: 5173, strictPort: true },
  preview: { port: 5173, strictPort: true },
  build: {
    target: "es2022",
    sourcemap: true,
    manifest: true,
    rollupOptions: { input: "src/entry-client.ts" },
  },
})
