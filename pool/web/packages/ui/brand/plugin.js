import { readFileSync } from "node:fs"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"

// What every Open Pool site shares that is not code: the logo, the icons
// made from it, the web app manifest, and the script that applies the
// visitor's theme. A site's Vite config adds brand(), and these are served
// from the site's root in development and written into its build.

const here = path.dirname(fileURLToPath(import.meta.url))

/** The ground of the dark theme and of the light one, for the browser's own chrome. */
export const themeColors = { dark: "#050b1a", light: "#f5f8fe" }

/** The files served at a site's root: the name there, the file here, and its type. */
const files = [
  { name: "favicon.svg", file: "mark.svg", type: "image/svg+xml" },
  { name: "favicon.ico", file: "favicon.ico", type: "image/x-icon" },
  { name: "apple-touch-icon.png", file: "apple-touch-icon.png", type: "image/png" },
  { name: "icon-192.png", file: "icon-192.png", type: "image/png" },
  { name: "icon-512.png", file: "icon-512.png", type: "image/png" },
  { name: "icon-maskable-512.png", file: "icon-maskable-512.png", type: "image/png" },
]

/**
 * The script that applies the visitor's theme. It is written into each page's
 * head, where it runs before the first paint without a request of its own.
 */
export const themeScript = readFileSync(path.join(here, "theme.js"), "utf8")
  .split("\n")
  .map((line) => line.trim())
  .filter((line) => line !== "" && !line.startsWith("//"))
  .join("\n")

/** The manifest's path at a site's root. */
const manifestName = "site.webmanifest"

/** What a site's head says about its icons, manifest and theme: one list, used by every site. */
export const headTags = [
  { tag: "link", attrs: { rel: "icon", href: "/favicon.ico", sizes: "48x48" } },
  { tag: "link", attrs: { rel: "icon", href: "/favicon.svg", type: "image/svg+xml", sizes: "any" } },
  { tag: "link", attrs: { rel: "apple-touch-icon", href: "/apple-touch-icon.png" } },
  { tag: "link", attrs: { rel: "manifest", href: `/${manifestName}` } },
  { tag: "meta", attrs: { name: "color-scheme", content: "light dark" } },
  { tag: "meta", attrs: { name: "theme-color", content: themeColors.light, media: "(prefers-color-scheme: light)" } },
  { tag: "meta", attrs: { name: "theme-color", content: themeColors.dark, media: "(prefers-color-scheme: dark)" } },
  { tag: "script", attrs: {}, children: themeScript },
]

/** A site's web app manifest. */
export const manifest = (site) => ({
  name: site.name,
  short_name: site.shortName ?? site.name,
  description: site.description,
  id: "/",
  start_url: "/",
  scope: "/",
  display: site.display ?? "standalone",
  background_color: themeColors.dark,
  theme_color: themeColors.dark,
  icons: [
    { src: "/icon-192.png", sizes: "192x192", type: "image/png", purpose: "any" },
    { src: "/icon-512.png", sizes: "512x512", type: "image/png", purpose: "any" },
    { src: "/icon-maskable-512.png", sizes: "512x512", type: "image/png", purpose: "maskable" },
  ],
})

/**
 * The Vite plugin. `head: false` leaves the page's head alone, for a site
 * that writes its own pages and puts headTags there itself.
 */
export const brand = (site) => ({
  name: "openpool:brand",
  configureServer(server) {
    const manifestJson = JSON.stringify(manifest(site), null, 2)
    server.middlewares.use((req, res, next) => {
      const name = (req.url ?? "").split("?")[0].slice(1)
      if (name === manifestName) {
        res.setHeader("content-type", "application/manifest+json")
        res.end(manifestJson)
        return
      }
      const entry = files.find((candidate) => candidate.name === name)
      if (entry === undefined) return next()
      readFile(path.join(here, entry.file)).then(
        (body) => {
          res.setHeader("content-type", entry.type)
          res.end(body)
        },
        (error) => next(error),
      )
    })
  },
  async generateBundle() {
    for (const entry of files) {
      this.emitFile({ type: "asset", fileName: entry.name, source: await readFile(path.join(here, entry.file)) })
    }
    this.emitFile({ type: "asset", fileName: manifestName, source: JSON.stringify(manifest(site), null, 2) })
  },
  transformIndexHtml() {
    return site.head === false ? [] : headTags.map((tag) => ({ ...tag, injectTo: "head" }))
  },
})
