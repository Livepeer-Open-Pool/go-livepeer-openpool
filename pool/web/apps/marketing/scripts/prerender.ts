import { mkdir, readFile, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath } from "node:url"
import type { Assets } from "../src/document.js"
import { renderRoute } from "../src/render.js"
import { notFound, routes } from "../src/routes.js"
import { site as readSite } from "../src/site.js"

// Writes every page of the public site as an HTML file, after Vite has
// bundled the scripts and styles the pages load. Also writes the sitemap,
// robots.txt and the not-found page. The result, dist/, is plain files.

const dist = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../dist")

interface ManifestEntry {
  readonly file: string
  readonly css?: ReadonlyArray<string>
}

const manifest = JSON.parse(await readFile(path.join(dist, ".vite/manifest.json"), "utf8")) as Record<string, ManifestEntry>
const entry = manifest["src/entry-client.ts"]
if (entry === undefined) {
  throw new Error("the build has no entry for src/entry-client.ts; run vite build first")
}
const assets: Assets = { scripts: [`/${entry.file}`], styles: (entry.css ?? []).map((file) => `/${file}`) }
const site = readSite()

for (const route of routes) {
  const file = path.join(dist, route.path, "index.html")
  await mkdir(path.dirname(file), { recursive: true })
  await writeFile(file, await renderRoute(route, assets, site))
}
await writeFile(path.join(dist, "404.html"), await renderRoute(notFound, assets, site))

const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${routes.filter((route) => route.meta(site).index).map((route) => `  <url><loc>${site.url}${route.path}</loc></url>`).join("\n")}
</urlset>
`
await writeFile(path.join(dist, "sitemap.xml"), sitemap)
await writeFile(path.join(dist, "robots.txt"), `User-agent: *\nAllow: /\n\nSitemap: ${site.url}/sitemap.xml\n`)

console.log(`wrote ${routes.length} pages, 404.html, sitemap.xml and robots.txt for ${site.url}`)
