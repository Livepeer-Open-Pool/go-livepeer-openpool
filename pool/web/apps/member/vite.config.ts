import { brand } from "@openpool/ui/brand"
import { defineConfig, type Plugin } from "vite"

/**
 * Pages are routed in the browser, so the web server must answer index.html
 * for paths it has no file for. Many static hosts do that with a 404.html;
 * this writes one that is the same page.
 */
const notFoundIsTheApp = (): Plugin => ({
  name: "openpool:404-is-the-app",
  apply: "build",
  enforce: "post",
  generateBundle(_options, bundle) {
    const index = bundle["index.html"]
    if (index?.type === "asset") {
      this.emitFile({ type: "asset", fileName: "404.html", source: index.source })
    }
  },
})

// The member site is a set of plain files: index.html, its scripts and
// styles, and config.json, which names the pool's regions and is read when
// the site starts. Any web server can host them.
export default defineConfig({
  plugins: [
    brand({ name: "Open Pool members", shortName: "Open Pool", description: "Apply to Open Pool, connect your machines, and follow your runners, earnings and payouts." }),
    notFoundIsTheApp(),
  ],
  server: { port: 5174, strictPort: true },
  preview: { port: 5174, strictPort: true },
  build: { target: "es2022", sourcemap: true },
})
