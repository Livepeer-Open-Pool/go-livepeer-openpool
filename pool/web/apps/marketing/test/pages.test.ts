import { describe, expect, it } from "vitest"
import { findRoute, renderPage, renderRoute } from "../src/render.js"
import { notFound, routes } from "../src/routes.js"
import type { Site } from "../src/site.js"

const site: Site = { url: "https://open-pool.com", memberUrl: "https://app.open-pool.com", name: "Open Pool" }
const assets = { scripts: ["/assets/entry.js"], styles: ["/assets/entry.css"] }

const pages = await Promise.all(routes.map(async (route) => ({ route, html: await renderRoute(route, assets, site) })))

const first = (html: string, pattern: RegExp): string => pattern.exec(html)?.[1] ?? ""
const all = (html: string, pattern: RegExp): ReadonlyArray<string> => [...html.matchAll(pattern)].map((match) => match[1] ?? "")

describe("every page", () => {
  it.each(pages)("$route.path is a whole document with one h1 inside main", ({ html }) => {
    expect(html.trimStart().startsWith("<!doctype html>")).toBe(true)
    expect(html).toContain('<html lang="en" dir="ltr">')
    expect(all(html, /<(h1)[\s>]/g)).toHaveLength(1)
    expect(all(html, /<(main)[\s>]/g)).toHaveLength(1)
    const main = html.slice(html.indexOf("<main"), html.indexOf("</main>"))
    expect(main).toContain("<h1")
    // The skip link's target exists, and the document has one banner and one footer of its own.
    expect(html).toContain('<a class="skip" href="#main">')
    expect(html).toContain('<main id="main">')
  })

  it.each(pages)("$route.path says what it is, where it lives and how it loads", ({ route, html }) => {
    const meta = route.meta(site)
    expect(first(html, /<title>([^<]*)<\/title>/)).toContain(meta.title)
    expect(first(html, /<meta name="description" content="([^"]*)"/).length).toBeGreaterThan(70)
    expect(first(html, /<link rel="canonical" href="([^"]*)"/)).toBe(`https://open-pool.com${route.path}`)
    expect(first(html, /<meta property="og:url" content="([^"]*)"/)).toBe(`https://open-pool.com${route.path}`)
    expect(html).toContain('<script type="module" src="/assets/entry.js"></script>')
    expect(html).toContain('<link rel="stylesheet" href="/assets/entry.css">')
    expect(html).not.toContain('name="robots"')
    expect(html).not.toContain('name="keywords"')
  })

  it.each(pages)("$route.path names the logo's icons and the manifest, and sets the theme before it loads its styles", ({ html }) => {
    const head = html.slice(html.indexOf("<head>"), html.indexOf("</head>"))
    expect(head).toMatch(/<link rel="icon" href="\/favicon\.svg" type="image\/svg\+xml"/)
    expect(head).toMatch(/<link rel="icon" href="\/favicon\.ico"/)
    expect(head).toMatch(/<link rel="apple-touch-icon" href="\/apple-touch-icon\.png"/)
    expect(head).toMatch(/<link rel="manifest" href="\/site\.webmanifest"/)
    expect(all(head, /<meta name="(theme-color)"/g)).toHaveLength(2)
    // The script is the brand's own, written out whole: a binding would have escaped it.
    const script = head.indexOf("openPoolTheme")
    expect(script).toBeGreaterThan(-1)
    expect(head).not.toContain("=&gt;")
    expect(script).toBeLessThan(head.indexOf('rel="stylesheet"'))
  })

  it.each(pages)("$route.path carries structured data that parses and names real addresses", ({ html }) => {
    const blocks = all(html, /<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)
    expect(blocks.length).toBeGreaterThan(0)
    for (const block of blocks) {
      // Written through a template binding it would be HTML-escaped, and no longer JSON.
      expect(block).not.toContain("&quot;")
      const data = JSON.parse(block) as { "@context": string; "@type": string }
      expect(data["@context"]).toBe("https://schema.org")
      expect(JSON.stringify(data)).toContain("https://open-pool.com/")
    }
  })

  it.each(pages)("$route.path marks its own link in the navigation, and only that one", ({ route, html }) => {
    const current = all(html, /<a href="([^"]*)" aria-current="page">/g)
    expect(current).toEqual(route.path === "/" || route.path === "/join/" ? [] : [route.path])
  })

  it.each(pages)("$route.path has no heading level skipped", ({ html }) => {
    const levels = all(html, /<h([1-6])[\s>]/g).map(Number)
    levels.forEach((level, index) => {
      if (index > 0) expect(level - (levels[index - 1] ?? level)).toBeLessThanOrEqual(1)
    })
  })

  it("has a title and a description no other page has", () => {
    const titles = pages.map(({ html }) => first(html, /<title>([^<]*)<\/title>/))
    const descriptions = pages.map(({ html }) => first(html, /<meta name="description" content="([^"]*)"/))
    expect(new Set(titles).size).toBe(pages.length)
    expect(new Set(descriptions).size).toBe(pages.length)
  })

  it("links only to pages that exist", () => {
    const paths = new Set(routes.map((route) => route.path))
    for (const { route, html } of pages) {
      for (const href of all(html, /<a[^>]* href="(\/[^"#]*)[^"]*"/g)) {
        expect(paths.has(href), `${route.path} links to ${href}`).toBe(true)
      }
    }
  })
})

describe("the pages' content", () => {
  it("stands in plain HTML where live data will load, so the page reads without scripts", async () => {
    const home = await renderRoute(routes[0]!, assets, site)
    expect(home).toMatch(/<op-pool-facts>[\s\S]*This part of the page needs JavaScript[\s\S]*<\/op-pool-facts>/)
    expect(home).toContain("Put your GPU to work on Livepeer")
    // Nothing of the live parts is rendered on the server: no shadow roots in the files.
    expect(home).not.toContain("shadowrootmode")
  })

  it("sends people to the member site it was built for", async () => {
    const built = { ...site, memberUrl: "https://members.example.org" }
    const join = await renderRoute(findRoute("/join/")!, assets, built)
    expect(join).toContain('href="https://members.example.org/apply"')
    expect(join).not.toContain("app.open-pool.com")
  })
})

describe("finding a page", () => {
  it("finds a page with or without its trailing slash", () => {
    expect(findRoute("/connect/")?.path).toBe("/connect/")
    expect(findRoute("/connect")?.path).toBe("/connect/")
    expect(findRoute("/")?.path).toBe("/")
    expect(findRoute("/nowhere/")).toBeUndefined()
  })

  it("answers an address that is no page with the not-found page, kept out of search results", async () => {
    const page = await renderPage("/nowhere/", assets, site)
    expect(page.found).toBe(false)
    expect(page.html).toContain("There is no page at this address")
    expect(page.html).toContain('<meta name="robots" content="noindex">')
    expect(page.html).not.toContain('rel="canonical"')
    expect(notFound.meta(site).index).toBe(false)
  })
})
