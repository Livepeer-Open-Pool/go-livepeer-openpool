import { html } from "@lit-labs/ssr"
import type { TemplateResult } from "lit"
import { unsafeHTML } from "lit/directives/unsafe-html.js"
import type { Site } from "./site.js"

/** What a page says about itself to browsers, search engines and link previews. */
export interface PageMeta {
  /** The page's own title, without the site's name. */
  readonly title: string
  /** One or two sentences, unique to the page: the search result's pitch. */
  readonly description: string
  /** The page's path, with its trailing slash, e.g. /connect/. */
  readonly path: string
  /** Structured data that describes the page, as schema.org JSON-LD. */
  readonly jsonLd: ReadonlyArray<object>
  /** False for pages that should not be in search results. */
  readonly index: boolean
}

/** The scripts and styles a page loads: source files in development, hashed bundles when built. */
export interface Assets {
  readonly scripts: ReadonlyArray<string>
  readonly styles: ReadonlyArray<string>
}

/** JSON that cannot break out of the script element it is written into. */
const safeJson = (data: unknown): string =>
  JSON.stringify(data).replace(/</g, "\\u003c").replace(/\u2028/g, "\\u2028").replace(/\u2029/g, "\\u2029")

/**
 * A script element holding JSON. Template bindings are HTML-escaped even
 * inside <script>, which would corrupt the JSON, so the element is written
 * as raw HTML. It is safe because safeJson's output contains no "<". This is
 * the site's one use of unsafeHTML.
 */
const jsonLdScript = (data: object) => unsafeHTML(`<script type="application/ld+json">${safeJson(data)}</script>`)

const navigation = [
  { href: "/how-it-works/", label: "How it works" },
  { href: "/connect/", label: "Connect a machine" },
  { href: "/apps/", label: "Apps and prices" },
  { href: "/leaderboard/", label: "Leaderboard" },
  { href: "/terms/", label: "Terms" },
] as const

/** The whole document around a page's content: head, banner, navigation, footer. */
export const document = (site: Site, meta: PageMeta, assets: Assets, content: TemplateResult): TemplateResult => {
  const canonical = `${site.url}${meta.path}`
  const fullTitle = meta.path === "/" ? `${site.name}: ${meta.title}` : `${meta.title} · ${site.name}`
  return html`<!doctype html>
<html lang="en" dir="ltr">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
    <title>${fullTitle}</title>
    <meta name="description" content=${meta.description}>
    ${meta.index ? html`<link rel="canonical" href=${canonical}>` : html`<meta name="robots" content="noindex">`}
    <meta name="color-scheme" content="light dark">
    <link rel="icon" href="/favicon.svg" type="image/svg+xml">
    <meta property="og:type" content="website">
    <meta property="og:site_name" content=${site.name}>
    <meta property="og:title" content=${meta.title}>
    <meta property="og:description" content=${meta.description}>
    <meta property="og:url" content=${canonical}>
    <meta name="twitter:card" content="summary">
    ${assets.styles.map((href) => html`<link rel="stylesheet" href=${href}>`)}
    ${meta.jsonLd.map(jsonLdScript)}
    ${assets.scripts.map((src) => html`<script type="module" src=${src}></script>`)}
  </head>
  <body>
    <a class="skip" href="#main">Skip to main content</a>
    <header class="banner">
      <div class="bar">
        <a class="brand" href="/" translate="no">Open <span>Pool</span></a>
        <nav aria-label="Primary">
          <ul>
            ${navigation.map((item) => html`<li><a href=${item.href} aria-current=${meta.path === item.href ? "page" : "false"}>${item.label}</a></li>`)}
          </ul>
        </nav>
        <a class="button primary join" href="/join/">Join the pool</a>
      </div>
    </header>
    <main id="main">${content}</main>
    <footer class="site">
      <div class="bar">
        <p translate="no"><strong>Open Pool</strong></p>
        <nav aria-label="Footer">
          <ul>
            ${navigation.map((item) => html`<li><a href=${item.href}>${item.label}</a></li>`)}
            <li><a href="${site.memberUrl}/">Member sign-in</a></li>
          </ul>
        </nav>
        <op-region-status><p class="muted">The pool's regions are listed here once the page has loaded.</p></op-region-status>
        <p><small>Open Pool runs on <a href="https://livepeer.org/" rel="external">Livepeer</a>. Payouts are made in ETH on Arbitrum One.</small></p>
      </div>
    </footer>
  </body>
</html>`
}
