// What the public site is built with. Its pages are files, so the addresses
// written into them (canonical links, the sitemap, links to the member site)
// are fixed when it is built:
//
//   OPENPOOL_SITE_URL=https://open-pool.com OPENPOOL_MEMBER_URL=https://app.open-pool.com pnpm build
//
// The pool's regions are not fixed here: the live parts of each page read
// them from config.json when the page is opened.

const env = (name: string, fallback: string): string => (process.env[name] ?? fallback).replace(/\/+$/, "")

export interface Site {
  /** Where the public site is served, e.g. https://open-pool.com. */
  readonly url: string
  /** Where the member site is served, e.g. https://app.open-pool.com. */
  readonly memberUrl: string
  readonly name: string
}

export const site = (): Site => ({
  url: env("OPENPOOL_SITE_URL", "https://open-pool.com"),
  memberUrl: env("OPENPOOL_MEMBER_URL", "https://app.open-pool.com"),
  name: "Open Pool",
})
