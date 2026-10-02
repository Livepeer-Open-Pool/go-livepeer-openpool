import type { TemplateResult } from "lit"
import type { PageMeta } from "./document.js"
import { appsPage } from "./pages/apps.js"
import { connectPage } from "./pages/connect.js"
import { homePage } from "./pages/home.js"
import { howItWorksPage } from "./pages/how-it-works.js"
import { joinPage } from "./pages/join.js"
import { leaderboardPage } from "./pages/leaderboard.js"
import { notFoundPage } from "./pages/not-found.js"
import { termsPage } from "./pages/terms.js"
import type { Site } from "./site.js"

/** One page of the public site. Every page is written to a file when the site is built. */
export interface Route {
  /** The page's path, with its trailing slash. */
  readonly path: string
  readonly mode: "ssg"
  readonly meta: (site: Site) => PageMeta
  readonly template: (site: Site) => TemplateResult
}

/** The trail from the home page to a page, for search results to show in place of the address. */
export const breadcrumb = (site: Site, name: string, path: string): object => ({
  "@context": "https://schema.org",
  "@type": "BreadcrumbList",
  itemListElement: [
    { "@type": "ListItem", position: 1, name: site.name, item: `${site.url}/` },
    { "@type": "ListItem", position: 2, name, item: `${site.url}${path}` },
  ],
})

export const routes: ReadonlyArray<Route> = [homePage, howItWorksPage, connectPage, appsPage, leaderboardPage, termsPage, joinPage]

/** Shown, with status 404, for an address that is no page. */
export const notFound: Route = notFoundPage
