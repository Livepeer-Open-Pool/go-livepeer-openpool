import { render } from "@lit-labs/ssr"
import { collectResult } from "@lit-labs/ssr/lib/render-result.js"
import { type Assets, document } from "./document.js"
import { notFound, type Route, routes } from "./routes.js"
import { site as readSite, type Site } from "./site.js"

/** The route for a path. A path without its trailing slash finds the same page. */
export const findRoute = (pathname: string): Route | undefined => {
  const path = pathname.endsWith("/") ? pathname : `${pathname}/`
  return routes.find((route) => route.path === path)
}

/** A route's whole document, as HTML. */
export const renderRoute = (route: Route, assets: Assets, site: Site = readSite()): Promise<string> =>
  collectResult(render(document(site, route.meta(site), assets, route.template(site))))

/** The document for a path: the page there, or the not-found page. */
export const renderPage = (pathname: string, assets: Assets, site: Site = readSite()): Promise<{ readonly found: boolean; readonly html: string }> => {
  const route = findRoute(pathname)
  return renderRoute(route ?? notFound, assets, site).then((html) => ({ found: route !== undefined, html }))
}
