import type { ReactiveController, ReactiveControllerHost, TemplateResult } from "lit"
import type {} from "./platform.js"

/** One page of a site. */
export interface Route {
  /** A URLPattern pathname, e.g. /members/:region/:id. */
  readonly path: string
  /** The page's title, without the site's name. */
  readonly title: string
  /** Loads the page's element; its code is fetched when the page is first opened. */
  readonly enter: () => Promise<unknown>
  /** The page's element, given what the path's parameters matched. */
  readonly template: (params: Readonly<Record<string, string>>) => TemplateResult
}

/** The route a URL leads to, and what its path's parameters matched. */
export interface Match {
  readonly route: Route
  readonly params: Readonly<Record<string, string>>
}

export const matchRoute = (routes: ReadonlyArray<Route>, url: URL): Match | undefined => {
  for (const route of routes) {
    const match = new URLPattern({ pathname: route.path }).exec(url)
    if (match) {
      const params: Record<string, string> = {}
      for (const [name, value] of Object.entries(match.pathname.groups)) {
        if (value !== undefined) params[name] = decodeURIComponent(value)
      }
      return { route, params }
    }
  }
  return undefined
}

// The Navigation API, as far as this router uses it.
interface NavigateEvent extends Event {
  readonly canIntercept: boolean
  readonly hashChange: boolean
  readonly downloadRequest: string | null
  readonly destination: { readonly url: string }
  intercept(options: { handler: () => Promise<void> }): void
}

/**
 * Routing on the platform: URLPattern matches paths, and the Navigation API
 * turns ordinary link clicks into page changes without a reload, with a
 * click and popstate fallback for browsers that lack it. Links are plain
 * <a href>; there is no link component.
 */
export class Router implements ReactiveController {
  /** The current page's element, to render in the app's main region. */
  outlet: TemplateResult | undefined = undefined
  /** The current page's path, for marking the current link. */
  path = ""

  readonly #host: ReactiveControllerHost & HTMLElement
  readonly #routes: ReadonlyArray<Route>
  readonly #site: string
  readonly #notFound: Route
  #stop = new AbortController()

  constructor(host: ReactiveControllerHost & HTMLElement, options: { routes: ReadonlyArray<Route>; site: string; notFound: Route }) {
    this.#host = host
    this.#routes = options.routes
    this.#site = options.site
    this.#notFound = options.notFound
    host.addController(this)
  }

  hostConnected(): void {
    this.#stop = new AbortController()
    const signal = this.#stop.signal
    if ("navigation" in window) {
      const navigation = window.navigation as EventTarget
      navigation.addEventListener(
        "navigate",
        (event) => {
          const navigate = event as NavigateEvent
          if (!navigate.canIntercept || navigate.hashChange || navigate.downloadRequest !== null) return
          const url = new URL(navigate.destination.url)
          navigate.intercept({ handler: () => this.#show(url) })
        },
        { signal },
      )
    } else {
      document.addEventListener("click", this.#onClick, { signal })
      window.addEventListener("popstate", () => void this.#show(new URL(location.href)), { signal })
    }
    void this.#show(new URL(location.href))
  }

  hostDisconnected(): void {
    this.#stop.abort()
  }

  /** Goes to a path of this site, as a link click would. */
  go(path: string): void {
    const url = new URL(path, location.origin)
    if ("navigation" in window) {
      ;(window.navigation as unknown as { navigate: (url: string) => unknown }).navigate(url.href)
    } else {
      history.pushState(null, "", url)
      void this.#show(url)
    }
  }

  #show(url: URL): Promise<void> {
    const match = matchRoute(this.#routes, url) ?? { route: this.#notFound, params: {} }
    return match.route.enter().then(() => {
      const apply = (): void => {
        this.outlet = match.route.template(match.params)
        this.path = url.pathname
        document.title = `${match.route.title} · ${this.#site}`
        this.#host.requestUpdate()
      }
      if ("startViewTransition" in document && !matchMedia("(prefers-reduced-motion: reduce)").matches) {
        document.startViewTransition(() => {
          apply()
          return this.#host.updateComplete.then(() => undefined)
        })
      } else {
        apply()
      }
    })
  }

  readonly #onClick = (event: MouseEvent): void => {
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
    const anchor = event.composedPath().find((el): el is HTMLAnchorElement => el instanceof HTMLAnchorElement)
    if (!anchor || anchor.origin !== location.origin || anchor.target !== "" || anchor.hasAttribute("download")) return
    if (anchor.pathname === location.pathname && anchor.hash !== "") return
    event.preventDefault()
    history.pushState(null, "", anchor.href)
    void this.#show(new URL(anchor.href))
  }
}
