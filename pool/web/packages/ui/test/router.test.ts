import { html, LitElement } from "lit"
import { afterEach, describe, expect, it } from "vitest"
import { matchRoute, primaryNav, type Route, Router } from "../src/index.js"
import { settle, text } from "./kit.js"

const page = (path: string, title: string, body: (params: Readonly<Record<string, string>>) => string): Route => ({
  path, title, enter: () => Promise.resolve(), template: (params) => html`<p id="page">${body(params)}</p>`,
})

const routes: ReadonlyArray<Route> = [
  page("/", "Overview", () => "overview"),
  page("/machines", "Machines", () => "machines"),
  page("/members/:region/:id", "Member", (p) => `member ${p["id"]} in ${p["region"]}`),
]
const notFound = page("*", "Not found", () => "nothing here")

class RoutedApp extends LitElement {
  router = new Router(this, { routes, site: "Open Pool", notFound })
  override render() {
    return html`${primaryNav([{ href: "/", label: "Overview" }, { href: "/machines", label: "Machines" }, { href: "/members", label: "Members" }], this.router.path)}
      <main>${this.router.outlet}</main>`
  }
}
customElements.define("routed-app", RoutedApp)

const start = async (path: string): Promise<RoutedApp> => {
  history.replaceState(null, "", path)
  const app = document.createElement("routed-app") as RoutedApp
  document.body.append(app)
  await settle(app)
  return app
}

const original = location.pathname + location.search
afterEach(() => {
  document.querySelectorAll("routed-app").forEach((app) => app.remove())
  history.replaceState(null, "", original)
})

describe("matchRoute", () => {
  it("finds the page for a path and what its parameters matched", () => {
    const match = matchRoute(routes, new URL("https://admin.open-pool.com/members/us-east/mem%5F1"))
    expect(match?.route.title).toBe("Member")
    expect(match?.params).toEqual({ region: "us-east", id: "mem_1" })
    expect(matchRoute(routes, new URL("https://admin.open-pool.com/nowhere"))).toBeUndefined()
  })
})

describe("the router", () => {
  it("opens the page the address names, titles the document and marks the current link", async () => {
    const app = await start("/machines")
    expect(text(app.shadowRoot?.querySelector("#page"))).toBe("machines")
    expect(document.title).toBe("Machines · Open Pool")
    const current = [...(app.shadowRoot?.querySelectorAll("nav a") ?? [])].filter((a) => a.getAttribute("aria-current") === "page")
    expect(current.map(text)).toEqual(["Machines"])
  })

  it("changes page on a link click without loading the document again", async () => {
    const app = await start("/")
    expect(text(app.shadowRoot?.querySelector("#page"))).toBe("overview")
    const marker = (window as unknown as { marker?: number }).marker = Math.random()
    app.shadowRoot?.querySelector<HTMLAnchorElement>('nav a[href="/machines"]')?.click()
    await new Promise((resolve) => setTimeout(resolve, 100))
    await settle(app)
    expect(location.pathname).toBe("/machines")
    expect(text(app.shadowRoot?.querySelector("#page"))).toBe("machines")
    expect((window as unknown as { marker?: number }).marker).toBe(marker)
  })

  it("goes where it is told, with parameters, and marks the section a page is in", async () => {
    const app = await start("/")
    app.router.go("/members/eu-west/mem_9")
    await new Promise((resolve) => setTimeout(resolve, 100))
    await settle(app)
    expect(text(app.shadowRoot?.querySelector("#page"))).toBe("member mem_9 in eu-west")
    const current = [...(app.shadowRoot?.querySelectorAll("nav a") ?? [])].filter((a) => a.getAttribute("aria-current") === "page")
    expect(current.map(text)).toEqual(["Members"])
  })

  it("shows the not-found page for an address that is no page", async () => {
    const app = await start("/nowhere")
    expect(text(app.shadowRoot?.querySelector("#page"))).toBe("nothing here")
    expect(document.title).toBe("Not found · Open Pool")
  })
})
