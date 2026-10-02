import { Api, everyRegion, NoSession, RegionUnreachable, RequestRefused, type SiteRuntime } from "@openpool/core"
import { Effect } from "effect"
import { html, type TemplateResult } from "lit"
import { afterEach, beforeEach, describe, expect, it } from "vitest"
import { actionStatus, badge, regionNotices, regionSections, renderQuery, resetSession, sessionState, SitePage } from "../src/index.js"
import { cleanup, mount, runtimeAnswering, settle, text } from "./kit.js"

const pool = (id: string, name: string, apps: number) => ({
  region: { id, name }, taking_applications: true, apps_offered: apps, sign_in: { domains: [], chain_id: 42161 }, commission_bps: 1500, window_rounds: 7,
})

class PoolsPage extends SitePage {
  pools = this.query(() => everyRegion(Api.pool))
  refuse = this.action(
    (why: string) => Effect.fail(why === "session" ? new NoSession() : new RequestRefused({ region: "us-east" as never, status: 409, message: why })),
  )
  saved: Array<string> = []
  save = this.action((what: string) => Effect.succeed(what.toUpperCase()), (value, what) => this.saved.push(`${what}->${value}`))

  override render(): TemplateResult {
    return html`
      ${renderQuery(this.pools, (results) => html`
        ${regionNotices(results)}
        ${regionSections(results, (_region, info) => html`<p class="offered">${info.apps_offered} apps ${badge("active")}</p>`)}
      `)}
      <div id="status">${actionStatus(this.refuse)}</div>
    `
  }
}
customElements.define("pools-page", PoolsPage)

const open = async (runtime: SiteRuntime): Promise<PoolsPage> => mount<PoolsPage>(html`<pools-page></pools-page>`, runtime)

beforeEach(resetSession)
afterEach(cleanup)

describe("a page", () => {
  it("shows each region that answered under its name", async () => {
    const page = await open(runtimeAnswering((region) => Effect.succeed(pool(region.id, region.name, region.id === "us-east" ? 2 : 5))))
    const sections = [...(page.shadowRoot?.querySelectorAll("section") ?? [])]
    expect(sections.map((s) => text(s.querySelector("h2")))).toEqual(["US East", "EU West"])
    expect(sections.map((s) => text(s.querySelector(".offered")))).toEqual(["2 apps active", "5 apps active"])
    expect(sections[0]?.getAttribute("aria-labelledby")).toBe(sections[0]?.querySelector("h2")?.id)
    expect(page.shadowRoot?.querySelector(".badge")?.classList.contains("good")).toBe(true)
    expect(page.shadowRoot?.querySelector(".notice")).toBeNull()
  })

  it("still shows the rest when a region is down, and names the one that is", async () => {
    const page = await open(
      runtimeAnswering((region) =>
        region.id === "eu-west"
          ? Effect.fail(new RegionUnreachable({ region: region.id, reason: "it could not be reached" }))
          : Effect.succeed(pool(region.id, region.name, 2)),
      ),
    )
    expect([...(page.shadowRoot?.querySelectorAll("section h2") ?? [])].map(text)).toEqual(["US East"])
    const notice = page.shadowRoot?.querySelector(".notice.warn")
    expect(notice?.getAttribute("role")).toBe("status")
    expect(text(notice)).toContain("Not every region answered")
    expect(text(notice)).toContain("EU West: The eu-west region did not answer (it could not be reached).")
  })

  it("says so when no region answered", async () => {
    const page = await open(runtimeAnswering((region) => Effect.fail(new RegionUnreachable({ region: region.id, reason: "it could not be reached" }))))
    expect(text(page.shadowRoot?.querySelector(".notice.warn"))).toContain("No region answered.")
    expect(page.shadowRoot?.querySelectorAll("section").length).toBe(0)
  })

  it("keeps the last answer on the page while asking again", async () => {
    let apps = 2
    const page = await open(runtimeAnswering((region) => Effect.delay(Effect.succeed(pool(region.id, region.name, apps)), "30 millis")))
    await new Promise((resolve) => setTimeout(resolve, 80))
    await settle(page)
    expect(text(page.shadowRoot?.querySelector(".offered"))).toBe("2 apps active")
    apps = 3
    void page.pools.run()
    await page.updateComplete
    expect(page.shadowRoot?.querySelector(".skeleton")).toBeNull()
    expect(text(page.shadowRoot?.querySelector(".offered"))).toBe("2 apps active")
    await new Promise((resolve) => setTimeout(resolve, 80))
    await settle(page)
    expect(text(page.shadowRoot?.querySelector(".offered"))).toBe("3 apps active")
  })

  it("shows what an action was refused for, in the service's words", async () => {
    const page = await open(runtimeAnswering((region) => Effect.succeed(pool(region.id, region.name, 2))))
    void page.refuse.run(["machine box-1 has not been approved yet"]).catch(() => undefined)
    await settle(page)
    const alert = page.shadowRoot?.querySelector("#status .notice.bad")
    expect(alert?.getAttribute("role")).toBe("alert")
    expect(text(alert)).toBe("machine box-1 has not been approved yet")
  })

  it("signs the site out when the session turns out to be gone", async () => {
    const page = await open(runtimeAnswering((region) => Effect.succeed(pool(region.id, region.name, 2))))
    expect(sessionState.get()._tag).toBe("Unknown")
    void page.refuse.run(["session"]).catch(() => undefined)
    await settle(page)
    expect(sessionState.get()._tag).toBe("SignedOut")
  })

  it("tells the page when an action is done", async () => {
    const page = await open(runtimeAnswering((region) => Effect.succeed(pool(region.id, region.name, 2))))
    await page.save.run(["terms"])
    expect(page.saved).toEqual(["terms->TERMS"])
  })

  it("says it is not inside a site when it is not", async () => {
    const page = document.createElement("pools-page") as PoolsPage
    document.body.append(page)
    await settle(page)
    expect(text(page.shadowRoot?.querySelector(".notice.bad"))).toContain("Something went wrong on this page.")
    page.remove()
  })
})
