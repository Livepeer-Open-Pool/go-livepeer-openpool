import { ContextProvider } from "@lit/context"
import {
  type ApiError,
  type ApiRequest,
  PoolApi,
  type RegionConfig,
  SessionStoreMemory,
  SiteConfig,
  type SiteRuntime,
  SiteSettings,
  Wallet,
  WalletMissing,
} from "@openpool/core"
import { Effect, Layer, ManagedRuntime, Schema } from "effect"
import { html, LitElement, render, type TemplateResult } from "lit"
import { runtimeContext } from "../src/index.js"

export const config = Schema.decodeSync(SiteConfig)({
  regions: [
    { id: "us-east", name: "US East", api: "https://us-east.api.open-pool.com" },
    { id: "eu-west", name: "EU West", api: "https://eu-west.api.open-pool.com" },
  ],
  sites: { marketing: "https://open-pool.com", member: "https://app.open-pool.com", admin: "https://admin.open-pool.com" },
  explorer: "https://arbiscan.io",
})

/** A runtime whose regions answer from a function. */
export const runtimeAnswering = (
  answer: (region: RegionConfig, request: ApiRequest) => Effect.Effect<unknown, ApiError>,
): SiteRuntime =>
  ManagedRuntime.make(
    Layer.mergeAll(
      Layer.succeed(PoolApi, {
        send: <A, I>(region: RegionConfig, request: ApiRequest, schema: Schema.Schema<A, I>) =>
          Effect.flatMap(answer(region, request), (value) => Effect.orDie(Schema.decodeUnknown(schema)(value))),
      }),
      Layer.succeed(SiteSettings, { config, location: { host: "app.open-pool.com", origin: "https://app.open-pool.com" } }),
      SessionStoreMemory,
      Layer.succeed(Wallet, { connect: Effect.fail(new WalletMissing()), sign: () => Effect.fail(new WalletMissing()) }),
    ),
  )

/** Stands where a site's root would: it gives the elements inside it the runtime. */
export class TestSite extends LitElement {
  readonly provider = new ContextProvider(this, { context: runtimeContext })
  override render() {
    return html`<slot></slot>`
  }
}
customElements.define("test-site", TestSite)

/** Puts a template on the page and waits for every element in it to render. */
export const mount = async <T extends Element>(template: TemplateResult, runtime?: SiteRuntime): Promise<T> => {
  const site = document.createElement("test-site") as TestSite
  if (runtime) site.provider.setValue(runtime)
  document.body.append(site)
  render(template, site)
  const element = site.firstElementChild as T
  await settle(element)
  return element
}

/** Waits until an element has nothing left to render. */
export const settle = async (element: Element): Promise<void> => {
  for (let i = 0; i < 20; i++) {
    await (element as Partial<LitElement>).updateComplete
    await new Promise((resolve) => setTimeout(resolve, 0))
  }
}

export const cleanup = (): void => {
  document.querySelectorAll("test-site").forEach((site) => site.remove())
}

export const text = (root: ParentNode | null | undefined): string => (root?.textContent ?? "").replace(/\s+/g, " ").trim()
