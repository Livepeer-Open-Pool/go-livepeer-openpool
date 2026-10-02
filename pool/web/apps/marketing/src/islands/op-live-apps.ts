import { Api, everyRegion, type Gpu, type PublicApp, type RegionConfig } from "@openpool/core"
import { formatPrice, plural, regionNotices, regionSections, renderQuery, SitePage } from "@openpool/ui"
import { Effect } from "effect"
import { html } from "lit"
import { customElement } from "lit/decorators.js"

const offer = (region: RegionConfig) =>
  Effect.all({ apps: Effect.map(Api.publicApps(region), (a) => a.apps), gpus: Effect.map(Api.publicGpus(region), (g) => g.gpus) }, { concurrency: 2 })

const modes: Readonly<Record<PublicApp["mode"], string>> = {
  "single-shot": "one request, one answer",
  persistent: "a session that stays open",
}

/** Every region's apps: what each is, what customers pay, which GPUs run it, and how many machines serve it now. */
@customElement("op-live-apps")
export class OpLiveApps extends SitePage {
  readonly #offers = this.query(() => everyRegion(offer))

  override render() {
    return html`
      <div class="stack">
        ${renderQuery(this.#offers, (results) => html`
          ${regionNotices(results)}
          ${regionSections(results, (region, { apps, gpus }) =>
            apps.length === 0
              ? html`<p class="muted">${region.name} offers no apps yet.</p>`
              : html`
                  <div class="table-scroll">
                    <table>
                      <caption>Apps served in ${region.name}</caption>
                      <thead>
                        <tr>
                          <th scope="col">App</th><th scope="col">Price</th><th scope="col">Runs on</th><th scope="col" class="num">Machines connected</th>
                        </tr>
                      </thead>
                      <tbody>
                        ${apps.map((app) => html`
                          <tr>
                            <th scope="row">
                              <code translate="no">${app.name}</code>
                              <div class="small muted">${modes[app.mode]}</div>
                              ${app.description === "" ? "" : html`<p class="small">${app.description}</p>`}
                            </th>
                            <td>${formatPrice(app.price)}</td>
                            <td>${this.#gpus(app, gpus)}</td>
                            <td class="num">${plural(app.runners, "machine")}</td>
                          </tr>
                        `)}
                      </tbody>
                    </table>
                  </div>
                `,
          )}
        `)}
      </div>
    `
  }

  #gpus(app: PublicApp, gpus: ReadonlyArray<Gpu>) {
    if (app.eligible_gpus.length === 0) return html`any accepted GPU`
    const names = gpus.filter((gpu) => app.eligible_gpus.includes(gpu.id)).map((gpu) => gpu.name)
    return names.length === 0 ? html`<span class="muted">models no longer listed</span>` : html`<ul class="plain">${names.map((name) => html`<li>${name}</li>`)}</ul>`
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-live-apps": OpLiveApps
  }
}
