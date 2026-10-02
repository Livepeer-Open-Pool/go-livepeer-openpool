import { Api, type App, regions as allRegions } from "@openpool/core"
import { actionStatus, badge, formatPrice, regionNotices, regionSections, renderQuery, SitePage, time } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement } from "lit/decorators.js"
import { everywhere, inRegion } from "../lib.js"

const page = Effect.all({ regions: allRegions, apps: everywhere(Api.apps) })

/** The apps each region offers: what customers ask for, what it pays, and whether it is offered. */
@customElement("apps-page")
export class AppsPage extends SitePage {
  readonly #page = this.query(() => page)
  readonly #offer = this.action(
    (region: string, app: App, offered: boolean) => inRegion(region, offered ? Api.resumeApp(app.id) : Api.pauseApp(app.id)),
    () => void this.#page.run(),
  )

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Apps</h1>
          <p class="muted">
            An app is what customers ask the orchestrator for by name, the price they pay, the container members' machines run
            to serve it, and the test that shows a runner really does. Apps and prices are per region.
          </p>
        </header>
        ${actionStatus(this.#offer)}
        ${renderQuery(this.#page, ({ regions, apps }) => html`
          ${regionNotices(apps)}
          ${regionSections(apps, (region, detail) => html`
            <p><a class="button" href="/apps/${region.id}/new">New app in ${region.name}</a></p>
            ${detail.apps.length === 0
              ? html`<p class="muted">${region.name} offers no apps yet.</p>`
              : html`
                  <div class="table-scroll">
                    <table>
                      <caption>Apps in ${region.name}</caption>
                      <thead>
                        <tr>
                          <th scope="col">App</th><th scope="col">Price</th><th scope="col">Version</th><th scope="col">Test</th>
                          <th scope="col">Customers</th><th scope="col"><span class="visually-hidden">Actions</span></th>
                        </tr>
                      </thead>
                      <tbody>
                        ${detail.apps.map((app) => html`
                          <tr>
                            <th scope="row">
                              <a href="/apps/${region.id}/${app.id}"><code translate="no">${app.name}</code></a>
                              <div class="small muted">${app.mode}</div>
                            </th>
                            <td>${formatPrice(app.price)}</td>
                            <td>
                              ${app.required_version}
                              ${Option.match(app.previous_version_deadline, {
                                onNone: () => nothing,
                                onSome: (deadline) => html`<div class="small muted">${Option.getOrElse(app.previous_version, () => "the previous one")} accepted until ${time(deadline)}</div>`,
                              })}
                            </td>
                            <td>${app.test.path === "" ? badge("none", "warn") : html`<code translate="no">${app.test.method} ${app.test.path}</code>`}</td>
                            <td>${app.offered ? badge("offered") : badge("paused")}</td>
                            <td class="actions">
                              <button type="button" class="small" @click=${() => void this.#offer.run([region.id, app, !app.offered])}>
                                ${app.offered ? "Pause" : "Resume"}<span class="visually-hidden"> ${app.name} in ${region.name}</span>
                              </button>
                              ${regions
                                .filter((other) => other.id !== region.id)
                                .map((other) => html`<a class="button small" href="/apps/${other.id}/new?from=${region.id}:${app.id}">Copy to ${other.name}</a>`)}
                            </td>
                          </tr>
                        `)}
                      </tbody>
                    </table>
                  </div>
                `}
          `)}
        `)}
      </div>
    `
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "apps-page": AppsPage
  }
}
