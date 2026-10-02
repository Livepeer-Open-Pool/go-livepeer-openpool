import { Api } from "@openpool/core"
import { regionNotices, regionSections, renderQuery, SitePage, time } from "@openpool/ui"
import { html } from "lit"
import { customElement } from "lit/decorators.js"
import { everywhere } from "../lib.js"

/** Who did what, when: every operator action and every change the pool made by itself. */
@customElement("audit-page")
export class AuditPage extends SitePage {
  readonly #audit = this.query(() => everywhere(Api.audit(200)))

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Audit log</h1>
          <p class="muted">The last 200 entries of each region, newest first. "pool" is the service acting by itself.</p>
        </header>
        ${renderQuery(this.#audit, (results) => html`
          ${regionNotices(results)}
          ${regionSections(results, (region, { entries }) =>
            entries.length === 0
              ? html`<p class="muted">Nothing has been recorded in ${region.name}.</p>`
              : html`
                  <div class="table-scroll">
                    <table>
                      <caption>What happened in ${region.name}</caption>
                      <thead>
                        <tr><th scope="col">When</th><th scope="col">Who</th><th scope="col">What</th><th scope="col">To</th><th scope="col">Detail</th></tr>
                      </thead>
                      <tbody>
                        ${entries.map((entry) => html`
                          <tr>
                            <td>${time(entry.at)}</td>
                            <td>${entry.actor}</td>
                            <td><code translate="no">${entry.action}</code></td>
                            <td><code translate="no">${entry.subject}</code></td>
                            <td>${entry.detail}</td>
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
}

declare global {
  interface HTMLElementTagNameMap {
    "audit-page": AuditPage
  }
}
