import { Api } from "@openpool/core"
import { badge, day, regionNotices, regionSections, renderQuery, shortAddress, SitePage } from "@openpool/ui"
import { html } from "lit"
import { customElement } from "lit/decorators.js"
import { everywhere } from "../lib.js"

/** Every member of every region. */
@customElement("members-page")
export class MembersPage extends SitePage {
  readonly #members = this.query(() => everywhere(Api.members))

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Members</h1>
          <p class="muted">A person who joined two regions is a member of each, under the same address.</p>
        </header>
        ${renderQuery(this.#members, (results) => html`
          ${regionNotices(results)}
          ${regionSections(results, (region, { members }) =>
            members.length === 0
              ? html`<p class="muted">${region.name} has no members yet.</p>`
              : html`
                  <div class="table-scroll">
                    <table>
                      <caption>Members of ${region.name}</caption>
                      <thead>
                        <tr><th scope="col">Name</th><th scope="col">Address</th><th scope="col">Status</th><th scope="col">Member since</th></tr>
                      </thead>
                      <tbody>
                        ${members.map((member) => html`
                          <tr>
                            <th scope="row"><a href="/members/${region.id}/${member.id}">${member.display_name}</a></th>
                            <td>${shortAddress(member.address)}</td>
                            <td>${badge(member.status)}</td>
                            <td>${day(member.created_at)}</td>
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
    "members-page": MembersPage
  }
}
