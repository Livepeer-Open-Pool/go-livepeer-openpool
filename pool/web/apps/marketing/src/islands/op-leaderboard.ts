import { answered, Api, everyRegion, mergeLeaderboards } from "@openpool/core"
import { eth, regionNotices, renderQuery, shortAddress, SitePage } from "@openpool/ui"
import { Option } from "effect"
import { html, nothing } from "lit"
import { customElement, state } from "lit/decorators.js"

type Window = "current" | "all"

/** The pool's members ranked by fees earned, one line per member whatever regions they earn in. */
@customElement("op-leaderboard")
export class OpLeaderboard extends SitePage {
  @state() private window: Window = "current"

  readonly #boards = this.query((window: Window) => everyRegion(Api.leaderboard(window, 100)), () => [this.window] as const)

  override render() {
    return html`
      <div class="stack">
        <fieldset>
          <legend>Fees earned</legend>
          <div class="row">
            <label class="inline">
              <input type="radio" name="window" value="current" ?checked=${this.window === "current"} @change=${() => (this.window = "current")} />
              <span>in the payout window now running</span>
            </label>
            <label class="inline">
              <input type="radio" name="window" value="all" ?checked=${this.window === "all"} @change=${() => (this.window = "all")} />
              <span>since the pool began</span>
            </label>
          </div>
        </fieldset>
        ${renderQuery(this.#boards, (results) => {
          const boards = answered(results)
          const standings = mergeLeaderboards(boards.map(({ region, value }) => ({ region, entries: value.entries })))
          return html`
            ${regionNotices(results)}
            ${this.window === "current"
              ? html`
                  <p class="muted small">
                    ${boards.map(({ region, value }) =>
                      Option.match(Option.all([value.first_round, value.last_round]), {
                        onNone: () => html`${region.name} has no payout window running. `,
                        onSome: ([first, last]) => html`${region.name}: rounds ${first} to ${last}. `,
                      }),
                    )}
                  </p>
                `
              : nothing}
            ${standings.length === 0
              ? html`<p class="muted">Nobody has earned fees ${this.window === "current" ? "in this window yet" : "yet"}.</p>`
              : html`
                  <div class="table-scroll">
                    <table>
                      <caption>Members by fees earned</caption>
                      <thead>
                        <tr>
                          <th scope="col" class="num">Rank</th><th scope="col">Member</th><th scope="col" class="num">Fees earned</th>
                          <th scope="col" class="num">Sessions served</th><th scope="col">Regions</th>
                        </tr>
                      </thead>
                      <tbody>
                        ${standings.map((standing, index) => html`
                          <tr>
                            <td class="num">${index + 1}</td>
                            <th scope="row"><bdi>${standing.displayName}</bdi> <span class="small">${shortAddress(standing.address)}</span></th>
                            <td class="num">${eth(standing.fees)}</td>
                            <td class="num">${standing.sessions}</td>
                            <td>${standing.regions.join(", ")}</td>
                          </tr>
                        `)}
                      </tbody>
                    </table>
                  </div>
                `}
          `
        })}
      </div>
    `
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-leaderboard": OpLeaderboard
  }
}
