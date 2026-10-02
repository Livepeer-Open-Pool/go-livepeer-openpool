import { Api, everyRegion, formatBasisPoints } from "@openpool/core"
import { answered } from "@openpool/core"
import { badge, plural, regionNotices, renderQuery, SitePage } from "@openpool/ui"
import { html } from "lit"
import { customElement } from "lit/decorators.js"

/** Each region's payout terms and whether it takes applications, as the regions say them now. */
@customElement("op-pool-facts")
export class OpPoolFacts extends SitePage {
  readonly #pools = this.query(() => everyRegion(Api.pool))

  override render() {
    return renderQuery(this.#pools, (results) => {
      const regions = answered(results)
      return html`
        <div class="stack-s">
          ${regionNotices(results)}
          ${regions.length === 0
            ? ""
            : html`
                <div class="table-scroll">
                  <table>
                    <caption>The pool's regions</caption>
                    <thead>
                      <tr>
                        <th scope="col">Region</th><th scope="col">Applications</th><th scope="col" class="num">Pool commission</th>
                        <th scope="col" class="num">Payout window</th><th scope="col" class="num">Apps offered</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${regions.map(({ region, value }) => html`
                        <tr>
                          <th scope="row">${region.name}</th>
                          <td>${value.taking_applications ? badge("open", "good") : badge("not open yet", "warn")}</td>
                          <td class="num">${formatBasisPoints(value.commission_bps)}</td>
                          <td class="num">${plural(value.window_rounds, "round")}</td>
                          <td class="num">${value.apps_offered}</td>
                        </tr>
                      `)}
                    </tbody>
                  </table>
                </div>
                <p class="muted small">A round is a Livepeer round, about a day. Members share what is left after the commission.</p>
              `}
        </div>
      `
    })
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-pool-facts": OpPoolFacts
  }
}
