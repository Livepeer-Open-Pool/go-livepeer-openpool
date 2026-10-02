import { Api, type Earnings, membershipEverywhere, sumWei } from "@openpool/core"
import { eth, regionNotices, renderQuery, SitePage } from "@openpool/ui"
import { Array, Option, Order, pipe } from "effect"
import { html } from "lit"
import { customElement } from "lit/decorators.js"
import { memberSections } from "../lib.js"

/** What the member's machines earned in fees, by round and app, in every region. */
@customElement("earnings-page")
export class EarningsPage extends SitePage {
  readonly #earnings = this.query(() => membershipEverywhere(Api.myEarnings))

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Earnings</h1>
          <p class="muted">
            These are the fees customers paid for work your machines did. They decide your share: at the end of each payout
            window the region's pot, less the pool's commission, is split among members in proportion to their fees.
          </p>
        </header>
        ${renderQuery(this.#earnings, (results) => html`${regionNotices(results)} ${memberSections(results, (_region, detail) => this.#region(detail))}`)}
      </div>
    `
  }

  #region(earnings: Earnings) {
    const rows = pipe(
      earnings.earnings,
      Array.sortBy(
        Order.reverse(Order.mapInput(Order.number, (e: Earnings["earnings"][number]) => e.round)),
        Order.mapInput(Order.string, (e: Earnings["earnings"][number]) => e.app),
      ),
    )
    return html`
      ${Option.match(earnings.window, {
        onNone: () => html`<p class="muted">No payout window is running in this region yet. The first one starts with the region's first paid work.</p>`,
        onSome: (window) => html`<p>The payout window now running covers rounds <strong class="num">${window.first_round}</strong> to <strong class="num">${window.last_round}</strong>.</p>`,
      })}
      ${rows.length === 0
        ? html`<p class="muted">Nothing earned here yet.</p>`
        : html`
            <div class="table-scroll">
              <table>
                <caption>Fees by round and app</caption>
                <thead>
                  <tr><th scope="col" class="num">Round</th><th scope="col">App</th><th scope="col" class="num">Sessions</th><th scope="col" class="num">Fees</th></tr>
                </thead>
                <tbody>
                  ${rows.map((e) => html`
                    <tr>
                      <th scope="row" class="num">${e.round}</th>
                      <td><code translate="no">${e.app}</code></td>
                      <td class="num">${e.sessions}</td>
                      <td class="num">${eth(e.fee_wei)}</td>
                    </tr>
                  `)}
                </tbody>
                <tfoot>
                  <tr>
                    <th scope="row" colspan="2">Total</th>
                    <td class="num">${rows.reduce((total, e) => total + e.sessions, 0)}</td>
                    <td class="num">${eth(sumWei(rows.map((e) => e.fee_wei)))}</td>
                  </tr>
                </tfoot>
              </table>
            </div>
          `}
    `
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "earnings-page": EarningsPage
  }
}
