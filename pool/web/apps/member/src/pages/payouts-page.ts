import { Api, type MemberPayout, membershipEverywhere, siteConfig } from "@openpool/core"
import { badge, eth, regionNotices, renderQuery, SitePage } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement } from "lit/decorators.js"
import { memberSections } from "../lib.js"

const payouts = Effect.all({
  explorer: Effect.map(siteConfig, (config) => config.explorer),
  regions: membershipEverywhere(Api.myPayouts),
})

/** What a payment's state means for the member. */
const paymentNote = (payout: MemberPayout): string => {
  if (!payout.include) return "left out of this batch by the operator"
  if (payout.status === "confirmed") return "paid"
  if (payout.status === "failed") return "the payment failed; the operator has to send it again"
  if (payout.batch_status === "ready") return "waiting for the operator's approval"
  if (payout.batch_status === "blocked") return "the pool's payout wallet needs funding"
  if (payout.status === "broadcast" || payout.status === "signed") return "sent, waiting for the network to confirm"
  return "queued"
}

/** Every payout window the member was part of, in every region, and what became of each payment. */
@customElement("payouts-page")
export class PayoutsPage extends SitePage {
  readonly #payouts = this.query(() => payouts)

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Payouts</h1>
          <p class="muted">
            A region pays once per payout window, in ETH on Arbitrum, to the address you signed in with. The pool pays the
            transaction fee. There is no minimum: whatever your share is, it is sent.
          </p>
        </header>
        ${renderQuery(this.#payouts, ({ explorer, regions }) => html`
          ${regionNotices(regions)}
          ${memberSections(regions, (_region, detail) =>
            detail.payouts.length === 0
              ? html`<p class="muted">No payout window has closed with earnings of yours here yet.</p>`
              : html`
                  <div class="table-scroll">
                    <table>
                      <caption>Payout windows</caption>
                      <thead>
                        <tr>
                          <th scope="col">Rounds</th><th scope="col" class="num">Your fees</th><th scope="col" class="num">Your payout</th>
                          <th scope="col">Payment</th><th scope="col">Transaction</th>
                        </tr>
                      </thead>
                      <tbody>
                        ${detail.payouts.map((p) => html`
                          <tr>
                            <th scope="row" class="num">${p.first_round}–${p.last_round}</th>
                            <td class="num">${eth(p.fees_wei)}</td>
                            <td class="num">${eth(p.amount_wei)}</td>
                            <td>${badge(p.include ? p.status : "left out", p.include ? undefined : "warn")} <span class="small muted">${paymentNote(p)}</span></td>
                            <td>
                              ${Option.match(p.tx_hash, {
                                onNone: () => nothing,
                                onSome: (hash) => html`<a href="${explorer}/tx/${hash}" rel="external noopener" target="_blank"><code translate="no">${hash.slice(0, 10)}…</code></a>`,
                              })}
                            </td>
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
    "payouts-page": PayoutsPage
  }
}
