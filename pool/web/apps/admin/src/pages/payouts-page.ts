import { Api, type Batch, formatBasisPoints, type WalletStatus } from "@openpool/core"
import { badge, eth, regionNotices, regionSections, renderQuery, SitePage } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement } from "lit/decorators.js"
import { everywhere } from "../lib.js"

interface RegionPayouts {
  readonly wallet: WalletStatus
  readonly batches: ReadonlyArray<Batch>
}

const payouts: Api.AskAs<RegionPayouts> = (region, token) =>
  Effect.all({ wallet: Api.payoutWallet(region, token), batches: Effect.map(Api.batches(region, token), (b) => b.batches) }, { concurrency: 2 })

/** What each status means for the operator. */
export const batchNote: Readonly<Record<Batch["status"], string>> = {
  open: "the window is still running",
  ready: "waiting for your approval",
  approved: "approved, about to be sent",
  sending: "payments are going out",
  blocked: "the payout wallet cannot cover what is left",
  failed: "some payments failed and need you",
  sent: "every payment confirmed",
  empty: "closed with nothing to pay",
}

/** Each region's payout wallet and its payout windows, newest first. */
@customElement("payouts-page")
export class PayoutsPage extends SitePage {
  readonly #payouts = this.query(() => everywhere(payouts))

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Payouts</h1>
          <p class="muted">
            A window's pot is what the region's orchestrator redeemed in it. The pool keeps its commission and the rest is split
            among members by the fees their machines earned. Each region pays from its own wallet.
          </p>
        </header>
        ${renderQuery(this.#payouts, (results) => html`
          ${regionNotices(results)}
          ${regionSections(results, (region, detail) => html`${this.#wallet(detail.wallet)} ${this.#batches(region.id, detail.batches)}`)}
        `)}
      </div>
    `
  }

  #wallet(wallet: WalletStatus) {
    if (!wallet.configured) {
      return html`<p class="notice warn">This region has no payout wallet. Batches are built and can be approved, and nothing is paid. ${eth(wallet.awaiting_approval_wei)} is waiting for approval.</p>`
    }
    return html`
      <dl class="facts card">
        <dt>Payout wallet</dt>
        <dd><code translate="no">${Option.getOrElse(wallet.address, () => "unknown")}</code></dd>
        <dt>Balance</dt>
        <dd>${Option.match(wallet.balance_wei, { onNone: () => html`<span class="muted">not known</span>`, onSome: (b) => eth(b) })}</dd>
        <dt>Owed in approved batches</dt>
        <dd>${eth(wallet.owed_wei)}</dd>
        <dt>Waiting for approval</dt>
        <dd>${eth(wallet.awaiting_approval_wei)}</dd>
        ${Option.match(wallet.error, { onNone: () => nothing, onSome: (error) => html`<dt>Problem</dt><dd>${error}</dd>` })}
      </dl>
    `
  }

  #batches(region: string, batches: ReadonlyArray<Batch>) {
    if (batches.length === 0) return html`<p class="muted">No payout window has started here. The first starts with the region's first paid work.</p>`
    return html`
      <div class="table-scroll">
        <table>
          <caption>Payout windows</caption>
          <thead>
            <tr>
              <th scope="col">Rounds</th><th scope="col">Status</th><th scope="col" class="num">Pot</th>
              <th scope="col" class="num">Commission</th><th scope="col" class="num">To members</th>
            </tr>
          </thead>
          <tbody>
            ${batches.map((batch) => html`
              <tr>
                <th scope="row" class="num"><a href="/payouts/${region}/${batch.id}">${batch.first_round}–${batch.last_round}</a></th>
                <td>${badge(batch.status)} <span class="small muted">${batchNote[batch.status]}</span></td>
                <td class="num">${eth(batch.pot_wei)}</td>
                <td class="num">${formatBasisPoints(batch.commission_bps)}</td>
                <td class="num">${eth(batch.payable_wei)}</td>
              </tr>
            `)}
          </tbody>
        </table>
      </div>
    `
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "payouts-page": PayoutsPage
  }
}
