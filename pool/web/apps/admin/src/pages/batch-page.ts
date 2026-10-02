import { TaskStatus } from "@lit/task"
import { Api, type Batch, BatchId, type BatchItem, formatBasisPoints, parseInput, siteConfig } from "@openpool/core"
import { actionStatus, badge, type ConfirmRequest, eth, renderQuery, shortAddress, SitePage, time } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement, property, state } from "lit/decorators.js"
import { inRegion } from "../lib.js"
import { batchNote } from "./payouts-page.js"

const load = (region: string, id: string) =>
  Effect.all({
    explorer: Effect.map(siteConfig, (config) => config.explorer),
    batch: Effect.flatMap(parseInput(BatchId, "The batch's id")(id), (batchId) => inRegion(region, Api.batch(batchId))),
  })

/** One payout window: who is paid what, and approving it. */
@customElement("batch-page")
export class BatchPage extends SitePage {
  @property() region = ""
  @property() batch = ""
  @state() private confirm: Option.Option<ConfirmRequest> = Option.none()

  readonly #batch = this.query(load, () => [this.region, this.batch] as const)
  readonly #refresh = (): void => void this.#batch.run()
  readonly #include = this.action((batch: Batch, item: BatchItem, include: boolean) => inRegion(this.region, Api.setBatchItem(batch.id, item.member_id, include)), this.#refresh)
  readonly #approve = this.action((batch: Batch) => inRegion(this.region, Api.approveBatch(batch.id)), this.#refresh)
  readonly #retry = this.action((batch: Batch) => inRegion(this.region, Api.retryBatch(batch.id)), this.#refresh)

  override render() {
    return html`
      <div class="stack">
        <p><a href="/payouts">All payout windows</a></p>
        ${renderQuery(this.#batch, ({ batch, explorer }) => this.#detail(batch, explorer))}
        ${actionStatus(this.#include)} ${actionStatus(this.#approve)} ${actionStatus(this.#retry)}
        <op-confirm .request=${this.confirm} @closed=${() => (this.confirm = Option.none())}></op-confirm>
      </div>
    `
  }

  #detail(batch: Batch, explorer: string) {
    const editable = batch.status === "ready"
    const included = batch.items.filter((item) => item.include)
    return html`
      <header class="stack-s">
        <h1>Rounds ${batch.first_round}–${batch.last_round} in ${this.region}</h1>
        <p>${badge(batch.status)} ${batchNote[batch.status]}</p>
        ${Option.match(batch.note, { onNone: () => nothing, onSome: (note) => html`<p class="notice warn">${note}</p>` })}
      </header>
      <dl class="facts card">
        <dt>Pot: what the orchestrator redeemed</dt>
        <dd>${eth(batch.pot_wei)}</dd>
        <dt>Pool commission</dt>
        <dd>${formatBasisPoints(batch.commission_bps)}</dd>
        <dt>To members</dt>
        <dd>${eth(batch.payable_wei)}</dd>
        <dt>Fees members earned</dt>
        <dd>${eth(batch.total_fees_wei)}</dd>
        ${batch.other_fees_wei > 0n ? html`<dt>Fees earned by runners that are no member's</dt><dd>${eth(batch.other_fees_wei)}</dd>` : nothing}
        ${Option.match(batch.closed_at, { onNone: () => nothing, onSome: (at) => html`<dt>Closed</dt><dd>${time(at)}</dd>` })}
        ${Option.match(batch.approved_at, {
          onNone: () => nothing,
          onSome: (at) => html`<dt>Approved</dt><dd>${time(at)} by ${Option.getOrElse(batch.approved_by, () => "the pool")}</dd>`,
        })}
        ${Option.match(batch.sent_at, { onNone: () => nothing, onSome: (at) => html`<dt>Sent</dt><dd>${time(at)}</dd>` })}
      </dl>

      ${batch.items.length === 0
        ? html`<p class="muted">No member earned fees in this window.</p>`
        : html`
            <div class="table-scroll">
              <table>
                <caption>Payments</caption>
                <thead>
                  <tr>
                    <th scope="col">Member</th><th scope="col" class="num">Fees earned</th><th scope="col" class="num">Payment</th>
                    <th scope="col">Status</th><th scope="col">Pay?</th>
                  </tr>
                </thead>
                <tbody>
                  ${batch.items.map((item) => html`
                    <tr>
                      <th scope="row">
                        <a href="/members/${this.region}/${item.member_id}">${item.display_name}</a> ${item.banned ? badge("banned") : nothing}
                        <div class="small">${shortAddress(item.address)}</div>
                      </th>
                      <td class="num">${eth(item.fees_wei)}</td>
                      <td class="num">${eth(item.amount_wei)}</td>
                      <td>
                        ${item.include ? badge(item.status) : badge("left out", "warn")}
                        ${Option.match(item.tx_hash, {
                          onNone: () => nothing,
                          onSome: (hash) => html`<a class="small" href="${explorer}/tx/${hash}" rel="external noopener" target="_blank"><code translate="no">${hash.slice(0, 10)}…</code></a>`,
                        })}
                        ${Option.match(item.error, { onNone: () => nothing, onSome: (error) => html`<div class="small">${error}</div>` })}
                      </td>
                      <td>
                        <label class="inline">
                          <input
                            type="checkbox"
                            ?checked=${item.include}
                            ?disabled=${!editable}
                            @change=${(event: Event) => void this.#include.run([batch, item, (event.target as HTMLInputElement).checked])}
                          />
                          <span class="visually-hidden">Pay ${item.display_name}</span>
                        </label>
                      </td>
                    </tr>
                  `)}
                </tbody>
              </table>
            </div>
            <p class="small muted">A member left out is not paid in this batch, and their share stays in the payout wallet.</p>
          `}

      ${batch.status === "ready"
        ? html`<p><button type="button" class="primary" ?disabled=${this.#approve.status === TaskStatus.PENDING} @click=${() => this.#askApprove(batch, included.length)}>Approve this batch</button></p>`
        : nothing}
      ${batch.status === "failed" || batch.status === "blocked"
        ? html`<p><button type="button" ?disabled=${this.#retry.status === TaskStatus.PENDING} @click=${() => void this.#retry.run([batch])}>Try the unpaid payments again</button></p>`
        : nothing}
    `
  }

  #askApprove(batch: Batch, payments: number): void {
    this.confirm = Option.some({
      heading: `Approve the payout for rounds ${batch.first_round}–${batch.last_round}?`,
      message: `${payments === 1 ? "One payment is" : `${payments} payments are`} sent from the region's payout wallet, each as its own transaction. A payment that has been sent cannot be taken back.`,
      confirm: "Approve and pay",
      danger: false,
      reason: "none",
      run: () => void this.#approve.run([batch]),
    })
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "batch-page": BatchPage
  }
}
