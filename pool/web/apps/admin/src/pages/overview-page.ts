import { type AdminStatus, Api, type Batch, type WalletStatus } from "@openpool/core"
import { badge, eth, plural, regionNotices, regionSections, renderQuery, shortAddress, SitePage } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement } from "lit/decorators.js"
import { everywhere } from "../lib.js"

interface RegionOverview {
  readonly status: AdminStatus
  readonly wallet: WalletStatus
  readonly batches: ReadonlyArray<Batch>
}

const overview: Api.AskAs<RegionOverview> = (region, token) =>
  Effect.all(
    { status: Api.adminStatus(region, token), wallet: Api.payoutWallet(region, token), batches: Effect.map(Api.batches(region, token), (b) => b.batches) },
    { concurrency: 3 },
  )

/** Each region's health and what in it is waiting for the operator. */
@customElement("overview-page")
export class OverviewPage extends SitePage {
  readonly #overview = this.query(() => everywhere(overview))

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Overview</h1>
          <p class="muted">Every region is its own pool service beside its own orchestrator. What needs you is listed first.</p>
        </header>
        ${renderQuery(this.#overview, (results) => html`${regionNotices(results)} ${regionSections(results, (region, o) => this.#region(region.id, o))}`)}
      </div>
    `
  }

  #region(region: string, { status, wallet, batches }: RegionOverview) {
    const waiting = batches.filter((b) => b.status === "ready")
    const stuck = batches.filter((b) => b.status === "blocked" || b.status === "failed")
    return html`
      <div class="cards">
        <div class="card stack-s">
          <h3>Waiting for you</h3>
          ${status.pending_applications + status.pending_machines + waiting.length + stuck.length === 0
            ? html`<p class="muted">Nothing is waiting.</p>`
            : html`
                <ul class="stack-s" style="padding-inline-start: 1.1rem">
                  ${status.pending_applications > 0
                    ? html`<li><a href="/applications">${plural(status.pending_applications, "application")}</a> to decide</li>`
                    : nothing}
                  ${status.pending_machines > 0 ? html`<li><a href="/machines">${plural(status.pending_machines, "machine")}</a> to approve</li>` : nothing}
                  ${waiting.map((b) => html`<li><a href="/payouts/${region}/${b.id}">Payout for rounds ${b.first_round}–${b.last_round}</a> to approve: ${eth(b.payable_wei)}</li>`)}
                  ${stuck.map((b) => html`<li><a href="/payouts/${region}/${b.id}">Payout for rounds ${b.first_round}–${b.last_round}</a> is ${badge(b.status)}</li>`)}
                </ul>
              `}
        </div>
        <div class="card stack-s">
          <h3>Orchestrator</h3>
          ${status.orchestrator.reachable
            ? html`<p>${badge("reachable", "good")} <a href="/runners">${plural(status.orchestrator.runners, "runner")}</a> registered.</p>`
            : html`
                <p>${badge("unreachable", "bad")}</p>
                <p class="small">${Option.getOrElse(status.orchestrator.error, () => "The orchestrator's CLI port did not answer.")}</p>
              `}
        </div>
        <div class="card stack-s">
          <h3>Payout wallet</h3>
          ${wallet.configured
            ? html`
                <dl class="facts">
                  <dt>Address</dt>
                  <dd>${Option.match(wallet.address, { onNone: () => "unknown", onSome: shortAddress })}</dd>
                  <dt>Balance</dt>
                  <dd>${Option.match(wallet.balance_wei, { onNone: () => html`<span class="muted">not known</span>`, onSome: (b) => eth(b) })}</dd>
                  <dt>Owed in approved batches</dt>
                  <dd>${eth(wallet.owed_wei)}</dd>
                  <dt>Waiting for approval</dt>
                  <dd>${eth(wallet.awaiting_approval_wei)}</dd>
                </dl>
                ${Option.match(wallet.error, { onNone: () => nothing, onSome: (error) => html`<p class="notice bad small">${error}</p>` })}
              `
            : html`
                <p>${badge("not configured", "warn")}</p>
                <p class="small">Batches are built and can be approved, and nothing is paid until this region is given a payout wallet.</p>
                <dl class="facts">
                  <dt>Waiting for approval</dt>
                  <dd>${eth(wallet.awaiting_approval_wei)}</dd>
                </dl>
              `}
        </div>
      </div>
    `
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "overview-page": OverviewPage
  }
}
