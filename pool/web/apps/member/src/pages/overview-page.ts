import { Api, type Application, type Machine, type Me, membershipEverywhere, type RegionConfig } from "@openpool/core"
import { badge, day, plural, regionNotices, regionSections, renderQuery, SitePage, time } from "@openpool/ui"
import { Array, Option } from "effect"
import { html, nothing } from "lit"
import { customElement } from "lit/decorators.js"

/** Where the member stands in every region: member, applicant, or neither. */
@customElement("overview-page")
export class OverviewPage extends SitePage {
  readonly #standing = this.query(() => membershipEverywhere(Api.myMachines))

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Your overview</h1>
          <p class="muted">Each region of the pool is its own membership, with its own machines, earnings and payouts.</p>
        </header>
        ${renderQuery(this.#standing, (results) => html`
          ${regionNotices(results)}
          ${regionSections(results, (region, membership) =>
            Option.match(membership.me.member, {
              onNone: () => this.#applicant(region, membership.me),
              onSome: (member) =>
                this.#member(
                  region,
                  member,
                  Option.match(membership.detail, { onNone: () => [], onSome: (detail) => detail.machines }),
                ),
            }),
          )}
        `)}
      </div>
    `
  }

  #member(region: RegionConfig, member: NonNullable<Option.Option.Value<Me["member"]>>, machines: ReadonlyArray<Machine>) {
    const online = machines.filter((m) => Option.isSome(m.last_seen_at) && m.status === "approved").length
    const waiting = machines.filter((m) => m.status === "pending").length
    if (member.status === "banned") {
      return html`
        <div class="notice bad">
          <p><strong>This membership is banned.</strong> ${Option.getOrElse(member.reason, () => "The operator gave no reason.")}</p>
          <p>Your runners in ${region.name} were removed and the machine credentials no longer work.</p>
        </div>
      `
    }
    return html`
      <div class="card stack-s">
        <p>${badge(member.status)} Member as <strong>${member.display_name}</strong> since ${day(member.created_at)}.</p>
        <dl class="facts">
          <dt>Machines</dt>
          <dd>
            <a href="/machines">${plural(machines.length, "machine")}</a>${waiting > 0 ? html`, ${waiting} waiting for approval` : nothing}
          </dd>
          <dt>Agents heard from</dt>
          <dd>${online} of ${machines.filter((m) => m.status === "approved").length} approved</dd>
        </dl>
        <p class="row">
          <a class="button" href="/runners">Runners and tests</a>
          <a class="button" href="/earnings">Earnings</a>
          <a class="button" href="/payouts">Payouts</a>
        </p>
      </div>
    `
  }

  #applicant(region: RegionConfig, me: Me) {
    return Option.match(Array.head(me.applications), {
      onNone: () => html`
        <div class="card stack-s">
          <p>You have not applied in ${region.name}.</p>
          <p><a class="button primary" href="/apply?region=${region.id}">Apply to ${region.name}</a></p>
        </div>
      `,
      onSome: (application) => this.#application(region, application),
    })
  }

  #application(region: RegionConfig, application: Application) {
    if (application.status === "pending") {
      return html`
        <div class="notice warn stack-s">
          <p>
            ${badge("pending")} Your application as <strong>${application.display_name}</strong>, sent ${time(application.created_at)},
            is waiting for the operator. There is nothing you need to do until it is decided.
          </p>
          <p class="small muted">It lists ${plural(application.machines.length, "machine")}.</p>
        </div>
      `
    }
    if (application.status === "rejected") {
      return html`
        <div class="notice bad stack-s">
          <p>
            ${badge("rejected")} Your application was not accepted${Option.match(application.decided_at, {
              onNone: () => nothing,
              onSome: (at) => html` on ${day(at)}`,
            })}.
          </p>
          <p>${Option.getOrElse(application.reason, () => "The operator gave no reason.")}</p>
          <p><a class="button" href="/apply?region=${region.id}">Apply again</a></p>
        </div>
      `
    }
    return html`<p class="muted">Your application was approved. <a href="/machines">Go to your machines</a>.</p>`
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "overview-page": OverviewPage
  }
}
