import { Api, type Member, type MemberDetail, MemberId, parseInput } from "@openpool/core"
import { actionStatus, badge, type ConfirmRequest, day, plural, renderQuery, SitePage, time } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement, property, state } from "lit/decorators.js"
import { inRegion } from "../lib.js"

const memberDetail = (region: string, id: string) =>
  Effect.flatMap(parseInput(MemberId, "The member's id")(id), (memberId) => inRegion(region, Api.member(memberId)))

/** One member of one region: their machines and applications, and banning them. */
@customElement("member-page")
export class MemberPage extends SitePage {
  @property() region = ""
  @property() member = ""
  @state() private confirm: Option.Option<ConfirmRequest> = Option.none()

  readonly #detail = this.query(memberDetail, () => [this.region, this.member] as const)
  readonly #refresh = (): void => void this.#detail.run()
  readonly #ban = this.action((member: Member, reason: string) => inRegion(this.region, Api.banMember(member.id, reason)), this.#refresh)
  readonly #unban = this.action((member: Member) => inRegion(this.region, Api.unbanMember(member.id)), this.#refresh)

  override render() {
    return html`
      <div class="stack">
        <p><a href="/members">All members</a></p>
        ${renderQuery(this.#detail, (detail) => this.#member(detail))} ${actionStatus(this.#ban)} ${actionStatus(this.#unban)}
        <op-confirm .request=${this.confirm} @closed=${() => (this.confirm = Option.none())}></op-confirm>
      </div>
    `
  }

  #member({ member, machines, applications }: MemberDetail) {
    return html`
      <header class="stack-s">
        <h1>${member.display_name}</h1>
        <p>${badge(member.status)} in ${this.region}, member since ${day(member.created_at)}</p>
      </header>
      <dl class="facts">
        <dt>Address</dt>
        <dd><code translate="no">${member.address}</code> <span class="muted small">This is where the member is paid.</span></dd>
        ${Option.match(member.reason, { onNone: () => nothing, onSome: (reason) => html`<dt>Reason</dt><dd>${reason}</dd>` })}
      </dl>
      <p>
        ${member.status === "banned"
          ? html`<button type="button" @click=${() => void this.#unban.run([member])}>Lift the ban</button>`
          : html`<button type="button" class="danger" @click=${() => this.#askBan(member)}>Ban this member</button>`}
      </p>

      <section class="stack-s" aria-labelledby="machines-heading">
        <h2 id="machines-heading">Machines</h2>
        ${machines.length === 0
          ? html`<p class="muted">No machines.</p>`
          : html`
              <div class="table-scroll">
                <table>
                  <thead>
                    <tr><th scope="col">Machine</th><th scope="col">Status</th><th scope="col">Runs</th><th scope="col">Agent last seen</th></tr>
                  </thead>
                  <tbody>
                    ${machines.map((machine) => html`
                      <tr>
                        <th scope="row">${machine.label}</th>
                        <td>${badge(machine.status)}</td>
                        <td>${machine.assignments.length === 0 ? html`<span class="muted">nothing assigned</span>` : machine.assignments.map((a) => html`<code translate="no">${a.app_name}</code> `)}</td>
                        <td>${Option.match(machine.last_seen_at, { onNone: () => html`<span class="muted">never</span>`, onSome: time })}</td>
                      </tr>
                    `)}
                  </tbody>
                </table>
              </div>
              <p><a href="/machines">Assign apps on the machines page</a></p>
            `}
      </section>

      <section class="stack-s" aria-labelledby="applications-heading">
        <h2 id="applications-heading">Applications</h2>
        <ul>
          ${applications.map((application) => html`
            <li>${badge(application.status)} ${time(application.created_at)}, ${plural(application.machines.length, "machine")}, terms version ${application.global_terms_version}</li>
          `)}
        </ul>
      </section>
    `
  }

  #askBan(member: Member): void {
    this.confirm = Option.some({
      heading: `Ban ${member.display_name}?`,
      message:
        "Their runners are removed from the orchestrator at once, sessions in progress included, and their machines can no longer connect. By default a banned member is left out of payouts.",
      confirm: "Ban member",
      danger: true,
      reason: "required",
      run: (reason) => void this.#ban.run([member, reason]),
    })
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "member-page": MemberPage
  }
}
