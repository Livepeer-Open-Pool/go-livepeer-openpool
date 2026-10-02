import { TaskStatus } from "@lit/task"
import { Api, type App, type AppDemand, AssignmentInput, type Demand, type Gpu, type Machine, type MachineStatus, type Member, parseInput, type RegionConfig } from "@openpool/core"
import { actionStatus, badge, type ConfirmRequest, eth, formatMemory, regionNotices, regionSections, renderQuery, SitePage, time } from "@openpool/ui"
import { Effect, Option, Schema } from "effect"
import { css, html, nothing } from "lit"
import { customElement, state } from "lit/decorators.js"
import { count, everywhere, inRegion } from "../lib.js"

interface RegionMachines {
  readonly machines: ReadonlyArray<Machine>
  readonly apps: ReadonlyArray<App>
  readonly gpus: ReadonlyArray<Gpu>
  readonly members: ReadonlyArray<Member>
  readonly demand: Demand
}

const machines =
  (status: MachineStatus | ""): Api.AskAs<RegionMachines> =>
  (region, token) =>
    Effect.all(
      {
        machines: Effect.map(Api.machines(status)(region, token), (m) => m.machines),
        apps: Effect.map(Api.apps(region, token), (a) => a.apps),
        gpus: Effect.map(Api.gpus(region, token), (g) => g.gpus),
        members: Effect.map(Api.members(region, token), (m) => m.members),
        demand: Api.demand(region, token),
      },
      { concurrency: 5 },
    )

const assign = (region: string, machine: Machine, input: unknown) =>
  Effect.flatMap(parseInput(Schema.Array(AssignmentInput), "The assignments")(input), (assignments) =>
    inRegion(region, Api.setAssignments(machine.id, assignments)),
  )

/** Every machine in every region, what it runs, and the demand that says what it should run. */
@customElement("machines-page")
export class MachinesPage extends SitePage {
  static override styles = [
    ...SitePage.styles,
    css`
      .machine > header { display: flex; flex-wrap: wrap; align-items: baseline; gap: var(--space-xs) var(--space); }
      details > summary { cursor: pointer; font-weight: 600; }
    `,
  ]

  @state() private status: MachineStatus | "" = ""
  @state() private confirm: Option.Option<ConfirmRequest> = Option.none()

  readonly #machines = this.query((status: MachineStatus | "") => everywhere(machines(status)), () => [this.status] as const)
  readonly #refresh = (): void => void this.#machines.run()
  readonly #approve = this.action((region: string, machine: Machine) => inRegion(region, Api.approveMachine(machine.id)), this.#refresh)
  readonly #assign = this.action(assign, this.#refresh)
  readonly #remove = this.action((region: string, machine: Machine) => inRegion(region, Api.removeMachine(machine.id)), this.#refresh)

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Machines</h1>
          <p class="muted">
            You decide which apps each machine runs. Changing an assignment lets the sessions the machine is serving finish, and
            its agent then switches containers by itself.
          </p>
          <label style="max-inline-size: 16rem">
            Show
            <select @change=${(event: Event) => (this.status = (event.target as HTMLSelectElement).value as MachineStatus | "")}>
              <option value="" selected>All machines</option>
              <option value="pending">Waiting for approval</option>
              <option value="approved">Approved</option>
            </select>
          </label>
        </header>
        ${actionStatus(this.#approve)} ${actionStatus(this.#assign)} ${actionStatus(this.#remove)}
        ${renderQuery(this.#machines, (results) => html`
          ${regionNotices(results)}
          ${regionSections(results, (region, detail) => html`
            ${this.#demand(detail.demand)}
            ${detail.machines.length === 0 ? html`<p class="muted">No machines here${this.status === "" ? "" : ` that are ${this.status}`}.</p>` : nothing}
            ${detail.machines.map((machine) => this.#machine(region, machine, detail))}
          `)}
        `)}
        <op-confirm .request=${this.confirm} @closed=${() => (this.confirm = Option.none())}></op-confirm>
      </div>
    `
  }

  #demand(demand: Demand) {
    return html`
      ${Option.match(demand.orchestrator_error, {
        onNone: () => nothing,
        onSome: (error) => html`<p class="notice warn small">The orchestrator did not answer, so runners and slots are missing below: ${error}</p>`,
      })}
      <div class="table-scroll">
        <table>
          <caption>Demand, to decide what machines should run</caption>
          <thead>
            <tr>
              <th scope="col">App</th><th scope="col" class="num">Runners online</th><th scope="col" class="num">Slots busy</th>
              <th scope="col" class="num">Sessions, 24 h</th><th scope="col" class="num">Fees, 24 h</th><th scope="col" class="num">Turned away, 24 h</th>
            </tr>
          </thead>
          <tbody>
            ${demand.demand.map((line: AppDemand) => html`
              <tr>
                <th scope="row"><code translate="no">${line.app}</code></th>
                <td class="num">${line.runners_online}</td>
                <td class="num">${line.slots_busy} of ${line.slots_total}</td>
                <td class="num">${line.sessions_24h}</td>
                <td class="num">${eth(line.fees_wei_24h)}</td>
                <td class="num">${line.refused_24h > 0 ? html`<strong>${line.refused_24h}</strong>` : 0}</td>
              </tr>
            `)}
          </tbody>
        </table>
      </div>
    `
  }

  #machine(region: RegionConfig, machine: Machine, detail: RegionMachines) {
    const owner = detail.members.find((m) => m.id === machine.member_id)
    const gpuName = (id: string): string => detail.gpus.find((g) => g.id === id)?.name ?? id
    const eligible = (app: App): boolean => app.eligible_gpus.length === 0 || machine.gpus.some((g) => app.eligible_gpus.includes(g.gpu_id))
    const assigned = (app: App) => machine.assignments.find((a) => a.app_id === app.id && a.status === "active")
    return html`
      <article class="card stack machine" aria-labelledby="machine-${machine.id}">
        <header>
          <h3 id="machine-${machine.id}">${machine.label}</h3>
          ${badge(machine.status)}
          <span class="small">
            ${owner === undefined ? html`<span class="muted">unknown member</span>` : html`<a href="/members/${region.id}/${owner.id}">${owner.display_name}</a>`}
          </span>
          <span class="muted small">declared ${machine.gpus.map((g) => `${g.count} × ${gpuName(g.gpu_id)}`).join(", ") || "no GPUs"}</span>
        </header>

        ${Option.match(machine.reported, {
          onNone: () => html`<p class="muted small">Its agent has not reported.</p>`,
          onSome: (report) => html`
            <p class="small">
              Agent ${report.agent_version}${Option.match(report.reported_at, { onNone: () => nothing, onSome: (at) => html`, reported ${time(at)}` })}.
              Found
              ${report.gpus.length > 0 ? report.gpus.map((g) => `${g.name} (${formatMemory(g.memory_mb)})`).join(", ") : Option.getOrElse(report.gpu_error, () => "no GPUs")}.
              ${report.containers.map((c) => html`<code translate="no">${c.service}</code> ${badge(Option.getOrElse(c.health, () => c.state) || c.state)} `)}
            </p>
            ${Option.match(report.error, { onNone: () => nothing, onSome: (error) => html`<p class="notice bad small">The agent could not apply its compose file: ${error}</p>` })}
          `,
        })}

        ${machine.status === "pending"
          ? html`<p><button type="button" class="primary" @click=${() => void this.#approve.run([region.id, machine])}>Approve this machine</button></p>`
          : html`
              <form class="stack-s" @submit=${(event: SubmitEvent) => this.#submitAssign(event, region.id, machine, detail.apps)}>
                <fieldset>
                  <legend>Apps this machine runs</legend>
                  ${detail.apps.length === 0 ? html`<p class="muted">This region has no apps yet. <a href="/apps">Add one</a>.</p>` : nothing}
                  ${detail.apps.map((app) => {
                    const current = assigned(app)
                    const draining = machine.assignments.find((a) => a.app_id === app.id && a.status === "draining")
                    return html`
                      <div class="row">
                        <label class="inline">
                          <input type="checkbox" name="assign:${app.id}" ?checked=${current !== undefined} ?disabled=${!eligible(app) && current === undefined} />
                          <span>
                            <code translate="no">${app.name}</code>
                            ${app.offered ? nothing : badge("paused")}
                            ${current === undefined ? nothing : html`<span class="hint">told to run ${current.version}</span>`}
                            ${current?.upgrading ? badge("upgrading") : nothing} ${draining === undefined ? nothing : badge("draining")}
                            ${eligible(app) ? nothing : html`<span class="hint">needs a GPU this machine lacks</span>`}
                          </span>
                        </label>
                        <label class="inline">
                          <span class="hint">sessions at once</span>
                          <input type="number" name="capacity:${app.id}" min="1" max=${app.max_capacity} value=${current?.capacity ?? app.default_capacity} style="inline-size: 5rem" />
                        </label>
                      </div>
                    `
                  })}
                </fieldset>
                <p class="row">
                  <button type="submit" ?disabled=${this.#assign.status === TaskStatus.PENDING}>Save assignments</button>
                  <button type="button" class="danger small push" @click=${() => this.#askRemove(region, machine)}>Remove machine</button>
                </p>
              </form>
            `}
      </article>
    `
  }

  #submitAssign(event: SubmitEvent, region: string, machine: Machine, apps: ReadonlyArray<App>): void {
    event.preventDefault()
    const data = new FormData(event.target as HTMLFormElement)
    const assignments = apps.filter((app) => data.has(`assign:${app.id}`)).map((app) => ({ app_id: app.id, capacity: count(data, `capacity:${app.id}`) }))
    void this.#assign.run([region, machine, assignments])
  }

  #askRemove(region: RegionConfig, machine: Machine): void {
    this.confirm = Option.some({
      heading: `Remove ${machine.label}?`,
      message: "Its runners stop taking new work and are removed once their sessions have finished. Its credential stops working. The member keeps their membership.",
      confirm: "Remove machine",
      danger: true,
      reason: "none",
      run: () => void this.#remove.run([region.id, machine]),
    })
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "machines-page": MachinesPage
  }
}
