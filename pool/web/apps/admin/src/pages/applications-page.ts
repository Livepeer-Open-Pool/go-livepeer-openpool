import { TaskStatus } from "@lit/task"
import { Api, type App, type Application, type ApplicationStatus, type Gpu, MachineAssignments, type MachineSpec, parseInput, type RegionConfig } from "@openpool/core"
import { actionStatus, badge, type ConfirmRequest, regionNotices, regionSections, renderQuery, shortAddress, SitePage, time } from "@openpool/ui"
import { Effect, Option, Schema } from "effect"
import { html, nothing } from "lit"
import { customElement, state } from "lit/decorators.js"
import { count, everywhere, inRegion } from "../lib.js"

interface RegionApplications {
  readonly applications: ReadonlyArray<Application>
  readonly apps: ReadonlyArray<App>
  readonly gpus: ReadonlyArray<Gpu>
}

const applications =
  (status: ApplicationStatus | ""): Api.AskAs<RegionApplications> =>
  (region, token) =>
    Effect.all(
      {
        applications: Effect.map(Api.applications(status)(region, token), (a) => a.applications),
        apps: Effect.map(Api.apps(region, token), (a) => a.apps),
        gpus: Effect.map(Api.gpus(region, token), (g) => g.gpus),
      },
      { concurrency: 3 },
    )

const approve = (region: string, application: Application, input: unknown) =>
  Effect.flatMap(parseInput(Schema.Array(MachineAssignments), "The assignments")(input), (machines) =>
    inRegion(region, Api.approveApplication(application.id, machines)),
  )

/** Whether an app may run on a machine: it lists no GPU models, or one the machine has. */
const eligible = (app: App, machine: MachineSpec): boolean =>
  app.eligible_gpus.length === 0 || machine.gpus.some((gpu) => app.eligible_gpus.includes(gpu.gpu_id))

/** Applications in every region: who applied, with what, and the decision. */
@customElement("applications-page")
export class ApplicationsPage extends SitePage {
  @state() private status: ApplicationStatus | "" = "pending"
  @state() private confirm: Option.Option<ConfirmRequest> = Option.none()

  readonly #applications = this.query((status: ApplicationStatus | "") => everywhere(applications(status)), () => [this.status] as const)
  readonly #refresh = (): void => void this.#applications.run()
  readonly #approve = this.action(approve, this.#refresh)
  readonly #reject = this.action(
    (region: string, application: Application, reason: string) => inRegion(region, Api.rejectApplication(application.id, reason)),
    this.#refresh,
  )

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Applications</h1>
          <p class="muted">
            Approving makes the applicant a member and their machines approved. Assign apps now or later: a machine with nothing
            assigned connects nothing.
          </p>
          <label style="max-inline-size: 16rem">
            Show
            <select @change=${(event: Event) => (this.status = (event.target as HTMLSelectElement).value as ApplicationStatus | "")}>
              <option value="pending" selected>Waiting for a decision</option>
              <option value="approved">Approved</option>
              <option value="rejected">Rejected</option>
              <option value="">All</option>
            </select>
          </label>
        </header>
        ${actionStatus(this.#approve)} ${actionStatus(this.#reject)}
        ${renderQuery(this.#applications, (results) => html`
          ${regionNotices(results)}
          ${regionSections(results, (region, detail) =>
            detail.applications.length === 0
              ? html`<p class="muted">No applications ${this.status === "" ? "" : `that are ${this.status}`} in ${region.name}.</p>`
              : detail.applications.map((application) => this.#application(region, application, detail)),
          )}
        `)}
        <op-confirm .request=${this.confirm} @closed=${() => (this.confirm = Option.none())}></op-confirm>
      </div>
    `
  }

  #application(region: RegionConfig, application: Application, detail: RegionApplications) {
    const gpuName = (id: string): string => detail.gpus.find((g) => g.id === id)?.name ?? id
    const offered = detail.apps.filter((app) => app.offered)
    return html`
      <article class="card stack" aria-labelledby="application-${application.id}">
        <header class="row">
          <h3 id="application-${application.id}">${application.display_name}</h3>
          ${badge(application.status)} ${shortAddress(application.address)}
          <span class="muted small">applied ${time(application.created_at)}</span>
        </header>
        ${Option.match(application.reason, { onNone: () => nothing, onSome: (reason) => html`<p class="small">Reason given: ${reason}</p>` })}
        ${application.status === "pending"
          ? html`
              <form class="stack" @submit=${(event: SubmitEvent) => this.#submitApprove(event, region.id, application, offered)}>
                ${application.machines.map((machine, index) => html`
                  <fieldset>
                    <legend>${machine.label}: ${machine.gpus.map((g) => `${g.count} × ${gpuName(g.gpu_id)}`).join(", ")}</legend>
                    ${offered.length === 0 ? html`<p class="muted">This region has no apps to assign yet.</p>` : nothing}
                    ${offered.map((app) => html`
                      <div class="row">
                        <label class="inline">
                          <input type="checkbox" name="assign:${index}:${app.id}" ?disabled=${!eligible(app, machine)} />
                          <span><code translate="no">${app.name}</code> ${eligible(app, machine) ? nothing : html`<span class="hint">needs a GPU this machine lacks</span>`}</span>
                        </label>
                        <label class="inline">
                          <span class="hint">sessions at once</span>
                          <input type="number" name="capacity:${index}:${app.id}" min="1" max=${app.max_capacity} value=${app.default_capacity} style="inline-size: 5rem" ?disabled=${!eligible(app, machine)} />
                        </label>
                      </div>
                    `)}
                  </fieldset>
                `)}
                <p class="row">
                  <button type="submit" class="primary" ?disabled=${this.#approve.status === TaskStatus.PENDING}>Approve ${application.display_name}</button>
                  <button type="button" class="danger" @click=${() => this.#askReject(region, application)}>Reject</button>
                </p>
              </form>
            `
          : html`
              <ul>
                ${application.machines.map((machine) => html`<li>${machine.label}: ${machine.gpus.map((g) => `${g.count} × ${gpuName(g.gpu_id)}`).join(", ")}</li>`)}
              </ul>
            `}
      </article>
    `
  }

  #submitApprove(event: SubmitEvent, region: string, application: Application, apps: ReadonlyArray<App>): void {
    event.preventDefault()
    const data = new FormData(event.target as HTMLFormElement)
    const machines = application.machines.map((_machine, index) => ({
      machine: index,
      assignments: apps
        .filter((app) => data.has(`assign:${index}:${app.id}`))
        .map((app) => ({ app_id: app.id, capacity: count(data, `capacity:${index}:${app.id}`) })),
    }))
    void this.#approve.run([region, application, machines])
  }

  #askReject(region: RegionConfig, application: Application): void {
    this.confirm = Option.some({
      heading: `Reject ${application.display_name}'s application?`,
      message: "They can read your reason and may apply again.",
      confirm: "Reject application",
      danger: true,
      reason: "required",
      run: (reason) => void this.#reject.run([region.id, application, reason]),
    })
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "applications-page": ApplicationsPage
  }
}
