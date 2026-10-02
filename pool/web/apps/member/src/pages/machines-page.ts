import { TaskStatus } from "@lit/task"
import {
  type AgentReport,
  Api,
  type Gpu,
  inRegionAs,
  type Machine,
  MachineSpec,
  membershipEverywhere,
  type NewCredential,
  parseInput,
  type RegionConfig,
} from "@openpool/core"
import { actionStatus, badge, type ConfirmRequest, formatMemory, regionNotices, renderQuery, SitePage, time } from "@openpool/ui"
import { Effect, Option, Redacted } from "effect"
import { css, html, nothing } from "lit"
import { customElement, state } from "lit/decorators.js"
import { describeGpus, memberSections } from "../lib.js"

interface RegionMachines {
  readonly gpus: ReadonlyArray<Gpu>
  readonly machines: ReadonlyArray<Machine>
}

const machinesAndGpus: Api.AskAs<RegionMachines> = (region, token) =>
  Effect.all(
    { gpus: Effect.map(Api.publicGpus(region), (g) => g.gpus), machines: Effect.map(Api.myMachines(region, token), (m) => m.machines) },
    { concurrency: 2 },
  )

const requestMachine = (region: string, input: unknown) =>
  Effect.flatMap(parseInput(MachineSpec, "The machine")(input), (spec) => inRegionAs("wallet", region, Api.requestMachine(spec)))

/** A credential just created, kept on the page until the member says they have saved it. */
interface FreshCredential {
  readonly region: string
  readonly machine: string
  readonly credential: NewCredential
}

/** The member's machines in every region: what each runs, what its agent reports, and its credential. */
@customElement("machines-page")
export class MachinesPage extends SitePage {
  static override styles = [
    ...SitePage.styles,
    css`
      .machine > header { display: flex; flex-wrap: wrap; align-items: baseline; gap: var(--space-xs) var(--space); }
      .machine h3 { font-size: var(--step-1); }
      details > summary { cursor: pointer; font-weight: 600; }
    `,
  ]

  @state() private fresh: Option.Option<FreshCredential> = Option.none()
  @state() private confirm: Option.Option<ConfirmRequest> = Option.none()

  readonly #machines = this.query(() => membershipEverywhere(machinesAndGpus))
  readonly #refresh = (): void => void this.#machines.run()

  readonly #create = this.action(
    (region: string, machine: Machine) => inRegionAs("wallet", region, Api.createCredential(machine.id)),
    (credential, region, machine) => {
      this.fresh = Option.some({ region, machine: machine.label, credential })
      this.#refresh()
    },
  )
  readonly #revoke = this.action((region: string, machine: Machine) => inRegionAs("wallet", region, Api.revokeCredential(machine.id)), this.#refresh)
  readonly #remove = this.action((region: string, machine: Machine) => inRegionAs("wallet", region, Api.removeMyMachine(machine.id)), this.#refresh)
  readonly #request = this.action(requestMachine, this.#refresh)

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Your machines</h1>
          <p class="muted">
            A machine is one box with one or more GPUs. The operator decides which apps it runs; the agent on the machine starts
            them and keeps them up to date.
          </p>
        </header>
        ${this.#freshCredential()} ${actionStatus(this.#create)} ${actionStatus(this.#revoke)} ${actionStatus(this.#remove)}
        ${renderQuery(this.#machines, (results) => html`
          ${regionNotices(results)}
          ${memberSections(results, (region, detail) => html`
            ${detail.machines.length === 0 ? html`<p class="muted">You have no machines in ${region.name}.</p>` : nothing}
            ${detail.machines.map((machine) => this.#machine(region, machine, detail.gpus))} ${this.#requestForm(region, detail.gpus)}
          `)}
        `)}
        <op-confirm .request=${this.confirm} @closed=${() => (this.confirm = Option.none())}></op-confirm>
      </div>
    `
  }

  #freshCredential() {
    return Option.match(this.fresh, {
      onNone: () => nothing,
      onSome: ({ machine, credential }) => html`
        <section class="notice good stack" aria-labelledby="fresh-heading">
          <h2 id="fresh-heading">The credential for ${machine}</h2>
          <p>
            <strong>This is shown once.</strong> Run the command below on the machine. It starts the Open Pool agent, which then
            starts whatever the machine is assigned. The machine needs Linux, an NVIDIA GPU, Docker and the NVIDIA container toolkit.
          </p>
          ${Option.match(credential.agent_command, {
            onNone: () => html`<p class="notice warn">This region does not offer the agent. Start each runner by hand with the commands below.</p>`,
            onSome: (command) => html`<op-copy label="install command" .text=${Redacted.value(command)}></op-copy>`,
          })}
          <details>
            <summary>Other ways to use this credential</summary>
            <div class="stack-s">
              <op-copy label="the credential alone" .text=${Redacted.value(credential.secret)}></op-copy>
              ${credential.commands.length === 0
                ? html`<p class="muted">Commands for starting runners by hand appear here once the machine is assigned an app.</p>`
                : credential.commands.map((command) => html`<op-copy label="start one runner by hand" .text=${Redacted.value(command)}></op-copy>`)}
            </div>
          </details>
          <p><button type="button" @click=${() => (this.fresh = Option.none())}>I have saved it</button></p>
        </section>
      `,
    })
  }

  #machine(region: RegionConfig, machine: Machine, gpus: ReadonlyArray<Gpu>) {
    const hasCredential = Option.isSome(machine.credential_created_at)
    return html`
      <article class="card stack machine" aria-labelledby="machine-${machine.id}">
        <header>
          <h3 id="machine-${machine.id}">${machine.label}</h3>
          ${badge(machine.status)}
          <span class="muted small">${describeGpus(machine.gpus, gpus)}</span>
        </header>

        ${machine.status === "pending"
          ? html`<p class="notice warn">Waiting for the operator to approve this machine. It can be given a credential once approved.</p>`
          : html`
              ${this.#assignments(machine)} ${this.#report(machine)}
              <div class="stack-s">
                <p>
                  ${Option.match(machine.credential_created_at, {
                    onNone: () => html`This machine has no credential, so nothing on it can connect.`,
                    onSome: (at) => html`Credential created ${time(at)}.`,
                  })}
                </p>
                <p class="row">
                  <button
                    type="button"
                    class=${hasCredential ? "" : "primary"}
                    ?disabled=${this.#create.status === TaskStatus.PENDING}
                    @click=${() => (hasCredential ? this.#askReplace(region, machine) : void this.#create.run([region.id, machine]))}
                  >
                    ${hasCredential ? "Replace credential" : "Create credential"}
                  </button>
                  ${hasCredential ? html`<button type="button" @click=${() => this.#askRevoke(region, machine)}>Revoke credential</button>` : nothing}
                </p>
              </div>
            `}
        <p><button type="button" class="danger small" @click=${() => this.#askRemove(region, machine)}>Remove machine</button></p>
      </article>
    `
  }

  #assignments(machine: Machine) {
    if (machine.assignments.length === 0) {
      return html`<p class="muted">The operator has not assigned this machine an app yet.</p>`
    }
    return html`
      <div class="table-scroll">
        <table>
          <caption>What this machine runs</caption>
          <thead>
            <tr><th scope="col">App</th><th scope="col">Version</th><th scope="col" class="num">Sessions at once</th><th scope="col">Status</th></tr>
          </thead>
          <tbody>
            ${machine.assignments.map((a) => html`
              <tr>
                <th scope="row"><code translate="no">${a.app_name}</code></th>
                <td>${a.version}</td>
                <td class="num">${a.capacity}</td>
                <td>${badge(a.status)} ${a.upgrading ? badge("upgrading") : nothing}</td>
              </tr>
            `)}
          </tbody>
        </table>
      </div>
    `
  }

  #report(machine: Machine) {
    return Option.match(machine.reported, {
      onNone: () =>
        Option.isSome(machine.credential_created_at)
          ? html`<p class="muted">The agent on this machine has not reported yet. It reports within a minute of being started.</p>`
          : nothing,
      onSome: (report: AgentReport) => html`
        <div class="stack-s">
          ${Option.match(report.error, {
            onNone: () => nothing,
            onSome: (error) => html`<p class="notice bad"><strong>The agent could not apply what it was told to run.</strong> ${error}</p>`,
          })}
          <dl class="facts">
            <dt>Agent</dt>
            <dd>
              version ${report.agent_version}${Option.match(report.reported_at, { onNone: () => nothing, onSome: (at) => html`, last reported ${time(at)}` })}
            </dd>
            <dt>GPUs found</dt>
            <dd>
              ${report.gpus.length > 0
                ? report.gpus.map((g) => `${g.name} (${formatMemory(g.memory_mb)})`).join(", ")
                : Option.getOrElse(report.gpu_error, () => "none")}
            </dd>
            <dt>Containers</dt>
            <dd>
              ${report.containers.length === 0
                ? "none running"
                : report.containers.map((c) => html`<span class="row small"><code translate="no">${c.service}</code> ${badge(Option.getOrElse(c.health, () => c.state) || c.state)}</span>`)}
            </dd>
          </dl>
        </div>
      `,
    })
  }

  #requestForm(region: RegionConfig, gpus: ReadonlyArray<Gpu>) {
    const accepted = gpus.filter((gpu) => gpu.accepted)
    return html`
      <details>
        <summary>Add a machine in ${region.name}</summary>
        <form class="card" @submit=${(event: SubmitEvent) => this.#submitRequest(event, region.id, accepted)}>
          <p class="muted">The operator approves a new machine before it can be given a credential.</p>
          <label>
            Name for this machine
            <input name="label" required maxlength="60" />
          </label>
          <fieldset>
            <legend>GPUs in this machine</legend>
            ${accepted.map((gpu) => html`
              <label class="inline">
                <input type="number" name="gpu:${gpu.id}" min="0" max="64" step="1" value="0" inputmode="numeric" style="inline-size: 5rem" />
                <span>${gpu.name} <span class="hint">${formatMemory(gpu.vram_mb)}</span></span>
              </label>
            `)}
          </fieldset>
          <p><button type="submit" ?disabled=${this.#request.status === TaskStatus.PENDING}>Ask for approval</button></p>
          ${actionStatus(this.#request)}
        </form>
      </details>
    `
  }

  #submitRequest(event: SubmitEvent, region: string, gpus: ReadonlyArray<Gpu>): void {
    event.preventDefault()
    const form = event.target as HTMLFormElement
    const data = new FormData(form)
    const spec = {
      label: String(data.get("label") ?? "").trim(),
      gpus: gpus.map((gpu) => ({ gpu_id: gpu.id, count: Number(data.get(`gpu:${gpu.id}`) ?? 0) })).filter((gpu) => gpu.count > 0),
    }
    void this.#request.run([region, spec]).then(() => {
      if (this.#request.status === TaskStatus.COMPLETE) form.reset()
    })
  }

  #askReplace(region: RegionConfig, machine: Machine): void {
    this.confirm = Option.some({
      heading: `Replace the credential of ${machine.label}?`,
      message:
        "The credential it has now stops working at once, and its runners can no longer reconnect with it. You will need to start the agent on the machine again with the new one.",
      confirm: "Replace credential",
      danger: true,
      reason: "none",
      run: () => void this.#create.run([region.id, machine]),
    })
  }

  #askRevoke(region: RegionConfig, machine: Machine): void {
    this.confirm = Option.some({
      heading: `Revoke the credential of ${machine.label}?`,
      message: "Nothing on the machine will be able to connect until you create a new credential. Use this if the credential may have leaked.",
      confirm: "Revoke credential",
      danger: true,
      reason: "none",
      run: () => void this.#revoke.run([region.id, machine]),
    })
  }

  #askRemove(region: RegionConfig, machine: Machine): void {
    this.confirm = Option.some({
      heading: `Remove ${machine.label} from ${region.name}?`,
      message:
        "Its runners stop taking new work and are removed once the sessions they are serving have finished. Its credential stops working. What it earned so far is still paid.",
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
