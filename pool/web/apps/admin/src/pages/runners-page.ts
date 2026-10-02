import { Api, type RegionConfig, type Runner, type RunnerState, type TestResult } from "@openpool/core"
import { actionStatus, badge, type ConfirmRequest, regionNotices, regionSections, renderQuery, SitePage, time } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement, state } from "lit/decorators.js"
import { everywhere, inRegion } from "../lib.js"

interface RegionRunners {
  readonly runners: ReadonlyArray<Runner>
  readonly tests: ReadonlyArray<TestResult>
}

const runners: Api.AskAs<RegionRunners> = (region, token) =>
  Effect.all(
    { runners: Effect.map(Api.runners(region, token), (r) => r.runners), tests: Effect.map(Api.tests(50)(region, token), (t) => t.tests) },
    { concurrency: 2 },
  )

/** Every runner on every region's orchestrator, and what the operator can do to one. */
@customElement("runners-page")
export class RunnersPage extends SitePage {
  @state() private confirm: Option.Option<ConfirmRequest> = Option.none()
  @state() private tested: Option.Option<{ readonly runner: Runner; readonly result: TestResult }> = Option.none()

  readonly #runners = this.query(() => everywhere(runners))
  readonly #refresh = (): void => void this.#runners.run()
  readonly #test = this.action(
    (region: string, runner: Runner) => inRegion(region, Api.testRunner(runner.runner_id)),
    (result, _region, runner) => {
      this.tested = Option.some({ runner, result })
      this.#refresh()
    },
  )
  readonly #setState = this.action(
    (region: string, runner: Runner, next: RunnerState, reason: string) => inRegion(region, Api.setRunnerState(runner.runner_id, next, reason)),
    this.#refresh,
  )
  readonly #hold = this.action(
    (region: string, runner: Runner, hold: boolean, reason: string) => inRegion(region, Api.holdRunner(runner.runner_id, hold, reason)),
    this.#refresh,
  )
  readonly #evict = this.action(
    (region: string, runner: Runner, reason: string) => inRegion(region, Api.evictRunner(runner.runner_id, reason)),
    this.#refresh,
  )

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Runners</h1>
          <p class="muted">
            The pool tests, promotes, suspends and drains runners by itself. Setting a runner's state by hand puts it on hold: the
            pool keeps testing it and otherwise leaves it alone until you release the hold.
          </p>
          <p><button type="button" class="small" @click=${this.#refresh}>Refresh</button></p>
        </header>
        ${Option.match(this.tested, {
          onNone: () => nothing,
          onSome: ({ runner, result }) => html`
            <p class="notice ${result.ok ? "good" : "bad"}" role="status">
              Tested <code translate="no">${runner.app}</code> on ${Option.getOrElse(runner.machine_label, () => runner.runner_id)}:
              ${result.ok ? "passed" : "failed"} in ${result.latency_ms} ms. ${Option.getOrElse(result.detail, () => "")}
            </p>
          `,
        })}
        ${actionStatus(this.#test, "Testing…")} ${actionStatus(this.#setState)} ${actionStatus(this.#hold)} ${actionStatus(this.#evict)}
        ${renderQuery(this.#runners, (results) => html`
          ${regionNotices(results)}
          ${regionSections(results, (region, detail) => html`${this.#table(region, detail.runners)} ${this.#tests(detail)}`)}
        `)}
        <op-confirm .request=${this.confirm} @closed=${() => (this.confirm = Option.none())}></op-confirm>
      </div>
    `
  }

  #table(region: RegionConfig, runners: ReadonlyArray<Runner>) {
    if (runners.length === 0) return html`<p class="muted">No runner is registered on ${region.name}'s orchestrator.</p>`
    return html`
      <div class="table-scroll">
        <table>
          <caption>Runners in ${region.name}</caption>
          <thead>
            <tr>
              <th scope="col">Member and machine</th><th scope="col">App</th><th scope="col">State</th>
              <th scope="col" class="num">Sessions</th><th scope="col">Last test</th><th scope="col">Do</th>
            </tr>
          </thead>
          <tbody>
            ${runners.map((runner) => {
              const who = `${Option.getOrElse(runner.member_name, () => "unknown member")}, ${Option.getOrElse(runner.machine_label, () => "unknown machine")}`
              return html`
                <tr>
                  <th scope="row">
                    ${who}
                    ${runner.unknown ? html`<div class="small">${badge("not admitted by the pool", "warn")}</div>` : nothing}
                    <div class="small muted"><code translate="no">${runner.runner_id}</code></div>
                  </th>
                  <td><code translate="no">${runner.app}</code><div class="small muted">${runner.version}</div></td>
                  <td>
                    ${badge(runner.state)} ${runner.hold ? badge("on hold") : nothing} ${runner.ready ? nothing : badge("not ready", "warn")}
                    ${Option.match(runner.suspend_reason, { onNone: () => nothing, onSome: (why) => (runner.state === "suspended" ? html`<div class="small muted">suspended by ${why}</div>` : nothing) })}
                    ${Option.match(runner.hold_reason, { onNone: () => nothing, onSome: (why) => html`<div class="small muted">held: ${why}</div>` })}
                  </td>
                  <td class="num">${runner.capacity_used} of ${runner.capacity}</td>
                  <td>
                    ${Option.match(runner.last_test_at, {
                      onNone: () => html`<span class="muted">never</span>`,
                      onSome: (at) => html`${badge(runner.last_test_ok ? "passed" : "failed")} ${time(at)}`,
                    })}
                  </td>
                  <td class="actions">
                    <button type="button" class="small" @click=${() => void this.#test.run([region.id, runner])}>Test now<span class="visually-hidden"> ${who}</span></button>
                    ${runner.state === "active"
                      ? html`<button type="button" class="small" @click=${() => this.#askState(region, runner, "suspended", who)}>Suspend</button>`
                      : html`<button type="button" class="small" @click=${() => this.#askState(region, runner, "active", who)}>Activate</button>`}
                    ${runner.hold
                      ? html`<button type="button" class="small" @click=${() => void this.#hold.run([region.id, runner, false, ""])}>Release hold</button>`
                      : nothing}
                    <button type="button" class="small danger" @click=${() => this.#askEvict(region, runner, who)}>Evict</button>
                  </td>
                </tr>
              `
            })}
          </tbody>
        </table>
      </div>
    `
  }

  #tests({ tests }: RegionRunners) {
    if (tests.length === 0) return nothing
    return html`
      <details>
        <summary>The last ${tests.length} tests</summary>
        <div class="table-scroll">
          <table>
            <thead>
              <tr><th scope="col">When</th><th scope="col">Runner</th><th scope="col">App</th><th scope="col">Result</th><th scope="col" class="num">Took</th><th scope="col">What was wrong</th></tr>
            </thead>
            <tbody>
              ${tests.map((t) => html`
                <tr>
                  <td>${time(t.at)}</td>
                  <td><code translate="no">${t.runner_id}</code></td>
                  <td><code translate="no">${t.app}</code></td>
                  <td>${badge(t.ok ? "passed" : "failed")}</td>
                  <td class="num">${t.latency_ms} ms</td>
                  <td>${Option.getOrElse(t.detail, () => "")}</td>
                </tr>
              `)}
            </tbody>
          </table>
        </div>
      </details>
    `
  }

  #askState(region: RegionConfig, runner: Runner, next: RunnerState, who: string): void {
    this.confirm = Option.some({
      heading: next === "active" ? `Activate the runner of ${who}?` : `Suspend the runner of ${who}?`,
      message:
        next === "active"
          ? "It starts taking customers' work whatever its tests say, and stays that way until you release the hold this sets."
          : "It takes no new work; sessions in progress continue. It stays suspended until you release the hold this sets.",
      confirm: next === "active" ? "Activate and hold" : "Suspend and hold",
      danger: next !== "active",
      reason: "required",
      run: (reason) => void this.#setState.run([region.id, runner, next, reason]),
    })
  }

  #askEvict(region: RegionConfig, runner: Runner, who: string): void {
    this.confirm = Option.some({
      heading: `Evict the runner of ${who}?`,
      message: "It is removed from the orchestrator at once and its sessions end. Its machine may register again; to keep it out, unassign the app or ban the member.",
      confirm: "Evict runner",
      danger: true,
      reason: "required",
      run: (reason) => void this.#evict.run([region.id, runner, reason]),
    })
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "runners-page": RunnersPage
  }
}
