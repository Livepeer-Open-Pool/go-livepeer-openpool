import { Api, membershipEverywhere, type Runner, type TestResult } from "@openpool/core"
import { badge, regionNotices, renderQuery, SitePage, time } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement } from "lit/decorators.js"
import { memberSections } from "../lib.js"

interface RegionRunners {
  readonly runners: ReadonlyArray<Runner>
  readonly tests: ReadonlyArray<TestResult>
}

const runnersAndTests: Api.AskAs<RegionRunners> = (region, token) =>
  Effect.all(
    { runners: Effect.map(Api.myRunners(region, token), (r) => r.runners), tests: Effect.map(Api.myTests(50)(region, token), (t) => t.tests) },
    { concurrency: 2 },
  )

/** Why a suspended runner is suspended, in a sentence. */
const whySuspended: Readonly<Record<string, string>> = {
  tests: "It failed its test several times in a row. Passing tests bring it back by themselves.",
  version: "It runs a version the pool no longer accepts. Update what the machine runs.",
  draining: "Its app was unassigned. It is finishing the sessions it has and is then removed.",
  upgrade: "Its machine is being moved to a new version. It finishes its sessions first.",
}

/** The member's runners as the orchestrator sees them, and every test the pool ran against them. */
@customElement("runners-page")
export class RunnersPage extends SitePage {
  readonly #runners = this.query(() => membershipEverywhere(runnersAndTests))

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Runners and tests</h1>
          <p class="muted">
            A runner is one app on one machine, connected to the region's orchestrator. The pool tests each runner with a real
            request. A new runner serves customers once it passes; one that keeps failing is suspended until it passes again.
          </p>
          <p><button type="button" class="small" @click=${() => void this.#runners.run()}>Refresh</button></p>
        </header>
        ${renderQuery(this.#runners, (results) => html`
          ${regionNotices(results)}
          ${memberSections(results, (_region, detail) => html`${this.#runnerTable(detail.runners)} ${this.#testTable(detail.tests)}`)}
        `)}
      </div>
    `
  }

  #runnerTable(runners: ReadonlyArray<Runner>) {
    if (runners.length === 0) {
      return html`<p class="muted">No runner of yours is connected here. A machine connects once its agent is running and it is assigned an app.</p>`
    }
    return html`
      <div class="table-scroll">
        <table>
          <caption>Connected runners</caption>
          <thead>
            <tr>
              <th scope="col">Machine</th><th scope="col">App</th><th scope="col">State</th><th scope="col">Version</th>
              <th scope="col" class="num">Sessions</th><th scope="col">Last test</th>
            </tr>
          </thead>
          <tbody>
            ${runners.map((r) => html`
              <tr>
                <th scope="row">${Option.getOrElse(r.machine_label, () => "unknown machine")}</th>
                <td><code translate="no">${r.app}</code></td>
                <td>
                  ${badge(r.state)} ${r.hold ? badge("on hold") : nothing} ${r.ready ? nothing : badge("not ready", "warn")}
                  ${r.state === "suspended"
                    ? html`<p class="small muted">${Option.match(r.suspend_reason, { onNone: () => "Suspended by the operator.", onSome: (why) => whySuspended[why] ?? why })}</p>`
                    : nothing}
                  ${Option.match(r.hold_reason, { onNone: () => nothing, onSome: (why) => html`<p class="small muted">Held by the operator: ${why}</p>` })}
                </td>
                <td>${r.version}</td>
                <td class="num">${r.capacity_used} of ${r.capacity}</td>
                <td>
                  ${Option.match(r.last_test_at, {
                    onNone: () => html`<span class="muted">not tested yet</span>`,
                    onSome: (at) => html`${badge(r.last_test_ok ? "passed" : "failed")} ${time(at)}`,
                  })}
                </td>
              </tr>
            `)}
          </tbody>
        </table>
      </div>
    `
  }

  #testTable(tests: ReadonlyArray<TestResult>) {
    if (tests.length === 0) return nothing
    return html`
      <div class="table-scroll">
        <table>
          <caption>Recent tests</caption>
          <thead>
            <tr><th scope="col">When</th><th scope="col">App</th><th scope="col">Result</th><th scope="col" class="num">Took</th><th scope="col">What was wrong</th></tr>
          </thead>
          <tbody>
            ${tests.map((t) => html`
              <tr>
                <td>${time(t.at)}</td>
                <td><code translate="no">${t.app}</code></td>
                <td>${badge(t.ok ? "passed" : "failed")}</td>
                <td class="num">${t.latency_ms} ms</td>
                <td>${Option.getOrElse(t.detail, () => "")}</td>
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
    "runners-page": RunnersPage
  }
}
