import { TaskStatus } from "@lit/task"
import { Api, parseInput, regions as allRegions, type RegionConfig, type RegionResult, Settings } from "@openpool/core"
import { actionStatus, regionNotices, regionSections, renderQuery, SitePage } from "@openpool/ui"
import { Effect, Option } from "effect"
import { html, nothing } from "lit"
import { customElement, state } from "lit/decorators.js"
import { count, everywhere, field, inRegions, outcomes } from "../lib.js"

const page = Effect.all({ regions: allRegions, settings: everywhere(Api.settings) })

const save = (regions: ReadonlyArray<string>, input: unknown) =>
  Effect.flatMap(parseInput(Settings, "The settings")(input), (settings) => inRegions(regions, Api.saveSettings(settings)))

/** Each region's policy: commission, payout windows, approval, and how runners are tested. */
@customElement("settings-page")
export class SettingsPage extends SitePage {
  @state() private saved: Option.Option<ReadonlyArray<RegionResult<Settings>>> = Option.none()

  readonly #page = this.query(() => page)
  readonly #save = this.action(save, (results) => {
    this.saved = Option.some(results)
    void this.#page.run()
  })

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Settings</h1>
          <p class="muted">Each region has its own. A change takes effect at once; the commission and window length of a payout window already running are the ones it started with.</p>
        </header>
        ${Option.match(this.saved, { onNone: () => nothing, onSome: (results) => outcomes(results, "Saved") })} ${actionStatus(this.#save)}
        ${renderQuery(this.#page, ({ regions, settings }) => html`
          ${regionNotices(settings)} ${regionSections(settings, (region, current) => this.#form(region, current, regions))}
        `)}
      </div>
    `
  }

  #form(region: RegionConfig, settings: Settings, regions: ReadonlyArray<RegionConfig>) {
    return html`
      <form class="card" @submit=${(event: SubmitEvent) => this.#submit(event, region, regions)}>
        <fieldset>
          <legend>Payouts</legend>
          <div class="fields">
            <label>
              Commission in basis points
              <input type="number" name="commission_bps" required min="0" max="10000" .value=${String(settings.commission_bps)} />
              <span class="hint">1500 is 15%. The pool keeps this share of each window's pot.</span>
            </label>
            <label>
              Window length in rounds
              <input type="number" name="window_rounds" required min="1" .value=${String(settings.window_rounds)} />
              <span class="hint">A round is about a day.</span>
            </label>
            <label>
              Paying a closed window
              <select name="payout_approval">
                <option value="required" ?selected=${settings.payout_approval === "required"}>waits for my approval</option>
                <option value="automatic" ?selected=${settings.payout_approval === "automatic"}>is automatic</option>
              </select>
            </label>
          </div>
        </fieldset>
        <fieldset>
          <legend>Testing runners</legend>
          <div class="fields">
            <label>
              Seconds between tests
              <input type="number" name="test_interval_seconds" required min="10" .value=${String(settings.test_interval_seconds)} />
            </label>
            <label>
              Failures in a row before suspension
              <input type="number" name="test_failures_to_suspend" required min="1" .value=${String(settings.test_failures_to_suspend)} />
            </label>
            <label>
              Passes in a row to come back
              <input type="number" name="test_passes_to_reinstate" required min="1" .value=${String(settings.test_passes_to_reinstate)} />
            </label>
            <label>
              Hours an old version stays accepted
              <input type="number" name="version_deadline_hours" required min="0" .value=${String(settings.version_deadline_hours)} />
            </label>
          </div>
        </fieldset>
        ${regions.length > 1
          ? html`
              <label class="inline">
                <input type="checkbox" name="everywhere" />
                <span>Save these settings in every region, not only ${region.name}</span>
              </label>
            `
          : nothing}
        <p><button type="submit" class="primary" ?disabled=${this.#save.status === TaskStatus.PENDING}>Save ${region.name}'s settings</button></p>
      </form>
    `
  }

  #submit(event: SubmitEvent, region: RegionConfig, regions: ReadonlyArray<RegionConfig>): void {
    event.preventDefault()
    const data = new FormData(event.target as HTMLFormElement)
    this.saved = Option.none()
    void this.#save.run([
      data.has("everywhere") ? regions.map((r) => r.id) : [region.id],
      {
        commission_bps: count(data, "commission_bps"),
        window_rounds: count(data, "window_rounds"),
        payout_approval: field(data, "payout_approval"),
        test_interval_seconds: count(data, "test_interval_seconds"),
        test_failures_to_suspend: count(data, "test_failures_to_suspend"),
        test_passes_to_reinstate: count(data, "test_passes_to_reinstate"),
        version_deadline_hours: count(data, "version_deadline_hours"),
      },
    ])
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "settings-page": SettingsPage
  }
}
