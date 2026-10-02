import { TaskStatus } from "@lit/task"
import { Api, type Application, ApplicationDraft, applyToRegion, type CurrentTerms, everyRegion, type Gpu, parseInput, type PoolInfo, type RegionConfig, type Terms } from "@openpool/core"
import { actionStatus, day, formatMemory, regionNotices, renderQuery, SitePage } from "@openpool/ui"
import { answered } from "@openpool/core"
import { Effect, Option } from "effect"
import { css, html, nothing } from "lit"
import { customElement, state } from "lit/decorators.js"

interface RegionOffer {
  readonly pool: PoolInfo
  readonly gpus: ReadonlyArray<Gpu>
  readonly terms: CurrentTerms
}

const offer = (region: RegionConfig) =>
  Effect.all({ pool: Api.pool(region), gpus: Effect.map(Api.publicGpus(region), (g) => g.gpus), terms: Api.publicTerms(region) }, { concurrency: 3 })

const apply = (region: string, input: unknown) =>
  Effect.flatMap(parseInput(ApplicationDraft, "The application")(input), (draft) => applyToRegion(region, draft))

/** Applying to a region: who you are, what machines you bring, and the terms you accept by signing. */
@customElement("apply-page")
export class ApplyPage extends SitePage {
  static override styles = [
    ...SitePage.styles,
    css`
      .terms { white-space: pre-wrap; font-family: var(--font); max-block-size: 22rem; overflow-y: auto; padding: var(--space-s); background: var(--surface-sunken); border-radius: var(--radius-s); }
      details > summary { cursor: pointer; font-weight: 600; }
      .gpu-counts { display: grid; gap: var(--space-xs); grid-template-columns: repeat(auto-fit, minmax(min(15rem, 100%), 1fr)); }
      .gpu-counts label { grid-template-columns: 4.5rem 1fr; align-items: center; font-weight: 400; }
    `,
  ]

  /** The region chosen in the form; the address may preselect it with ?region=. */
  @state() private region = new URLSearchParams(location.search).get("region") ?? ""
  /** One key per machine in the form; keys only ever grow, so removing one keeps the others' fields. */
  @state() private machines: ReadonlyArray<number> = [0]
  @state() private sent: Option.Option<{ readonly region: string; readonly application: Application }> = Option.none()

  readonly #offers = this.query(() => everyRegion(offer))
  readonly #apply = this.action(apply, (application, region) => {
    this.sent = Option.some({ region, application })
  })

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Apply to a region</h1>
          <p class="muted">
            The operator reads every application and decides which apps each machine runs. You can apply to more than one region;
            each is a separate membership.
          </p>
        </header>
        ${Option.match(this.sent, {
          onSome: ({ region, application }) => html`
            <div class="notice good stack-s" role="status">
              <p><strong>Your application was sent.</strong> It is waiting for the operator of ${region}.</p>
              <p>
                You applied as ${application.display_name} with ${application.machines.length === 1 ? "one machine" : `${application.machines.length} machines`}.
                Your overview shows the decision when it is made.
              </p>
              <p><a class="button primary" href="/">Go to your overview</a></p>
            </div>
          `,
          onNone: () => renderQuery(this.#offers, (results) => html`${regionNotices(results)} ${this.#form(answered(results))}`),
        })}
      </div>
    `
  }

  #form(offers: ReadonlyArray<{ readonly region: RegionConfig; readonly value: RegionOffer }>) {
    const open = offers.filter((o) => o.value.pool.taking_applications)
    if (open.length === 0) {
      return html`<p class="notice warn">No region is taking applications right now. A region opens once its operator has published its terms.</p>`
    }
    const chosen = open.find((o) => o.region.id === this.region) ?? open[0]
    if (chosen === undefined) return nothing
    const accepted = chosen.value.gpus.filter((gpu) => gpu.accepted)
    return html`
      <form @submit=${(event: SubmitEvent) => this.#submit(event, chosen.region.id, accepted)}>
        <div class="fields">
          <label>
            Region
            <select name="region" @change=${(event: Event) => (this.region = (event.target as HTMLSelectElement).value)}>
              ${open.map((o) => html`<option value=${o.region.id} ?selected=${o.region.id === chosen.region.id}>${o.region.name}</option>`)}
            </select>
          </label>
          <label>
            Display name
            <input name="display_name" required maxlength="60" autocomplete="nickname" />
            <span class="hint">Shown on the public leaderboard beside your address.</span>
          </label>
        </div>

        ${this.machines.map((key, index) => html`
          <fieldset>
            <legend>Machine ${index + 1}</legend>
            <label>
              Name for this machine
              <input name="label:${key}" required maxlength="60" placeholder="e.g. garage-4090" />
              <span class="hint">Only you and the operator see it.</span>
            </label>
            <div class="stack-s">
              <p id="gpus-${key}"><strong>GPUs in this machine</strong> <span class="hint">Enter how many of each model it has.</span></p>
              ${accepted.length === 0
                ? html`<p class="notice warn">${chosen.region.name} lists no GPU models yet, so it cannot take applications.</p>`
                : html`
                    <div class="gpu-counts" role="group" aria-labelledby="gpus-${key}">
                      ${accepted.map((gpu) => html`
                        <label>
                          <input type="number" name="gpu:${key}:${gpu.id}" min="0" max="64" step="1" value="0" inputmode="numeric" />
                          <span>${gpu.name} <span class="hint">${formatMemory(gpu.vram_mb)}</span></span>
                        </label>
                      `)}
                    </div>
                  `}
            </div>
            ${this.machines.length > 1
              ? html`<p><button type="button" class="small quiet" @click=${() => (this.machines = this.machines.filter((k) => k !== key))}>Remove this machine</button></p>`
              : nothing}
          </fieldset>
        `)}
        <p>
          <button type="button" @click=${() => (this.machines = [...this.machines, Math.max(...this.machines) + 1])}>Add another machine</button>
        </p>

        <fieldset>
          <legend>Terms</legend>
          ${this.#terms("Open Pool terms", chosen.value.terms.global)} ${this.#terms(`${chosen.region.name} terms`, chosen.value.terms.regional)}
          <label class="inline">
            <input type="checkbox" name="accept" required />
            <span>I have read these terms and accept them. My signature on this application says so.</span>
          </label>
        </fieldset>

        <p class="muted small">
          Sending asks your wallet to sign a message that names the terms' versions and this application. Your address is the
          address you will be paid at.
        </p>
        <p>
          <button type="submit" class="primary" ?disabled=${this.#apply.status === TaskStatus.PENDING}>Sign and send application</button>
        </p>
        ${actionStatus(this.#apply, "Waiting for your wallet, then sending…")}
      </form>
    `
  }

  #terms(heading: string, terms: Option.Option<Terms>) {
    return Option.match(terms, {
      onNone: () => nothing,
      onSome: (t) => html`
        <details open>
          <summary>${heading}, version ${t.version} <span class="hint">published ${day(t.published_at)}</span></summary>
          <p class="terms" tabindex="0">${t.body}</p>
        </details>
      `,
    })
  }

  #submit(event: SubmitEvent, region: string, gpus: ReadonlyArray<Gpu>): void {
    event.preventDefault()
    const data = new FormData(event.target as HTMLFormElement)
    const machines = this.machines.map((key) => ({
      label: String(data.get(`label:${key}`) ?? "").trim(),
      gpus: gpus
        .map((gpu) => ({ gpu_id: gpu.id, count: Number(data.get(`gpu:${key}:${gpu.id}`) ?? 0) }))
        .filter((gpu) => gpu.count > 0),
    }))
    void this.#apply.run([region, { display_name: String(data.get("display_name") ?? "").trim(), machines }]).catch(() => undefined)
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "apply-page": ApplyPage
  }
}
