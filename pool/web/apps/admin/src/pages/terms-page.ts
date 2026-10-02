import { TaskStatus } from "@lit/task"
import { Api, regions as allRegions, type RegionConfig, type RegionResult, type Terms, type TermsScope } from "@openpool/core"
import { actionStatus, regionNotices, regionSections, renderQuery, SitePage, time } from "@openpool/ui"
import { Effect, Option } from "effect"
import { css, html, nothing } from "lit"
import { customElement, query, state } from "lit/decorators.js"
import { everywhere, field, inRegions, outcomes } from "../lib.js"

const page = Effect.all({ regions: allRegions, terms: everywhere(Api.terms) })

/** The terms members accept: the pool's, the same in every region, and each region's own. */
@customElement("terms-page")
export class TermsPage extends SitePage {
  static override styles = [
    ...SitePage.styles,
    css`
      .terms { white-space: pre-wrap; max-block-size: 20rem; overflow-y: auto; padding: var(--space-s); background: var(--surface-sunken); border-radius: var(--radius-s); }
      details > summary { cursor: pointer; font-weight: 600; }
    `,
  ]

  @state() private scope: TermsScope = "global"
  @state() private published: Option.Option<ReadonlyArray<RegionResult<Terms>>> = Option.none()
  @query("textarea[name=body]") private body!: HTMLTextAreaElement

  readonly #page = this.query(() => page)
  readonly #publish = this.action(
    (regions: ReadonlyArray<string>, scope: TermsScope, body: string) => inRegions(regions, Api.publishTerms(scope, body)),
    (results) => {
      this.published = Option.some(results)
      void this.#page.run()
    },
  )

  override render() {
    return html`
      <div class="stack">
        <header class="stack-s">
          <h1>Terms</h1>
          <p class="muted">
            An applicant accepts the terms in force when they apply, by signing their version numbers. Publishing new terms
            applies to applications from then on; members are not asked again. A region takes applications only once the
            pool's terms are published there.
          </p>
        </header>
        ${renderQuery(this.#page, ({ regions, terms }) => html`
          ${this.#form(regions)} ${Option.match(this.published, { onNone: () => nothing, onSome: (results) => outcomes(results, "Published") })}
          ${actionStatus(this.#publish)} ${regionNotices(terms)}
          ${regionSections(terms, (region, current) => html`
            ${this.#current("The pool's terms", current.global, `No pool terms are published in ${region.name}, so it takes no applications.`)}
            ${this.#current(`${region.name}'s own terms`, current.regional, `${region.name} has no terms of its own.`)}
          `)}
        `)}
      </div>
    `
  }

  #form(regions: ReadonlyArray<RegionConfig>) {
    return html`
      <form class="card" @submit=${(event: SubmitEvent) => this.#submit(event, regions)}>
        <h2>Publish new terms</h2>
        <fieldset>
          <legend>Which terms</legend>
          <label class="inline">
            <input type="radio" name="scope" value="global" ?checked=${this.scope === "global"} @change=${() => (this.scope = "global")} />
            <span>The pool's terms, published in every region so they stay the same everywhere</span>
          </label>
          <label class="inline">
            <input type="radio" name="scope" value="regional" ?checked=${this.scope === "regional"} @change=${() => (this.scope = "regional")} />
            <span>One region's own terms, on top of the pool's</span>
          </label>
          ${this.scope === "regional"
            ? html`
                <label style="max-inline-size: 16rem">
                  Region
                  <select name="region">
                    ${regions.map((region) => html`<option value=${region.id}>${region.name}</option>`)}
                  </select>
                </label>
              `
            : nothing}
        </fieldset>
        <label>
          Text
          <textarea name="body" required></textarea>
          <span class="hint">Plain text. Publishing the same text again changes nothing.</span>
        </label>
        <p><button type="submit" class="primary" ?disabled=${this.#publish.status === TaskStatus.PENDING}>Publish</button></p>
      </form>
    `
  }

  #current(heading: string, terms: Option.Option<Terms>, missing: string) {
    return Option.match(terms, {
      onNone: () => html`<p class="muted">${missing}</p>`,
      onSome: (t) => html`
        <details>
          <summary>${heading}, version ${t.version} <span class="hint">published ${time(t.published_at)}</span></summary>
          <div class="stack-s">
            <p class="terms">${t.body}</p>
            <p><button type="button" class="small" @click=${() => this.#startFrom(t)}>Start a new version from this text</button></p>
          </div>
        </details>
      `,
    })
  }

  #startFrom(terms: Terms): void {
    this.scope = terms.scope
    this.body.value = terms.body
    this.body.focus()
  }

  #submit(event: SubmitEvent, regions: ReadonlyArray<RegionConfig>): void {
    event.preventDefault()
    const data = new FormData(event.target as HTMLFormElement)
    const where = this.scope === "global" ? regions.map((region) => region.id) : [field(data, "region")]
    this.published = Option.none()
    void this.#publish.run([where, this.scope, String(data.get("body") ?? "")])
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "terms-page": TermsPage
  }
}
