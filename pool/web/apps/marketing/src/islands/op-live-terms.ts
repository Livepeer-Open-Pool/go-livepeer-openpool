import { answered, Api, everyRegion, type RegionConfig, type Terms } from "@openpool/core"
import { day, regionNotices, renderQuery, SitePage } from "@openpool/ui"
import { Array, Option, pipe } from "effect"
import { css, html, nothing } from "lit"
import { customElement } from "lit/decorators.js"

/**
 * The terms in force: the pool's, shown once when every region has the same
 * text and per region when they differ, and each region's own.
 */
@customElement("op-live-terms")
export class OpLiveTerms extends SitePage {
  static override styles = [
    ...SitePage.styles,
    css`
      .terms { white-space: pre-wrap; padding: var(--space); background: var(--surface-raised); border: 1px solid var(--border); border-radius: var(--radius); }
    `,
  ]

  readonly #terms = this.query(() => everyRegion(Api.publicTerms))

  override render() {
    return renderQuery(this.#terms, (results) => {
      const regions = answered(results)
      const globals = pipe(
        regions,
        Array.filterMap(({ region, value }) => Option.map(value.global, (terms) => ({ region, terms }))),
      )
      const same = pipe(
        globals,
        Array.map((g) => g.terms.hash),
        Array.dedupe,
      ).length <= 1
      const regional = pipe(
        regions,
        Array.filterMap(({ region, value }) => Option.map(value.regional, (terms) => ({ region, terms }))),
      )
      return html`
        <div class="stack">
          ${regionNotices(results)}
          ${globals.length === 0 && regions.length > 0 ? html`<p class="muted">The pool has not published its terms yet, so it is not taking applications.</p>` : nothing}
          ${same
            ? pipe(
                Array.head(globals),
                Option.match({ onNone: () => nothing, onSome: ({ terms }) => this.#section("The pool's terms", "pool", terms) }),
              )
            : globals.map(({ region, terms }) => this.#section(`The pool's terms as published in ${region.name}`, `pool-${region.id}`, terms))}
          ${regional.map(({ region, terms }: { region: RegionConfig; terms: Terms }) => this.#section(`${region.name}'s own terms`, region.id, terms))}
        </div>
      `
    })
  }

  #section(heading: string, id: string, terms: Terms) {
    return html`
      <section class="stack-s" aria-labelledby="terms-${id}">
        <h2 id="terms-${id}">${heading}</h2>
        <p class="muted small">Version ${terms.version}, published ${day(terms.published_at)}.</p>
        <p class="terms">${terms.body}</p>
      </section>
    `
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-live-terms": OpLiveTerms
  }
}
