import { Api, everyRegion } from "@openpool/core"
import { badge, renderQuery, SitePage } from "@openpool/ui"
import { Either } from "effect"
import { css, html } from "lit"
import { customElement } from "lit/decorators.js"

/** Whether each region of the pool is answering, in the footer of every page. */
@customElement("op-region-status")
export class OpRegionStatus extends SitePage {
  static override styles = [
    ...SitePage.styles,
    css`
      ul { display: flex; flex-wrap: wrap; gap: var(--space-xs) var(--space); list-style: none; padding: 0; }
    `,
  ]

  readonly #pools = this.query(() => everyRegion(Api.pool))

  override render() {
    return renderQuery(this.#pools, (results) => html`
      <ul aria-label="The pool's regions">
        ${results.map(({ region, result }) => html`<li>${region.name} ${Either.isRight(result) ? badge("up", "good") : badge("not answering", "bad")}</li>`)}
      </ul>
    `)
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-region-status": OpRegionStatus
  }
}
