import { SitePage } from "@openpool/ui"
import { html } from "lit"
import { customElement } from "lit/decorators.js"

@customElement("not-found-page")
export class NotFoundPage extends SitePage {
  override render() {
    return html`
      <section class="stack">
        <h1>There is no page at this address</h1>
        <p>It may have moved, or the link may be mistyped. <a href="/">Go to your overview</a>.</p>
      </section>
    `
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "not-found-page": NotFoundPage
  }
}
