import { html, nothing, type TemplateResult } from "lit"

/**
 * The logo beside the site's name, linking home. `site` names which of the
 * pool's sites this is, under the name: "members", "admin", or nothing on
 * the public site.
 */
export const lockup = (site?: string): TemplateResult => html`
  <a class="brand" href="/" translate="no">
    <span class="mark" aria-hidden="true"></span>
    <span class="name">
      <span class="word">Open Pool</span>
      ${site === undefined ? nothing : html`<small>${site}</small>`}
    </span>
  </a>
`
