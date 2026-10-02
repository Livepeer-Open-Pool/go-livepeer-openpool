import { css, html, LitElement } from "lit"
import { customElement, property, state } from "lit/decorators.js"
import { base, controls } from "./styles.js"

/**
 * A line or block of text meant to be copied: a command, a key, an address.
 * The text is shown whole, and a button copies it.
 */
@customElement("op-copy")
export class OpCopy extends LitElement {
  static override styles = [
    base,
    controls,
    css`
      :host { display: block; }
      figure {
        display: grid; grid-template-columns: 1fr auto; gap: var(--space-s); align-items: start;
        background: var(--surface-sunken); border: 1px solid var(--border); border-radius: var(--radius-s);
        padding: var(--space-s);
      }
      figcaption { grid-column: 1 / -1; font-size: var(--step--1); color: var(--text-muted); }
      pre { white-space: pre-wrap; overflow-wrap: anywhere; min-inline-size: 0; align-self: center; }
    `,
  ]

  /** The text to show and copy. */
  @property() text = ""
  /** What the text is, for the caption and the button's name. */
  @property() label = ""

  @state() private copied = false

  override render() {
    return html`
      <figure>
        ${this.label === "" ? "" : html`<figcaption>${this.label}</figcaption>`}
        <pre><code translate="no">${this.text}</code></pre>
        <button type="button" class="small" @click=${this.#copy} aria-label="Copy ${this.label === "" ? "text" : this.label}">
          ${this.copied ? "Copied" : "Copy"}
        </button>
      </figure>
    `
  }

  readonly #copy = (): void => {
    void navigator.clipboard.writeText(this.text).then(
      () => {
        this.copied = true
        setTimeout(() => (this.copied = false), 2000)
      },
      () => {
        // The browser refused the clipboard: the text is on the page to select by hand.
        this.copied = false
      },
    )
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-copy": OpCopy
  }
}
