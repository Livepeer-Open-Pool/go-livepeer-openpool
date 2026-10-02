import { css, html, LitElement, svg } from "lit"
import { customElement, state } from "lit/decorators.js"
import { base, controls } from "./styles.js"

/** Whether the site follows the system's theme or the visitor has chosen one. */
type Choice = "system" | "light" | "dark"

declare global {
  interface Window {
    /** Set by the theme script in every page's head, which also remembers the choice. */
    openPoolTheme?: { readonly get: () => Choice; readonly set: (choice: Choice) => void }
  }
}

const order: ReadonlyArray<Choice> = ["system", "light", "dark"]
const names: Readonly<Record<Choice, string>> = { system: "following your system", light: "light", dark: "dark" }
const targets: Readonly<Record<Choice, string>> = { system: "follow your system", light: "light", dark: "dark" }

const icons: Readonly<Record<Choice, ReturnType<typeof svg>>> = {
  system: svg`<rect x="3" y="4.5" width="18" height="12" rx="2"></rect><path d="M8.5 20h7M12 16.5V20"></path>`,
  light: svg`<circle cx="12" cy="12" r="4"></circle><path d="M12 2.5v2.5M12 19v2.5M2.5 12H5M19 12h2.5M5.3 5.3l1.8 1.8M16.9 16.9l1.8 1.8M5.3 18.7l1.8-1.8M16.9 7.1l1.8-1.8"></path>`,
  dark: svg`<path d="M20 14.2A8.3 8.3 0 0 1 9.8 4 8.3 8.3 0 1 0 20 14.2Z"></path>`,
}

/**
 * The switch between following the system's theme, light, and dark. Each
 * press moves to the next; the choice is kept by the theme script.
 */
@customElement("op-theme")
export class OpTheme extends LitElement {
  static override styles = [
    base,
    controls,
    css`
      :host { display: inline-block; }
      button { min-inline-size: 2.75rem; padding: 0; border-color: var(--border); background: transparent; color: var(--text-muted); }
      button:hover { color: var(--brand); }
      svg { inline-size: 1.25rem; block-size: 1.25rem; fill: none; stroke: currentColor; stroke-width: 1.7; stroke-linecap: round; stroke-linejoin: round; }
    `,
  ]

  @state() private choice: Choice = window.openPoolTheme?.get() ?? "system"

  override render() {
    const next = this.#next()
    const label = `Theme: ${names[this.choice]}. Switch to ${targets[next]}.`
    return html`
      <button type="button" aria-label=${label} title=${label} @click=${this.#advance}>
        <svg viewBox="0 0 24 24" aria-hidden="true">${icons[this.choice]}</svg>
      </button>
    `
  }

  #next(): Choice {
    return order[(order.indexOf(this.choice) + 1) % order.length] ?? "system"
  }

  readonly #advance = (): void => {
    this.choice = this.#next()
    if (window.openPoolTheme !== undefined) {
      window.openPoolTheme.set(this.choice)
    } else if (this.choice === "system") {
      // No theme script on this page: the choice holds until the page is left.
      document.documentElement.removeAttribute("data-theme")
    } else {
      document.documentElement.setAttribute("data-theme", this.choice)
    }
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "op-theme": OpTheme
  }
}
