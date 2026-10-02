import { css, html, type TemplateResult } from "lit"

/** A link in a site's primary navigation. */
export interface NavItem {
  readonly href: string
  readonly label: string
}

/** Whether a nav link leads to the page that is open, or to a section it is in. */
const isCurrent = (href: string, path: string): boolean => (href === "/" ? path === "/" : path === href || path.startsWith(`${href}/`))

/** The site's primary navigation; the open page's link is marked for assistive technology and for style. */
export const primaryNav = (items: ReadonlyArray<NavItem>, path: string): TemplateResult => html`
  <nav aria-label="Primary">
    <ul>
      ${items.map((item) => html`<li><a href=${item.href} aria-current=${isCurrent(item.href, path) ? "page" : "false"}>${item.label}</a></li>`)}
    </ul>
  </nav>
`

/**
 * The frame the members' and operator's sites share: a banner with the logo
 * and navigation, the page, a footer. Signed in on a wide screen, the banner
 * stands down the side; otherwise it lies across the top.
 */
export const shell = css`
  :host {
    display: grid; min-block-size: 100dvh;
    grid-template-rows: auto 1fr auto;
    grid-template-columns: [full-start] minmax(var(--space), 1fr) [content-start] min(100% - 2 * var(--space), 76rem) [content-end] minmax(var(--space), 1fr) [full-end];
  }
  :host > * { grid-column: content; }
  .skip {
    position: absolute; inset-block-start: var(--space-xs); inset-inline-start: var(--space-xs); z-index: 10;
    padding: var(--space-xs) var(--space-s); background: var(--surface-raised); border-radius: var(--radius-s);
    transform: translateY(-200%);
  }
  .skip:focus { transform: none; }
  header.banner {
    grid-column: full; display: grid; grid-template-columns: subgrid;
    border-block-end: 1px solid var(--border);
    background: color-mix(in oklch, var(--surface) 86%, transparent); backdrop-filter: blur(12px);
    position: sticky; inset-block-start: 0; z-index: 5;
  }
  header.banner > .bar { grid-column: content; display: flex; flex-wrap: wrap; align-items: center; gap: var(--space-xs) var(--space); padding-block: var(--space-s); }
  nav ul { display: flex; flex-wrap: wrap; gap: var(--space-2xs); list-style: none; padding: 0; }
  nav a {
    display: flex; align-items: center; min-block-size: 2.5rem; padding: 0.4rem 0.8rem; border-radius: var(--radius-s);
    color: var(--text-muted); text-decoration: none; font-weight: 550; font-size: var(--step--1);
  }
  nav a:hover { color: var(--text); background: var(--surface-sunken); }
  nav a[aria-current="page"] { color: var(--text); background: var(--brand-subtle); box-shadow: inset 0 -2px 0 var(--brand); }
  .who { margin-inline-start: auto; display: flex; flex-wrap: wrap; align-items: center; gap: var(--space-xs) var(--space-s); font-size: var(--step--1); }
  .who .subject { font-family: var(--font-mono); color: var(--text-muted); overflow-wrap: anywhere; }
  main { padding-block: var(--space-l); min-inline-size: 0; }
  main:focus { outline: none; }
  footer {
    grid-column: full; display: grid; grid-template-columns: subgrid;
    border-block-start: 1px solid var(--border); color: var(--text-muted); font-size: var(--step--1);
  }
  footer > .bar { grid-column: content; display: flex; flex-wrap: wrap; gap: var(--space-xs) var(--space); padding-block: var(--space); }

  /* The sign-in page is one card, centered, with the logo's rings behind it. */
  .signin {
    max-inline-size: 38rem; margin-inline: auto; margin-block-start: var(--space-l);
    padding: var(--space-l); border: 1px solid var(--border); border-radius: calc(var(--radius) * 1.25);
    background: var(--surface-raised); box-shadow: var(--shadow);
  }

  @media (min-width: 64rem) {
    :host([signed-in]) {
      grid-template-rows: 1fr auto;
      grid-template-columns:
        [side-start] 16.5rem [side-end full-start] minmax(var(--space-l), 1fr)
        [content-start] minmax(0, 76rem) [content-end]
        minmax(var(--space-l), 1fr) [full-end];
    }
    :host([signed-in]) header.banner {
      grid-column: side; grid-row: 1 / -1; display: block;
      block-size: 100dvh; overflow-y: auto; overscroll-behavior: contain;
      border-block-end: 0; border-inline-end: 1px solid var(--border);
      background: var(--surface-sunken); backdrop-filter: none;
    }
    :host([signed-in]) header.banner > .bar {
      flex-direction: column; flex-wrap: nowrap; align-items: stretch; gap: var(--space);
      min-block-size: 100%; padding: var(--space) var(--space-s);
    }
    :host([signed-in]) .brand { padding-inline: var(--space-xs); }
    :host([signed-in]) nav ul { flex-direction: column; flex-wrap: nowrap; }
    :host([signed-in]) nav a { font-size: var(--step-0); }
    :host([signed-in]) nav a[aria-current="page"] { box-shadow: inset 2px 0 0 var(--brand); }
    :host([signed-in]) .who {
      margin-inline-start: 0; margin-block-start: auto; align-items: stretch; flex-direction: column;
      padding: var(--space-s); border: 1px solid var(--border); border-radius: var(--radius-s);
    }
    :host([signed-in]) main { grid-row: 1; }
    :host([signed-in]) footer { grid-row: 2; }
  }
`
