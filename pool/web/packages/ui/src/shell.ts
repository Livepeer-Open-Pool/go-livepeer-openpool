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

/** The frame every signed-in site shares: a banner with navigation, the page, a footer. */
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
    border-block-end: 1px solid var(--border); background: var(--surface-raised);
    position: sticky; inset-block-start: 0; z-index: 5;
  }
  header.banner > .bar { grid-column: content; display: flex; flex-wrap: wrap; align-items: center; gap: var(--space-xs) var(--space); padding-block: var(--space-xs); }
  .brand { font-weight: 750; font-size: var(--step-0); letter-spacing: -0.02em; color: var(--text); text-decoration: none; white-space: nowrap; }
  .brand span { color: var(--brand); }
  .brand small { font-weight: 500; color: var(--text-muted); margin-inline-start: var(--space-2xs); }
  nav ul { display: flex; flex-wrap: wrap; gap: var(--space-2xs); list-style: none; padding: 0; }
  nav a {
    display: block; padding: 0.4rem 0.7rem; border-radius: var(--radius-s);
    color: var(--text-muted); text-decoration: none; font-weight: 550; font-size: var(--step--1);
  }
  nav a:hover { color: var(--text); background: var(--surface-sunken); }
  nav a[aria-current="page"] { color: var(--brand); background: var(--brand-subtle); }
  .who { margin-inline-start: auto; display: flex; align-items: center; gap: var(--space-s); font-size: var(--step--1); }
  main { padding-block: var(--space-l); min-inline-size: 0; }
  main:focus { outline: none; }
  footer {
    grid-column: full; display: grid; grid-template-columns: subgrid;
    border-block-start: 1px solid var(--border); color: var(--text-muted); font-size: var(--step--1);
  }
  footer > .bar { grid-column: content; display: flex; flex-wrap: wrap; gap: var(--space-xs) var(--space); padding-block: var(--space); }
`
