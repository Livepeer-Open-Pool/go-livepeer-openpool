import { css } from "lit"

// Styles shared by the sites' components. Each component lists the ones it
// uses in `static styles`; they are parsed once and shared by every instance.
// All of them read the tokens of global.css, which inherit into shadow roots.

/** What every shadow root starts from. */
export const base = css`
  *, *::before, *::after { box-sizing: border-box; }
  * { margin: 0; }
  :host { display: block; color: var(--text); }
  [hidden] { display: none !important; }
  h1, h2, h3, h4 { text-wrap: balance; line-height: 1.15; letter-spacing: -0.012em; }
  h1 { font-size: var(--step-2); }
  h2 { font-size: var(--step-1); }
  h3 { font-size: var(--step-0); }
  p, li, dd { text-wrap: pretty; }
  a { color: var(--brand); text-decoration-thickness: 0.08em; text-underline-offset: 0.18em; }
  a:hover { color: var(--brand-hover); }
  code, kbd, samp, pre { font-family: var(--font-mono); font-size: 0.92em; }
  :focus-visible { outline: 2px solid var(--brand); outline-offset: 2px; border-radius: 2px; }
  .muted { color: var(--text-muted); }
  .small { font-size: var(--step--1); }
  .num { font-variant-numeric: tabular-nums; }
  .stack { display: grid; gap: var(--space); align-content: start; }
  .stack-s { display: grid; gap: var(--space-s); align-content: start; }
  .row { display: flex; flex-wrap: wrap; gap: var(--space-s); align-items: center; }
  .push { margin-inline-start: auto; }
  .visually-hidden {
    position: absolute; inline-size: 1px; block-size: 1px; overflow: clip;
    clip-path: inset(50%); white-space: nowrap;
  }
`

/** Buttons, fields and forms. */
export const controls = css`
  button, .button {
    display: inline-flex; align-items: center; justify-content: center; gap: var(--space-xs);
    min-block-size: 2.75rem; padding: 0.5rem 1rem;
    border: 1px solid var(--border-strong); border-radius: var(--radius-s);
    background: var(--surface-raised); color: var(--text);
    font: inherit; font-weight: 550; line-height: 1.2; text-decoration: none;
    cursor: pointer; touch-action: manipulation;
    transition: background-color 0.15s ease-out, border-color 0.15s ease-out, color 0.15s ease-out;
  }
  button:hover, .button:hover { border-color: var(--brand); color: var(--brand); }
  button.primary, .button.primary { background: var(--brand); border-color: var(--brand); color: var(--on-brand); }
  button.primary:hover, .button.primary:hover { background: var(--brand-hover); border-color: var(--brand-hover); color: var(--on-brand); }
  button.danger { color: var(--bad); border-color: color-mix(in oklch, var(--bad) 45%, var(--surface)); }
  button.danger:hover { background: var(--bad-subtle); border-color: var(--bad); color: var(--bad); }
  button.quiet { border-color: transparent; background: transparent; color: var(--brand); padding-inline: 0.5rem; }
  button.quiet:hover { background: var(--brand-subtle); }
  button:disabled { opacity: 0.55; cursor: not-allowed; }
  button.small { min-block-size: 2.25rem; padding: 0.3rem 0.7rem; font-size: var(--step--1); }

  label { display: grid; gap: var(--space-2xs); font-weight: 550; }
  label .hint, .hint { font-weight: 400; font-size: var(--step--1); color: var(--text-muted); }
  label.inline { display: flex; align-items: center; gap: var(--space-xs); font-weight: 400; }
  input, textarea, select {
    inline-size: 100%; min-block-size: 2.75rem; padding: 0.5rem 0.75rem;
    border: 1px solid var(--border-strong); border-radius: var(--radius-s);
    background: var(--surface-raised); color: var(--text); font: inherit;
    transition: border-color 0.15s;
  }
  input[type="checkbox"], input[type="radio"] { inline-size: 1.15rem; min-block-size: 1.15rem; padding: 0; }
  textarea { field-sizing: content; min-block-size: 4lh; max-block-size: 24lh; resize: vertical; }
  input:user-invalid, textarea:user-invalid, select:user-invalid { border-color: var(--bad); }
  ::placeholder { color: var(--text-muted); }
  fieldset { border: 1px solid var(--border); border-radius: var(--radius); padding: var(--space); display: grid; gap: var(--space-s); min-inline-size: 0; }
  legend { padding-inline: var(--space-xs); font-weight: 650; }
  form { display: grid; gap: var(--space); }
  .fields { display: grid; gap: var(--space-s); grid-template-columns: repeat(auto-fit, minmax(min(14rem, 100%), 1fr)); align-items: end; }
`

/** Cards, notices and status badges. */
export const surfaces = css`
  .card {
    background: var(--surface-raised); border: 1px solid var(--border);
    border-radius: var(--radius); padding: var(--space); box-shadow: var(--shadow);
  }
  .cards { display: grid; gap: var(--space); grid-template-columns: repeat(auto-fit, minmax(min(17rem, 100%), 1fr)); }
  .notice {
    border: 1px solid var(--border); border-inline-start: 4px solid var(--border-strong);
    border-radius: var(--radius-s); padding: var(--space-s) var(--space); background: var(--surface-raised);
  }
  .notice.good { border-inline-start-color: var(--good); background: var(--good-subtle); }
  .notice.warn { border-inline-start-color: var(--warn); background: var(--warn-subtle); }
  .notice.bad { border-inline-start-color: var(--bad); background: var(--bad-subtle); }
  .badge {
    display: inline-block; padding: 0.1em 0.6em; border-radius: 999px;
    font-size: var(--step--1); font-weight: 600; line-height: 1.5; white-space: nowrap;
    background: var(--surface-sunken); color: var(--text-muted); border: 1px solid var(--border);
  }
  .badge.good { background: var(--good-subtle); color: var(--good); border-color: color-mix(in oklch, var(--good) 35%, var(--surface)); }
  .badge.warn { background: var(--warn-subtle); color: var(--warn); border-color: color-mix(in oklch, var(--warn) 35%, var(--surface)); }
  .badge.bad { background: var(--bad-subtle); color: var(--bad); border-color: color-mix(in oklch, var(--bad) 35%, var(--surface)); }
  dl.facts { display: grid; grid-template-columns: max-content 1fr; gap: var(--space-2xs) var(--space); }
  dl.facts dt { color: var(--text-muted); }
  dl.facts dd { min-inline-size: 0; overflow-wrap: anywhere; }
  .skeleton {
    --shine: color-mix(in oklch, var(--text) 7%, var(--surface));
    min-block-size: 3lh; border-radius: var(--radius-s);
    background: linear-gradient(100deg in oklch, var(--shine) 40%, color-mix(in oklch, var(--shine) 45%, var(--surface)) 50%, var(--shine) 60%) 0 0 / 200% 100%;
  }
  @media (prefers-reduced-motion: no-preference) {
    .skeleton { animation: shimmer 1.4s linear infinite; }
  }
  @keyframes shimmer { to { background-position: -200% 0; } }
`

/** Tables of data. They scroll sideways inside their own box on narrow screens. */
export const tables = css`
  .table-scroll { overflow-x: auto; border: 1px solid var(--border); border-radius: var(--radius); background: var(--surface-raised); }
  table { inline-size: 100%; border-collapse: collapse; font-size: var(--step--1); }
  caption { text-align: start; padding: var(--space-s) var(--space); font-weight: 650; font-size: var(--step-0); }
  th, td { padding: var(--space-xs) var(--space-s); text-align: start; vertical-align: top; }
  th:first-child, td:first-child { padding-inline-start: var(--space); }
  th:last-child, td:last-child { padding-inline-end: var(--space); }
  thead th { color: var(--text-muted); font-weight: 600; white-space: nowrap; border-block-end: 1px solid var(--border); }
  tbody tr + tr > * { border-block-start: 1px solid var(--border); }
  tbody th { font-weight: 600; }
  td.num, th.num { text-align: end; font-variant-numeric: tabular-nums; white-space: nowrap; }
  td.actions { white-space: nowrap; }
  td.actions > * + * { margin-inline-start: var(--space-2xs); }
  tfoot td, tfoot th { border-block-start: 1px solid var(--border-strong); font-weight: 650; }
`

/** Dialogs, with entry and exit transitions. */
export const dialogs = css`
  dialog {
    inline-size: min(34rem, calc(100vw - 2 * var(--space)));
    border: 1px solid var(--border); border-radius: var(--radius);
    background: var(--surface-raised); color: var(--text);
    box-shadow: var(--shadow); padding: var(--space-l);
    opacity: 1; scale: 1;
    transition: opacity 0.2s ease-out, scale 0.2s ease-out, display 0.2s allow-discrete, overlay 0.2s allow-discrete;
  }
  dialog::backdrop {
    background: oklch(15% 0.02 var(--hue) / 0.45); backdrop-filter: blur(4px);
    transition: opacity 0.2s, display 0.2s allow-discrete, overlay 0.2s allow-discrete;
  }
  dialog:not([open]) { opacity: 0; scale: 0.97; }
  @starting-style {
    dialog[open] { opacity: 0; scale: 0.97; }
    dialog[open]::backdrop { opacity: 0; }
  }
`
