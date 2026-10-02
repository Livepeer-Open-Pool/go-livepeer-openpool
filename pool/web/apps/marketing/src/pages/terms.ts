import { html } from "@lit-labs/ssr"
import { breadcrumb, type Route } from "../routes.js"

export const termsPage: Route = {
  path: "/terms/",
  mode: "ssg",
  meta: (site) => ({
    title: "Terms",
    description: "The terms an Open Pool member accepts by applying: the pool's terms, which are the same in every region, and each region's own.",
    path: "/terms/",
    index: true,
    jsonLd: [breadcrumb(site, "Terms", "/terms/")],
  }),
  template: () => html`
    <article class="prose">
      <header>
        <h1>Terms</h1>
        <p class="lede">
          Applying is accepting: your application carries your wallet's signature over the versions of the terms in force that day.
          The text below is the current one, loaded from the pool's regions.
        </p>
      </header>
      <op-live-terms>
        <p class="muted">The current terms load here from the pool's regions. This part of the page needs JavaScript.</p>
      </op-live-terms>
      <section aria-labelledby="short">
        <h2 id="short">In short</h2>
        <p>This summary is not the terms. It is here so nothing in them surprises you.</p>
        <ul>
          <li>By applying you accept the terms. You are responsible for reading them.</li>
          <li>A member can be banned for any reason at any time. Malicious use of the software is an immediate ban.</li>
          <li>The agent you install has access to your machine's Docker.</li>
          <li>You are paid per payout window, at the address you applied with, under the commission in force for that window.</li>
          <li>New versions of the terms apply from the day they are published.</li>
        </ul>
      </section>
    </article>
  `,
}
