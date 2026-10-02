import { html } from "@lit-labs/ssr"
import { breadcrumb, type Route } from "../routes.js"

export const appsPage: Route = {
  path: "/apps/",
  mode: "ssg",
  meta: (site) => ({
    title: "Apps and prices",
    description:
      "The AI and video apps Open Pool's machines serve in each region, what customers pay for each, which GPUs can run them, and how many machines are connected now.",
    path: "/apps/",
    index: true,
    jsonLd: [breadcrumb(site, "Apps and prices", "/apps/")],
  }),
  template: () => html`
    <article class="prose wide">
      <header>
        <h1>Apps and prices</h1>
        <p class="lede">What the pool's machines serve, region by region, loaded live from each region.</p>
      </header>
      <op-live-apps>
        <p class="muted">The apps and prices of each region load here. This part of the page needs JavaScript.</p>
      </op-live-apps>
      <section aria-labelledby="reading">
        <h2 id="reading">Reading the table</h2>
        <dl class="glossary">
          <div>
            <dt>Price</dt>
            <dd>What customers pay the orchestrator, in US dollars: per hour a session is open, per hour of 720p-equivalent video, or per request. Members are paid in ETH from what the orchestrator earns.</dd>
          </div>
          <div>
            <dt>Runs on</dt>
            <dd>The GPU models the operator allows for the app. A machine is only assigned apps its GPUs can run.</dd>
          </div>
          <div>
            <dt>Machines connected</dt>
            <dd>How many machines are connected for the app right now.</dd>
          </div>
        </dl>
        <p>The operator decides which app each machine runs, by where the demand is. <a href="/how-it-works/#earn">How earnings are split</a>.</p>
      </section>
    </article>
  `,
}
