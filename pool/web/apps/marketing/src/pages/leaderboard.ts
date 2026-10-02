import { html } from "@lit-labs/ssr"
import { breadcrumb, type Route } from "../routes.js"

export const leaderboardPage: Route = {
  path: "/leaderboard/",
  mode: "ssg",
  meta: (site) => ({
    title: "Leaderboard",
    description: "Open Pool's members ranked by the fees their machines have earned, across every region, for the current payout window or for all time.",
    path: "/leaderboard/",
    index: true,
    jsonLd: [breadcrumb(site, "Leaderboard", "/leaderboard/")],
  }),
  template: () => html`
    <article class="prose wide">
      <header>
        <h1>Leaderboard</h1>
        <p class="lede">Members ranked by the fees their machines earned, added up across every region.</p>
      </header>
      <op-leaderboard>
        <p class="muted">The leaderboard loads here from each region. This part of the page needs JavaScript.</p>
      </op-leaderboard>
      <p>
        Fees are what customers paid for the work a member's machines did. They decide each member's share of a payout window;
        <a href="/how-it-works/#earn">the payout itself is a share of what the orchestrator redeemed</a>.
      </p>
    </article>
  `,
}
