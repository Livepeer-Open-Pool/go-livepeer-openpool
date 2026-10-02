import { html } from "@lit-labs/ssr"
import { breadcrumb, type Route } from "../routes.js"

export const joinPage: Route = {
  path: "/join/",
  mode: "ssg",
  meta: (site) => ({
    title: "Join the pool",
    description: "Apply to Open Pool with your Ethereum wallet: choose a region, list your GPUs, accept the terms by signing. No account to create and nothing to pay.",
    path: "/join/",
    index: true,
    jsonLd: [breadcrumb(site, "Join the pool", "/join/")],
  }),
  template: (site) => html`
    <article class="prose">
      <header>
        <h1>Join the pool</h1>
        <p class="lede">You apply on the member site, with your Ethereum wallet. There is no account to create and nothing to pay.</p>
      </header>
      <section aria-labelledby="steps">
        <h2 id="steps">Three steps</h2>
        <ol class="steps">
        <li>
          <h3>Have a wallet in your browser</h3>
          <p>Any Ethereum wallet that works as a browser extension will do. The address you sign in with is your membership and the address you are paid at.</p>
        </li>
        <li>
          <h3>Sign in</h3>
          <p>Your wallet signs a short message. It is not a transaction and costs nothing.</p>
        </li>
        <li>
          <h3>Apply to a region</h3>
          <p>Give a display name for the leaderboard, list each machine and its GPUs, read the terms and sign your application.</p>
        </li>
        </ol>
        <p class="actions"><a class="button primary" href="${site.memberUrl}/apply">Go to the member site and apply</a></p>
      </section>
      <section aria-labelledby="regions">
        <h2 id="regions">Which region?</h2>
        <p>Pick the one closest to your machines. You can apply to more than one; each is its own membership with its own payouts.</p>
        <op-pool-facts>
          <p class="muted">The pool's regions, and whether each is taking applications, load here. This part of the page needs JavaScript.</p>
        </op-pool-facts>
      </section>
      <p>Before you apply, <a href="/connect/#before">check what a machine needs</a> and <a href="/terms/">read the terms</a>.</p>
    </article>
  `,
}
