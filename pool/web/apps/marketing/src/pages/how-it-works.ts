import { html } from "@lit-labs/ssr"
import { breadcrumb, type Route } from "../routes.js"

export const howItWorksPage: Route = {
  path: "/how-it-works/",
  mode: "ssg",
  meta: (site) => ({
    title: "How Open Pool works",
    description:
      "How a GPU in Open Pool gets work and gets paid: what your machine runs, how it is tested, how a payout window's earnings are split, and what the pool can see.",
    path: "/how-it-works/",
    index: true,
    jsonLd: [breadcrumb(site, "How it works", "/how-it-works/")],
  }),
  template: () => html`
    <article class="prose">
      <header>
        <h1>How Open Pool works</h1>
        <p class="lede">
          Customers pay a Livepeer orchestrator to run AI and video apps. Open Pool is the part that lets machines the orchestrator
          does not own do that work, and pays their owners for it.
        </p>
      </header>

      <section aria-labelledby="pieces">
        <h2 id="pieces">The pieces</h2>
        <dl class="glossary">
          <div>
            <dt><dfn>Region</dfn></dt>
            <dd>One orchestrator node and the pool service beside it. Each region has its own members, apps, prices and payout wallet. You can join more than one.</dd>
          </div>
          <div>
            <dt><dfn>App</dfn></dt>
            <dd>Something customers ask for by name, such as a language model or an image model, with a price. The operator defines the apps of a region.</dd>
          </div>
          <div>
            <dt><dfn>Machine</dfn></dt>
            <dd>One of your boxes, with one or more GPUs. The operator assigns it the apps it runs.</dd>
          </div>
          <div>
            <dt><dfn>Runner</dfn></dt>
            <dd>One app running on one machine, connected to the orchestrator and able to take work.</dd>
          </div>
          <div>
            <dt><dfn>Payout window</dfn></dt>
            <dd>A number of Livepeer rounds, each about a day long, after which the region pays its members.</dd>
          </div>
        </dl>
      </section>

      <section aria-labelledby="runs">
        <h2 id="runs">What your machine runs</h2>
        <p>
          You install one thing: the Open Pool agent, a small container with access to the machine's Docker. Every half minute it
          asks the pool what the machine should run and makes it so. For each assigned app that is two containers: the app itself,
          from an image the operator chose, and a tunnel that connects it to the orchestrator.
        </p>
        <p>
          You do not choose the apps. The operator assigns them by where the demand is, and may change them. When that happens,
          or when an app gets a new version, your machine finishes the sessions it is serving and switches by itself.
        </p>
        <p>
          The agent only applies files the pool has signed, and refuses anything in them beyond app containers on a private
          network: no privileged mode, no host network, no mounts of your disks, no published ports.
          <a href="/connect/#agent">More about the agent</a>.
        </p>
      </section>

      <section aria-labelledby="tests">
        <h2 id="tests">How machines are tested</h2>
        <p>
          The pool tests each runner by sending it a real request through the orchestrator, the same way a customer's request
          arrives. The runner cannot tell a test from paid work.
        </p>
        <ul>
          <li>A new runner is on <strong>probation</strong> and takes no customers until it passes.</li>
          <li>An <strong>active</strong> runner is tested again every few minutes. One that fails several times in a row is suspended.</li>
          <li>A <strong>suspended</strong> runner comes back by itself after passing several times in a row.</li>
        </ul>
        <p>Every test result, with what was wrong, is on your runners page, so a suspended runner always has an explanation.</p>
      </section>

      <section aria-labelledby="earn">
        <h2 id="earn">What you earn</h2>
        <p>
          Customers pay the orchestrator with Livepeer payment tickets, and the orchestrator redeems the winning ones for ETH.
          What a region's orchestrator redeemed during a payout window is that window's <dfn>pot</dfn>. The pool keeps its
          commission, and the rest is split among members in proportion to the fees their machines earned:
        </p>
        <p class="formula">
          <math display="block">
            <mrow>
              <mtext>your payout</mtext><mo>=</mo><mtext>pot</mtext><mo>×</mo>
              <mrow><mo>(</mo><mn>1</mn><mo>−</mo><mtext>commission</mtext><mo>)</mo></mrow><mo>×</mo>
              <mfrac><mtext>your fees</mtext><mtext>all members' fees</mtext></mfrac>
            </mrow>
          </math>
        </p>
        <p>
          For example: a window's pot is 1 ETH and the commission is 15%, so 0.85 ETH goes to members. Your machines earned a
          quarter of all fees in the window, so you are paid 0.2125 ETH.
        </p>
        <p>
          Fees decide your share; the pot decides the amount. They differ because Livepeer payments are probabilistic: customers
          pay with tickets, only some of which win, and what wins in one window is not exactly what was earned in it.
        </p>
        <op-pool-facts>
          <p class="muted">The current commission and window length of each region load here. This part of the page needs JavaScript.</p>
        </op-pool-facts>
      </section>

      <section aria-labelledby="paid">
        <h2 id="paid">How you are paid</h2>
        <ul>
          <li>In ETH on Arbitrum One, to the address you applied with. That address cannot be changed; a different address is a different membership.</li>
          <li>Once per payout window, per region. There is no minimum: whatever your share is, it is sent.</li>
          <li>The pool pays the transaction fees.</li>
          <li>By default the operator approves each window's payments before they are sent, and can leave a member out. Your payouts page shows every window and what became of it.</li>
        </ul>
      </section>

      <section aria-labelledby="trust">
        <h2 id="trust">What to know before you join</h2>
        <ul>
          <li>The agent has access to your machine's Docker, which is as good as root on that machine. Run it on a machine you are willing to give that to. Its source is public.</li>
          <li>Customers' inputs and outputs pass through your machine. The <a href="/terms/">terms</a> say what you may not do with them.</li>
          <li>The operator can ban a member at any time. A banned member's machines are disconnected at once.</li>
        </ul>
        <p class="actions"><a class="button primary" href="/join/">Join the pool</a> <a class="button" href="/connect/">Connect a machine</a></p>
      </section>
    </article>
  `,
}
