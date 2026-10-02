import { html } from "@lit-labs/ssr"
import type { Route } from "../routes.js"

export const homePage: Route = {
  path: "/",
  mode: "ssg",
  meta: (site) => ({
    title: "earn ETH with your GPU on Livepeer",
    description:
      "Open Pool pays GPU owners in ETH for serving AI and video apps on Livepeer. You run one command on your machine; the pool brings the work and pays every window.",
    path: "/",
    index: true,
    jsonLd: [
      { "@context": "https://schema.org", "@type": "WebSite", name: site.name, url: `${site.url}/` },
      {
        "@context": "https://schema.org",
        "@type": "Organization",
        name: site.name,
        url: `${site.url}/`,
        logo: `${site.url}/favicon.svg`,
        description: "A pool of GPU machines serving AI and video apps on the Livepeer network.",
      },
    ],
  }),
  template: () => html`
    <section class="hero">
      <hgroup>
        <h1>Put your GPU to work on Livepeer, and get paid in ETH</h1>
        <p>
          Open Pool is a pool of GPU machines that serve AI and video apps on the Livepeer network. You bring the machine. The pool
          brings the customers, runs the orchestrator, and pays you for the work your machine does.
        </p>
      </hgroup>
      <p class="actions">
        <a class="button primary" href="/join/">Join the pool</a>
        <a class="button" href="/how-it-works/">See how it works</a>
      </p>
    </section>

    <section aria-labelledby="split-heading">
      <h2 id="split-heading">Who does what</h2>
      <div class="cards">
        <article class="card">
          <h3>You</h3>
          <ul>
            <li>Apply with your wallet and say which GPUs you have.</li>
            <li>Run one command on each machine. It starts an agent that keeps the machine running what the pool assigns it.</li>
            <li>Keep the machine on and connected.</li>
          </ul>
        </article>
        <article class="card">
          <h3>The pool</h3>
          <ul>
            <li>Runs the Livepeer orchestrator that customers send work to, and takes their payments.</li>
            <li>Chooses which app each machine runs, by where the demand is, and rolls out new versions.</li>
            <li>Tests every machine with real requests, so customers only reach machines that work.</li>
            <li>Splits what it earned among members every payout window, and pays the transaction fees.</li>
          </ul>
        </article>
      </div>
    </section>

    <section aria-labelledby="steps-heading">
      <h2 id="steps-heading">From application to first payout</h2>
      <ol class="steps">
        <li>
          <h3>Apply</h3>
          <p>Sign in with your Ethereum wallet, choose a region, list your machines and their GPUs, and accept the terms by signing them. Your address is where you are paid.</p>
        </li>
        <li>
          <h3>Get approved</h3>
          <p>The operator reads every application. Once yours is approved, each machine is assigned the apps it will run.</p>
        </li>
        <li>
          <h3>Connect</h3>
          <p>Create the machine's credential and run the one command shown with it. The agent pulls the app, starts it, and connects it to the orchestrator.</p>
        </li>
        <li>
          <h3>Pass the test</h3>
          <p>The pool sends your machine a real request. When it answers correctly, it starts taking customers' work. This usually takes minutes, most of them spent downloading the model.</p>
        </li>
        <li>
          <h3>Get paid</h3>
          <p>At the end of each payout window your share is sent to your address in ETH on Arbitrum. There is no minimum.</p>
        </li>
      </ol>
      <p><a href="/connect/">The full guide to connecting a machine</a></p>
    </section>

    <section aria-labelledby="terms-heading">
      <h2 id="terms-heading">Payout terms right now</h2>
      <p>These come straight from each region of the pool, so they are what a member is paid under today.</p>
      <op-pool-facts>
        <p class="muted">The pool's commission and payout window load here from each region. This part of the page needs JavaScript.</p>
      </op-pool-facts>
    </section>

    <section aria-labelledby="need-heading">
      <h2 id="need-heading">What you need</h2>
      <dl class="needs">
        <div>
          <dt>A machine with an NVIDIA GPU</dt>
          <dd>Each region lists the models it accepts and each app the models it runs on. <a href="/apps/">See the apps and what they need</a>.</dd>
        </div>
        <div>
          <dt>Linux with Docker</dt>
          <dd>And the NVIDIA container toolkit, so containers can use the GPU. Other platforms are not supported yet.</dd>
        </div>
        <div>
          <dt>An Ethereum wallet</dt>
          <dd>It signs you in and receives your payouts. You never send anything: the pool pays the fees.</dd>
        </div>
        <div>
          <dt>An outbound internet connection</dt>
          <dd>The machine connects out to the orchestrator. No ports need opening and no public address is needed.</dd>
        </div>
      </dl>
    </section>

    <section class="callout" aria-labelledby="cta-heading">
      <h2 id="cta-heading">Ready when your GPU is idle</h2>
      <p>Applying takes a few minutes. Nothing is installed until you are approved.</p>
      <p class="actions"><a class="button primary" href="/join/">Join the pool</a></p>
    </section>
  `,
}
