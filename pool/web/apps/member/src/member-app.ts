import { SignalWatcher } from "@lit-labs/signals"
import { ContextProvider } from "@lit/context"
import { Task, TaskStatus } from "@lit/task"
import { currentSession, type SiteRuntime, signInWithWallet, signOut, siteConfig } from "@openpool/core"
import {
  actionStatus,
  asFailure,
  base,
  controls,
  failureNotice,
  primaryNav,
  Router,
  run,
  runtimeContext,
  sessionState,
  setSession,
  shell,
  shortAddress,
  skeleton,
  surfaces,
} from "@openpool/ui"
import { Effect, Option } from "effect"
import { css, html, LitElement, nothing } from "lit"
import { nav, notFound, routes } from "./routes.js"

/**
 * The member site's root: it gives every page the runtime, knows who is
 * signed in, and shows either the sign-in or the page the address names.
 */
export class MemberApp extends SignalWatcher(LitElement) {
  /** Set once by main.ts before the element is defined. */
  static runtime: SiteRuntime

  static override styles = [
    base,
    controls,
    surfaces,
    shell,
    css`
      .signin { max-inline-size: 38rem; margin-inline: auto; }
      .signin ol { padding-inline-start: 1.2rem; display: grid; gap: var(--space-xs); }
    `,
  ]

  constructor() {
    super()
    new ContextProvider(this, { context: runtimeContext, initialValue: MemberApp.runtime })
    void this.#restore.run()
  }

  readonly #router = new Router(this, { routes, site: "Open Pool members", notFound })

  /** Reads the session this browser remembers, once, when the site starts. */
  readonly #restore = new Task(this, {
    task: (_args: readonly [], { signal }) => run(MemberApp.runtime, currentSession("wallet"), signal).then(setSession),
    autoRun: false,
  })

  readonly #signIn = new Task(this, {
    task: (_args: readonly [], { signal }) =>
      run(MemberApp.runtime, signInWithWallet, signal).then((session) => setSession(Option.some(session))),
    autoRun: false,
  })

  readonly #signOut = new Task(this, {
    task: (_args: readonly [], { signal }) => run(MemberApp.runtime, signOut("wallet"), signal).then(() => setSession(Option.none())),
    autoRun: false,
  })

  readonly #links = new Task(this, {
    task: (_args: readonly [], { signal }) => run(MemberApp.runtime, Effect.map(siteConfig, (config) => config.sites), signal),
    args: () => [] as const,
  })

  override render() {
    const state = sessionState.get()
    const marketing = this.#links.value?.marketing
    return html`
      <a class="skip" href="#main" @click=${this.#skip}>Skip to main content</a>
      <header class="banner">
        <div class="bar">
          <a class="brand" href="/">Open <span>Pool</span><small>members</small></a>
          ${state._tag === "SignedIn" ? primaryNav(nav, this.#router.path) : nothing}
          ${state._tag === "SignedIn"
            ? html`
                <div class="who">
                  <span>Signed in as ${shortAddress(state.session.subject)}</span>
                  <button type="button" class="small" @click=${() => void this.#signOut.run()}>Sign out</button>
                </div>
              `
            : nothing}
        </div>
      </header>
      <main id="main" tabindex="-1">${this.#main(state._tag)}</main>
      <footer>
        <div class="bar">
          <span>Open Pool: GPUs serving Livepeer, paid by the round.</span>
          ${marketing === undefined
            ? nothing
            : html`
                <a href=${marketing}>About the pool</a>
                <a href="${marketing}/leaderboard/">Leaderboard</a>
                <a href="${marketing}/terms/">Terms</a>
              `}
        </div>
      </footer>
    `
  }

  #main(state: "Unknown" | "SignedOut" | "SignedIn") {
    if (state === "Unknown") {
      // Nothing can be shown until the site's settings and the stored session are read.
      return this.#restore.render({
        initial: skeleton,
        pending: skeleton,
        complete: skeleton,
        error: (thrown) => failureNotice(asFailure(thrown), () => void this.#restore.run()),
      })
    }
    if (state === "SignedIn") return this.#router.outlet ?? skeleton()
    return html`
      <section class="signin stack" aria-labelledby="signin-heading">
        <h1 id="signin-heading">Sign in with your wallet</h1>
        <p>
          Your Ethereum address is your account here, and the address the pool pays. Signing in asks your wallet to sign a short
          text. It is not a transaction, costs nothing, and gives this site no power over your funds.
        </p>
        <ol>
          <li>Your wallet shows which address to use.</li>
          <li>You sign a message that names this site and a one-time code.</li>
          <li>The pool checks the signature and signs you in for twelve hours.</li>
        </ol>
        <p>
          <button type="button" class="primary" ?disabled=${this.#signIn.status === TaskStatus.PENDING} @click=${() => void this.#signIn.run()}>
            Sign in with your wallet
          </button>
        </p>
        ${actionStatus(this.#signIn, "Waiting for your wallet…")}
        <p class="muted small">
          The address you sign in with is the one you apply with and are paid at. It cannot be changed later; a different address
          is a different membership.
        </p>
      </section>
    `
  }

  readonly #skip = (event: Event): void => {
    event.preventDefault()
    this.renderRoot.querySelector<HTMLElement>("#main")?.focus()
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "member-app": MemberApp
  }
}
