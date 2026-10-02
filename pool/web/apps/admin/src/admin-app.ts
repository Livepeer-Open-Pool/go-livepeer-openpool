import { SignalWatcher } from "@lit-labs/signals"
import { ContextProvider } from "@lit/context"
import { Task, TaskStatus } from "@lit/task"
import { currentSession, type RegionId, requestAdminCode, signInAsAdmin, type SiteRuntime, signOut } from "@openpool/core"
import {
  actionStatus,
  asFailure,
  base,
  controls,
  failureNotice,
  lockup,
  lockupStyles,
  primaryNav,
  Router,
  run,
  runtimeContext,
  sessionState,
  setSession,
  shell,
  skeleton,
  surfaces,
} from "@openpool/ui"
import { Option } from "effect"
import { css, html, LitElement, nothing } from "lit"
import { state } from "lit/decorators.js"
import { nav, notFound, routes } from "./routes.js"

/** A code was asked for: it is good in the region that sent it, for the address it was sent to. */
interface CodeSent {
  readonly region: RegionId
  readonly email: string
}

/**
 * The admin site's root: it gives every page the runtime, signs the operator
 * in with a code emailed by a region, and shows the page the address names.
 */
export class AdminApp extends SignalWatcher(LitElement) {
  /** Set once by main.ts before the element is defined. */
  static runtime: SiteRuntime

  static override styles = [
    base,
    controls,
    surfaces,
    lockupStyles,
    shell,
    css`
      .signin { max-inline-size: 30rem; }
    `,
  ]

  @state() private sent: Option.Option<CodeSent> = Option.none()

  constructor() {
    super()
    new ContextProvider(this, { context: runtimeContext, initialValue: AdminApp.runtime })
    void this.#restore.run()
  }

  readonly #router = new Router(this, { routes, site: "Open Pool admin", notFound })

  readonly #restore = new Task(this, {
    task: (_args: readonly [], { signal }) => run(AdminApp.runtime, currentSession("admin"), signal).then(setSession),
    autoRun: false,
  })

  readonly #askCode = new Task(this, {
    task: ([email]: readonly [string], { signal }) =>
      run(AdminApp.runtime, requestAdminCode(email), signal).then((region) => {
        this.sent = Option.some({ region, email })
      }),
    autoRun: false,
  })

  readonly #signIn = new Task(this, {
    task: ([sent, code]: readonly [CodeSent, string], { signal }) =>
      run(AdminApp.runtime, signInAsAdmin(sent.region, sent.email, code), signal).then((session) => {
        this.sent = Option.none()
        setSession(Option.some(session))
      }),
    autoRun: false,
  })

  readonly #signOut = new Task(this, {
    task: (_args: readonly [], { signal }) => run(AdminApp.runtime, signOut("admin"), signal).then(() => setSession(Option.none())),
    autoRun: false,
  })

  /** The frame puts the banner down the side only for someone signed in. */
  protected override updated(): void {
    this.toggleAttribute("signed-in", sessionState.get()._tag === "SignedIn")
  }

  override render() {
    const session = sessionState.get()
    return html`
      <a class="skip" href="#main" @click=${this.#skip}>Skip to main content</a>
      <header class="banner">
        <div class="bar">
          ${lockup("admin")}
          ${session._tag === "SignedIn" ? primaryNav(nav, this.#router.path) : nothing}
          <div class="who">
            ${session._tag === "SignedIn"
              ? html`
                  <span>Signed in as <span class="subject">${session.session.subject}</span></span>
                  <button type="button" class="small" @click=${() => void this.#signOut.run()}>Sign out</button>
                `
              : nothing}
            <op-theme></op-theme>
          </div>
        </div>
      </header>
      <main id="main" tabindex="-1">${this.#main(session._tag)}</main>
      <footer>
        <div class="bar"><span>Open Pool admin. Every action here is recorded in the audit log.</span></div>
      </footer>
    `
  }

  #main(session: "Unknown" | "SignedOut" | "SignedIn") {
    if (session === "Unknown") {
      return this.#restore.render({
        initial: skeleton,
        pending: skeleton,
        complete: skeleton,
        error: (thrown) => failureNotice(asFailure(thrown), () => void this.#restore.run()),
      })
    }
    if (session === "SignedIn") return this.#router.outlet ?? skeleton()
    return html`
      <section class="signin stack" aria-labelledby="signin-heading">
        <h1 id="signin-heading">Operator sign-in</h1>
        ${Option.match(this.sent, {
          onNone: () => html`
            <p>Enter the operator's email address. A region of the pool emails it a six-digit code.</p>
            <form @submit=${this.#submitEmail}>
              <label>
                Email
                <input type="email" name="email" required autocomplete="email" inputmode="email" />
              </label>
              <p><button type="submit" class="primary" ?disabled=${this.#askCode.status === TaskStatus.PENDING}>Email me a code</button></p>
              ${actionStatus(this.#askCode, "Asking a region to send the code…")}
            </form>
          `,
          onSome: (sent) => html`
            <p class="notice" role="status">
              If <strong>${sent.email}</strong> is the operator's address, a code is on its way. It is good for ten minutes and
              for one use.
            </p>
            <form @submit=${(event: SubmitEvent) => this.#submitCode(event, sent)}>
              <label>
                Code
                <input name="code" required inputmode="numeric" pattern="[0-9]{6}" maxlength="6" autocomplete="one-time-code" autofocus />
              </label>
              <p class="row">
                <button type="submit" class="primary" ?disabled=${this.#signIn.status === TaskStatus.PENDING}>Sign in</button>
                <button type="button" class="quiet" @click=${() => (this.sent = Option.none())}>Use another address</button>
              </p>
              ${actionStatus(this.#signIn, "Checking the code…")}
            </form>
          `,
        })}
      </section>
    `
  }

  readonly #submitEmail = (event: SubmitEvent): void => {
    event.preventDefault()
    const email = String(new FormData(event.target as HTMLFormElement).get("email") ?? "").trim()
    void this.#askCode.run([email])
  }

  #submitCode(event: SubmitEvent, sent: CodeSent): void {
    event.preventDefault()
    const code = String(new FormData(event.target as HTMLFormElement).get("code") ?? "").trim()
    void this.#signIn.run([sent, code])
  }

  readonly #skip = (event: Event): void => {
    event.preventDefault()
    this.renderRoot.querySelector<HTMLElement>("#main")?.focus()
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "admin-app": AdminApp
  }
}
