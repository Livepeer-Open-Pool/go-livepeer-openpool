import { SignalWatcher } from "@lit-labs/signals"
import { ContextConsumer } from "@lit/context"
import { Task } from "@lit/task"
import type { SiteError, SiteRuntime, SiteServices } from "@openpool/core"
import { type Effect, Option } from "effect"
import { LitElement } from "lit"
import { asFailure, run, runtimeContext } from "./runtime.js"
import { setSession } from "./session.js"
import { base, controls, dialogs, surfaces, tables } from "./styles.js"

/**
 * What every page element starts from: the shared styles, the site's runtime
 * from context, and two ways to run effects. A query runs when the page
 * opens and when its arguments change; an action runs when called. Either
 * one that finds the session gone signs the page out, so the site shows its
 * sign-in instead of an error.
 */
export class SitePage extends SignalWatcher(LitElement) {
  static override styles = [base, controls, surfaces, tables, dialogs]

  readonly #runtime = new ContextConsumer(this, { context: runtimeContext })

  #run<A, E extends SiteError>(effect: Effect.Effect<A, E, SiteServices>, signal: AbortSignal): Promise<A> {
    const runtime: SiteRuntime | undefined = this.#runtime.value
    if (runtime === undefined) {
      return Promise.reject({ _tag: "Fault", detail: `${this.localName} is not inside a site` })
    }
    return run(runtime, effect, signal).catch((thrown: unknown) => {
      if (asFailure(thrown)._tag === "NoSession") setSession(Option.none())
      return Promise.reject(thrown)
    })
  }

  /** Something to show: asked when the page opens and again when args change. */
  protected query<A, E extends SiteError, const Args extends ReadonlyArray<unknown> = readonly []>(
    effect: (...args: Args) => Effect.Effect<A, E, SiteServices>,
    args?: () => Args,
  ): Task<Args, A> {
    return new Task(this, {
      task: (taskArgs, { signal }) => this.#run(effect(...taskArgs), signal),
      args: args ?? ((): Args => [] as unknown as Args),
    })
  }

  /** Something to do: run with `.run([…])`; `done` is called with its result. */
  protected action<A, E extends SiteError, const Args extends ReadonlyArray<unknown> = readonly []>(
    effect: (...args: Args) => Effect.Effect<A, E, SiteServices>,
    done?: (value: A, ...args: Args) => void,
  ): Task<Args, A> {
    return new Task<Args, A>(this, {
      task: (taskArgs, { signal }) =>
        this.#run(effect(...taskArgs), signal).then((value) => {
          done?.(value, ...taskArgs)
          return value
        }),
      autoRun: false,
    })
  }
}
