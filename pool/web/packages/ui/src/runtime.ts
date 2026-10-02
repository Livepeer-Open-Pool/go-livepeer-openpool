import { createContext } from "@lit/context"
import { describeError, type SiteError, type SiteRuntime, type SiteServices } from "@openpool/core"
import { Cause, type Effect, Exit, Option, Predicate } from "effect"

/** The site's one runtime, provided by the app's root element. */
export const runtimeContext = createContext<SiteRuntime>(Symbol("openpool.runtime"))

/** Something went wrong that is this site's own fault, not an answer. */
export interface Fault {
  readonly _tag: "Fault"
  readonly detail: string
}

/** What a failed effect is rejected with: one of the site's errors, or a fault. */
export type Failure = SiteError | Fault

const siteErrorTags: ReadonlySet<string> = new Set<SiteError["_tag"]>([
  "RegionUnreachable",
  "RequestRefused",
  "NotSignedIn",
  "UnexpectedAnswer",
  "NoSession",
  "WalletMissing",
  "WalletDeclined",
  "NoRegionReachable",
  "ConfigUnavailable",
  "UnknownRegion",
  "InvalidInput",
])

const isSiteError = (value: unknown): value is SiteError =>
  Predicate.hasProperty(value, "_tag") && Predicate.isString(value._tag) && siteErrorTags.has(value._tag)

/** What a component's error state received, as a failure it can show. */
export const asFailure = (thrown: unknown): Failure =>
  isSiteError(thrown) || (Predicate.isTagged(thrown, "Fault") && Predicate.hasProperty(thrown, "detail") && Predicate.isString(thrown.detail))
    ? (thrown as Failure)
    : { _tag: "Fault", detail: String(thrown) }

/** One sentence a person can act on. */
export const describeFailure = (failure: Failure): string =>
  failure._tag === "Fault" ? "Something went wrong on this page. Reload it, and tell the pool if it happens again." : describeError(failure)

/**
 * Runs an effect for a component. This is the one place the sites leave
 * Effect for promises, which is what Lit's Task takes: a success resolves,
 * and a failure rejects with the typed error itself, so a component's error
 * state can say what happened. Interrupting through `signal` cancels the
 * effect, requests in flight included.
 */
export const run = <A, E extends SiteError>(
  runtime: SiteRuntime,
  effect: Effect.Effect<A, E, SiteServices>,
  signal?: AbortSignal,
): Promise<A> =>
  runtime.runPromiseExit(effect, signal ? { signal } : undefined).then(
    Exit.match({
      onSuccess: (value) => value,
      onFailure: (cause): Promise<A> =>
        Promise.reject(
          Option.getOrElse(Cause.failureOption(cause) as Option.Option<Failure>, (): Failure => ({ _tag: "Fault", detail: Cause.pretty(cause) })),
        ),
    }),
  )
