import { signal } from "@lit-labs/signals"
import type { Session } from "@openpool/core"
import { Option } from "effect"

/**
 * Who is signed in to this site, shared by every component on the page. It
 * starts unknown: the app's root reads the stored session once it is up.
 */
export type SessionState =
  | { readonly _tag: "Unknown" }
  | { readonly _tag: "SignedOut" }
  | { readonly _tag: "SignedIn"; readonly session: Session }

export const sessionState = signal<SessionState>({ _tag: "Unknown" })

export const setSession = (session: Option.Option<Session>): void => {
  sessionState.set(
    Option.match(session, {
      onNone: (): SessionState => ({ _tag: "SignedOut" }),
      onSome: (value): SessionState => ({ _tag: "SignedIn", session: value }),
    }),
  )
}

/** For tests: back to not knowing. */
export const resetSession = (): void => {
  sessionState.set({ _tag: "Unknown" })
}
