import { Data, Match } from "effect"
import type { RegionId } from "./ids.js"

/** The region did not answer: it is down, unreachable, or too slow. */
export class RegionUnreachable extends Data.TaggedError("RegionUnreachable")<{
  readonly region: RegionId
  readonly reason: string
}> {}

/** The region answered that it will not do this, and says why. */
export class RequestRefused extends Data.TaggedError("RequestRefused")<{
  readonly region: RegionId
  readonly status: number
  /** The service's own sentence, written for the person to read. */
  readonly message: string
}> {}

/** The region does not accept the session: it expired, or there is none. */
export class NotSignedIn extends Data.TaggedError("NotSignedIn")<{
  readonly region: RegionId
}> {}

/** The region answered with something this site does not understand. */
export class UnexpectedAnswer extends Data.TaggedError("UnexpectedAnswer")<{
  readonly region: RegionId
  readonly detail: string
}> {}

/** Everything that can go wrong asking a region something. */
export type ApiError = RegionUnreachable | RequestRefused | NotSignedIn | UnexpectedAnswer

/** There is no session stored in this browser, or it has expired. */
export class NoSession extends Data.TaggedError("NoSession") {}

/** The browser has no wallet to sign with. */
export class WalletMissing extends Data.TaggedError("WalletMissing") {}

/** The person, or their wallet, declined. */
export class WalletDeclined extends Data.TaggedError("WalletDeclined")<{
  readonly reason: string
}> {}

/** No region of the pool could be reached. */
export class NoRegionReachable extends Data.TaggedError("NoRegionReachable") {}

/** The site's own settings could not be loaded or read. */
export class ConfigUnavailable extends Data.TaggedError("ConfigUnavailable")<{
  readonly detail: string
}> {}

/** A region named in a link or a form is not one of this pool's. */
export class UnknownRegion extends Data.TaggedError("UnknownRegion")<{
  readonly region: string
}> {}

/** What a person filled in cannot be sent as it is. */
export class InvalidInput extends Data.TaggedError("InvalidInput")<{
  readonly message: string
}> {}

/** Every error a site shows to a person. */
export type SiteError =
  | ApiError
  | NoSession
  | WalletMissing
  | WalletDeclined
  | NoRegionReachable
  | ConfigUnavailable
  | UnknownRegion
  | InvalidInput

/** One sentence a person can act on, for each error. */
export const describeError: (error: SiteError) => string = Match.type<SiteError>().pipe(
  Match.tag("RegionUnreachable", (e) => `The ${e.region} region did not answer (${e.reason}).`),
  Match.tag("RequestRefused", (e) => e.message),
  Match.tag("NotSignedIn", "NoSession", () => "You are not signed in, or your session has ended. Sign in again."),
  Match.tag(
    "UnexpectedAnswer",
    (e) => `The ${e.region} region answered with something this site does not understand. It may be running a different version.`,
  ),
  Match.tag("WalletMissing", () => "No Ethereum wallet was found in this browser. Install one, such as MetaMask, and reload."),
  Match.tag("WalletDeclined", (e) => `Your wallet did not sign: ${e.reason}.`),
  Match.tag("NoRegionReachable", () => "No region of the pool could be reached. Try again in a moment."),
  Match.tag("ConfigUnavailable", (e) => `This site's settings could not be loaded (${e.detail}).`),
  Match.tag("UnknownRegion", (e) => `There is no region called ${e.region}.`),
  Match.tag("InvalidInput", (e) => e.message),
  Match.exhaustive,
)
