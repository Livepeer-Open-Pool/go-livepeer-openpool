import { Array, Either, Option, Order, pipe, Schema } from "effect"
import type { ApiError } from "./errors.js"
import { Address, RegionId } from "./ids.js"
import type { LeaderboardEntry } from "./model.js"
import { sumWei, type Wei } from "./money.js"

/** A region of the pool as a site is told about it. */
export const RegionConfig = Schema.Struct({
  id: RegionId,
  name: Schema.NonEmptyString,
  /** The region's pool service, e.g. https://us-east.api.open-pool.com. */
  api: Schema.NonEmptyString,
})
export type RegionConfig = typeof RegionConfig.Type

/** What a site is deployed with: the pool's regions and its sister sites. */
export const SiteConfig = Schema.Struct({
  regions: Schema.NonEmptyArray(RegionConfig),
  sites: Schema.Struct({
    marketing: Schema.NonEmptyString,
    member: Schema.NonEmptyString,
    admin: Schema.NonEmptyString,
  }),
  /** A block explorer for transaction links, e.g. https://arbiscan.io. */
  explorer: Schema.NonEmptyString,
})
export type SiteConfig = typeof SiteConfig.Type

/** One region's answer to a question asked of every region. */
export interface RegionResult<A> {
  readonly region: RegionConfig
  readonly result: Either.Either<A, ApiError>
}

/** The regions that answered, with what they said. */
export const answered = <A>(results: ReadonlyArray<RegionResult<A>>): ReadonlyArray<{ readonly region: RegionConfig; readonly value: A }> =>
  pipe(
    results,
    Array.filterMap((r) => (Either.isRight(r.result) ? Option.some({ region: r.region, value: r.result.right }) : Option.none())),
  )

/** The regions that did not answer, with why. */
export const unanswered = <A>(
  results: ReadonlyArray<RegionResult<A>>,
): ReadonlyArray<{ readonly region: RegionConfig; readonly error: ApiError }> =>
  pipe(
    results,
    Array.filterMap((r) => (Either.isLeft(r.result) ? Option.some({ region: r.region, error: r.result.left }) : Option.none())),
  )

/** Whether any region says the session is not accepted. */
export const anyNotSignedIn = <A>(results: ReadonlyArray<RegionResult<A>>): boolean =>
  pipe(
    unanswered(results),
    Array.some((r) => r.error._tag === "NotSignedIn"),
  )

/** A member's place on the leaderboard across every region that answered. */
export interface Standing {
  readonly address: Address
  readonly displayName: string
  readonly fees: Wei
  readonly sessions: number
  /** The regions the member earned in, by name. */
  readonly regions: ReadonlyArray<string>
}

const byFeesThenName: Order.Order<Standing> = Order.combine(
  Order.reverse(Order.mapInput(Order.bigint, (s: Standing) => s.fees)),
  Order.mapInput(Order.string, (s: Standing) => s.displayName.toLowerCase()),
)

/**
 * Merges the regions' leaderboards into one: a member who earns in several
 * regions is one line with the sum. Addresses are compared without regard to
 * case, and the member's name is the one from the region they earned most in.
 */
export const mergeLeaderboards = (
  boards: ReadonlyArray<{ readonly region: RegionConfig; readonly entries: ReadonlyArray<LeaderboardEntry> }>,
): ReadonlyArray<Standing> =>
  pipe(
    boards,
    Array.flatMap((board) => Array.map(board.entries, (entry) => ({ region: board.region.name, entry }))),
    Array.groupBy((line) => line.entry.address.toLowerCase()),
    (groups) => Object.values(groups),
    Array.map((lines) => {
      const top = pipe(
        lines,
        Array.sortBy(Order.reverse(Order.mapInput(Order.bigint, (l: (typeof lines)[number]) => l.entry.fees_wei))),
        Array.headNonEmpty,
      )
      return {
        address: top.entry.address,
        displayName: top.entry.display_name,
        fees: sumWei(Array.map(lines, (l) => l.entry.fees_wei)),
        sessions: pipe(
          lines,
          Array.reduce(0, (total, l) => total + l.entry.sessions),
        ),
        regions: pipe(
          lines,
          Array.map((l) => l.region),
          Array.dedupe,
          Array.sort(Order.string),
        ),
      } satisfies Standing
    }),
    Array.sort(byFeesThenName),
  )
