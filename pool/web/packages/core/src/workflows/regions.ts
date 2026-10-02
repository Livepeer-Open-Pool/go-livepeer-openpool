import { Array, Effect, Option } from "effect"
import { type ApiError, NoRegionReachable, UnknownRegion } from "../domain/errors.js"
import type { RegionConfig, RegionResult } from "../domain/regions.js"
import { SiteSettings } from "../services/SiteSettings.js"

/** The pool's regions, in the order the site lists them. */
export const regions: Effect.Effect<Array.NonEmptyReadonlyArray<RegionConfig>, never, SiteSettings> = Effect.map(
  SiteSettings,
  (settings) => settings.config.regions,
)

/** The region with this id. */
export const regionById = (id: string): Effect.Effect<RegionConfig, UnknownRegion, SiteSettings> =>
  Effect.flatMap(regions, (all) =>
    Option.match(
      Array.findFirst(all, (region) => region.id === id),
      { onNone: () => Effect.fail(new UnknownRegion({ region: id })), onSome: Effect.succeed },
    ),
  )

/**
 * Asks every region the same question at once and returns each one's answer
 * or the reason there is none. It never fails: a region that is down is part
 * of the answer, so the rest of a page can still be shown.
 */
export const everyRegion = <A, R>(
  ask: (region: RegionConfig) => Effect.Effect<A, ApiError, R>,
): Effect.Effect<ReadonlyArray<RegionResult<A>>, never, R | SiteSettings> =>
  Effect.flatMap(regions, (all) =>
    Effect.forEach(all, (region) => Effect.map(Effect.either(ask(region)), (result) => ({ region, result })), {
      concurrency: 8,
    }),
  ).pipe(Effect.withSpan("everyRegion"))

/**
 * Asks the regions one after another until one answers. A region that is
 * unreachable is passed over; any other answer, a refusal included, is the
 * answer. Used for what any region can do, such as signing in.
 */
export const anyRegion = <A, R>(
  ask: (region: RegionConfig) => Effect.Effect<A, ApiError, R>,
): Effect.Effect<{ readonly region: RegionConfig; readonly value: A }, ApiError | NoRegionReachable, R | SiteSettings> => {
  const from = (
    remaining: ReadonlyArray<RegionConfig>,
  ): Effect.Effect<{ readonly region: RegionConfig; readonly value: A }, ApiError | NoRegionReachable, R> =>
    Array.matchLeft(remaining, {
      onEmpty: () => Effect.fail(new NoRegionReachable()),
      onNonEmpty: (region, rest) =>
        ask(region).pipe(
          Effect.map((value) => ({ region, value })),
          Effect.catchTag("RegionUnreachable", () => from(rest)),
        ),
    })
  return Effect.flatMap(regions, from).pipe(Effect.withSpan("anyRegion"))
}
