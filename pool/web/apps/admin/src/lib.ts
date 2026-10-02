import { type Api, everyRegionAs, inRegionAs, type RegionResult, unanswered } from "@openpool/core"
import { describeFailure } from "@openpool/ui"
import { Array, Effect, Either, Option, pipe } from "effect"
import { html, nothing } from "lit"

/** Asks every region something as the operator. */
export const everywhere = <A>(ask: Api.AskAs<A>) => everyRegionAs("admin", (region, session) => ask(region, session.token))

/** Asks one region something as the operator. */
export const inRegion = <A>(region: string, ask: Api.AskAs<A>) => inRegionAs("admin", region, ask)

/**
 * Does the same thing in several regions, such as publishing the pool's
 * terms in all of them. Each region's outcome comes back, so one region
 * being down does not hide that the others were done.
 */
export const inRegions = <A>(regions: ReadonlyArray<string>, ask: Api.AskAs<A>) =>
  Effect.map(
    everyRegionAs("admin", (region, session) =>
      regions.includes(region.id) ? Effect.map(ask(region, session.token), Option.some) : Effect.succeed(Option.none<A>()),
    ),
    (results): ReadonlyArray<RegionResult<A>> =>
      pipe(
        results,
        Array.filterMap(
          ({ region, result }): Option.Option<RegionResult<A>> =>
            Either.isLeft(result)
              ? Option.some({ region, result: Either.left(result.left) })
              : Option.map(result.right, (value) => ({ region, result: Either.right(value) })),
        ),
      ),
  )

/** What became of something done in several regions: where it was done, and where it was not and why. */
export const outcomes = <A>(results: ReadonlyArray<RegionResult<A>>, done: string): unknown => {
  const failed = unanswered(results)
  const succeeded = results.filter((r) => Either.isRight(r.result))
  return html`
    ${succeeded.length > 0
      ? html`<p class="notice good" role="status">${done} in ${succeeded.map((r) => r.region.name).join(" and ")}.</p>`
      : nothing}
    ${failed.length > 0
      ? html`
          <div class="notice bad" role="alert">
            <p>Not done in every region:</p>
            <ul>
              ${failed.map(({ region, error }) => html`<li><strong>${region.name}</strong>: ${describeFailure(error)}</li>`)}
            </ul>
          </div>
        `
      : nothing}
  `
}

/** The non-empty lines of a text box, trimmed. */
export const lines = (text: FormDataEntryValue | null): ReadonlyArray<string> =>
  String(text ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "")

/** "KEY=value" or "Key: value" lines as a dictionary. */
export const pairs = (text: FormDataEntryValue | null, separator: "=" | ":"): Readonly<Record<string, string>> =>
  Object.fromEntries(
    lines(text).flatMap((line) => {
      const at = line.indexOf(separator)
      return at <= 0 ? [] : [[line.slice(0, at).trim(), line.slice(at + 1).trim()] as const]
    }),
  )

export const field = (data: FormData, name: string): string => String(data.get(name) ?? "").trim()
export const count = (data: FormData, name: string): number => Number(data.get(name) ?? 0)
