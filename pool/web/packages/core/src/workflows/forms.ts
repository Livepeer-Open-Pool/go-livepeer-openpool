import { Effect, ParseResult, Schema } from "effect"
import { InvalidInput } from "../domain/errors.js"
import type { SiteConfig } from "../domain/regions.js"
import { SiteSettings } from "../services/SiteSettings.js"

/**
 * Turns what a form collected into a domain value, or says what is wrong
 * with it. The pool service checks everything again; this catches what
 * cannot even be sent, such as a count that is not a number.
 */
export const parseInput =
  <A, I>(schema: Schema.Schema<A, I>, what: string) =>
  (input: unknown): Effect.Effect<A, InvalidInput> =>
    Schema.decodeUnknown(schema, { errors: "all" })(input).pipe(
      Effect.mapError(
        (error) =>
          new InvalidInput({
            message: `${what} cannot be sent as filled in: ${ParseResult.ArrayFormatter.formatErrorSync(error)
              .map((issue) => `${issue.path.join(".")} ${issue.message}`.trim())
              .join("; ")}`,
          }),
      ),
    )

/** What this site was deployed with. */
export const siteConfig: Effect.Effect<SiteConfig, never, SiteSettings> = Effect.map(SiteSettings, (settings) => settings.config)
