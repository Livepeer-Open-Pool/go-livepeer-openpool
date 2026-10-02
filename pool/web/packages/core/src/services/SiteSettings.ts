import { HttpClient } from "@effect/platform"
import { Context, Effect, Layer, Schema } from "effect"
import { ConfigUnavailable } from "../domain/errors.js"
import { SiteConfig } from "../domain/regions.js"

/** Where this site itself is served from, for sign-in messages. */
export interface SiteLocation {
  /** e.g. app.open-pool.com */
  readonly host: string
  /** e.g. https://app.open-pool.com */
  readonly origin: string
}

/**
 * What the site was deployed with. It is loaded when the site starts, from a
 * file beside the site's pages, so regions are added by editing a file and
 * not by building the site again.
 */
export class SiteSettings extends Context.Tag("openpool/SiteSettings")<
  SiteSettings,
  { readonly config: SiteConfig; readonly location: SiteLocation }
>() {}

const decodeConfig = Schema.decodeUnknown(SiteConfig)

/** Loads the settings file at url. */
export const loadSiteConfig = (url: string): Effect.Effect<SiteConfig, ConfigUnavailable, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = (yield* HttpClient.HttpClient).pipe(HttpClient.withTracerPropagation(false))
    const response = yield* client.get(url)
    if (response.status !== 200) {
      return yield* new ConfigUnavailable({ detail: `${url} answered ${response.status}` })
    }
    const body = yield* response.json
    return yield* decodeConfig(body)
  }).pipe(
    Effect.timeoutFail({
      duration: "15 seconds",
      onTimeout: () => new ConfigUnavailable({ detail: `${url} did not answer` }),
    }),
    Effect.catchTags({
      RequestError: (e) => Effect.fail(new ConfigUnavailable({ detail: `${url} could not be fetched: ${e.reason}` })),
      ResponseError: (e) => Effect.fail(new ConfigUnavailable({ detail: `${url} could not be read: ${e.reason}` })),
      ParseError: (e) => Effect.fail(new ConfigUnavailable({ detail: `${url} is not a settings file: ${e.message}` })),
    }),
    Effect.withSpan("loadSiteConfig"),
  )

/** The settings as loaded from the file at url, for a site served at location. */
export const SiteSettingsFromFile = (url: string, location: SiteLocation) =>
  Layer.effect(
    SiteSettings,
    Effect.map(loadSiteConfig(url), (config) => ({ config, location })),
  )
