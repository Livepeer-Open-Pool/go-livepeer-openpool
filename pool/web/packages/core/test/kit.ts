import { Effect, Either, Layer, Option, Schema } from "effect"
import {
  type ApiError,
  type ApiRequest,
  NotSignedIn,
  PoolApi,
  type RegionConfig,
  RegionId,
  RegionUnreachable,
  RequestRefused,
  SessionStoreMemory,
  SiteConfig,
  SiteSettings,
  UnexpectedAnswer,
  Wallet,
  Address,
} from "../src/index.js"

export const config = Schema.decodeSync(SiteConfig)({
  regions: [
    { id: "us-east", name: "US East", api: "https://us-east.api.open-pool.com" },
    { id: "eu-west", name: "EU West", api: "https://eu-west.api.open-pool.com" },
  ],
  sites: { marketing: "https://open-pool.com", member: "https://app.open-pool.com", admin: "https://admin.open-pool.com" },
  explorer: "https://arbiscan.io",
})

export const usEast = config.regions[0]
export const euWest = config.regions[1] as RegionConfig
export const regionId = Schema.decodeSync(RegionId)

export const SettingsTest = Layer.succeed(SiteSettings, {
  config,
  location: { host: "app.open-pool.com", origin: "https://app.open-pool.com" },
})

/** What a stand-in region answers: a JSON value, or a failure. */
export type Answer =
  | { readonly status: 200; readonly json: unknown }
  | { readonly status: 204 }
  | { readonly status: number; readonly error: string }
  | { readonly down: true }

export const json = (value: unknown): Answer => ({ status: 200, json: value })
export const refuse = (status: number, error: string): Answer => ({ status, error })
export const down: Answer = { down: true }

/** Every call the stand-in regions received, in order. */
export interface Call {
  readonly region: string
  readonly request: ApiRequest
}

/**
 * A stand-in for the pool's regions. `answers` maps "region METHOD /path" to
 * what the region answers; an unlisted call is a 404. Answers are decoded
 * with the caller's schema, as the real service's JSON is.
 */
export const regionsAnswering = (answers: Readonly<Record<string, Answer | ((request: ApiRequest) => Answer)>>, calls: Array<Call> = []) =>
  Layer.succeed(PoolApi, {
    send: <A, I>(region: RegionConfig, request: ApiRequest, answer: Schema.Schema<A, I>): Effect.Effect<A, ApiError> => {
      calls.push({ region: region.id, request })
      const found = answers[`${region.id} ${request.method} ${request.path}`]
      const reply: Answer = found === undefined ? refuse(404, "no such thing") : typeof found === "function" ? found(request) : found
      if ("down" in reply) {
        return Effect.fail(new RegionUnreachable({ region: region.id, reason: "it could not be reached" }))
      }
      if (reply.status === 401) {
        return Effect.fail(new NotSignedIn({ region: region.id }))
      }
      if ("error" in reply) {
        return Effect.fail(new RequestRefused({ region: region.id, status: reply.status, message: reply.error }))
      }
      return Schema.decodeUnknown(answer)("json" in reply ? reply.json : undefined).pipe(
        Effect.mapError((e) => new UnexpectedAnswer({ region: region.id, detail: e.message })),
      )
    },
  })

export const address = Schema.decodeSync(Address)("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")

/** A wallet that holds one address and signs by echoing what it was asked. */
export const WalletTest = (signed: Array<string> = []) =>
  Layer.succeed(Wallet, {
    connect: Effect.succeed(address),
    sign: (_address, message) =>
      Effect.sync(() => {
        signed.push(message)
        return `0xsigned:${message.length}`
      }),
  })

export const site = (answers: Parameters<typeof regionsAnswering>[0], calls: Array<Call> = [], signed: Array<string> = []) =>
  Layer.mergeAll(regionsAnswering(answers, calls), SettingsTest, SessionStoreMemory, WalletTest(signed))

export const right = <A, E>(either: Either.Either<A, E>): A =>
  Either.getOrThrowWith(either, (e) => new Error(`expected an answer, got ${JSON.stringify(e)}`))

export const some = <A>(option: Option.Option<A>): A =>
  Option.getOrThrowWith(option, () => new Error("expected a value, got none"))
