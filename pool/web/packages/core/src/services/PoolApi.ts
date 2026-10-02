import { HttpClient, HttpClientRequest } from "@effect/platform"
import { Context, Effect, Layer, Option, Schema } from "effect"
import { type ApiError, NotSignedIn, RegionUnreachable, RequestRefused, UnexpectedAnswer } from "../domain/errors.js"
import type { Token } from "../domain/ids.js"
import type { RegionConfig } from "../domain/regions.js"

/** One call to a region's pool service. */
export interface ApiRequest {
  readonly method: "GET" | "POST" | "PUT" | "DELETE"
  /** Under /v1, e.g. /public/pool. */
  readonly path: string
  readonly query: Readonly<Record<string, string>>
  readonly token: Option.Option<Token>
  /** A JSON body, already encoded for the wire. */
  readonly body: Option.Option<unknown>
}

/**
 * Talks to the pool service of a region. This is the only place that knows
 * the service speaks HTTP: callers get a decoded answer or one of four
 * errors, never a status code or a network exception.
 */
export class PoolApi extends Context.Tag("openpool/PoolApi")<
  PoolApi,
  {
    readonly send: <A, I>(
      region: RegionConfig,
      request: ApiRequest,
      answer: Schema.Schema<A, I>,
    ) => Effect.Effect<A, ApiError>
  }
>() {}

/** How long a region has to answer before it counts as unreachable. */
const answerWithin = "15 seconds"

const ErrorBody = Schema.Struct({ error: Schema.String })
const decodeErrorBody = Schema.decodeUnknownOption(Schema.parseJson(ErrorBody))

const refusal = (status: number): string =>
  status >= 500 ? `The region had a problem of its own (${status}).` : `The region refused the request (${status}).`

/** The pool service over HTTP. */
export const PoolApiLive = Layer.effect(
  PoolApi,
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient

    const send = <A, I>(region: RegionConfig, request: ApiRequest, answer: Schema.Schema<A, I>): Effect.Effect<A, ApiError> =>
      Effect.gen(function* () {
        const url = `${region.api.replace(/\/+$/, "")}/v1${request.path}`
        const httpRequest = HttpClientRequest.make(request.method)(url).pipe(
          HttpClientRequest.setUrlParams(request.query),
          HttpClientRequest.acceptJson,
          Option.match(request.token, {
            onNone: () => (r: HttpClientRequest.HttpClientRequest) => r,
            onSome: (token) => HttpClientRequest.bearerToken(token),
          }),
          Option.match(request.body, {
            onNone: () => (r: HttpClientRequest.HttpClientRequest) => r,
            onSome: (body) => HttpClientRequest.bodyUnsafeJson(body),
          }),
        )
        const response = yield* client.execute(httpRequest)
        const text = yield* response.text
        if (response.status === 401) {
          return yield* new NotSignedIn({ region: region.id })
        }
        if (response.status >= 400) {
          const message = Option.match(decodeErrorBody(text), {
            onNone: () => refusal(response.status),
            onSome: (body) => body.error,
          })
          return yield* new RequestRefused({ region: region.id, status: response.status, message })
        }
        // An answer with no body, such as 204, decodes from nothing.
        const decode = text.trim() === "" ? Schema.decodeUnknown(answer)(undefined) : Schema.decodeUnknown(Schema.parseJson(answer))(text)
        return yield* decode
      }).pipe(
        Effect.timeoutFail({
          duration: answerWithin,
          onTimeout: () => new RegionUnreachable({ region: region.id, reason: `no answer within ${answerWithin}` }),
        }),
        Effect.catchTags({
          RequestError: (e) => Effect.fail(new RegionUnreachable({ region: region.id, reason: describeTransport(e.reason) })),
          ResponseError: (e) => Effect.fail(new RegionUnreachable({ region: region.id, reason: describeTransport(e.reason) })),
          ParseError: (e) => Effect.fail(new UnexpectedAnswer({ region: region.id, detail: e.message })),
        }),
        Effect.withSpan("PoolApi.send", { attributes: { region: region.id, method: request.method, path: request.path } }),
      )

    return { send }
  }),
)

const describeTransport = (reason: string): string =>
  reason === "Transport" ? "it could not be reached" : `the exchange failed in ${reason.toLowerCase()}`
