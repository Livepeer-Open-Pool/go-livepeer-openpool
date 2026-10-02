import { HttpClient, HttpClientResponse } from "@effect/platform"
import { describe, expect, it } from "@effect/vitest"
import { Duration, Effect, Fiber, Layer, Option, Schema, TestClock } from "effect"
import { Api, loadSiteConfig, PoolApi, PoolApiLive, Token } from "../src/index.js"
import { usEast } from "./kit.js"

/** An HTTP client that answers from a function and remembers what it was asked. */
const http = (
  answer: (request: { readonly method: string; readonly url: string }) => Response | "hang",
  seen: Array<{ method: string; url: string; headers: Record<string, string>; body: string }> = [],
) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request, url) => {
      const body = request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : ""
      seen.push({ method: request.method, url: url.toString(), headers: { ...request.headers }, body })
      const reply = answer({ method: request.method, url: url.toString() })
      return reply === "hang" ? Effect.never : Effect.succeed(HttpClientResponse.fromWeb(request, reply))
    }),
  )

const api = (answer: Parameters<typeof http>[0], seen?: Parameters<typeof http>[1]) => PoolApiLive.pipe(Layer.provide(http(answer, seen)))
const reply = (status: number, body: unknown) => new Response(body === undefined ? null : JSON.stringify(body), { status })
const token = Schema.decodeSync(Token)("the-token")

describe("the pool service over HTTP", () => {
  it.effect("asks under /v1 with the query, the token and a JSON body", () => {
    const seen: Parameters<typeof http>[1] = []
    return Effect.gen(function* () {
      const tests = yield* Api.myTests(25)(usEast, token)
      expect(tests.tests).toEqual([])
      yield* Api.adminCode("ops@open-pool.com")(usEast)
      expect(seen[0]?.method).toBe("GET")
      expect(seen[0]?.url).toBe("https://us-east.api.open-pool.com/v1/me/tests?limit=25")
      expect(seen[0]?.headers["authorization"]).toBe("Bearer the-token")
      expect(seen[1]?.method).toBe("POST")
      expect(seen[1]?.headers["authorization"]).toBeUndefined()
      expect(JSON.parse(seen[1]?.body ?? "")).toEqual({ email: "ops@open-pool.com" })
    }).pipe(Effect.provide(api(({ url }) => (url.includes("/me/tests") ? reply(200, { tests: null }) : reply(204, undefined)), seen)))
  })

  it.effect("turns a refusal into the service's own sentence", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(Api.myMachines(usEast, token))
      expect(error._tag).toBe("RequestRefused")
      expect(error._tag === "RequestRefused" && [error.status, error.message]).toEqual([403, "this address is not a member in this region"])
    }).pipe(Effect.provide(api(() => reply(403, { error: "this address is not a member in this region" })))),
  )

  it.effect("still says something when a refusal carries no sentence", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(Api.pool(usEast))
      expect(error._tag === "RequestRefused" && error.message).toBe("The region had a problem of its own (502).")
    }).pipe(Effect.provide(api(() => new Response("<html>bad gateway</html>", { status: 502 })))),
  )

  it.effect("says the session is not accepted on 401", () =>
    Effect.gen(function* () {
      expect((yield* Effect.flip(Api.me(usEast, token)))._tag).toBe("NotSignedIn")
    }).pipe(Effect.provide(api(() => reply(401, { error: "not signed in" })))),
  )

  it.effect("says the answer is not understood when it has another shape", () =>
    Effect.gen(function* () {
      expect((yield* Effect.flip(Api.pool(usEast)))._tag).toBe("UnexpectedAnswer")
      expect((yield* Effect.flip(Api.publicTerms(usEast)))._tag).toBe("UnexpectedAnswer")
    }).pipe(Effect.provide(api(({ url }) => (url.endsWith("/pool") ? reply(200, { region: "us-east" }) : new Response("not json", { status: 200 }))))),
  )

  it.effect("gives up on a region that does not answer in time", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.fork(Effect.flip(Api.pool(usEast)))
      yield* TestClock.adjust(Duration.seconds(14))
      expect(Option.isNone(yield* Fiber.poll(fiber))).toBe(true)
      yield* TestClock.adjust(Duration.seconds(2))
      const error = yield* Fiber.join(fiber)
      expect(error._tag).toBe("RegionUnreachable")
      expect(error._tag === "RegionUnreachable" && error.reason).toBe("no answer within 15 seconds")
    }).pipe(Effect.provide(api(() => "hang"))),
  )

  it.effect("needs the PoolApi service and nothing else", () =>
    Effect.gen(function* () {
      const pool = yield* PoolApi
      const info = yield* pool.send(usEast, { method: "GET", path: "/public/pool", query: {}, token: Option.none(), body: Option.none() }, Schema.Unknown)
      expect(info).toEqual({ ok: true })
    }).pipe(Effect.provide(api(() => reply(200, { ok: true })))),
  )
})

describe("the site's settings file", () => {
  const good = {
    regions: [{ id: "us-east", name: "US East", api: "https://us-east.api.open-pool.com" }],
    sites: { marketing: "https://open-pool.com", member: "https://app.open-pool.com", admin: "https://admin.open-pool.com" },
    explorer: "https://arbiscan.io",
  }

  it.effect("is read from beside the site", () =>
    Effect.gen(function* () {
      const config = yield* loadSiteConfig("https://app.open-pool.com/config.json")
      expect(config.regions.map((r) => r.id)).toEqual(["us-east"])
    }).pipe(Effect.provide(http(() => reply(200, good)))),
  )

  it.effect("is refused when it is missing", () =>
    Effect.gen(function* () {
      const missing = yield* Effect.flip(loadSiteConfig("https://app.open-pool.com/config.json"))
      expect(missing.detail).toBe("https://app.open-pool.com/config.json answered 404")
    }).pipe(Effect.provide(http(() => reply(404, undefined)))),
  )

  it.effect("is refused when it names no region", () =>
    Effect.gen(function* () {
      const empty = yield* Effect.flip(loadSiteConfig("https://app.open-pool.com/config.json"))
      expect(empty.detail).toContain("https://app.open-pool.com/config.json is not a settings file")
    }).pipe(Effect.provide(http(() => reply(200, { ...good, regions: [] })))),
  )
})
