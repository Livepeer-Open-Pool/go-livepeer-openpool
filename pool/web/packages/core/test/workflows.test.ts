import { describe, expect, it } from "@effect/vitest"
import { Duration, Effect, Option, Schema, TestClock } from "effect"
import {
  Api,
  ApplicationDraft,
  answered,
  anyRegion,
  applyToRegion,
  currentSession,
  everyRegion,
  everyRegionAs,
  inRegionAs,
  membershipEverywhere,
  requestAdminCode,
  requireSession,
  Session,
  SessionStore,
  signInAsAdmin,
  signInWithWallet,
  signOut,
  unanswered,
} from "../src/index.js"
import { type Call, down, json, refuse, site, some } from "./kit.js"

const poolInfo = (id: string, name: string) => ({
  region: { id, name }, taking_applications: true, apps_offered: 2, sign_in: { domains: ["app.open-pool.com"], chain_id: 42161 },
  commission_bps: 1500, window_rounds: 7,
})

const sessionFor = (role: "wallet" | "admin", subject: string, expires = "1970-01-01T12:00:00Z") => ({
  token: `token-for-${subject}`, role, subject, expires_at: expires,
})

const storeSession = (role: "wallet" | "admin", subject: string, expires?: string) =>
  Effect.flatMap(SessionStore, (store) => store.write(Schema.decodeUnknownSync(Session)(sessionFor(role, subject, expires))))

const alice = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
const member = { id: "mem_1", address: alice, display_name: "Alice", status: "active", created_at: "2026-10-01T12:00:00Z", updated_at: "2026-10-01T12:00:00Z" }

describe("asking every region", () => {
  it.effect("returns what answered and names what did not", () =>
    Effect.gen(function* () {
      const results = yield* everyRegion(Api.pool)
      expect(answered(results).map((r) => [r.region.id, r.value.region.name])).toEqual([["us-east", "US East"]])
      expect(unanswered(results).map((r) => [r.region.id, r.error._tag])).toEqual([["eu-west", "RegionUnreachable"]])
    }).pipe(Effect.provide(site({ "us-east GET /public/pool": json(poolInfo("us-east", "US East")), "eu-west GET /public/pool": down }))),
  )
})

describe("asking any region", () => {
  it.effect("passes over a region that is down", () =>
    Effect.gen(function* () {
      const { region, value } = yield* anyRegion(Api.pool)
      expect([region.id, value.region.name]).toEqual(["eu-west", "EU West"])
    }).pipe(Effect.provide(site({ "us-east GET /public/pool": down, "eu-west GET /public/pool": json(poolInfo("eu-west", "EU West")) }))),
  )

  it.effect("takes a refusal as the answer and does not shop around", () => {
    const calls: Array<Call> = []
    return Effect.gen(function* () {
      const error = yield* Effect.flip(anyRegion(Api.pool))
      expect(error._tag).toBe("RequestRefused")
      expect(calls.map((c) => c.region)).toEqual(["us-east"])
    }).pipe(Effect.provide(site({ "us-east GET /public/pool": refuse(429, "wait a minute"), "eu-west GET /public/pool": json(poolInfo("eu-west", "EU West")) }, calls)))
  })

  it.effect("says so when no region can be reached", () =>
    Effect.gen(function* () {
      expect((yield* Effect.flip(anyRegion(Api.pool)))._tag).toBe("NoRegionReachable")
    }).pipe(Effect.provide(site({ "us-east GET /public/pool": down, "eu-west GET /public/pool": down }))),
  )
})

describe("signing in with a wallet", () => {
  it.effect("signs the region's one-time value and keeps the session", () => {
    const calls: Array<Call> = []
    const signed: Array<string> = []
    return Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2026-10-01T12:00:00Z"))
      const session = yield* signInWithWallet
      expect(session.subject).toBe(alice)
      // The first region is down, so the second issues the value and checks the signature.
      expect(calls.map((c) => `${c.region} ${c.request.path}`)).toEqual([
        "us-east /auth/wallet/nonce", "eu-west /auth/wallet/nonce", "eu-west /auth/wallet/verify",
      ])
      expect(signed[0]).toContain("app.open-pool.com wants you to sign in with your Ethereum account:\n" + alice)
      expect(signed[0]).toContain("Nonce: abcdef0123456789\nIssued At: 2026-10-01T12:00:00.000Z")
      expect(some(calls[2]?.request.body ?? Option.none())).toEqual({ message: signed[0], signature: `0xsigned:${signed[0]?.length}` })
      expect(some(yield* currentSession("wallet")).token).toBe(session.token)
    }).pipe(
      Effect.provide(
        site(
          {
            "us-east POST /auth/wallet/nonce": down,
            "eu-west POST /auth/wallet/nonce": json({ nonce: "abcdef0123456789", sign_in: { domains: ["app.open-pool.com"], chain_id: 42161 } }),
            "eu-west POST /auth/wallet/verify": json(sessionFor("wallet", alice, "2026-10-02T00:00:00Z")),
          },
          calls,
          signed,
        ),
      ),
    )
  })

  it.effect("explains a signature the pool did not accept", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(signInWithWallet)
      expect(error._tag === "RequestRefused" && error.message).toContain("did not accept the signature")
      expect(Option.isNone(yield* currentSession("wallet"))).toBe(true)
    }).pipe(
      Effect.provide(
        site({
          "us-east POST /auth/wallet/nonce": json({ nonce: "abcdef0123456789", sign_in: { domains: [], chain_id: 42161 } }),
          "us-east POST /auth/wallet/verify": refuse(401, "not signed in"),
        }),
      ),
    ),
  )
})

describe("signing in as the operator", () => {
  it.effect("uses the code in the region that emailed it", () => {
    const calls: Array<Call> = []
    return Effect.gen(function* () {
      const region = yield* requestAdminCode("ops@open-pool.com")
      expect(region).toBe("eu-west")
      const session = yield* signInAsAdmin(region, "ops@open-pool.com", "123456")
      expect([session.role, session.subject]).toEqual(["admin", "ops@open-pool.com"])
      expect(calls.at(-1)?.region).toBe("eu-west")
      expect((yield* Effect.flip(signInAsAdmin("mars", "ops@open-pool.com", "123456")))._tag).toBe("UnknownRegion")
    }).pipe(
      Effect.provide(
        site(
          {
            "us-east POST /auth/admin/code": down,
            "eu-west POST /auth/admin/code": { status: 204 },
            "eu-west POST /auth/admin/verify": json(sessionFor("admin", "ops@open-pool.com", "2026-10-02T00:00:00Z")),
          },
          calls,
        ),
      ),
    )
  })

  it.effect("explains a code that was not accepted", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(signInAsAdmin("us-east", "ops@open-pool.com", "000000"))
      expect(error._tag === "RequestRefused" && error.message).toContain("That code was not accepted")
    }).pipe(Effect.provide(site({ "us-east POST /auth/admin/verify": refuse(401, "not signed in") }))),
  )
})

describe("a session", () => {
  it.effect("ends when its time runs out", () =>
    Effect.gen(function* () {
      yield* storeSession("wallet", alice, "1970-01-01T12:00:00Z")
      yield* TestClock.adjust(Duration.hours(11))
      expect(Option.isSome(yield* currentSession("wallet"))).toBe(true)
      yield* TestClock.adjust(Duration.hours(2))
      expect(Option.isNone(yield* currentSession("wallet"))).toBe(true)
      expect((yield* Effect.flip(requireSession("wallet")))._tag).toBe("NoSession")
    }).pipe(Effect.provide(site({}))),
  )

  it.effect("is one per role, and signing out forgets it", () =>
    Effect.gen(function* () {
      yield* storeSession("wallet", alice)
      expect(Option.isNone(yield* currentSession("admin"))).toBe(true)
      yield* signOut("wallet")
      expect(Option.isNone(yield* currentSession("wallet"))).toBe(true)
    }).pipe(Effect.provide(site({}))),
  )

  it.effect("ends everywhere when one region no longer accepts it", () =>
    Effect.gen(function* () {
      yield* storeSession("admin", "ops@open-pool.com")
      expect((yield* Effect.flip(everyRegionAs("admin", (region, session) => Api.adminStatus(region, session.token))))._tag).toBe("NoSession")
      expect(Option.isNone(yield* currentSession("admin"))).toBe(true)

      yield* storeSession("admin", "ops@open-pool.com")
      expect((yield* Effect.flip(inRegionAs("admin", "eu-west", Api.adminStatus)))._tag).toBe("NoSession")
      expect(Option.isNone(yield* currentSession("admin"))).toBe(true)
    }).pipe(
      Effect.provide(
        site({
          "us-east GET /admin/status": json({ region: { id: "us-east", name: "US East" }, orchestrator: { reachable: true, runners: 3 }, pending_applications: 1, pending_machines: 0 }),
          "eu-west GET /admin/status": refuse(401, "not signed in"),
        }),
      ),
    ),
  )

  it.effect("is sent with what it asks, and a region that is down does not end it", () => {
    const calls: Array<Call> = []
    return Effect.gen(function* () {
      yield* storeSession("admin", "ops@open-pool.com")
      const results = yield* everyRegionAs("admin", (region, session) => Api.adminStatus(region, session.token))
      expect(answered(results).map((r) => r.value.orchestrator.runners)).toEqual([3])
      expect(unanswered(results).map((r) => r.region.id)).toEqual(["eu-west"])
      expect(calls.map((c) => Option.getOrNull(c.request.token))).toEqual(["token-for-ops@open-pool.com", "token-for-ops@open-pool.com"])
      expect(Option.isSome(yield* currentSession("admin"))).toBe(true)
    }).pipe(
      Effect.provide(
        site(
          {
            "us-east GET /admin/status": json({ region: { id: "us-east", name: "US East" }, orchestrator: { reachable: true, runners: 3 }, pending_applications: 1, pending_machines: 0 }),
            "eu-west GET /admin/status": down,
          },
          calls,
        ),
      ),
    )
  })
})

describe("a member across regions", () => {
  it.effect("is asked for more only where the address is a member", () => {
    const calls: Array<Call> = []
    return Effect.gen(function* () {
      yield* storeSession("wallet", alice)
      const results = yield* membershipEverywhere(Api.myMachines)
      const standing = answered(results).map((r) => [r.region.id, Option.map(r.value.detail, (d) => d.machines.length)])
      expect(standing).toEqual([["us-east", Option.some(0)], ["eu-west", Option.none()]])
      expect(calls.map((c) => `${c.region} ${c.request.path}`).sort()).toEqual(["eu-west /me", "us-east /me", "us-east /me/machines"])
    }).pipe(
      Effect.provide(
        site(
          {
            "us-east GET /me": json({ region: { id: "us-east", name: "US East" }, address: alice, member, applications: [] }),
            "us-east GET /me/machines": json({ machines: [] }),
            "eu-west GET /me": json({ region: { id: "eu-west", name: "EU West" }, address: alice, member: null, applications: [] }),
          },
          calls,
        ),
      ),
    )
  })

  it.effect("applies by signing the message the region writes", () => {
    const calls: Array<Call> = []
    const signed: Array<string> = []
    const draft = Schema.decodeUnknownSync(ApplicationDraft)({ display_name: "Alice", machines: [{ label: "box-1", gpus: [{ gpu_id: "gpu_4090", count: 2 }] }] })
    return Effect.gen(function* () {
      yield* storeSession("wallet", alice)
      const application = yield* applyToRegion("eu-west", draft)
      expect(application.status).toBe("pending")
      expect(signed).toEqual(["I accept the Open Pool terms, version 3."])
      expect(calls.map((c) => `${c.region} ${c.request.path}`)).toEqual(["eu-west /applications/message", "eu-west /applications"])
      expect(some(calls[1]?.request.body ?? Option.none())).toEqual({
        display_name: "Alice", machines: [{ label: "box-1", gpus: [{ gpu_id: "gpu_4090", count: 2 }] }], signature: "0xsigned:40",
      })
    }).pipe(
      Effect.provide(
        site(
          {
            "eu-west POST /applications/message": json({ message: "I accept the Open Pool terms, version 3." }),
            "eu-west POST /applications": json({
              id: "apn_1", address: alice, display_name: "Alice", machines: [], global_terms_version: 3, status: "pending", created_at: "2026-10-01T12:00:00Z",
            }),
          },
          calls,
          signed,
        ),
      ),
    )
  })

  it.effect("cannot apply without a session, or to a region that does not exist", () =>
    Effect.gen(function* () {
      const draft = Schema.decodeUnknownSync(ApplicationDraft)({ display_name: "Alice", machines: [] })
      expect((yield* Effect.flip(applyToRegion("us-east", draft)))._tag).toBe("NoSession")
      yield* storeSession("wallet", alice)
      expect((yield* Effect.flip(applyToRegion("mars", draft)))._tag).toBe("UnknownRegion")
    }).pipe(Effect.provide(site({}))),
  )
})
