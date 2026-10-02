import { describe, expect, it } from "@effect/vitest"
import { Arbitrary, DateTime, Option, Redacted, Schema } from "effect"
import * as M from "../src/index.js"
import { some } from "./kit.js"

// Round trips: what is decoded encodes back to what decodes to the same value.
const roundTrips = <A, I>(name: string, schema: Schema.Schema<A, I>) =>
  it.prop(`${name} survives the wire and back`, [Arbitrary.make(schema)], ([value]) => {
    const again = Schema.decodeSync(schema)(Schema.encodeSync(schema)(value))
    return JSON.stringify(Schema.encodeSync(schema)(again)) === JSON.stringify(Schema.encodeSync(schema)(value))
  })

describe("wire schemas", () => {
  roundTrips("Session", M.Session)
  roundTrips("Price", M.Price)
  roundTrips("ApplicationDraft", M.ApplicationDraft)
  roundTrips("Settings", M.Settings)
  roundTrips("AppInput", M.AppInput)
  roundTrips("Earning", M.Earning)
  roundTrips("LeaderboardEntry", M.LeaderboardEntry)
  roundTrips("SiteConfig", M.SiteConfig)
})

describe("the pool service's JSON", () => {
  it("reads a machine the way the service writes one", () => {
    const machine = Schema.decodeUnknownSync(M.Machine)({
      id: "mch_abc",
      member_id: "mem_abc",
      label: "box-1",
      gpus: [{ gpu_id: "gpu_4090", count: 2 }],
      status: "approved",
      created_at: "2026-10-01T12:00:00Z",
      last_seen_at: "2026-10-01T12:05:00.123456789Z",
      assignments: [
        {
          id: "asg_1", machine_id: "mch_abc", app_id: "app_1", app_name: "vllm/qwen2.5-7b-instruct", capacity: 2,
          status: "active", version: "1.0.0", upgrading: true, created_at: "2026-10-01T12:00:00Z", updated_at: "2026-10-01T12:00:00Z",
        },
      ],
      reported: {
        agent_version: "0.1.0", generation: "abc", containers: [{ service: "app-asg_1", state: "running", health: "healthy" }],
        gpus: [{ name: "NVIDIA GeForce RTX 4090", memory_mb: 24564 }], reported_at: "2026-10-01T12:05:00Z",
      },
    })
    expect(machine.assignments[0]?.upgrading).toBe(true)
    expect(Option.isNone(machine.credential_created_at)).toBe(true)
    expect(DateTime.formatIso(some(machine.last_seen_at))).toBe("2026-10-01T12:05:00.123Z")
    const report = some(machine.reported)
    expect(Option.isNone(report.error)).toBe(true)
    expect(report.gpus[0]?.memory_mb).toBe(24564)
  })

  it("reads the lists Go writes as null as empty", () => {
    const machine = Schema.decodeUnknownSync(M.Machine)({
      id: "mch_abc", member_id: "mem_abc", label: "box-1", gpus: null, status: "pending",
      created_at: "2026-10-01T12:00:00Z", assignments: null,
    })
    expect(machine.gpus).toEqual([])
    expect(machine.assignments).toEqual([])
    expect(Schema.decodeUnknownSync(M.Applications)({ applications: null }).applications).toEqual([])
  })

  it("reads who the caller is, member or not", () => {
    const stranger = Schema.decodeUnknownSync(M.Me)({
      region: { id: "us-east", name: "US East" }, address: "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", member: null, applications: [],
    })
    expect(Option.isNone(stranger.member)).toBe(true)
  })

  it("keeps a credential's secret out of anything printed", () => {
    const credential = Schema.decodeUnknownSync(M.NewCredential)({
      machine_id: "mch_abc", secret: "opc_secret", agent_command: "docker run -e OPENPOOL_CREDENTIAL=opc_secret agent", commands: [],
    })
    expect(JSON.stringify(credential)).not.toContain("opc_secret")
    expect(String(credential.secret)).not.toContain("opc_secret")
    expect(Redacted.value(credential.secret)).toBe("opc_secret")
    expect(Redacted.value(some(credential.agent_command))).toContain("OPENPOOL_CREDENTIAL=opc_secret")
  })

  it("reads amounts of wei larger than a JSON number", () => {
    const batch = Schema.decodeUnknownSync(M.Batch)({
      id: "bat_1", first_round: 4100, last_round: 4106, status: "ready", pot_wei: "123456789012345678901234567890",
      commission_bps: 1500, payable_wei: "104938270660493827066049382706", total_fees_wei: "5", other_fees_wei: "0",
      created_at: "2026-10-01T12:00:00Z",
    })
    expect(batch.pot_wei).toBe(123456789012345678901234567890n)
    expect(batch.items).toEqual([])
  })

  it("refuses what it does not understand instead of guessing", () => {
    expect(Schema.decodeUnknownEither(M.Member)({ id: "mem_1", address: "not an address" })._tag).toBe("Left")
    expect(Schema.decodeUnknownEither(M.Runner)({ runner_id: "r", state: "sleeping" })._tag).toBe("Left")
  })
})
