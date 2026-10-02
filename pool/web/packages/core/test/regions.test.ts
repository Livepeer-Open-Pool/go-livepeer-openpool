import { describe, expect, it } from "@effect/vitest"
import { Arbitrary, Either, FastCheck, Schema } from "effect"
import {
  Address,
  answered,
  anyNotSignedIn,
  LeaderboardEntry,
  mergeLeaderboards,
  NotSignedIn,
  RegionUnreachable,
  sumWei,
  unanswered,
} from "../src/index.js"
import { euWest, usEast } from "./kit.js"

const entry = Schema.decodeSync(LeaderboardEntry)
const alice = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
const bob = "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359"

describe("mergeLeaderboards", () => {
  it("makes one line of a member who earns in two regions", () => {
    const merged = mergeLeaderboards([
      {
        region: usEast,
        entries: [
          entry({ address: alice, display_name: "Alice", fees_wei: "30", sessions: 3 }),
          entry({ address: bob, display_name: "Bob", fees_wei: "50", sessions: 1 }),
        ],
      },
      // The same address, written in lower case, under another name.
      { region: euWest, entries: [entry({ address: alice.toLowerCase(), display_name: "alice-eu", fees_wei: "40", sessions: 2 })] },
    ])
    expect(merged.map((s) => [s.displayName, s.fees, s.sessions, s.regions])).toEqual([
      ["alice-eu", 70n, 5, ["EU West", "US East"]],
      ["Bob", 50n, 1, ["US East"]],
    ])
  })

  const entries = FastCheck.array(
    FastCheck.record({
      address: FastCheck.constantFrom(Schema.decodeSync(Address)(alice), Schema.decodeSync(Address)(bob)),
      display_name: FastCheck.string(),
      fees_wei: Arbitrary.make(LeaderboardEntry.fields.fees_wei),
      sessions: FastCheck.nat(1000),
    }),
  )

  it.prop("loses and invents no fees", [entries, entries], ([east, west]) => {
    const merged = mergeLeaderboards([
      { region: usEast, entries: east },
      { region: euWest, entries: west },
    ])
    return sumWei(merged.map((s) => s.fees)) === sumWei([...east, ...west].map((e) => e.fees_wei))
  })

  it.prop("is ordered from the most earned to the least", [entries], ([east]) => {
    const merged = mergeLeaderboards([{ region: usEast, entries: east }])
    return merged.every((s, i) => i === 0 || (merged[i - 1]?.fees ?? 0n) >= s.fees)
  })

  it.prop("does not depend on which region is asked first", [entries, entries], ([east, west]) => {
    const one = mergeLeaderboards([{ region: usEast, entries: east }, { region: euWest, entries: west }])
    const other = mergeLeaderboards([{ region: euWest, entries: west }, { region: usEast, entries: east }])
    return JSON.stringify(one.map((s) => [s.address.toLowerCase(), s.fees.toString(), s.sessions])) ===
      JSON.stringify(other.map((s) => [s.address.toLowerCase(), s.fees.toString(), s.sessions]))
  })
})

describe("region results", () => {
  const results = [
    { region: usEast, result: Either.right(1) },
    { region: euWest, result: Either.left(new RegionUnreachable({ region: euWest.id, reason: "it could not be reached" })) },
  ]

  it("separates the regions that answered from those that did not", () => {
    expect(answered(results)).toEqual([{ region: usEast, value: 1 }])
    expect(unanswered(results).map((r) => r.region.id)).toEqual(["eu-west"])
    expect(anyNotSignedIn(results)).toBe(false)
    expect(anyNotSignedIn([{ region: usEast, result: Either.left(new NotSignedIn({ region: usEast.id })) }])).toBe(true)
  })
})
