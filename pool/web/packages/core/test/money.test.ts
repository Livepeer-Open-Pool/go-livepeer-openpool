import { describe, expect, it } from "@effect/vitest"
import { Arbitrary, FastCheck, Schema } from "effect"
import { BasisPoints, formatBasisPoints, formatEth, sumWei, Wei } from "../src/index.js"

const wei = Schema.decodeSync(Wei)
const bps = Schema.decodeSync(BasisPoints)
const weiArb = Arbitrary.make(Wei)

describe("formatEth", () => {
  it("writes wei as ETH, without trailing zeros", () => {
    expect(formatEth(wei("0"))).toBe("0")
    expect(formatEth(wei("1000000000000000000"))).toBe("1")
    expect(formatEth(wei("1500000000000000000"))).toBe("1.5")
    expect(formatEth(wei("12345678900000000000"))).toBe("12.345678")
    expect(formatEth(wei("10000000000000000"))).toBe("0.01")
  })

  it("rounds down, never up: nobody is shown more than they have", () => {
    expect(formatEth(wei("1999999999999999999"))).toBe("1.999999")
    expect(formatEth(wei("999999999999999999"), 2)).toBe("0.99")
  })

  it("never shows a real amount as zero", () => {
    expect(formatEth(wei("1"))).toBe("<0.000001")
    expect(formatEth(wei("999999999999"))).toBe("<0.000001")
    expect(formatEth(wei("1000000000000"))).toBe("0.000001")
  })

  it.prop("what is shown is at most the amount, and within one step of it", [weiArb], ([amount]) => {
    const shown = formatEth(amount)
    if (shown.startsWith("<")) {
      return amount > 0n && amount < 10n ** 12n
    }
    const [whole = "0", fraction = ""] = shown.split(".")
    const back = BigInt(whole) * 10n ** 18n + BigInt(fraction.padEnd(18, "0"))
    return back <= amount && amount - back < 10n ** 12n
  })
})

describe("sumWei", () => {
  it.prop("does not depend on order", [FastCheck.array(weiArb)], ([amounts]) => sumWei(amounts) === sumWei([...amounts].reverse()))

  it("adds amounts no JSON number could hold", () => {
    expect(sumWei([wei("9007199254740993"), wei("9007199254740993")])).toBe(18014398509481986n)
    expect(sumWei([])).toBe(0n)
  })
})

describe("Wei", () => {
  it.prop("goes to the wire and back unchanged", [weiArb], ([amount]) => wei(Schema.encodeSync(Wei)(amount)) === amount)

  it("is refused when negative or not a whole number", () => {
    expect(Schema.decodeUnknownEither(Wei)("-1")._tag).toBe("Left")
    expect(Schema.decodeUnknownEither(Wei)("1.5")._tag).toBe("Left")
    expect(Schema.decodeUnknownEither(Wei)(15)._tag).toBe("Left")
  })
})

describe("formatBasisPoints", () => {
  it("writes basis points as a percentage", () => {
    expect(formatBasisPoints(bps(1500))).toBe("15%")
    expect(formatBasisPoints(bps(1250))).toBe("12.5%")
    expect(formatBasisPoints(bps(1))).toBe("0.01%")
    expect(formatBasisPoints(bps(0))).toBe("0%")
    expect(formatBasisPoints(bps(10000))).toBe("100%")
  })
})
