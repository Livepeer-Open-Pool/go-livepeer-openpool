import { Address, Price, Wei } from "@openpool/core"
import { DateTime, Schema } from "effect"
import { html, render } from "lit"
import { describe, expect, it } from "vitest"
import { eth, formatMemory, formatPrice, plural, shortAddress, time } from "../src/index.js"

const rendered = (template: unknown): HTMLElement => {
  const box = document.createElement("div")
  render(html`${template}`, box)
  return box.firstElementChild as HTMLElement
}

describe("formatting", () => {
  it("writes an amount as ETH and keeps the exact wei", () => {
    const el = rendered(eth(Schema.decodeSync(Wei)("1500000000000000001")))
    expect(el.localName).toBe("data")
    expect(el.textContent).toBe("1.5 ETH")
    expect(el.getAttribute("value")).toBe("1500000000000000001")
    expect(el.title).toBe("1500000000000000001 wei")
  })

  it("writes a moment in a time element with the machine-readable value", () => {
    const el = rendered(time(DateTime.unsafeMake("2026-10-01T12:00:00Z")))
    expect(el.localName).toBe("time")
    expect(el.getAttribute("datetime")).toBe("2026-10-01T12:00:00.000Z")
    expect(el.textContent).toContain("2026")
  })

  it("shortens an address and keeps the whole of it", () => {
    const el = rendered(shortAddress(Schema.decodeSync(Address)("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")))
    expect(el.textContent).toBe("0x5aAe…eAed")
    expect(el.title).toBe("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")
    expect(el.getAttribute("translate")).toBe("no")
  })

  it("writes prices, memory and counts the way people say them", () => {
    const price = Schema.decodeSync(Price)
    expect(formatPrice(price({ price: "0.50", unit: "hour" }))).toBe("$0.50 per hour")
    expect(formatPrice(price({ price: "0.002", unit: "fixed" }))).toBe("$0.002 per request")
    expect(formatMemory(24564)).toBe("24 GB")
    expect(plural(1, "machine")).toBe("1 machine")
    expect(plural(3, "machine")).toBe("3 machines")
    expect(plural(0, "entry", "entries")).toBe("0 entries")
  })
})
