import { Option } from "effect"
import { html } from "lit"
import { afterEach, describe, expect, it, vi } from "vitest"
import { type ConfirmRequest, type OpConfirm, type OpCopy, type OpTheme } from "../src/index.js"
import "../src/index.js"
import { cleanup, mount, settle, text } from "./kit.js"

afterEach(cleanup)

describe("op-copy", () => {
  it("shows the text whole and copies it", async () => {
    const written = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue()
    const el = await mount<OpCopy>(html`<op-copy label="install command" text="docker run -d agent"></op-copy>`)
    expect(text(el.shadowRoot?.querySelector("code"))).toBe("docker run -d agent")
    expect(text(el.shadowRoot?.querySelector("figcaption"))).toBe("install command")
    const button = el.shadowRoot?.querySelector("button")
    expect(button?.getAttribute("aria-label")).toBe("Copy install command")
    button?.click()
    await settle(el)
    expect(written).toHaveBeenCalledWith("docker run -d agent")
    expect(text(button)).toBe("Copied")
  })
})

describe("op-theme", () => {
  afterEach(() => document.documentElement.removeAttribute("data-theme"))

  it("starts by following the system, and each press moves to the next theme", async () => {
    const el = await mount<OpTheme>(html`<op-theme></op-theme>`)
    const button = el.shadowRoot?.querySelector("button")
    expect(button?.getAttribute("aria-label")).toBe("Theme: following your system. Switch to light.")
    expect(document.documentElement.hasAttribute("data-theme")).toBe(false)
    button?.click()
    await settle(el)
    expect(document.documentElement.getAttribute("data-theme")).toBe("light")
    expect(button?.getAttribute("aria-label")).toBe("Theme: light. Switch to dark.")
    button?.click()
    await settle(el)
    expect(document.documentElement.getAttribute("data-theme")).toBe("dark")
    button?.click()
    await settle(el)
    expect(document.documentElement.hasAttribute("data-theme")).toBe(false)
  })

  it("hands the choice to the page's theme script, which remembers it", async () => {
    const set = vi.fn()
    window.openPoolTheme = { get: () => "dark", set }
    const el = await mount<OpTheme>(html`<op-theme></op-theme>`)
    const button = el.shadowRoot?.querySelector("button")
    expect(button?.getAttribute("aria-label")).toBe("Theme: dark. Switch to follow your system.")
    button?.click()
    await settle(el)
    expect(set).toHaveBeenCalledWith("system")
    delete window.openPoolTheme
  })
})

describe("op-confirm", () => {
  const request = (overrides: Partial<ConfirmRequest>, run = vi.fn()): ConfirmRequest => ({
    heading: "Ban Alice?", message: "Her runners are removed at once.", confirm: "Ban member", danger: true, reason: "required", run, ...overrides,
  })

  it("stays closed until asked, then asks in a modal", async () => {
    const el = await mount<OpConfirm>(html`<op-confirm></op-confirm>`)
    const dialog = el.shadowRoot?.querySelector("dialog")
    expect(dialog?.open).toBe(false)
    el.request = Option.some(request({}))
    await settle(el)
    expect(dialog?.open).toBe(true)
    expect(dialog?.matches(":modal")).toBe(true)
    expect(text(dialog?.querySelector("h2"))).toBe("Ban Alice?")
    expect(dialog?.getAttribute("aria-labelledby")).toBe("heading")
  })

  it("does not go ahead without a required reason", async () => {
    const run = vi.fn()
    const el = await mount<OpConfirm>(html`<op-confirm></op-confirm>`)
    el.request = Option.some(request({}, run))
    await settle(el)
    el.shadowRoot?.querySelector<HTMLButtonElement>("button[type=submit]")?.click()
    await settle(el)
    expect(run).not.toHaveBeenCalled()
    expect(el.shadowRoot?.querySelector("dialog")?.open).toBe(true)
  })

  it("hands over the reason, closes and says so", async () => {
    const run = vi.fn()
    const closed = vi.fn()
    const el = await mount<OpConfirm>(html`<op-confirm @closed=${closed}></op-confirm>`)
    el.request = Option.some(request({}, run))
    await settle(el)
    const reason = el.shadowRoot?.querySelector("textarea")
    if (reason) reason.value = "  ran a modified image  "
    el.shadowRoot?.querySelector<HTMLButtonElement>("button[type=submit]")?.click()
    await settle(el)
    expect(run).toHaveBeenCalledWith("ran a modified image")
    expect(el.shadowRoot?.querySelector("dialog")?.open).toBe(false)
    expect(closed).toHaveBeenCalledOnce()
    expect(Option.isNone(el.request)).toBe(true)
  })

  it("asks for no reason when none is needed, and cancelling does nothing", async () => {
    const run = vi.fn()
    const el = await mount<OpConfirm>(html`<op-confirm></op-confirm>`)
    el.request = Option.some(request({ reason: "none", danger: false }, run))
    await settle(el)
    expect(el.shadowRoot?.querySelector("textarea")).toBeNull()
    const [confirm, cancel] = el.shadowRoot?.querySelectorAll("button") ?? []
    expect(confirm?.className).toBe("primary")
    cancel?.click()
    await settle(el)
    expect(run).not.toHaveBeenCalled()
    expect(el.shadowRoot?.querySelector("dialog")?.open).toBe(false)
  })
})
