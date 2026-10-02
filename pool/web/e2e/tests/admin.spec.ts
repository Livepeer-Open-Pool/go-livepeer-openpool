import type { Page } from "@playwright/test"
import { apply, dave, walletToken } from "../harness/seed.js"
import { ADMIN_EMAIL, expect, regionSection, signInAsOperator, sites, test } from "./fixtures.js"

// These tests build on each other: an applicant is approved, connects a
// runner, and the operator then works with it. They use their own applicant,
// Dave, and leave the people the other sites' tests rely on alone.
test.describe.configure({ mode: "serial" })

const host = new URL(sites.member).host
const card = (page: Page, region: string, heading: string) => regionSection(page, region).getByRole("article").filter({ has: page.getByRole("heading", { name: heading }) })

test.describe("the admin site", () => {
  test("signs the operator in with an emailed code, and refuses a wrong one", async ({ page, pool }) => {
    await page.goto(sites.admin)
    await expect(page.getByRole("heading", { level: 1, name: "Operator sign-in" })).toBeVisible()
    await expect(page.getByRole("navigation", { name: "Primary" })).toHaveCount(0)
    const before = pool.usEast.codes().length
    await page.getByLabel("Email").fill(ADMIN_EMAIL)
    await page.getByRole("button", { name: "Email me a code" }).click()
    await page.getByLabel("Code").fill("000000")
    await page.getByRole("button", { name: "Sign in", exact: true }).click()
    await expect(page.getByRole("alert")).toContainText("That code was not accepted")
    // The first region emailed the code; its log stands in for the mailbox.
    await page.getByLabel("Code").fill(await pool.usEast.nextCode(before))
    await page.getByRole("button", { name: "Sign in", exact: true }).click()
    await expect(page.getByText(`Signed in as ${ADMIN_EMAIL}`)).toBeVisible()
    await expect(page).toHaveTitle("Overview · Open Pool admin")
    await page.getByRole("button", { name: "Sign out" }).click()
    await expect(page.getByRole("heading", { level: 1, name: "Operator sign-in" })).toBeVisible()
  })

  test("shows each region's health and what is waiting", async ({ page, pool }) => {
    const first = pool.usEast.orchestrator.round - 7
    await signInAsOperator(page, pool)
    for (const region of ["US East", "EU West"]) {
      const section = regionSection(page, region)
      await expect(section).toContainText("1 application to decide")
      await expect(section).toContainText(`Payout for rounds ${first}–${first + 6} to approve: 0.0425 ETH`)
      await expect(section).toContainText("reachable")
      await expect(section).toContainText("1 runner registered")
      await expect(section).toContainText("not configured")
    }
  })

  test("approves an application with what its machine runs, and rejects one with a reason", async ({ page, pool }) => {
    await apply(pool.usEast, dave, host, "Dave", { label: "shed-3090", gpu: (await gpuId(pool.usEast, "RTX 3090")), count: 1 })
    await apply(pool.euWest, dave, host, "Dave", { label: "shed-3090", gpu: (await gpuId(pool.euWest, "RTX 3090")), count: 1 })
    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "Applications", exact: true }).click()

    const east = card(page, "US East", "Dave")
    await expect(east).toContainText("shed-3090: 1 × NVIDIA GeForce RTX 3090")
    // The image app needs a 4090, which this machine does not have.
    await expect(east.getByRole("checkbox", { name: /flux\/schnell/ })).toBeDisabled()
    await east.getByRole("checkbox", { name: /vllm\/qwen2.5-7b-instruct/ }).check()
    await east.getByRole("button", { name: "Approve Dave" }).click()
    await expect(card(page, "US East", "Dave")).toHaveCount(0)

    await card(page, "EU West", "Dave").getByRole("button", { name: "Reject" }).click()
    const dialog = page.getByRole("dialog")
    await dialog.getByLabel("Reason").fill("We have enough 3090s in this region for now.")
    await dialog.getByRole("button", { name: "Reject application" }).click()
    await expect(card(page, "EU West", "Dave")).toHaveCount(0)

    await page.getByLabel("Show").selectOption("rejected")
    await expect(card(page, "EU West", "Dave")).toContainText("We have enough 3090s in this region for now.")
    await page.goto(`${sites.admin}/members`)
    await expect(regionSection(page, "US East").getByRole("row", { name: /Dave/ })).toContainText("active")
    await expect(regionSection(page, "EU West").getByRole("row", { name: /Dave/ })).toHaveCount(0)
  })

  test("shows demand beside the machines, approves a machine and changes what one runs", async ({ page, pool }) => {
    // Dave asks for a second machine, as he would on the member site.
    const token = await walletToken(pool.usEast, dave, host)
    await pool.usEast.call("POST", "/me/machines", token, { label: "loft-4090", gpus: [{ gpu_id: await gpuId(pool.usEast, "RTX 4090"), count: 1 }] })
    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "Machines", exact: true }).click()

    const demand = regionSection(page, "US East").getByRole("table", { name: /Demand/ })
    await expect(demand.getByRole("row", { name: /vllm\/qwen2.5-7b-instruct/ })).toContainText("0 of 2")
    await expect(demand.getByRole("row", { name: /flux\/schnell/ })).toContainText("4")

    const loft = card(page, "US East", "loft-4090")
    await expect(loft).toContainText("pending")
    await loft.getByRole("button", { name: "Approve this machine" }).click()
    await expect(card(page, "US East", "loft-4090").getByRole("button", { name: "Save assignments" })).toBeVisible()
    await card(page, "US East", "loft-4090").getByRole("checkbox", { name: /flux\/schnell/ }).check()
    await card(page, "US East", "loft-4090").getByRole("button", { name: "Save assignments" }).click()
    await expect(card(page, "US East", "loft-4090")).toContainText("told to run 2.1.0")

    const shed = card(page, "US East", "shed-3090")
    await expect(shed.getByRole("checkbox", { name: /vllm\/qwen2.5-7b-instruct/ })).toBeChecked()
    await expect(shed).toContainText("told to run 1.0.0")
  })

  test("adds a GPU model to every region and retires one in one", async ({ page, pool }) => {
    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "GPUs", exact: true }).click()
    await page.getByLabel("Model name").fill("NVIDIA GeForce RTX 5090")
    await page.getByLabel("Memory in MB").fill("32607")
    await page.getByRole("button", { name: "Add model" }).click()
    await expect(page.getByRole("status")).toContainText("Added in US East and EU West.")
    for (const region of ["US East", "EU West"]) {
      await expect(regionSection(page, region).getByRole("row", { name: /RTX 5090/ })).toContainText("32 GB")
    }
    await regionSection(page, "EU West").getByRole("button", { name: /Retire NVIDIA GeForce RTX 5090/ }).click()
    await expect(regionSection(page, "EU West").getByRole("row", { name: /RTX 5090/ })).toContainText("retired")
    await expect(regionSection(page, "US East").getByRole("row", { name: /RTX 5090/ })).toContainText("accepted")
  })

  test("creates an app, pauses it, releases a version and copies it to another region", async ({ page, pool }) => {
    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "Apps", exact: true }).click()
    await regionSection(page, "EU West").getByRole("link", { name: "New app in EU West" }).click()
    await expect(page.getByRole("heading", { level: 1, name: "New app in eu-west" })).toBeVisible()

    await page.getByRole("textbox", { name: /^Name/ }).fill("whisper/large-v3")
    await page.getByLabel("Price in US dollars").fill("0.20")
    await page.getByRole("textbox", { name: /^Description/ }).fill("Speech to text.")
    await page.getByRole("checkbox", { name: /RTX 3090/ }).check()
    await page.getByLabel("Sessions at once, at most").fill("3")
    await page.getByRole("textbox", { name: /^Version/ }).fill("1.0.0")
    await page.getByRole("textbox", { name: /^Image/ }).fill("ubuntu:latest")
    await page.getByRole("textbox", { name: /^Arguments/ }).fill("--model\nlarge-v3")
    await page.getByRole("textbox", { name: /^Environment/ }).fill("HF_HOME=/cache")
    await page.getByRole("textbox", { name: /^Path/ }).fill("/health")
    await page.getByRole("button", { name: "Create app" }).click()
    // The service refuses an image that is not pinned, and says why.
    await expect(page.getByRole("alert")).toContainText("container image must be pinned by digest")
    await page.getByRole("textbox", { name: /^Image/ }).fill(`ghcr.io/speaches-ai/speaches@sha256:${"c3".repeat(32)}`)
    await page.getByRole("button", { name: "Create app" }).click()
    await expect(page.getByRole("heading", { level: 1, name: "whisper/large-v3" })).toBeVisible()

    // A new version is its name and its container together.
    const release = page.locator("form").filter({ has: page.getByRole("heading", { name: "Release a new version" }) })
    await release.getByRole("textbox", { name: /^New version/ }).fill("1.1.0")
    await release.getByRole("textbox", { name: /^Image/ }).fill(`ghcr.io/speaches-ai/speaches@sha256:${"d4".repeat(32)}`)
    await release.getByRole("button", { name: "Release version" }).click()
    await expect(page.getByRole("status")).toContainText("Saved.")
    await expect(release).toContainText("Now at 1.1.0")

    await page.getByRole("link", { name: "All apps" }).click()
    const row = regionSection(page, "EU West").getByRole("row", { name: /whisper\/large-v3/ })
    await expect(row).toContainText("$0.20 per hour")
    await expect(row).toContainText("1.1.0")
    await row.getByRole("button", { name: /Pause/ }).click()
    await expect(regionSection(page, "EU West").getByRole("row", { name: /whisper\/large-v3/ })).toContainText("paused")
    const apps = await pool.euWest.call<{ apps: Array<{ name: string }> }>("GET", "/public/apps", undefined)
    expect(apps.apps.map((a) => a.name)).not.toContain("whisper/large-v3")
    await regionSection(page, "EU West").getByRole("row", { name: /whisper\/large-v3/ }).getByRole("button", { name: /Resume/ }).click()
    await expect(regionSection(page, "EU West").getByRole("row", { name: /whisper\/large-v3/ })).toContainText("offered")

    // Copying starts the other region's form from this app, GPU models matched by name.
    await regionSection(page, "EU West").getByRole("row", { name: /whisper\/large-v3/ }).getByRole("link", { name: "Copy to US East" }).click()
    await expect(page.getByRole("textbox", { name: /^Name/ })).toHaveValue("whisper/large-v3")
    await expect(page.getByRole("checkbox", { name: /RTX 3090/ })).toBeChecked()
    await expect(page.getByRole("checkbox", { name: /RTX 4090/ })).not.toBeChecked()
    await expect(page.getByRole("textbox", { name: /^Arguments/ })).toHaveValue("--model\nlarge-v3")
    await page.getByRole("button", { name: "Create app" }).click()
    await expect(page.getByRole("heading", { level: 1, name: "whisper/large-v3" })).toBeVisible()
    await expect(page).toHaveURL(/\/apps\/us-east\/app_/)
  })

  test("tests, suspends, releases and evicts a runner", async ({ page, pool }) => {
    // Dave's machine connects, as his agent would make it.
    const token = await walletToken(pool.usEast, dave, host)
    const { machines } = await pool.usEast.call<{ machines: Array<{ id: string; label: string }> }>("GET", "/me/machines", token)
    const shed = machines.find((m) => m.label === "shed-3090")
    const credential = await pool.usEast.call<{ secret: string }>("POST", `/me/machines/${shed?.id}/credential`, token)
    await pool.usEast.admit(credential.secret, { runner_id: "runner_us-east_dave", app: "vllm/qwen2.5-7b-instruct", version: "1.0.0" })

    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "Runners", exact: true }).click()
    const runner = () => regionSection(page, "US East").getByRole("row", { name: /Dave, shed-3090/ })
    await expect(runner()).toContainText("active") // the pool tested and promoted it by itself

    await runner().getByRole("button", { name: /Test now/ }).click()
    await expect(page.getByRole("status").filter({ hasText: "Tested" })).toContainText("passed")

    await runner().getByRole("button", { name: "Suspend" }).click()
    await page.getByRole("dialog").getByLabel("Reason").fill("Checking a complaint about slow answers.")
    await page.getByRole("dialog").getByRole("button", { name: "Suspend and hold" }).click()
    await expect(runner()).toContainText("suspended")
    await expect(runner()).toContainText("on hold")
    await expect(runner()).toContainText("held: Checking a complaint about slow answers.")
    expect(pool.usEast.orchestrator.runners.find((r) => r.runner_id === "runner_us-east_dave")?.state).toBe("suspended")

    // Released, it is the pool's again: passing its tests brings it back.
    await runner().getByRole("button", { name: "Release hold" }).click()
    await expect(runner()).not.toContainText("on hold")

    await runner().getByRole("button", { name: "Evict" }).click()
    await page.getByRole("dialog").getByLabel("Reason").fill("Machine is being replaced.")
    await page.getByRole("dialog").getByRole("button", { name: "Evict runner" }).click()
    await expect(runner()).toHaveCount(0)
    expect(pool.usEast.orchestrator.runners.some((r) => r.runner_id === "runner_us-east_dave")).toBe(false)
  })

  test("bans a member with a reason and lifts the ban", async ({ page, pool }) => {
    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "Members", exact: true }).click()
    await regionSection(page, "US East").getByRole("link", { name: "Dave" }).click()
    await expect(page.getByRole("heading", { level: 1, name: "Dave" })).toBeVisible()
    await expect(page.getByRole("row", { name: /shed-3090/ })).toContainText("vllm/qwen2.5-7b-instruct")
    await page.getByRole("button", { name: "Ban this member" }).click()
    await page.getByRole("dialog").getByLabel("Reason").fill("Served a modified image.")
    await page.getByRole("dialog").getByRole("button", { name: "Ban member" }).click()
    await expect(page.getByText("banned in us-east")).toBeVisible()
    await expect(page.getByText("Served a modified image.")).toBeVisible()
    await page.getByRole("button", { name: "Lift the ban" }).click()
    await expect(page.getByText("active in us-east")).toBeVisible()
  })

  test("leaves a member out of a payout batch, puts them back, and approves it", async ({ page, pool }) => {
    const first = pool.euWest.orchestrator.round - 7
    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "Payouts", exact: true }).click()
    const window = regionSection(page, "EU West").getByRole("row", { name: new RegExp(`${first}–${first + 6}`) })
    await expect(window).toContainText("waiting for your approval")
    await expect(window).toContainText("0.05 ETH")
    await expect(window).toContainText("15%")
    await window.getByRole("link").click()

    await expect(page.getByRole("heading", { level: 1 })).toHaveText(`Rounds ${first}–${first + 6} in eu-west`)
    const payment = page.getByRole("row", { name: /Alice/ })
    await expect(payment).toContainText("0.002 ETH")
    await expect(payment).toContainText("0.0425 ETH")
    await payment.getByRole("checkbox", { name: "Pay Alice" }).uncheck()
    await expect(page.getByRole("row", { name: /Alice/ })).toContainText("left out")
    await page.getByRole("row", { name: /Alice/ }).getByRole("checkbox", { name: "Pay Alice" }).check()
    await expect(page.getByRole("row", { name: /Alice/ })).toContainText("pending")

    await page.getByRole("button", { name: "Approve this batch" }).click()
    await expect(page.getByRole("dialog")).toContainText("One payment is sent")
    await page.getByRole("dialog").getByRole("button", { name: "Approve and pay" }).click()
    await expect(page.getByText(/approved|blocked/).first()).toBeVisible()
    await expect(page.getByRole("button", { name: "Approve this batch" })).toHaveCount(0)
    await expect(page.getByRole("row", { name: /Alice/ }).getByRole("checkbox")).toBeDisabled()
  })

  test("publishes one region's own terms and saves settings in every region", async ({ page, pool }) => {
    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "Terms", exact: true }).click()
    await expect(regionSection(page, "US East")).toContainText("US East has no terms of its own.")
    await page.getByRole("radio", { name: /One region's own terms/ }).check()
    await page.getByRole("combobox", { name: "Region" }).selectOption("us-east")
    await page.getByRole("textbox", { name: /^Text/ }).fill("US East terms\n\nMachines must be reachable around the clock.")
    await page.getByRole("button", { name: "Publish" }).click()
    await expect(page.getByRole("status")).toContainText("Published in US East.")
    await expect(regionSection(page, "US East")).toContainText("US East's own terms, version 1")

    await page.getByRole("link", { name: "Settings", exact: true }).click()
    const form = regionSection(page, "US East").locator("form")
    await expect(form.getByLabel("Commission in basis points")).toHaveValue("1500")
    await form.getByLabel("Seconds between tests").fill("300")
    await form.getByRole("checkbox", { name: /Save these settings in every region/ }).check()
    await form.getByRole("button", { name: "Save US East's settings" }).click()
    await expect(page.getByRole("status")).toContainText("Saved in US East and EU West.")
    await expect(regionSection(page, "EU West").getByLabel("Seconds between tests")).toHaveValue("300")
    const saved = await pool.euWest.call<{ test_interval_seconds: number }>("GET", "/admin/settings", pool.operator.token)
    expect(saved.test_interval_seconds).toBe(300)
  })

  test("records what was done, and by whom", async ({ page, pool }) => {
    await signInAsOperator(page, pool)
    await page.getByRole("link", { name: "Audit", exact: true }).click()
    const log = regionSection(page, "US East").getByRole("table")
    await expect(log.getByRole("row", { name: /member\.ban/ }).first()).toContainText(`admin:${ADMIN_EMAIL}`)
    await expect(log.getByRole("row", { name: /member\.ban/ }).first()).toContainText("Served a modified image.")
    await expect(log.getByRole("row", { name: /runner\.activate/ }).first()).toContainText("pool")
  })
})

/** The id a region gave the GPU model whose name contains `name`. */
const gpuId = async (region: { call: <T>(method: string, path: string, token: string | undefined) => Promise<T> }, name: string): Promise<string> => {
  const { gpus } = await region.call<{ gpus: Array<{ id: string; name: string }> }>("GET", "/public/gpus", undefined)
  const gpu = gpus.find((g) => g.name.includes(name))
  if (!gpu) throw new Error(`no GPU model called ${name}`)
  return gpu.id
}
