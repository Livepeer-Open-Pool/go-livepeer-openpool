import { expect, regionSection, sites, test } from "./fixtures.js"

test.describe("the public site", () => {
  test("says what the pool is, and shows each region's payout terms live", async ({ page, pool }) => {
    void pool
    const response = await page.goto(sites.marketing)
    expect(response?.status()).toBe(200)
    await expect(page).toHaveTitle("Open Pool: earn ETH with your GPU on Livepeer")
    await expect(page.getByRole("heading", { level: 1 })).toHaveText("Put your GPU to work on Livepeer, and get paid in ETH")

    const facts = page.getByRole("table", { name: "The pool's regions" })
    for (const region of ["US East", "EU West"]) {
      const row = facts.getByRole("row", { name: new RegExp(region) })
      await expect(row).toContainText("open")
      await expect(row).toContainText("15%")
      await expect(row).toContainText("7 rounds")
    }
    // Every page's footer says which regions are answering.
    const status = page.getByRole("list", { name: "The pool's regions" })
    await expect(status.getByRole("listitem")).toHaveText([/US East\s+up/, /EU West\s+up/])
  })

  test("is readable without scripts: the pages are files, the live parts say what they would show", async ({ browser, pool }) => {
    void pool
    const context = await browser.newContext({ javaScriptEnabled: false })
    const page = await context.newPage()
    await page.goto(`${sites.marketing}/how-it-works/`)
    await expect(page.getByRole("heading", { level: 1, name: "How Open Pool works" })).toBeVisible()
    await expect(page.getByText("For example: a window's pot is 1 ETH and the commission is 15%")).toBeVisible()
    await expect(page.getByText("The current commission and window length of each region load here.")).toBeVisible()
    await expect(page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "How it works" })).toHaveAttribute("aria-current", "page")
    await context.close()
  })

  test("lists each region's apps with prices, GPUs and machines connected", async ({ page, pool }) => {
    void pool
    await page.goto(`${sites.marketing}/apps/`)
    await expect(page).toHaveTitle("Apps and prices · Open Pool")
    for (const region of ["US East", "EU West"]) {
      const table = regionSection(page, region).getByRole("table", { name: `Apps served in ${region}` })
      const chat = table.getByRole("row", { name: /vllm\/qwen2.5-7b-instruct/ })
      await expect(chat).toContainText("$0.50 per hour")
      await expect(chat).toContainText("NVIDIA GeForce RTX 4090")
      await expect(chat).toContainText("NVIDIA GeForce RTX 3090")
      await expect(chat).toContainText("1 machine")
      await expect(chat).toContainText("A 7-billion-parameter instruction model served with vLLM.")
      await expect(table.getByRole("row", { name: /flux\/schnell/ })).toContainText("$0.002 per request")
    }
  })

  test("ranks members by fees added up across regions, for the window or for all time", async ({ page, pool }) => {
    const first = pool.usEast.orchestrator.round
    await page.goto(`${sites.marketing}/leaderboard/`)
    const alice = page.getByRole("table", { name: "Members by fees earned" }).getByRole("row", { name: /Alice/ })
    // The window now running: 0.003 ETH in each region.
    await expect(alice).toContainText("0.006 ETH")
    await expect(alice).toContainText("EU West, US East")
    await expect(page.getByText(`US East: rounds ${first} to ${first + 6}.`)).toBeVisible()

    await page.getByRole("radio", { name: "since the pool began" }).check()
    // All time: 0.011 ETH in US East and 0.005 ETH in EU West, over six sessions.
    await expect(alice).toContainText("0.016 ETH")
    await expect(alice.getByRole("cell").nth(2)).toHaveText("6")
    await expect(alice.getByRole("cell").first()).toHaveText("1")
  })

  test("shows the terms in force: the pool's once, and each region's own", async ({ page, pool }) => {
    void pool
    await page.goto(`${sites.marketing}/terms/`)
    await expect(page.getByRole("heading", { level: 2, name: "The pool's terms" })).toHaveCount(1)
    await expect(page.getByText("A member can be banned for any reason at any time. Malicious use is an immediate ban.")).toBeVisible()
    await expect(page.getByRole("heading", { level: 2, name: "EU West's own terms" })).toBeVisible()
    await expect(page.getByText("Machines in this region must be located in it.")).toBeVisible()
  })

  test("shows each region's real install command, and sends people to the member site to apply", async ({ page, pool }) => {
    await page.goto(`${sites.marketing}/connect/`)
    const info = await pool.usEast.call<{ agent: { image: string; key: string } }>("GET", "/public/pool", undefined)
    const command = page.locator("op-copy").filter({ hasText: "US East" })
    await expect(command).toContainText("docker run -d --name openpool-agent-us-east")
    await expect(command).toContainText(`${info.agent.image} --pool ${pool.usEast.api} --key ${info.agent.key}`)
    await expect(command).toContainText("OPENPOOL_CREDENTIAL=<shown once on your machines page>")

    // Questions open one at a time.
    await page.getByText("How do I update an app?").click()
    await expect(page.getByText("machines are moved to it a few at a time")).toBeVisible()
    await page.getByText("My runner is suspended. Why?").click()
    await expect(page.getByText("machines are moved to it a few at a time")).toBeHidden()

    await page.getByRole("link", { name: "Join the pool" }).first().click()
    await expect(page).toHaveURL(`${sites.marketing}/join/`)
    await expect(page.getByRole("link", { name: "Go to the member site and apply" })).toHaveAttribute("href", `${sites.member}/apply`)
  })

  test("answers an address that is no page with 404 and a way back", async ({ page, pool }) => {
    void pool
    const response = await page.goto(`${sites.marketing}/nowhere/`)
    expect(response?.status()).toBe(404)
    await expect(page.getByRole("heading", { level: 1, name: "There is no page at this address" })).toBeVisible()
    await page.getByRole("link", { name: "Go to the home page" }).click()
    await expect(page).toHaveURL(`${sites.marketing}/`)
  })
})
