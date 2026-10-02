import { alice } from "../harness/seed.js"
import { expect, regionSection, signInAs, signInAsOperator, sites, test } from "./fixtures.js"

// Named to run last: these tests stop a region's pool service, and every
// site has to keep working with the region that is left. Each puts the
// region back when it is done.
test.describe.configure({ mode: "serial" })

test.describe("with a region down", () => {
  test("the sites show what the other region says and name the one that is down", async ({ page, pool }) => {
    await pool.euWest.stopService()
    try {
      // The public site.
      await page.goto(sites.marketing)
      const facts = page.locator("op-pool-facts")
      await expect(facts.getByRole("row", { name: /US East/ })).toContainText("15%")
      await expect(facts.getByRole("row", { name: /EU West/ })).toHaveCount(0)
      await expect(facts.getByRole("status")).toContainText("Not every region answered")
      await expect(facts.getByRole("status")).toContainText("EU West: The eu-west region did not answer")
      await expect(page.getByRole("list", { name: "The pool's regions" }).getByRole("listitem")).toHaveText([/US East\s+up/, /EU West\s+not answering/])

      await page.goto(`${sites.marketing}/leaderboard/`)
      await page.getByRole("radio", { name: "since the pool began" }).check()
      const standing = page.getByRole("table", { name: "Members by fees earned" }).getByRole("row", { name: /Alice/ })
      await expect(standing).toContainText("0.011 ETH")
      await expect(standing).toContainText("US East")
      await expect(standing).not.toContainText("EU West")

      await page.goto(`${sites.marketing}/apps/`)
      await expect(regionSection(page, "US East").getByRole("row", { name: /vllm\/qwen2.5-7b-instruct/ })).toContainText("$0.50 per hour")
      await expect(regionSection(page, "EU West")).toHaveCount(0)

      // The member site: signing in still works, and each page shows the region that is up.
      await signInAs(page, alice)
      await expect(regionSection(page, "US East")).toContainText("Member as Alice")
      await expect(page.getByRole("status").filter({ hasText: "Not every region answered" })).toContainText("EU West")
      for (const path of ["machines", "runners", "earnings", "payouts"]) {
        await page.goto(`${sites.member}/${path}`)
        await expect(regionSection(page, "US East")).toBeVisible()
        await expect(regionSection(page, "EU West")).toHaveCount(0)
        await expect(page.getByRole("status").filter({ hasText: "Not every region answered" })).toBeVisible()
      }
      await page.goto(`${sites.member}/apply`)
      await expect(page.getByLabel("Region")).toHaveValue("us-east")
      await expect(page.getByLabel("Region").getByRole("option")).toHaveText(["US East"])
    } finally {
      await pool.euWest.startService()
    }
  })

  test("the operator still works in the region that is up, and is told where something was not done", async ({ page, pool }) => {
    await pool.euWest.stopService()
    try {
      await signInAsOperator(page, pool)
      await expect(regionSection(page, "US East")).toContainText("reachable")
      await expect(page.getByRole("status").filter({ hasText: "Not every region answered" })).toContainText("EU West")
      for (const path of ["applications", "members", "machines", "apps", "runners", "payouts", "terms", "settings", "audit"]) {
        await page.goto(`${sites.admin}/${path}`)
        await expect(regionSection(page, "US East")).toBeVisible()
        await expect(page.getByRole("status").filter({ hasText: "Not every region answered" })).toBeVisible()
      }

      // Adding a GPU model to every region is done where it can be, and says where it could not.
      await page.goto(`${sites.admin}/gpus`)
      await page.getByLabel("Model name").fill("NVIDIA RTX A6000")
      await page.getByLabel("Memory in MB").fill("49140")
      await page.getByRole("button", { name: "Add model" }).click()
      await expect(page.getByRole("status").filter({ hasText: "Added" })).toHaveText("Added in US East.")
      await expect(page.getByRole("alert")).toContainText("Not done in every region")
      await expect(page.getByRole("alert")).toContainText("EU West")
      await expect(regionSection(page, "US East").getByRole("row", { name: /RTX A6000/ })).toContainText("48 GB")
    } finally {
      await pool.euWest.startService()
    }
  })

  test("signing in goes through the second region when the first is down", async ({ page, pool }) => {
    await pool.usEast.stopService()
    try {
      await signInAs(page, alice)
      await expect(regionSection(page, "EU West")).toContainText("Member as Alice")
      await expect(page.getByRole("status").filter({ hasText: "Not every region answered" })).toContainText("US East")
    } finally {
      await pool.usEast.startService()
    }
  })

  test("everything is back once the regions are", async ({ page, pool }) => {
    void pool
    await signInAs(page, alice)
    await expect(regionSection(page, "US East")).toContainText("Member as Alice")
    await expect(regionSection(page, "EU West")).toContainText("Member as Alice")
    await expect(page.getByRole("status").filter({ hasText: "Not every region answered" })).toHaveCount(0)
  })
})
