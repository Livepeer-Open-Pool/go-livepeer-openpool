import { alice } from "../harness/seed.js"
import { signInAs, signInAsOperator, sites, test } from "./fixtures.js"

// Not a test of behavior: it saves pictures of the pages for a person to look at.
// Run with OPENPOOL_SHOTS=<dir>.
const dir = process.env["OPENPOOL_SHOTS"]

test.describe("pictures of the sites", () => {
  test.skip(!dir, "set OPENPOOL_SHOTS to a directory to save pictures")

  test("member pages", async ({ page, pool }) => {
    void pool
    await page.setViewportSize({ width: 1280, height: 900 })
    await page.goto(sites.member)
    await page.waitForTimeout(500)
    await page.screenshot({ path: `${dir}/member-signin.png`, fullPage: true })
    await signInAs(page, alice)
    await page.waitForTimeout(800)
    await page.screenshot({ path: `${dir}/member-overview.png`, fullPage: true })
    for (const path of ["machines", "runners", "earnings", "payouts", "apply"]) {
      await page.goto(`${sites.member}/${path}`)
      await page.waitForTimeout(1000)
      await page.screenshot({ path: `${dir}/member-${path}.png`, fullPage: true })
    }
    await page.emulateMedia({ colorScheme: "dark" })
    await page.setViewportSize({ width: 390, height: 844 })
    await page.goto(`${sites.member}/machines`)
    await page.waitForTimeout(1000)
    await page.screenshot({ path: `${dir}/member-machines-dark-phone.png`, fullPage: true })
  })

  test("admin pages", async ({ page, pool }) => {
    await page.setViewportSize({ width: 1280, height: 900 })
    await signInAsOperator(page, pool)
    await page.waitForTimeout(800)
    await page.screenshot({ path: `${dir}/admin-overview.png`, fullPage: true })
    for (const path of ["applications", "machines", "apps", "runners", "payouts", "settings"]) {
      await page.goto(`${sites.admin}/${path}`)
      await page.waitForTimeout(1000)
      await page.screenshot({ path: `${dir}/admin-${path}.png`, fullPage: true })
    }
    await page.goto(`${sites.admin}/apps`)
    await page.getByRole("link", { name: "vllm/qwen2.5-7b-instruct" }).first().click()
    await page.waitForTimeout(1000)
    await page.screenshot({ path: `${dir}/admin-app.png`, fullPage: true })
  })

  test("public pages", async ({ page, pool }) => {
    void pool
    await page.setViewportSize({ width: 1280, height: 900 })
    for (const [name, path] of [["home", "/"], ["how", "/how-it-works/"], ["connect", "/connect/"], ["apps", "/apps/"], ["leaderboard", "/leaderboard/"], ["terms", "/terms/"], ["join", "/join/"]] as const) {
      await page.goto(`${sites.marketing}${path}`)
      await page.waitForTimeout(1200)
      await page.screenshot({ path: `${dir}/public-${name}.png`, fullPage: true })
    }
    await page.emulateMedia({ colorScheme: "dark" })
    await page.setViewportSize({ width: 390, height: 844 })
    await page.goto(sites.marketing)
    await page.waitForTimeout(1200)
    await page.screenshot({ path: `${dir}/public-home-dark-phone.png`, fullPage: true })
  })
})
