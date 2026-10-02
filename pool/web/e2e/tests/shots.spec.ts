import { alice } from "../harness/seed.js"
import { signInAs, sites, test } from "./fixtures.js"

// Not a test of behavior: it saves pictures of the pages for a person to look at.
// Run with OPENPOOL_SHOTS=<dir>.
const dir = process.env["OPENPOOL_SHOTS"]

test.describe("pictures of the member site", () => {
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
})
