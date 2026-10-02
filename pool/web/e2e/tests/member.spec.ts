import { alice, bob, carol } from "../harness/seed.js"
import { expect, regionSection, signInAs, sites, test } from "./fixtures.js"

test.describe("the member site", () => {
  test("asks a visitor to sign in, and says so when the browser has no wallet", async ({ page, pool }) => {
    void pool
    await page.goto(sites.member)
    await expect(page.getByRole("heading", { level: 1, name: "Sign in with your wallet" })).toBeVisible()
    await expect(page.getByRole("navigation", { name: "Primary" })).toHaveCount(0)
    await page.getByRole("button", { name: "Sign in with your wallet" }).click()
    await expect(page.getByRole("alert")).toContainText("No Ethereum wallet was found in this browser")
  })

  test("signs a member in and shows where they stand in each region", async ({ page, pool }) => {
    void pool
    await signInAs(page, alice)
    await expect(page).toHaveTitle("Overview · Open Pool members")
    await expect(page.getByRole("heading", { level: 1, name: "Your overview" })).toBeVisible()
    for (const region of ["US East", "EU West"]) {
      const section = regionSection(page, region)
      await expect(section).toContainText("Member as Alice")
      await expect(section).toContainText("1 machine")
      await expect(section).toContainText("1 of 1 approved")
    }
    // The session survives a reload, and signing out ends it.
    await page.reload()
    await expect(page.getByText("Signed in as")).toBeVisible()
    await page.getByRole("button", { name: "Sign out" }).click()
    await expect(page.getByRole("heading", { level: 1, name: "Sign in with your wallet" })).toBeVisible()
  })

  test("shows a member's machines, what they run and what the agent reports", async ({ page, pool }) => {
    void pool
    await signInAs(page, alice)
    await page.getByRole("link", { name: "Machines", exact: true }).click()
    await expect(page).toHaveURL(`${sites.member}/machines`)
    await expect(page).toHaveTitle("Machines · Open Pool members")
    const machine = regionSection(page, "US East").getByRole("article")
    await expect(machine.getByRole("heading", { name: "garage-4090" })).toBeVisible()
    await expect(machine).toContainText("2 × NVIDIA GeForce RTX 4090")
    await expect(machine.getByRole("row", { name: /vllm\/qwen2.5-7b-instruct/ })).toContainText("1.0.0")
    await expect(machine).toContainText("version 0.1.0")
    await expect(machine).toContainText("NVIDIA GeForce RTX 4090 (24 GB)")
    await expect(machine).toContainText("app-seeded")
  })

  test("replaces a machine's credential and shows the install command once", async ({ page, pool }) => {
    await signInAs(page, alice)
    await page.goto(`${sites.member}/machines`)
    const machine = regionSection(page, "EU West").getByRole("article")
    await machine.getByRole("button", { name: "Replace credential" }).click()
    const dialog = page.getByRole("dialog")
    await expect(dialog.getByRole("heading", { name: "Replace the credential of garage-4090?" })).toBeVisible()
    await dialog.getByRole("button", { name: "Replace credential" }).click()
    const fresh = page.getByRole("region", { name: "The credential for garage-4090" })
    await expect(fresh).toContainText("This is shown once.")
    const command = fresh.locator("op-copy").first()
    await expect(command).toContainText("docker run -d --name openpool-agent-eu-west")
    await expect(command).toContainText(`--pool ${pool.euWest.api}`)
    await expect(command).toContainText("OPENPOOL_CREDENTIAL=opc_")
    await fresh.getByRole("button", { name: "I have saved it" }).click()
    await expect(fresh).toHaveCount(0)
    // The old credential is dead: a reload shows no secret anywhere.
    await page.reload()
    await expect(page.getByText("OPENPOOL_CREDENTIAL=opc_")).toHaveCount(0)
  })

  test("shows runners with their state and the tests the pool ran", async ({ page, pool }) => {
    void pool
    await signInAs(page, alice)
    await page.goto(`${sites.member}/runners`)
    const section = regionSection(page, "US East")
    const runner = section.getByRole("table", { name: "Connected runners" }).getByRole("row", { name: /garage-4090/ })
    await expect(runner).toContainText("vllm/qwen2.5-7b-instruct")
    await expect(runner).toContainText("active")
    await expect(runner).toContainText("passed")
    await expect(section.getByRole("table", { name: "Recent tests" }).getByRole("row").nth(1)).toContainText("passed")
  })

  test("shows earnings by round and what became of each payout window", async ({ page, pool }) => {
    await signInAs(page, alice)
    await page.goto(`${sites.member}/earnings`)
    const first = pool.usEast.orchestrator.round - 7
    const east = regionSection(page, "US East")
    await expect(east).toContainText(`covers rounds ${first + 7} to ${first + 13}`)
    const table = east.getByRole("table", { name: "Fees by round and app" })
    await expect(table.getByRole("row", { name: new RegExp(`^${first} `) })).toContainText("0.004 ETH")
    await expect(table.getByRole("row", { name: /Total/ })).toContainText("0.011 ETH")
    await expect(regionSection(page, "EU West").getByRole("row", { name: /Total/ })).toContainText("0.005 ETH")

    await page.getByRole("link", { name: "Payouts", exact: true }).click()
    const window = regionSection(page, "US East").getByRole("row", { name: new RegExp(`${first}–${first + 6}`) })
    await expect(window).toContainText("0.008 ETH")
    // The pot was 0.05 ETH and the pool keeps 15%: Alice, the only earner, is owed the rest.
    await expect(window).toContainText("0.0425 ETH")
    await expect(window).toContainText("waiting for the operator's approval")
  })

  test("shows an applicant that their application is waiting", async ({ page, pool }) => {
    void pool
    await signInAs(page, bob)
    for (const region of ["US East", "EU West"]) {
      await expect(regionSection(page, region)).toContainText("Your application as Bob")
      await expect(regionSection(page, region)).toContainText("is waiting for the operator")
    }
    await page.goto(`${sites.member}/machines`)
    await expect(regionSection(page, "US East")).toContainText("Your application in US East is waiting for the operator.")
  })

  test("takes an application: the region's terms, a signature, and it is waiting", async ({ page, pool }) => {
    void pool
    await signInAs(page, carol)
    await expect(regionSection(page, "EU West")).toContainText("You have not applied in EU West.")
    await regionSection(page, "EU West").getByRole("link", { name: "Apply to EU West" }).click()
    await expect(page).toHaveURL(`${sites.member}/apply?region=eu-west`)
    await expect(page.getByLabel("Region")).toHaveValue("eu-west")
    // Both the pool's terms and the region's own are shown to be accepted.
    await expect(page.getByText("Open Pool terms, version 1")).toBeVisible()
    await expect(page.getByText("EU West terms, version 1")).toBeVisible()
    await expect(page.getByText("Machines in this region must be located in it.")).toBeVisible()

    await page.getByLabel("Display name").fill("Carol")
    await page.getByLabel("Name for this machine").fill("closet-3090")
    await page.getByRole("spinbutton", { name: /RTX 3090/ }).fill("1")
    await page.getByRole("button", { name: "Add another machine" }).click()
    await page.getByRole("group", { name: "Machine 2" }).getByLabel("Name for this machine").fill("attic-4090")
    await page.getByRole("group", { name: "Machine 2" }).getByRole("spinbutton", { name: /RTX 4090/ }).fill("2")
    // The browser will not send it without the terms accepted.
    await page.getByRole("button", { name: "Sign and send application" }).click()
    await expect(page.getByText("Your application was sent.")).toHaveCount(0)
    await page.getByLabel("I have read these terms").check()
    await page.getByRole("button", { name: "Sign and send application" }).click()
    await expect(page.getByRole("status")).toContainText("Your application was sent.")
    await expect(page.getByRole("status")).toContainText("You applied as Carol with 2 machines.")

    await page.getByRole("link", { name: "Go to your overview" }).click()
    await expect(regionSection(page, "EU West")).toContainText("Your application as Carol")
    await expect(regionSection(page, "US East")).toContainText("You have not applied in US East.")
  })

  test("shows the page for an address that is not one", async ({ page, pool }) => {
    void pool
    await signInAs(page, alice)
    await page.goto(`${sites.member}/nowhere`)
    await expect(page.getByRole("heading", { level: 1, name: "There is no page at this address" })).toBeVisible()
    await expect(page).toHaveTitle("Page not found · Open Pool members")
  })
})
