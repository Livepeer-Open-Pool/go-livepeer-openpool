import { test as base, expect, type Page } from "@playwright/test"
import type { PrivateKeyAccount } from "viem/accounts"
import { type Pool, sites, startPool } from "../harness/pool.js"
import { ADMIN_EMAIL } from "../harness/region.js"

export { expect, sites, ADMIN_EMAIL }

/**
 * Gives a page a wallet that holds `account`: the same interface a browser
 * extension injects, answering with real signatures made by the test.
 */
export const useWallet = async (page: Page, account: PrivateKeyAccount): Promise<void> => {
  await page.exposeFunction("openpoolTestSign", (message: string) => account.signMessage({ message }))
  await page.addInitScript((address) => {
    const fromHex = (hex: string): string =>
      new TextDecoder().decode(Uint8Array.from(hex.slice(2).match(/../g) ?? [], (byte) => Number.parseInt(byte, 16)))
    Object.defineProperty(window, "ethereum", {
      value: {
        request: ({ method, params }: { method: string; params?: ReadonlyArray<string> }) => {
          if (method === "eth_requestAccounts") return Promise.resolve([address.toLowerCase()])
          if (method === "personal_sign") {
            return (window as unknown as { openpoolTestSign: (message: string) => Promise<string> }).openpoolTestSign(fromHex(params?.[0] ?? "0x"))
          }
          return Promise.reject(new Error(`this test wallet does not do ${method}`))
        },
      },
    })
  }, account.address)
}

/** Opens the member site signed in as `account`. */
export const signInAs = async (page: Page, account: PrivateKeyAccount): Promise<void> => {
  await useWallet(page, account)
  await page.goto(sites.member)
  await page.getByRole("button", { name: "Sign in with your wallet" }).click()
  await expect(page.getByText("Signed in as")).toBeVisible()
}

/** Opens the admin site signed in as the operator, with the code read from the region's log. */
export const signInAsOperator = async (page: Page, pool: Pool): Promise<void> => {
  await page.goto(sites.admin)
  await page.getByLabel("Email").fill(ADMIN_EMAIL)
  await page.getByRole("button", { name: "Email me a code" }).click()
  await expect(page.getByLabel("Code")).toBeVisible()
  const region = [pool.usEast, pool.euWest].find((r) => page.url() !== "" && r.log.includes("sign-in code written")) ?? pool.usEast
  await expect.poll(() => { try { return region.latestCode() } catch { return "" } }).toMatch(/^\d{6}$/)
  await page.getByLabel("Code").fill(region.latestCode())
  await page.getByRole("button", { name: "Sign in" }).click()
  await expect(page.getByText(`Signed in as ${ADMIN_EMAIL}`)).toBeVisible()
}

/** A section of a page for one region, found by its heading. */
export const regionSection = (page: Page, name: string) => page.locator("section").filter({ has: page.getByRole("heading", { level: 2, name, exact: true }) })

export const test = base.extend<object, { pool: Pool }>({
  pool: [
    // eslint-disable-next-line no-empty-pattern
    async ({}, use) => {
      const pool = await startPool()
      await use(pool)
      await pool.stop()
    },
    { scope: "worker", timeout: 180_000 },
  ],
})
