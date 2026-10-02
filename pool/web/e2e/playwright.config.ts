import { defineConfig, devices } from "@playwright/test"

// End-to-end tests of the three sites against a pool of two real regions.
// The regions are started by the tests themselves (see tests/fixtures.ts);
// the sites are served by their own dev servers, started here.
const site = (name: string, port: number) => ({
  command: `pnpm --filter @openpool/${name} dev`,
  url: `http://localhost:${port}`,
  reuseExistingServer: true,
  timeout: 120_000,
  // The public site's pages are written with the addresses of the sites they link to.
  env: { OPENPOOL_SITE_URL: "http://localhost:5173", OPENPOOL_MEMBER_URL: "http://localhost:5174" },
})

const ports = { marketing: 5173, member: 5174, admin: 5175 }
// OPENPOOL_SITES=member starts only that site's dev server, for running one site's tests.
const wanted = (process.env["OPENPOOL_SITES"] ?? "marketing,member,admin").split(",")

export default defineConfig({
  testDir: "tests",
  // One pool is shared by every test, and some tests stop a region: run in order, one at a time.
  workers: 1,
  fullyParallel: false,
  forbidOnly: true,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: [["list"]],
  use: { ...devices["Desktop Chrome"], trace: "retain-on-failure" },
  webServer: Object.entries(ports)
    .filter(([name]) => wanted.includes(name))
    .map(([name, port]) => site(name, port)),
})
