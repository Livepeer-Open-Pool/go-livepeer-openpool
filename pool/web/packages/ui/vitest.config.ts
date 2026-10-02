import { defineConfig } from "vitest/config"

// Components are tested in a real browser: shadow DOM, custom element
// upgrades and constructable stylesheets do not exist anywhere else.
export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    browser: {
      enabled: true,
      provider: "playwright",
      headless: true,
      screenshotFailures: false,
      instances: [{ browser: "chromium" }],
    },
  },
})
