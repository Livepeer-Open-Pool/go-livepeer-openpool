import { defineConfig } from "vitest/config"

// The pages are rendered to HTML in Node, at build time; so are their tests.
export default defineConfig({
  test: { include: ["test/**/*.test.ts"], environment: "node" },
})
