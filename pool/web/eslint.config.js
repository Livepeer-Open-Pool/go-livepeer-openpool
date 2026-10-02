import js from "@eslint/js"
import tseslint from "typescript-eslint"

// The rules the sites' code is written by, made mechanical. Application code
// is Effect: failures are values, not exceptions; nothing is async; nothing
// writes to the console. The few places that must leave Effect (the bridge
// to Lit's Task, the router's dynamic imports, the clipboard) use promise
// callbacks, never async functions, so they stay easy to find.
const effectOnly = [
  { selector: "ThrowStatement", message: "Fail with a tagged error in the Effect error channel instead of throwing." },
  { selector: "TryStatement", message: "Use Effect.try or Effect.tryPromise at the edge instead of try/catch." },
  {
    selector: "FunctionDeclaration[async=true], FunctionExpression[async=true], ArrowFunctionExpression[async=true]",
    message: "Use Effect.gen instead of an async function.",
  },
  { selector: "CallExpression[callee.object.name='console']", message: "Use Effect.log instead of the console." },
]

export default tseslint.config(
  { ignores: ["**/dist/**", "**/node_modules/**", "**/.vite/**", "**/test-results/**", "**/playwright-report/**"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["packages/*/src/**/*.ts", "apps/*/src/**/*.ts"],
    rules: {
      "no-restricted-syntax": ["error", ...effectOnly],
      "no-restricted-imports": ["error", { paths: [{ name: "lodash", message: "Use Effect's data modules." }] }],
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/no-non-null-assertion": "error",
    },
  },
  {
    // The domain is pure: it imports Effect and itself, nothing else.
    files: ["packages/core/src/domain/**/*.ts"],
    rules: {
      "no-restricted-imports": ["error", { patterns: [{ group: ["@effect/*", "@noble/*", "lit", "lit/*", "../services/*", "../workflows/*"], message: "The domain imports only effect and itself." }] }],
    },
  },
  {
    files: ["**/test/**/*.ts", "e2e/**/*.ts", "**/scripts/**/*.ts", "**/*.config.ts"],
    rules: { "@typescript-eslint/no-non-null-assertion": "off" },
  },
)
