// Platform APIs the sites use that this TypeScript version's DOM library
// does not describe yet. Only what is used is declared.

export {}

declare global {
interface URLPatternComponentResult {
  readonly groups: Readonly<Record<string, string | undefined>>
}

interface URLPatternResult {
  readonly pathname: URLPatternComponentResult
}

class URLPattern {
  constructor(init: { readonly pathname: string })
  exec(input: URL | string): URLPatternResult | null
  test(input: URL | string): boolean
}
}
