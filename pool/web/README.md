# Open Pool web sites

The three sites of Open Pool, in one pnpm workspace. Each builds to plain files that any web server or CDN can host; none has a server of its own. They run in the visitor's browser and call each region's pool service directly. The design is in [`../DESIGN.md`](../DESIGN.md#the-web-sites).

| Site | Package | Dev port | What it is |
| --- | --- | --- | --- |
| `open-pool.com` | `apps/marketing` | 5173 | The public site. Its pages are written as HTML at build time; prices, capacity, the leaderboard, the terms and each region's payout terms load live. |
| `app.open-pool.com` | `apps/member` | 5174 | Members sign in with their wallet, apply, connect machines, and follow runners, earnings and payouts. |
| `admin.open-pool.com` | `apps/admin` | 5175 | The operator signs in with an emailed code and runs the pool across every region. |

Two packages are shared:

- `packages/core` is everything that is not a component, written with [Effect](https://effect.website): schemas that decode every answer of the pool service, the endpoints, and the workflows (asking every region at once, signing in, applying). It has no DOM code beyond the browser's `fetch`, storage and wallet, each behind a service.
- `packages/ui` is what the sites' [Lit](https://lit.dev) components share: design tokens and base stylesheet, shared styles, the router, the page base class, and a few components.

## Commands

```bash
pnpm install
pnpm typecheck      # every package
pnpm lint
pnpm test           # unit tests: core in Node, ui in a real browser, the public site's pages rendered in Node
pnpm build          # every site, into apps/*/dist
pnpm e2e            # all three sites against two real pool services
```

`packages/ui` and the end-to-end tests drive Chromium through Playwright; `pnpm exec playwright install chromium` fetches it once.

### Running the sites against a real pool on your machine

```bash
pnpm --filter @openpool/e2e pool        # two regions, filled with a little history; needs Go
pnpm --filter @openpool/marketing dev   # and likewise member, admin
```

The first command builds `../cmd/openpool`, starts two pool services (ports 8091 and 8092), each beside a stand-in for the orchestrator's CLI and service ports, and fills them through the real API: a member whose machine is connected and earning, an applicant, a closed payout window and an open one. It prints the test wallets' keys and the operator's email; sign-in codes appear in its terminal, where the service logs them when no email is configured.

`pnpm e2e` starts the same pool itself, signs in with real signatures from the test keys, and ends by stopping a region and checking that every site still works with the one that is left.

## Deploying a site

1. **Build.** `pnpm --filter @openpool/member build` writes `apps/member/dist`. The public site's pages carry addresses that are fixed at build time:

   ```bash
   OPENPOOL_SITE_URL=https://open-pool.com OPENPOOL_MEMBER_URL=https://app.open-pool.com pnpm --filter @openpool/marketing build
   ```

2. **Say where the regions are.** Each site reads `config.json` from beside its pages when it starts. Edit it in `dist/` (or replace it at deploy time); adding a region needs no rebuild:

   ```json
   {
     "regions": [
       { "id": "us-east", "name": "US East", "api": "https://us-east.api.open-pool.com" },
       { "id": "eu-west", "name": "EU West", "api": "https://eu-west.api.open-pool.com" }
     ],
     "sites": { "marketing": "https://open-pool.com", "member": "https://app.open-pool.com", "admin": "https://admin.open-pool.com" },
     "explorer": "https://arbiscan.io"
   }
   ```

   The order of `regions` is the order the sites list them, and the order they are asked in for what any region can do, such as signing in.

3. **Host the files.** The member and admin sites route pages in the browser, so the host must answer `index.html` for a path it has no file for. Hosts that serve `404.html` for unknown paths do this already: the build writes one that is the same page. The public site needs nothing special: every page is a directory with an `index.html`.

4. **Let the sites in.** Each region's pool service must list the sites' origins in `OPENPOOL_ALLOWED_ORIGINS` and the member site's host in `OPENPOOL_SIGN_IN_DOMAINS`, and be reachable from the internet over HTTPS. Sessions are bearer tokens kept in the browser's storage and sent in the `Authorization` header; there are no cookies.

## How the code is arranged

- **One runtime per site.** `main.ts` (or `entry-client.ts`) builds the site's Effect runtime and hands it to the page through Lit context. `packages/ui/src/runtime.ts` is the one place effects are run: it turns an effect into the promise Lit's `Task` takes and a failure into the typed error itself, so a component can say what happened. `eslint.config.js` forbids `throw`, `try`, `async` functions and the console in the sites' source.
- **Every region, every time.** A page asks all regions at once (`everyRegion`, `everyRegionAs`) and gets each region's answer or the reason there is none. Pages render the answers under each region's name and list the regions that did not answer, so one region being down never blanks a page. What is done in several regions at once (publishing the pool's terms, adding a GPU model) reports each region's outcome.
- **Decode once.** The pool service's JSON is decoded by the schemas in `packages/core/src/domain/model.ts` and nowhere else. Wei is `bigint`, times are `DateTime`, absent values are `Option`, a credential's secret is `Redacted`.
- **The public site is files.** `apps/marketing/src/pages` are server-only Lit templates in light DOM, rendered by `scripts/prerender.ts` (and by the dev server, with the same code). The parts that show live data are custom elements defined only in the browser; in the file they hold a sentence saying what loads there.

## The look, which every site shares

Nothing about the look is written twice.

- **The theme** is `packages/ui/src/global.css`: the colors, typefaces, spacing and type scale as tokens, each color given once as its light and its dark value. The public site imports it in its stylesheet, the members' and operator's sites in `main.ts`; the tokens inherit into every component's shadow root. Components are styled by `packages/ui/src/styles.ts` and the page frame by `shell.ts`, both of which read only tokens.
- **Light and dark.** A site follows the system's theme until the visitor picks one with the switch in the banner (`op-theme`). The choice is kept by `packages/ui/brand/theme.js`, which is written into every page's head and runs before the first paint.
- **The logo** is `packages/ui/brand/mark.svg`. `pnpm --filter @openpool/ui brand` draws from it the icons a browser or phone asks for and the mark's shape as a CSS mask; what it writes is committed. A page paints the mark by filling that shape with the theme's gradient (`lockup.css`), so the one shape serves both themes.
- **Icons and manifest.** Each site's Vite config adds `brand()` from `@openpool/ui/brand`, which serves the favicon, the touch and home-screen icons and `site.webmanifest` from the site's root and writes them into its build. The same module lists the head tags that name them; the members' and operator's sites get them injected, and the public site's `document.ts` writes them itself.

## Where this departs from the skills it was built with

- **Effect 3.22, not 4.** Effect 4.0 was released while this was written; the Effect skill is verified against 3.x and says to stay there until it is re-verified.
- **Vitest 3, not 4.** `@effect/vitest` for Effect 3 requires Vitest 3. Its browser mode is what `packages/ui` uses.
- **No render server.** The Lit skill's stack includes a Hono server for pages rendered per request. No page here is: the public site is prerendered and the other two are client-rendered, which is what lets all three be plain files.
- **Hints inside labels.** A form field's hint is inside its `<label>`, so it is read as part of the field's name instead of as its description. It keeps the forms simple; moving hints to `aria-describedby` is a refinement.
