import { sites, startPool } from "./harness/pool.js"
import { ADMIN_EMAIL } from "./harness/region.js"
import { alice, bob, carol } from "./harness/seed.js"

// Runs a pool of two regions on this machine for clicking through the sites:
//
//   pnpm --filter @openpool/e2e pool
//   pnpm --filter @openpool/member dev     (and the other two sites)
//
// The regions are real pool services with a stand-in orchestrator, filled
// with a member, an applicant, a runner, earnings and a payout batch.

const pool = await startPool()

console.log(`
The pool is up.

  US East  ${pool.usEast.api}
  EU West  ${pool.euWest.api}

Sites, once their dev servers run:

  ${sites.marketing}   the public site
  ${sites.member}   the member site
  ${sites.admin}   the admin site

Operator: sign in as ${ADMIN_EMAIL}. The code is written to this terminal instead of being emailed.

Wallets (well-known test keys; import one into a browser wallet to sign in as it):

  Alice  ${alice.address}  a member in both regions, with a connected machine
  Bob    ${bob.address}  has applied in both regions and is waiting
  Carol  ${carol.address}  has never applied

  Alice's key  0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
  Bob's key    0x59c6995e998f97a5a0044966f0945389dc9d86dae88c7a8412f4603b6b78690d
  Carol's key  0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a

Press Ctrl+C to stop.
`)

// Sign-in codes go to the services' logs; repeat them here as they appear.
const shown = new Map([pool.usEast, pool.euWest].map((region) => [region.id, region.codes().length]))
const watch = setInterval(() => {
  for (const region of [pool.usEast, pool.euWest]) {
    const codes = region.codes()
    for (const code of codes.slice(shown.get(region.id) ?? 0)) {
      console.log(`Sign-in code from ${region.name}: ${code}`)
    }
    shown.set(region.id, codes.length)
  }
}, 500)

const shutdown = async (): Promise<void> => {
  clearInterval(watch)
  await pool.stop()
  process.exit(0)
}
process.on("SIGINT", () => void shutdown())
process.on("SIGTERM", () => void shutdown())
