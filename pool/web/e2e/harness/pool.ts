import { Region } from "./region.js"
import { adminSession, seedRegion, type StoredSession } from "./seed.js"

export const sites = {
  marketing: "http://localhost:5173",
  member: "http://localhost:5174",
  admin: "http://localhost:5175",
}

/** A pool of two regions, running for real and filled with a little history. */
export interface Pool {
  readonly usEast: Region
  readonly euWest: Region
  /** The operator's session, accepted by both regions. */
  readonly operator: StoredSession
  stop(): Promise<void>
}

export const startPool = async (): Promise<Pool> => {
  const origins = Object.values(sites)
  const signInHost = new URL(sites.member).host
  const usEast = new Region({ id: "us-east", name: "US East", port: 8091, origins, signInHost })
  const euWest = new Region({ id: "eu-west", name: "EU West", port: 8092, origins, signInHost })
  await Promise.all([usEast.start(), euWest.start()])
  // The code comes from the second region, which leaves the first, the one
  // the admin site asks, free to send a code to a person or a test.
  const operator = await adminSession(euWest)
  await seedRegion(usEast, signInHost, operator.token, { regionalTerms: false, aliceFeeWei: "4000000000000000" })
  await seedRegion(euWest, signInHost, operator.token, { regionalTerms: true, aliceFeeWei: "1000000000000000" })
  return {
    usEast,
    euWest,
    operator,
    stop: async () => {
      await Promise.all([usEast.stop(), euWest.stop()])
    },
  }
}
