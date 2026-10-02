import { Region } from "./region.js"
import { seedRegion } from "./seed.js"

export const sites = {
  marketing: "http://localhost:5173",
  member: "http://localhost:5174",
  admin: "http://localhost:5175",
}

/** A pool of two regions, running for real and filled with a little history. */
export interface Pool {
  readonly usEast: Region
  readonly euWest: Region
  stop(): Promise<void>
}

export const startPool = async (): Promise<Pool> => {
  const origins = Object.values(sites)
  const signInHost = new URL(sites.member).host
  const usEast = new Region({ id: "us-east", name: "US East", port: 8091, origins, signInHost })
  const euWest = new Region({ id: "eu-west", name: "EU West", port: 8092, origins, signInHost })
  await Promise.all([usEast.start(), euWest.start()])
  await seedRegion(usEast, signInHost, { regionalTerms: false, aliceFeeWei: "4000000000000000" })
  await seedRegion(euWest, signInHost, { regionalTerms: true, aliceFeeWei: "1000000000000000" })
  return {
    usEast,
    euWest,
    stop: async () => {
      await Promise.all([usEast.stop(), euWest.stop()])
    },
  }
}
