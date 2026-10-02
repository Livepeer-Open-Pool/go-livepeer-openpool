import { Array, BigInt as Big, pipe, Schema } from "effect"

/**
 * An amount of wei. On the wire it is a decimal string, because the amounts
 * do not fit a JSON number; in the domain it is a bigint and never a float.
 */
export const Wei = Schema.BigInt.pipe(Schema.nonNegativeBigInt(), Schema.brand("Wei"))
export type Wei = typeof Wei.Type

const make = Schema.decodeSync(Wei)

export const zeroWei: Wei = make("0")

const WEI_PER_ETH = 10n ** 18n

/** The sum of amounts. */
export const sumWei = (amounts: ReadonlyArray<Wei>): Wei =>
  // A sum of non-negative amounts is non-negative, so the brand holds.
  Wei.make(pipe(amounts, Array.reduce(0n, Big.sum)))

/**
 * Renders wei as ETH with up to `digits` decimals, rounding down and
 * dropping trailing zeros: 1500000000000000000 is "1.5". An amount too small
 * to show at that precision is shown as less than the smallest step, never
 * as zero, because "0" would say nothing was earned.
 */
export const formatEth = (wei: Wei, digits = 6): string => {
  const whole = wei / WEI_PER_ETH
  const scale = 10n ** globalThis.BigInt(18 - digits)
  const fraction = (wei % WEI_PER_ETH) / scale
  if (wei > 0n && whole === 0n && fraction === 0n) {
    return `<0.${"0".repeat(digits - 1)}1`
  }
  const decimals = fraction.toString().padStart(digits, "0").replace(/0+$/, "")
  return decimals === "" ? whole.toString() : `${whole}.${decimals}`
}

/** A commission in basis points, 0 to 10000. */
export const BasisPoints = Schema.Int.pipe(Schema.between(0, 10_000), Schema.brand("BasisPoints"))
export type BasisPoints = typeof BasisPoints.Type

/** 1500 basis points is "15%"; 1250 is "12.5%". */
export const formatBasisPoints = (bps: BasisPoints): string => {
  const whole = Math.trunc(bps / 100)
  const hundredths = bps % 100
  if (hundredths === 0) return `${whole}%`
  return `${whole}.${hundredths.toString().padStart(2, "0").replace(/0$/, "")}%`
}
