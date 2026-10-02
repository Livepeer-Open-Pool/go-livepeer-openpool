import { keccak_256 } from "@noble/hashes/sha3"
import { bytesToHex, utf8ToBytes } from "@noble/hashes/utils"
import { Context, Effect, Layer, Option, Predicate, Schema } from "effect"
import { WalletDeclined, WalletMissing } from "../domain/errors.js"
import { Address } from "../domain/ids.js"

/**
 * The person's Ethereum wallet. It is asked for two things only: which
 * address it holds, and to sign a text. It is never asked to send anything.
 */
export class Wallet extends Context.Tag("openpool/Wallet")<
  Wallet,
  {
    /** Asks the wallet for the address to use, which may prompt the person. */
    readonly connect: Effect.Effect<Address, WalletMissing | WalletDeclined>
    /** Asks the wallet to sign message with address, returning the signature. */
    readonly sign: (address: Address, message: string) => Effect.Effect<string, WalletMissing | WalletDeclined>
  }
>() {}

/** The wallet a browser extension injects (EIP-1193). */
export interface InjectedProvider {
  readonly request: (call: { readonly method: string; readonly params?: ReadonlyArray<unknown> }) => Promise<unknown>
}

/**
 * Writes an address with its EIP-55 checksum, the mixed case wallets expect
 * to see in a sign-in message.
 */
export const checksumAddress = (address: string): string => {
  const lower = address.toLowerCase().replace(/^0x/, "")
  const hash = bytesToHex(keccak_256(utf8ToBytes(lower)))
  const cased = Array.from(lower, (char, i) => (Number.parseInt(hash.charAt(i), 16) >= 8 ? char.toUpperCase() : char))
  return `0x${cased.join("")}`
}

const decodeAddress = Schema.decodeUnknownOption(Address)

const declined = (cause: unknown): WalletDeclined =>
  new WalletDeclined({
    reason:
      Predicate.hasProperty(cause, "message") && Predicate.isString(cause.message) ? cause.message : "the request was not completed",
  })

/** The wallet behind an injected provider, if the browser has one. */
export const WalletInjected = (provider: Option.Option<InjectedProvider>) =>
  Layer.succeed(Wallet, {
    connect: Effect.gen(function* () {
      const wallet = yield* Option.match(provider, {
        onNone: () => Effect.fail(new WalletMissing()),
        onSome: Effect.succeed,
      })
      const accounts = yield* Effect.tryPromise({
        try: () => wallet.request({ method: "eth_requestAccounts" }),
        catch: declined,
      })
      const first = Array.isArray(accounts) && Predicate.isString(accounts[0]) ? Option.some(checksumAddress(accounts[0])) : Option.none()
      return yield* Option.match(Option.flatMap(first, decodeAddress), {
        onNone: () => Effect.fail(new WalletDeclined({ reason: "the wallet shared no account" })),
        onSome: Effect.succeed,
      })
    }).pipe(Effect.withSpan("Wallet.connect")),

    sign: (address: Address, message: string) =>
      Effect.gen(function* () {
        const wallet = yield* Option.match(provider, {
          onNone: () => Effect.fail(new WalletMissing()),
          onSome: Effect.succeed,
        })
        const signature = yield* Effect.tryPromise({
          try: () => wallet.request({ method: "personal_sign", params: [`0x${bytesToHex(utf8ToBytes(message))}`, address] }),
          catch: declined,
        })
        if (!Predicate.isString(signature)) {
          return yield* new WalletDeclined({ reason: "the wallet returned no signature" })
        }
        return signature
      }).pipe(Effect.withSpan("Wallet.sign")),
  })
