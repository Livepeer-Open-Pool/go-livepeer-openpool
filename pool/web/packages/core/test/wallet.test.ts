import { describe, expect, it } from "@effect/vitest"
import { DateTime, Effect, Option } from "effect"
import { checksumAddress, signInMessage, Wallet, WalletInjected } from "../src/index.js"
import { address } from "./kit.js"

describe("checksumAddress", () => {
  it("matches the EIP-55 test vectors", () => {
    for (const vector of [
      "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
      "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
      "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
      "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
      "0x52908400098527886E0F7030069857D2E4169EE7",
      "0xde709f2102306220921060314715629080e2fb77",
    ]) {
      expect(checksumAddress(vector.toLowerCase())).toBe(vector)
      expect(checksumAddress(vector.toUpperCase().replace("0X", "0x"))).toBe(vector)
    }
  })
})

describe("signInMessage", () => {
  it("is the text the pool service parses", () => {
    const message = signInMessage({
      host: "app.open-pool.com",
      origin: "https://app.open-pool.com",
      address,
      chainId: 42161,
      nonce: "0123456789abcdef0123456789abcdef",
      issuedAt: DateTime.unsafeMake("2026-10-01T12:00:00Z"),
    })
    expect(message).toBe(`app.open-pool.com wants you to sign in with your Ethereum account:
0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed

Sign in to Open Pool.

URI: https://app.open-pool.com
Version: 1
Chain ID: 42161
Nonce: 0123456789abcdef0123456789abcdef
Issued At: 2026-10-01T12:00:00.000Z`)
  })
})

describe("the browser's wallet", () => {
  const provider = (answers: Record<string, unknown>, asked: Array<unknown> = []) =>
    Option.some({
      request: (call: { readonly method: string; readonly params?: ReadonlyArray<unknown> }) => {
        asked.push(call)
        const answer = answers[call.method]
        return answer instanceof Error ? Promise.reject(answer) : Promise.resolve(answer)
      },
    })

  it.effect("names its address with its checksum", () =>
    Effect.gen(function* () {
      const wallet = yield* Wallet
      expect(yield* wallet.connect).toBe("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")
    }).pipe(Effect.provide(WalletInjected(provider({ eth_requestAccounts: ["0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"] })))),
  )

  it.effect("signs the text as bytes, with the address", () => {
    const asked: Array<unknown> = []
    return Effect.gen(function* () {
      const wallet = yield* Wallet
      expect(yield* wallet.sign(address, "hi\n")).toBe("0xsig")
      expect(asked).toEqual([{ method: "personal_sign", params: ["0x68690a", address] }])
    }).pipe(Effect.provide(WalletInjected(provider({ personal_sign: "0xsig" }, asked))))
  })

  it.effect("says so when the person declines", () =>
    Effect.gen(function* () {
      const wallet = yield* Wallet
      const error = yield* Effect.flip(wallet.connect)
      expect(error._tag).toBe("WalletDeclined")
      expect(error._tag === "WalletDeclined" && error.reason).toBe("User rejected the request.")
    }).pipe(Effect.provide(WalletInjected(provider({ eth_requestAccounts: new Error("User rejected the request.") })))),
  )

  it.effect("says so when the browser has no wallet", () =>
    Effect.gen(function* () {
      const wallet = yield* Wallet
      expect((yield* Effect.flip(wallet.connect))._tag).toBe("WalletMissing")
      expect((yield* Effect.flip(wallet.sign(address, "hi")))._tag).toBe("WalletMissing")
    }).pipe(Effect.provide(WalletInjected(Option.none()))),
  )

  it.effect("does not take a wallet that shares no account", () =>
    Effect.gen(function* () {
      const wallet = yield* Wallet
      expect((yield* Effect.flip(wallet.connect))._tag).toBe("WalletDeclined")
    }).pipe(Effect.provide(WalletInjected(provider({ eth_requestAccounts: [] })))),
  )
})
