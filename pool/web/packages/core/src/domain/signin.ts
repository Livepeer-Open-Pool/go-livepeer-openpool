import { DateTime } from "effect"
import type { Address } from "./ids.js"

/** What goes into a wallet sign-in message. */
export interface SignInRequest {
  /** The site's host, e.g. app.open-pool.com. */
  readonly host: string
  /** The site's origin, e.g. https://app.open-pool.com. */
  readonly origin: string
  readonly address: Address
  readonly chainId: number
  readonly nonce: string
  readonly issuedAt: DateTime.Utc
}

/**
 * The EIP-4361 ("Sign-In with Ethereum") message a wallet signs to sign in.
 * The pool service parses this text, so its shape is a contract with it.
 */
export const signInMessage = (request: SignInRequest): string =>
  [
    `${request.host} wants you to sign in with your Ethereum account:`,
    request.address,
    "",
    "Sign in to Open Pool.",
    "",
    `URI: ${request.origin}`,
    "Version: 1",
    `Chain ID: ${request.chainId}`,
    `Nonce: ${request.nonce}`,
    `Issued At: ${DateTime.formatIso(request.issuedAt)}`,
  ].join("\n")
