import { Effect, Option, Schema } from "effect"
import * as Api from "../api.js"
import type { ApiError, NoSession, UnknownRegion, WalletDeclined, WalletMissing } from "../domain/errors.js"
import { InvalidInput } from "../domain/errors.js"
import { Address } from "../domain/ids.js"
import type { Application, ApplicationDraft, Me } from "../domain/model.js"
import type { RegionConfig, RegionResult } from "../domain/regions.js"
import type { PoolApi } from "../services/PoolApi.js"
import type { SessionStore } from "../services/SessionStore.js"
import type { SiteSettings } from "../services/SiteSettings.js"
import { Wallet } from "../services/Wallet.js"
import { everyRegionAs, inRegionAs, requireSession } from "./session.js"

/** Who the signed-in address is in a region, and what a member has there. */
export interface Membership<A> {
  readonly me: Me
  /** Present when the address is a member of the region. */
  readonly detail: Option.Option<A>
}

/**
 * Asks every region who the signed-in address is there, and, where it is a
 * member, for more. A region where the address only applied, or is unknown,
 * answers with who it is and nothing more: the member endpoints are closed
 * to it there.
 */
export const membershipEverywhere = <A>(
  more: Api.AskAs<A>,
): Effect.Effect<ReadonlyArray<RegionResult<Membership<A>>>, NoSession, PoolApi | SiteSettings | SessionStore> =>
  everyRegionAs("wallet", (region: RegionConfig, session) =>
    Effect.gen(function* () {
      const me = yield* Api.me(region, session.token)
      if (Option.isNone(me.member)) {
        return { me, detail: Option.none<A>() }
      }
      return { me, detail: Option.some(yield* more(region, session.token)) }
    }),
  ).pipe(Effect.withSpan("membershipEverywhere"))

/** Who the signed-in address is in every region. */
export const whoAmIEverywhere: Effect.Effect<ReadonlyArray<RegionResult<Me>>, NoSession, PoolApi | SiteSettings | SessionStore> =
  everyRegionAs("wallet", (region, session) => Api.me(region, session.token))

const decodeAddress = Schema.decodeUnknownOption(Address)

/**
 * Applies to a region: the region writes the message that accepts its terms
 * for this application, the wallet signs it, and the region takes both.
 */
export const applyToRegion = (
  region: string,
  draft: ApplicationDraft,
): Effect.Effect<
  Application,
  Exclude<ApiError, { _tag: "NotSignedIn" }> | NoSession | UnknownRegion | WalletMissing | WalletDeclined | InvalidInput,
  PoolApi | SiteSettings | SessionStore | Wallet
> =>
  Effect.gen(function* () {
    const session = yield* requireSession("wallet")
    const address = yield* Option.match(decodeAddress(session.subject), {
      onNone: () => Effect.fail(new InvalidInput({ message: "This session is not a wallet's. Sign in again with your wallet." })),
      onSome: Effect.succeed,
    })
    const wallet = yield* Wallet
    const { message } = yield* inRegionAs("wallet", region, Api.applicationMessage(draft))
    const signature = yield* wallet.sign(address, message)
    return yield* inRegionAs("wallet", region, Api.submitApplication(draft, signature))
  }).pipe(Effect.withSpan("applyToRegion", { attributes: { region } }))
