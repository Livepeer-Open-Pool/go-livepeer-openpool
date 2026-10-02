import { DateTime, Effect, Option } from "effect"
import * as Api from "../api.js"
import { type ApiError, NoSession, RequestRefused, UnknownRegion, type NoRegionReachable, type WalletDeclined, type WalletMissing } from "../domain/errors.js"
import type { RegionId } from "../domain/ids.js"
import type { Role, Session } from "../domain/model.js"
import type { RegionConfig, RegionResult } from "../domain/regions.js"
import { anyNotSignedIn } from "../domain/regions.js"
import { signInMessage } from "../domain/signin.js"
import type { PoolApi } from "../services/PoolApi.js"
import { SessionStore } from "../services/SessionStore.js"
import { SiteSettings } from "../services/SiteSettings.js"
import { Wallet } from "../services/Wallet.js"
import { anyRegion, everyRegion, regionById } from "./regions.js"

/** The session stored in this browser, unless it has run out. */
export const currentSession = (role: Role): Effect.Effect<Option.Option<Session>, never, SessionStore> =>
  Effect.gen(function* () {
    const store = yield* SessionStore
    const stored = yield* store.read(role)
    const now = yield* DateTime.now
    const live = Option.filter(stored, (session) => DateTime.lessThan(now, session.expires_at))
    if (Option.isSome(stored) && Option.isNone(live)) {
      yield* store.clear(role)
    }
    return live
  })

/** The session, or NoSession when the person has to sign in first. */
export const requireSession = (role: Role): Effect.Effect<Session, NoSession, SessionStore> =>
  Effect.flatMap(
    currentSession(role),
    Option.match({ onNone: () => Effect.fail(new NoSession()), onSome: Effect.succeed }),
  )

/** Forgets the session. */
export const signOut = (role: Role): Effect.Effect<void, never, SessionStore> =>
  Effect.flatMap(SessionStore, (store) => store.clear(role))

const remember = (session: Session): Effect.Effect<Session, never, SessionStore> =>
  Effect.flatMap(SessionStore, (store) => Effect.as(store.write(session), session))

/** A sign-in the region did not accept is a refusal with a reason, not "sign in again". */
const refusedSignIn =
  (message: string) =>
  <A, R>(effect: Effect.Effect<A, ApiError, R>): Effect.Effect<A, ApiError, R> =>
    Effect.catchTag(effect, "NotSignedIn", (e) => Effect.fail(new RequestRefused({ region: e.region, status: 401, message })))

/**
 * Signs a person in with their wallet: the wallet names its address, a
 * region issues a one-time value, the wallet signs a message carrying it,
 * and the same region checks the signature. The session it returns is
 * accepted by every region.
 */
export const signInWithWallet: Effect.Effect<
  Session,
  ApiError | NoRegionReachable | WalletMissing | WalletDeclined,
  Wallet | PoolApi | SiteSettings | SessionStore
> = Effect.gen(function* () {
  const wallet = yield* Wallet
  const { location } = yield* SiteSettings
  const address = yield* wallet.connect
  const { region, value: challenge } = yield* anyRegion(Api.walletNonce)
  const message = signInMessage({
    host: location.host,
    origin: location.origin,
    address,
    chainId: challenge.sign_in.chain_id,
    nonce: challenge.nonce,
    issuedAt: yield* DateTime.now,
  })
  const signature = yield* wallet.sign(address, message)
  const session = yield* Api.walletVerify(message, signature)(region).pipe(
    refusedSignIn("The pool did not accept the signature. Check that your wallet is on the account you chose and try again."),
  )
  return yield* remember(session)
}).pipe(Effect.withSpan("signInWithWallet"))

/**
 * Asks a region to email the operator a sign-in code, and says which region
 * did: the code is only good there.
 */
export const requestAdminCode = (
  email: string,
): Effect.Effect<RegionId, ApiError | NoRegionReachable, PoolApi | SiteSettings> =>
  Effect.map(anyRegion(Api.adminCode(email)), ({ region }) => region.id).pipe(Effect.withSpan("requestAdminCode"))

/** Signs the operator in with the code the region emailed. */
export const signInAsAdmin = (
  region: string,
  email: string,
  code: string,
): Effect.Effect<Session, ApiError | UnknownRegion, PoolApi | SiteSettings | SessionStore> =>
  Effect.gen(function* () {
    const target = yield* regionById(region)
    const session = yield* Api.adminVerify(email, code)(target).pipe(
      refusedSignIn("That code was not accepted. Codes last ten minutes and can be used once."),
    )
    return yield* remember(session)
  }).pipe(Effect.withSpan("signInAsAdmin"))

/** A region that no longer accepts the session ends it everywhere. */
const endSession = (role: Role): Effect.Effect<never, NoSession, SessionStore> =>
  Effect.zipRight(signOut(role), Effect.fail(new NoSession()))

/** Asks one region something as the signed-in person. */
export const inRegionAs = <A>(
  role: Role,
  region: string,
  ask: Api.AskAs<A>,
): Effect.Effect<A, Exclude<ApiError, { _tag: "NotSignedIn" }> | NoSession | UnknownRegion, PoolApi | SiteSettings | SessionStore> =>
  Effect.gen(function* () {
    const session = yield* requireSession(role)
    const target = yield* regionById(region)
    return yield* ask(target, session.token).pipe(Effect.catchTag("NotSignedIn", () => endSession(role)))
  })

/** Asks every region something as the signed-in person. */
export const everyRegionAs = <A, R>(
  role: Role,
  ask: (region: RegionConfig, session: Session) => Effect.Effect<A, ApiError, R>,
): Effect.Effect<ReadonlyArray<RegionResult<A>>, NoSession, R | SiteSettings | SessionStore> =>
  Effect.gen(function* () {
    const session = yield* requireSession(role)
    const results = yield* everyRegion((region) => ask(region, session))
    if (anyNotSignedIn(results)) {
      return yield* endSession(role)
    }
    return results
  })
