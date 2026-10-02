import { Context, Effect, Layer, Option, Ref, Schema } from "effect"
import { type Role, Session } from "../domain/model.js"

/**
 * Where the signed-in session is kept between page loads. A session is a
 * bearer token: it lives in this browser only and is sent to the pool's
 * regions in the Authorization header, never in a cookie.
 */
export class SessionStore extends Context.Tag("openpool/SessionStore")<
  SessionStore,
  {
    readonly read: (role: Role) => Effect.Effect<Option.Option<Session>>
    readonly write: (session: Session) => Effect.Effect<void>
    readonly clear: (role: Role) => Effect.Effect<void>
  }
>() {}

/** Kept in memory only: for tests, and for browsers that forbid storage. */
export const SessionStoreMemory = Layer.effect(
  SessionStore,
  Effect.map(Ref.make<ReadonlyMap<Role, Session>>(new Map()), (sessions) => ({
    read: (role: Role) => Effect.map(Ref.get(sessions), (all) => Option.fromNullable(all.get(role))),
    write: (session: Session) => Ref.update(sessions, (all) => new Map(all).set(session.role, session)),
    clear: (role: Role) =>
      Ref.update(sessions, (all) => {
        const next = new Map(all)
        next.delete(role)
        return next
      }),
  })),
)

const StoredSession = Schema.parseJson(Session)
const decodeStored = Schema.decodeUnknownOption(StoredSession)
const encodeStored = Schema.encodeSync(StoredSession)
const key = (role: Role): string => `openpool.session.${role}`

/**
 * Kept in the browser's localStorage. A browser that refuses storage (some
 * private modes do) still works for as long as the page stays open, which is
 * why reading and writing fall back instead of failing.
 */
export const SessionStoreBrowser = (storage: Storage) =>
  Layer.effect(
    SessionStore,
    Effect.gen(function* () {
      const fallback = yield* Ref.make<ReadonlyMap<Role, Session>>(new Map())
      return {
        read: (role: Role) =>
          Effect.gen(function* () {
            const remembered = Option.fromNullable((yield* Ref.get(fallback)).get(role))
            if (Option.isSome(remembered)) return remembered
            return yield* Effect.try(() => storage.getItem(key(role))).pipe(
              Effect.map((raw) => Option.flatMap(Option.fromNullable(raw), decodeStored)),
              Effect.orElseSucceed(() => Option.none<Session>()),
            )
          }),
        write: (session: Session) =>
          Ref.update(fallback, (all) => new Map(all).set(session.role, session)).pipe(
            Effect.zipRight(Effect.ignore(Effect.try(() => storage.setItem(key(session.role), encodeStored(session))))),
          ),
        clear: (role: Role) =>
          Ref.update(fallback, (all) => {
            const next = new Map(all)
            next.delete(role)
            return next
          }).pipe(Effect.zipRight(Effect.ignore(Effect.try(() => storage.removeItem(key(role)))))),
      }
    }),
  )
