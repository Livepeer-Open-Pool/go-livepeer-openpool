import { Effect, Option, Schema } from "effect"
import type { ApiError } from "./domain/errors.js"
import type { AppId, ApplicationId, BatchId, GpuId, MachineId, MemberId, RunnerId, Token } from "./domain/ids.js"
import * as M from "./domain/model.js"
import type { RegionConfig } from "./domain/regions.js"
import { type ApiRequest, PoolApi } from "./services/PoolApi.js"

// Every endpoint of the pool service, as a function of the region to ask.
// The endpoints that need a session take its token as well.

/** A question for one region. */
export type Ask<A> = (region: RegionConfig) => Effect.Effect<A, ApiError, PoolApi>

/** A question for one region that needs a session. */
export type AskAs<A> = (region: RegionConfig, token: Token) => Effect.Effect<A, ApiError, PoolApi>

interface CallOptions {
  readonly query?: Readonly<Record<string, string>>
  readonly body?: unknown
}

const send = <A, I>(
  region: RegionConfig,
  method: ApiRequest["method"],
  path: string,
  answer: Schema.Schema<A, I>,
  token: Option.Option<Token>,
  options: CallOptions,
): Effect.Effect<A, ApiError, PoolApi> =>
  Effect.flatMap(PoolApi, (api) =>
    api.send(region, { method, path, query: options.query ?? {}, token, body: Option.fromNullable(options.body) }, answer),
  )

const open =
  <A, I>(method: ApiRequest["method"], path: string, answer: Schema.Schema<A, I>, options: CallOptions = {}): Ask<A> =>
  (region) =>
    send(region, method, path, answer, Option.none(), options)

const signed =
  <A, I>(method: ApiRequest["method"], path: string, answer: Schema.Schema<A, I>, options: CallOptions = {}): AskAs<A> =>
  (region, token) =>
    send(region, method, path, answer, Option.some(token), options)

/** A typed domain value as the JSON the service takes. Domain values always encode. */
const wire = <A, I>(schema: Schema.Schema<A, I>, value: A): I => Schema.encodeSync(schema)(value)

const nothing = Schema.Void
const id = (value: string): string => encodeURIComponent(value)

// What anyone may ask

export const pool: Ask<M.PoolInfo> = open("GET", "/public/pool", M.PoolInfo)
export const publicApps: Ask<typeof M.PublicApps.Type> = open("GET", "/public/apps", M.PublicApps)
export const publicGpus: Ask<typeof M.Gpus.Type> = open("GET", "/public/gpus", M.Gpus)
export const publicTerms: Ask<M.CurrentTerms> = open("GET", "/public/terms", M.CurrentTerms)
export const leaderboard = (window: "current" | "all", limit: number): Ask<M.Leaderboard> =>
  open("GET", "/public/leaderboard", M.Leaderboard, { query: { window, limit: String(limit) } })

// Signing in

export const walletNonce: Ask<M.Nonce> = open("POST", "/auth/wallet/nonce", M.Nonce)
export const walletVerify = (message: string, signature: string): Ask<M.Session> =>
  open("POST", "/auth/wallet/verify", M.Session, { body: { message, signature } })
export const adminCode = (email: string): Ask<void> => open("POST", "/auth/admin/code", nothing, { body: { email } })
export const adminVerify = (email: string, code: string): Ask<M.Session> =>
  open("POST", "/auth/admin/verify", M.Session, { body: { email, code } })

// A member's own things

export const me: AskAs<M.Me> = signed("GET", "/me", M.Me)
export const applicationMessage = (draft: M.ApplicationDraft): AskAs<typeof M.ApplicationMessage.Type> =>
  signed("POST", "/applications/message", M.ApplicationMessage, { body: wire(M.ApplicationDraft, draft) })
export const submitApplication = (draft: M.ApplicationDraft, signature: string): AskAs<M.Application> =>
  signed("POST", "/applications", M.Application, { body: wire(M.SignedApplication, { ...draft, signature }) })
export const myMachines: AskAs<typeof M.Machines.Type> = signed("GET", "/me/machines", M.Machines)
export const requestMachine = (spec: M.MachineSpec): AskAs<M.Machine> =>
  signed("POST", "/me/machines", M.Machine, { body: wire(M.MachineSpec, spec) })
export const removeMyMachine = (machine: MachineId): AskAs<void> => signed("DELETE", `/me/machines/${id(machine)}`, nothing)
export const createCredential = (machine: MachineId): AskAs<M.NewCredential> =>
  signed("POST", `/me/machines/${id(machine)}/credential`, M.NewCredential)
export const revokeCredential = (machine: MachineId): AskAs<void> =>
  signed("DELETE", `/me/machines/${id(machine)}/credential`, nothing)
export const myRunners: AskAs<typeof M.Runners.Type> = signed("GET", "/me/runners", M.Runners)
export const myTests = (limit: number): AskAs<typeof M.TestResults.Type> =>
  signed("GET", "/me/tests", M.TestResults, { query: { limit: String(limit) } })
export const myEarnings: AskAs<M.Earnings> = signed("GET", "/me/earnings", M.Earnings)
export const myPayouts: AskAs<typeof M.MemberPayouts.Type> = signed("GET", "/me/payouts", M.MemberPayouts)

// The operator's

export const adminStatus: AskAs<M.AdminStatus> = signed("GET", "/admin/status", M.AdminStatus)

export const applications = (status: M.ApplicationStatus | ""): AskAs<typeof M.Applications.Type> =>
  signed("GET", "/admin/applications", M.Applications, { query: status === "" ? {} : { status } })
export const approveApplication = (application: ApplicationId, machines: ReadonlyArray<M.MachineAssignments>): AskAs<M.Member> =>
  signed("POST", `/admin/applications/${id(application)}/approve`, M.Member, {
    body: { machines: machines.map((m) => wire(M.MachineAssignments, m)) },
  })
export const rejectApplication = (application: ApplicationId, reason: string): AskAs<M.Application> =>
  signed("POST", `/admin/applications/${id(application)}/reject`, M.Application, { body: { reason } })

export const members: AskAs<typeof M.Members.Type> = signed("GET", "/admin/members", M.Members)
export const member = (memberId: MemberId): AskAs<M.MemberDetail> => signed("GET", `/admin/members/${id(memberId)}`, M.MemberDetail)
export const banMember = (memberId: MemberId, reason: string): AskAs<M.Member> =>
  signed("POST", `/admin/members/${id(memberId)}/ban`, M.Member, { body: { reason } })
export const unbanMember = (memberId: MemberId): AskAs<M.Member> => signed("POST", `/admin/members/${id(memberId)}/unban`, M.Member)

export const machines = (status: M.MachineStatus | ""): AskAs<typeof M.Machines.Type> =>
  signed("GET", "/admin/machines", M.Machines, { query: status === "" ? {} : { status } })
export const approveMachine = (machine: MachineId): AskAs<M.Machine> =>
  signed("POST", `/admin/machines/${id(machine)}/approve`, M.Machine)
export const setAssignments = (machine: MachineId, assignments: ReadonlyArray<M.AssignmentInput>): AskAs<M.Machine> =>
  signed("PUT", `/admin/machines/${id(machine)}/assignments`, M.Machine, {
    body: { assignments: assignments.map((a) => wire(M.AssignmentInput, a)) },
  })
export const removeMachine = (machine: MachineId): AskAs<void> => signed("DELETE", `/admin/machines/${id(machine)}`, nothing)
export const demand: AskAs<M.Demand> = signed("GET", "/admin/demand", M.Demand)

export const gpus: AskAs<typeof M.Gpus.Type> = signed("GET", "/admin/gpus", M.Gpus)
export const createGpu = (input: M.GpuInput): AskAs<M.Gpu> => signed("POST", "/admin/gpus", M.Gpu, { body: wire(M.GpuInput, input) })
export const updateGpu = (gpu: GpuId, input: M.GpuInput): AskAs<M.Gpu> =>
  signed("PUT", `/admin/gpus/${id(gpu)}`, M.Gpu, { body: wire(M.GpuInput, input) })

export const apps: AskAs<typeof M.Apps.Type> = signed("GET", "/admin/apps", M.Apps)
export const createApp = (input: M.AppInput): AskAs<M.App> => signed("POST", "/admin/apps", M.App, { body: wire(M.AppInput, input) })
export const updateApp = (app: AppId, input: M.AppInput): AskAs<M.App> =>
  signed("PUT", `/admin/apps/${id(app)}`, M.App, { body: wire(M.AppInput, input) })
export const setAppVersion = (app: AppId, input: M.AppVersionInput): AskAs<M.App> =>
  signed("POST", `/admin/apps/${id(app)}/version`, M.App, { body: wire(M.AppVersionInput, input) })
export const pauseApp = (app: AppId): AskAs<M.App> => signed("POST", `/admin/apps/${id(app)}/pause`, M.App)
export const resumeApp = (app: AppId): AskAs<M.App> => signed("POST", `/admin/apps/${id(app)}/resume`, M.App)

export const terms: AskAs<M.CurrentTerms> = signed("GET", "/admin/terms", M.CurrentTerms)
export const publishTerms = (scope: M.TermsScope, body: string): AskAs<M.Terms> =>
  signed("POST", "/admin/terms", M.Terms, { body: { scope, body } })

export const settings: AskAs<M.Settings> = signed("GET", "/admin/settings", M.Settings)
export const saveSettings = (value: M.Settings): AskAs<M.Settings> =>
  signed("PUT", "/admin/settings", M.Settings, { body: wire(M.Settings, value) })

export const runners: AskAs<typeof M.Runners.Type> = signed("GET", "/admin/runners", M.Runners)
export const setRunnerState = (runner: RunnerId, state: M.RunnerState, reason: string): AskAs<void> =>
  signed("POST", `/admin/runners/${id(runner)}/state`, nothing, { body: { state, reason } })
export const holdRunner = (runner: RunnerId, hold: boolean, reason: string): AskAs<void> =>
  signed("POST", `/admin/runners/${id(runner)}/hold`, nothing, { body: { hold, reason } })
export const evictRunner = (runner: RunnerId, reason: string): AskAs<void> =>
  signed("POST", `/admin/runners/${id(runner)}/evict`, nothing, { body: { reason } })
export const testRunner = (runner: RunnerId): AskAs<M.TestResult> => signed("POST", `/admin/runners/${id(runner)}/test`, M.TestResult)
export const tests = (limit: number): AskAs<typeof M.TestResults.Type> =>
  signed("GET", "/admin/tests", M.TestResults, { query: { limit: String(limit) } })

export const audit = (limit: number): AskAs<typeof M.Audit.Type> =>
  signed("GET", "/admin/audit", M.Audit, { query: { limit: String(limit) } })

export const batches: AskAs<typeof M.Batches.Type> = signed("GET", "/admin/batches", M.Batches)
export const batch = (batchId: BatchId): AskAs<M.Batch> => signed("GET", `/admin/batches/${id(batchId)}`, M.Batch)
export const setBatchItem = (batchId: BatchId, memberId: MemberId, include: boolean): AskAs<M.Batch> =>
  signed("PUT", `/admin/batches/${id(batchId)}/items/${id(memberId)}`, M.Batch, { body: { include } })
export const approveBatch = (batchId: BatchId): AskAs<M.Batch> => signed("POST", `/admin/batches/${id(batchId)}/approve`, M.Batch)
export const retryBatch = (batchId: BatchId): AskAs<M.Batch> => signed("POST", `/admin/batches/${id(batchId)}/retry`, M.Batch)
export const payoutWallet: AskAs<M.WalletStatus> = signed("GET", "/admin/wallet", M.WalletStatus)
