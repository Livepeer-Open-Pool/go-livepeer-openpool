import { Schema } from "effect"
import {
  Address,
  AppId,
  ApplicationId,
  AssignmentId,
  BatchId,
  GpuId,
  MachineId,
  MemberId,
  RegionId,
  Round,
  RunnerId,
  Token,
} from "./ids.js"
import { BasisPoints, Wei } from "./money.js"

// The pool service's JSON, decoded once at the boundary into domain types.
// On the wire a missing value is an absent key or null and a time is an ISO
// string; in the domain they are Option and DateTime.Utc.

const Time = Schema.DateTimeUtc

/** An optional key, or null, as Option. */
const maybe = <A, I, R>(schema: Schema.Schema<A, I, R>) =>
  Schema.optionalWith(schema, { as: "Option", nullable: true })

/** A list, which Go writes as null when it is empty and never allocated. */
const list = <A, I, R>(schema: Schema.Schema<A, I, R>) =>
  Schema.optionalWith(Schema.Array(schema), { nullable: true, default: () => [] })

const dictionary = Schema.optionalWith(Schema.Record({ key: Schema.String, value: Schema.String }), {
  nullable: true,
  default: () => ({}),
})

// Regions and the pool as anyone sees it

export const Region = Schema.Struct({ id: RegionId, name: Schema.String })
export type Region = typeof Region.Type

export const SignInInfo = Schema.Struct({ domains: list(Schema.String), chain_id: Schema.Int })
export type SignInInfo = typeof SignInInfo.Type

export const AgentInfo = Schema.Struct({ image: Schema.String, key: Schema.String })
export type AgentInfo = typeof AgentInfo.Type

export const PoolInfo = Schema.Struct({
  region: Region,
  taking_applications: Schema.Boolean,
  apps_offered: Schema.Int,
  sign_in: SignInInfo,
  agent: maybe(AgentInfo),
  commission_bps: BasisPoints,
  window_rounds: Schema.Int,
})
export type PoolInfo = typeof PoolInfo.Type

export const PriceUnit = Schema.Literal("hour", "720p", "fixed")
export type PriceUnit = typeof PriceUnit.Type

/** A price in US dollars per unit. The amount stays a decimal string. */
export const Price = Schema.Struct({ price: Schema.String, unit: PriceUnit })
export type Price = typeof Price.Type

export const AppMode = Schema.Literal("single-shot", "persistent")
export type AppMode = typeof AppMode.Type

export const PublicApp = Schema.Struct({
  id: AppId,
  name: Schema.String,
  mode: AppMode,
  price: Price,
  description: Schema.String,
  eligible_gpus: list(GpuId),
  runners: Schema.Int,
  updated_at: Time,
})
export type PublicApp = typeof PublicApp.Type

export const PublicApps = Schema.Struct({ region: Region, apps: list(PublicApp) })

export const Gpu = Schema.Struct({
  id: GpuId,
  name: Schema.String,
  vram_mb: Schema.Int,
  accepted: Schema.Boolean,
  created_at: Time,
})
export type Gpu = typeof Gpu.Type

export const Gpus = Schema.Struct({ gpus: list(Gpu) })

export const GpuInput = Schema.Struct({
  name: Schema.String,
  vram_mb: Schema.Int,
  accepted: Schema.optionalWith(Schema.Boolean, { exact: true }),
})
export type GpuInput = typeof GpuInput.Type

export const TermsScope = Schema.Literal("global", "regional")
export type TermsScope = typeof TermsScope.Type

export const Terms = Schema.Struct({
  scope: TermsScope,
  version: Schema.Int,
  body: Schema.String,
  hash: Schema.String,
  published_at: Time,
})
export type Terms = typeof Terms.Type

export const CurrentTerms = Schema.Struct({
  global: Schema.OptionFromNullOr(Terms),
  regional: Schema.OptionFromNullOr(Terms),
})
export type CurrentTerms = typeof CurrentTerms.Type

export const LeaderboardEntry = Schema.Struct({
  address: Address,
  display_name: Schema.String,
  fees_wei: Wei,
  sessions: Schema.Int,
})
export type LeaderboardEntry = typeof LeaderboardEntry.Type

export const Leaderboard = Schema.Struct({
  region: Region,
  window: Schema.Literal("current", "all"),
  first_round: maybe(Round),
  last_round: maybe(Round),
  entries: list(LeaderboardEntry),
})
export type Leaderboard = typeof Leaderboard.Type

// Signing in

export const Role = Schema.Literal("wallet", "admin")
export type Role = typeof Role.Type

export const Session = Schema.Struct({
  token: Token,
  role: Role,
  /** The wallet address, or the operator's email. */
  subject: Schema.String,
  expires_at: Time,
})
export type Session = typeof Session.Type

export const Nonce = Schema.Struct({ nonce: Schema.NonEmptyString, sign_in: SignInInfo })
export type Nonce = typeof Nonce.Type

// Applying and membership

export const MachineGpu = Schema.Struct({ gpu_id: GpuId, count: Schema.Int.pipe(Schema.positive()) })
export type MachineGpu = typeof MachineGpu.Type

export const MachineSpec = Schema.Struct({ label: Schema.String, gpus: list(MachineGpu) })
export type MachineSpec = typeof MachineSpec.Type

export const ApplicationDraft = Schema.Struct({ display_name: Schema.String, machines: list(MachineSpec) })
export type ApplicationDraft = typeof ApplicationDraft.Type

export const SignedApplication = Schema.Struct({ ...ApplicationDraft.fields, signature: Schema.String })

export const ApplicationMessage = Schema.Struct({ message: Schema.NonEmptyString })

export const ApplicationStatus = Schema.Literal("pending", "approved", "rejected")
export type ApplicationStatus = typeof ApplicationStatus.Type

export const Application = Schema.Struct({
  id: ApplicationId,
  address: Address,
  display_name: Schema.String,
  machines: list(MachineSpec),
  global_terms_version: Schema.Int,
  regional_terms_version: maybe(Schema.Int),
  status: ApplicationStatus,
  reason: maybe(Schema.String),
  created_at: Time,
  decided_at: maybe(Time),
})
export type Application = typeof Application.Type

export const Applications = Schema.Struct({ applications: list(Application) })

export const MemberStatus = Schema.Literal("active", "banned", "left")
export type MemberStatus = typeof MemberStatus.Type

export const Member = Schema.Struct({
  id: MemberId,
  address: Address,
  display_name: Schema.String,
  status: MemberStatus,
  reason: maybe(Schema.String),
  created_at: Time,
  updated_at: Time,
})
export type Member = typeof Member.Type

export const Members = Schema.Struct({ members: list(Member) })

export const Me = Schema.Struct({
  region: Region,
  address: Address,
  member: Schema.OptionFromNullOr(Member),
  applications: list(Application),
})
export type Me = typeof Me.Type

// Machines

export const AssignmentStatus = Schema.Literal("active", "draining", "removed")

export const Assignment = Schema.Struct({
  id: AssignmentId,
  machine_id: MachineId,
  app_id: AppId,
  app_name: Schema.String,
  capacity: Schema.Int,
  status: AssignmentStatus,
  /** The app version the machine is told to run. */
  version: Schema.String,
  /** Set while this machine is the one being moved to a new version. */
  upgrading: Schema.Boolean,
  created_at: Time,
  updated_at: Time,
})
export type Assignment = typeof Assignment.Type

export const AssignmentInput = Schema.Struct({ app_id: AppId, capacity: Schema.Int })
export type AssignmentInput = typeof AssignmentInput.Type

export const AgentContainer = Schema.Struct({
  service: Schema.String,
  state: Schema.String,
  health: maybe(Schema.String),
})

export const AgentGpu = Schema.Struct({ name: Schema.String, memory_mb: Schema.Int })

export const AgentReport = Schema.Struct({
  agent_version: Schema.String,
  generation: Schema.String,
  error: maybe(Schema.String),
  containers: list(AgentContainer),
  gpus: list(AgentGpu),
  gpu_error: maybe(Schema.String),
  reported_at: maybe(Time),
})
export type AgentReport = typeof AgentReport.Type

export const MachineStatus = Schema.Literal("pending", "approved", "removed")
export type MachineStatus = typeof MachineStatus.Type

export const Machine = Schema.Struct({
  id: MachineId,
  member_id: MemberId,
  label: Schema.String,
  gpus: list(MachineGpu),
  status: MachineStatus,
  created_at: Time,
  last_seen_at: maybe(Time),
  assignments: list(Assignment),
  credential_created_at: maybe(Time),
  reported: maybe(AgentReport),
})
export type Machine = typeof Machine.Type

export const Machines = Schema.Struct({ machines: list(Machine) })

/** A credential at the one moment its secret is known. */
export const NewCredential = Schema.Struct({
  machine_id: MachineId,
  secret: Schema.Redacted(Schema.String),
  agent_command: maybe(Schema.Redacted(Schema.String)),
  commands: list(Schema.Redacted(Schema.String)),
})
export type NewCredential = typeof NewCredential.Type

// Runners and tests

export const RunnerState = Schema.Literal("probation", "active", "suspended")
export type RunnerState = typeof RunnerState.Type

export const Runner = Schema.Struct({
  runner_id: RunnerId,
  member_id: Schema.String,
  state: RunnerState,
  status: Schema.String,
  ready: Schema.Boolean,
  app: Schema.String,
  mode: Schema.String,
  version: Schema.String,
  capacity: Schema.Int,
  capacity_used: Schema.Int,
  tunnel_connected: Schema.Boolean,
  remote_addr: Schema.String,
  machine_id: maybe(MachineId),
  machine_label: maybe(Schema.String),
  member_name: maybe(Schema.String),
  /** A runner the pool did not admit. */
  unknown: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  hold: Schema.Boolean,
  hold_reason: maybe(Schema.String),
  suspend_reason: maybe(Schema.String),
  last_test_at: maybe(Time),
  last_test_ok: Schema.Boolean,
  failures: Schema.Int,
})
export type Runner = typeof Runner.Type

export const Runners = Schema.Struct({ runners: list(Runner) })

export const TestResult = Schema.Struct({
  id: Schema.Int,
  runner_id: RunnerId,
  machine_id: MachineId,
  app: Schema.String,
  at: Time,
  ok: Schema.Boolean,
  latency_ms: Schema.Int,
  detail: maybe(Schema.String),
})
export type TestResult = typeof TestResult.Type

export const TestResults = Schema.Struct({ tests: list(TestResult) })

// Money

export const Earning = Schema.Struct({ round: Round, app: Schema.String, fee_wei: Wei, sessions: Schema.Int })
export type Earning = typeof Earning.Type

export const Window = Schema.Struct({ first_round: Round, last_round: Round })
export type Window = typeof Window.Type

export const Earnings = Schema.Struct({ earnings: list(Earning), window: maybe(Window) })
export type Earnings = typeof Earnings.Type

export const BatchStatus = Schema.Literal("open", "ready", "approved", "sending", "blocked", "failed", "sent", "empty")
export type BatchStatus = typeof BatchStatus.Type

export const PaymentStatus = Schema.Literal("pending", "signed", "broadcast", "confirmed", "failed")
export type PaymentStatus = typeof PaymentStatus.Type

export const MemberPayout = Schema.Struct({
  batch_id: BatchId,
  first_round: Round,
  last_round: Round,
  batch_status: BatchStatus,
  fees_wei: Wei,
  amount_wei: Wei,
  include: Schema.Boolean,
  status: PaymentStatus,
  tx_hash: maybe(Schema.String),
})
export type MemberPayout = typeof MemberPayout.Type

export const MemberPayouts = Schema.Struct({ payouts: list(MemberPayout) })

export const BatchItem = Schema.Struct({
  batch_id: BatchId,
  member_id: MemberId,
  address: Address,
  display_name: Schema.String,
  fees_wei: Wei,
  amount_wei: Wei,
  include: Schema.Boolean,
  banned: Schema.Boolean,
  status: PaymentStatus,
  tx_hash: maybe(Schema.String),
  error: maybe(Schema.String),
})
export type BatchItem = typeof BatchItem.Type

export const Batch = Schema.Struct({
  id: BatchId,
  first_round: Round,
  last_round: Round,
  status: BatchStatus,
  pot_wei: Wei,
  commission_bps: BasisPoints,
  payable_wei: Wei,
  total_fees_wei: Wei,
  other_fees_wei: Wei,
  note: maybe(Schema.String),
  created_at: Time,
  closed_at: maybe(Time),
  approved_at: maybe(Time),
  approved_by: maybe(Schema.String),
  sent_at: maybe(Time),
  items: list(BatchItem),
})
export type Batch = typeof Batch.Type

export const Batches = Schema.Struct({ batches: list(Batch) })

export const WalletStatus = Schema.Struct({
  configured: Schema.Boolean,
  address: maybe(Address),
  chain_id: maybe(Schema.Int),
  balance_wei: maybe(Wei),
  owed_wei: Wei,
  awaiting_approval_wei: Wei,
  error: maybe(Schema.String),
})
export type WalletStatus = typeof WalletStatus.Type

// The operator's side

export const Container = Schema.Struct({
  image: Schema.String,
  command: list(Schema.String),
  env: dictionary,
  port: Schema.Int,
  health_path: Schema.String,
  gpus: Schema.Int,
  no_gpu: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  shm_size: Schema.optionalWith(Schema.String, { default: () => "" }),
  cache_path: Schema.optionalWith(Schema.String, { default: () => "" }),
})
export type Container = typeof Container.Type

export const AppTest = Schema.Struct({
  method: Schema.String,
  path: Schema.String,
  headers: dictionary,
  body: Schema.optionalWith(Schema.String, { default: () => "" }),
  expect_status: Schema.Int,
  expect_contains: list(Schema.String),
  timeout_ms: Schema.Int,
})
export type AppTest = typeof AppTest.Type

const appFields = {
  name: Schema.String,
  mode: AppMode,
  price: Price,
  required_version: Schema.String,
  container: Container,
  eligible_gpus: list(GpuId),
  default_capacity: Schema.Int,
  max_capacity: Schema.Int,
  test: AppTest,
  discovery: Schema.String,
  description: Schema.String,
}

export const AppInput = Schema.Struct(appFields)
export type AppInput = typeof AppInput.Type

export const App = Schema.Struct({
  id: AppId,
  ...appFields,
  previous_version: maybe(Schema.String),
  previous_version_deadline: maybe(Time),
  offered: Schema.Boolean,
  created_at: Time,
  updated_at: Time,
})
export type App = typeof App.Type

export const Apps = Schema.Struct({ apps: list(App) })

export const AppVersionInput = Schema.Struct({
  version: Schema.String,
  container: Container,
})
export type AppVersionInput = typeof AppVersionInput.Type

export const PayoutApproval = Schema.Literal("required", "automatic")

export const Settings = Schema.Struct({
  commission_bps: BasisPoints,
  window_rounds: Schema.Int,
  payout_approval: PayoutApproval,
  test_interval_seconds: Schema.Int,
  test_failures_to_suspend: Schema.Int,
  test_passes_to_reinstate: Schema.Int,
  version_deadline_hours: Schema.Int,
})
export type Settings = typeof Settings.Type

export const AdminStatus = Schema.Struct({
  region: Region,
  orchestrator: Schema.Struct({
    reachable: Schema.Boolean,
    error: maybe(Schema.String),
    runners: Schema.Int,
  }),
  pending_applications: Schema.Int,
  pending_machines: Schema.Int,
})
export type AdminStatus = typeof AdminStatus.Type

export const MemberDetail = Schema.Struct({
  member: Member,
  machines: list(Machine),
  applications: list(Application),
})
export type MemberDetail = typeof MemberDetail.Type

export const MachineAssignments = Schema.Struct({
  /** The machine's place in the application's list. */
  machine: Schema.Int,
  assignments: list(AssignmentInput),
})
export type MachineAssignments = typeof MachineAssignments.Type

export const AppDemand = Schema.Struct({
  app: Schema.String,
  runners_online: Schema.Int,
  slots_busy: Schema.Int,
  slots_total: Schema.Int,
  sessions_24h: Schema.Int,
  fees_wei_24h: Wei,
  refused_24h: Schema.Int,
})
export type AppDemand = typeof AppDemand.Type

export const Demand = Schema.Struct({ demand: list(AppDemand), orchestrator_error: maybe(Schema.String) })
export type Demand = typeof Demand.Type

export const AuditEntry = Schema.Struct({
  id: Schema.Int,
  at: Time,
  actor: Schema.String,
  action: Schema.String,
  subject: Schema.String,
  detail: Schema.String,
})
export type AuditEntry = typeof AuditEntry.Type

export const Audit = Schema.Struct({ entries: list(AuditEntry) })
