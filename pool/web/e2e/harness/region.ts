import { type ChildProcess, execFileSync, spawn } from "node:child_process"
import { mkdirSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { type FakeRunner, FakeOrchestrator } from "./orchestrator.js"

const here = path.dirname(fileURLToPath(import.meta.url))
const poolDir = path.resolve(here, "../../..")
const binary = path.resolve(here, "../.bin/openpool")

export const POOL_SECRET = "the-pool-secret-of-the-test-pool"
export const SESSION_SECRET = "a-session-signing-secret-shared-by-every-region"
export const ADMIN_EMAIL = "ops@open-pool.test"
export const TUNNEL_IMAGE = "tztcloud/livepeer-runner-tunnel@sha256:" + "ab".repeat(32)

/** Builds the pool service once; later calls reuse the binary. */
let built = false
export const buildPool = (): string => {
  if (!built) {
    mkdirSync(path.dirname(binary), { recursive: true })
    execFileSync("go", ["build", "-o", binary, "./cmd/openpool"], { cwd: poolDir, stdio: "inherit" })
    built = true
  }
  return binary
}

export interface RegionOptions {
  readonly id: string
  readonly name: string
  /** The pool service's port; its stand-in orchestrator takes the next one up by 1000. */
  readonly port: number
  /** The sites' origins, which may call the API from a browser. */
  readonly origins: ReadonlyArray<string>
  /** The member site's host, which sign-in messages are for. */
  readonly signInHost: string
}

/** One region of a pool, running for real: the pool service beside a stand-in orchestrator. */
export class Region {
  readonly id: string
  readonly name: string
  readonly api: string
  readonly orchestrator = new FakeOrchestrator()
  readonly #options: RegionOptions
  readonly #dir: string
  #process: ChildProcess | undefined
  #log = ""
  #events = 0

  constructor(options: RegionOptions) {
    this.#options = options
    this.id = options.id
    this.name = options.name
    this.api = `http://127.0.0.1:${options.port}`
    this.#dir = mkdtempSync(path.join(tmpdir(), `openpool-${options.id}-`))
  }

  /** Starts the orchestrator stand-in and the pool service, and waits until it answers. */
  async start(): Promise<void> {
    const orchestratorUrl = await this.orchestrator.start(this.#options.port - 1000)
    await this.startService(orchestratorUrl)
  }

  /** Starts the pool service alone: it keeps its database, so this is also a restart. */
  async startService(orchestratorUrl = `http://127.0.0.1:${this.#options.port - 1000}`): Promise<void> {
    this.#process = spawn(buildPool(), [], {
      env: {
        PATH: process.env["PATH"] ?? "",
        OPENPOOL_REGION_ID: this.id,
        OPENPOOL_REGION_NAME: this.name,
        OPENPOOL_LISTEN: `127.0.0.1:${this.#options.port}`,
        OPENPOOL_PUBLIC_URL: this.api,
        OPENPOOL_DB_PATH: path.join(this.#dir, "openpool.db"),
        OPENPOOL_COMPOSE_KEY_FILE: path.join(this.#dir, "compose.key"),
        OPENPOOL_TUNNEL_IMAGE: TUNNEL_IMAGE,
        OPENPOOL_AGENT_IMAGE: "tztcloud/openpool-agent:test",
        OPENPOOL_ORCHESTRATOR_URL: orchestratorUrl,
        OPENPOOL_ORCHESTRATOR_CLI_URL: orchestratorUrl,
        OPENPOOL_POOL_SECRET: POOL_SECRET,
        OPENPOOL_SESSION_SECRET: SESSION_SECRET,
        OPENPOOL_SIGN_IN_DOMAINS: this.#options.signInHost,
        OPENPOOL_CHAIN_ID: "42161",
        OPENPOOL_ADMIN_EMAIL: ADMIN_EMAIL,
        OPENPOOL_ALLOWED_ORIGINS: this.#options.origins.join(","),
        OPENPOOL_RECONCILE_INTERVAL: "500ms",
        OPENPOOL_PAYOUT_INTERVAL: "500ms",
        OPENPOOL_SETTLE_DELAY: "1s",
      },
      stdio: ["ignore", "ignore", "pipe"],
    })
    this.#process.stderr?.on("data", (chunk: Buffer) => (this.#log += chunk.toString("utf8")))
    const deadline = Date.now() + 20_000
    while (Date.now() < deadline) {
      if (this.#process.exitCode !== null) throw new Error(`the ${this.id} pool service stopped at start:\n${this.#log}`)
      const up = await fetch(`${this.api}/healthz`).then((r) => r.ok, () => false)
      if (up) return
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    throw new Error(`the ${this.id} pool service did not come up:\n${this.#log}`)
  }

  /** Stops the pool service, as if the region had gone down. Its orchestrator stand-in stays. */
  async stopService(): Promise<void> {
    const child = this.#process
    if (!child || child.exitCode !== null) return
    await new Promise<void>((resolve) => {
      child.once("exit", () => resolve())
      child.kill("SIGTERM")
    })
  }

  async stop(): Promise<void> {
    await this.stopService()
    await this.orchestrator.stop()
    rmSync(this.#dir, { recursive: true, force: true })
  }

  /** Everything the service has logged. */
  get log(): string {
    return this.#log
  }

  /** The sign-in codes the service wrote to its log in place of emails, oldest first. */
  codes(): ReadonlyArray<string> {
    return [...this.#log.matchAll(/sign-in code written to the log instead.*?code=(\d{6})/g)].map((match) => match[1] ?? "")
  }

  /** The code the service logs next, given how many it had logged before it was asked for one. */
  async nextCode(before: number): Promise<string> {
    return eventually(`${this.id} to log a sign-in code`, async () => this.codes()[before])
  }

  /** Calls the service's API and returns the decoded JSON answer. */
  async call<T>(method: string, apiPath: string, token: string | undefined, body?: unknown): Promise<T> {
    const response = await fetch(`${this.api}/v1${apiPath}`, {
      method,
      headers: { ...(token ? { authorization: `Bearer ${token}` } : {}), ...(body === undefined ? {} : { "content-type": "application/json" }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    })
    const text = await response.text()
    if (!response.ok) throw new Error(`${this.id} ${method} ${apiPath} answered ${response.status}: ${text}`)
    return (text === "" ? undefined : JSON.parse(text)) as T
  }

  /**
   * Registers a runner the way a real orchestrator does: it asks the pool
   * service whether the credential admits it, and lists it if so.
   */
  async admit(credential: string, runner: { runner_id: string; app: string; version: string; capacity?: number } & Partial<FakeRunner>): Promise<void> {
    const answer = await this.call<{ allow: boolean; reason?: string; member_id: string; payout_address: string; state: FakeRunner["state"] }>(
      "POST", "/orchestrator/admit", POOL_SECRET,
      {
        event: "register", orch_address: "0x00000000000000000000000000000000000000aa", remote_addr: "203.0.113.7:51820", credential,
        runner: { runner_id: runner.runner_id, tunnel: true, version: runner.version, status: "ready", mode: "single-shot", app: runner.app, capacity: runner.capacity ?? 2 },
      },
    )
    if (!answer.allow) throw new Error(`${this.id} refused runner ${runner.runner_id}: ${answer.reason}`)
    this.orchestrator.register({ ...runner, member_id: answer.member_id, payout_address: answer.payout_address, state: answer.state })
  }

  /** Uploads orchestrator events, as they would arrive through Kafka. */
  async events(events: ReadonlyArray<Record<string, unknown>>): Promise<void> {
    const stamped = events.map((event) => ({ event_id: `${this.id}-evt-${++this.#events}`, ts_ms: Date.now(), ...event }))
    await this.call("POST", "/orchestrator/events", POOL_SECRET, stamped)
  }

  /** One paid session: reserved, a fee, released. */
  async paidSession(session: { runner: string; member: string; app: string; round: number; feeWei: string }): Promise<void> {
    const id = `${this.id}-session-${this.#events + 1}`
    const common = { runner_id: session.runner, member_id: session.member, app: session.app, session_id: id, sender: "0x00000000000000000000000000000000000000c1", round: session.round }
    await this.events([
      { event_type: "live_runner_session_reserved", paid: true, ...common },
      { event_type: "live_runner_fee_debited", fee_wei: session.feeWei, ...common },
      { event_type: "live_runner_session_released", paid: true, reason: "completed", duration_ms: 1200, fee_wei_total: session.feeWei, ...common },
    ])
  }
}

/** Waits until check returns something, or fails with what it was waiting for. */
export const eventually = async <T>(what: string, check: () => Promise<T | undefined | false>, timeoutMs = 20_000): Promise<T> => {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const value = await check()
    if (value) return value
    await new Promise((resolve) => setTimeout(resolve, 200))
  }
  throw new Error(`timed out waiting for ${what}`)
}
