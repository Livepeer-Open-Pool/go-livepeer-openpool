import http from "node:http"
import type { AddressInfo } from "node:net"

/** A runner as the orchestrator's CLI port lists it. */
export interface FakeRunner {
  runner_id: string
  member_id: string
  payout_address: string
  state: "probation" | "active" | "suspended"
  status: string
  ready: boolean
  app: string
  mode: string
  version: string
  capacity: number
  capacity_registered: number
  capacity_used: number
  tunnel: boolean
  tunnel_connected: boolean
  remote_addr: string
  registered_at_ms: number
  last_heartbeat_ms: number
  session_ids: string[]
  static: boolean
}

const readBody = (req: http.IncomingMessage): Promise<string> =>
  new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on("data", (chunk: Buffer) => chunks.push(chunk))
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")))
    req.on("error", reject)
  })

/**
 * Stands in for an orchestrator node, as far as the pool service can tell:
 * the CLI port's runner list, state changes, evictions, price table and
 * current round, and the service port's app route, which answers the pool's
 * tests. It does no video and takes no payments.
 */
export class FakeOrchestrator {
  runners: FakeRunner[] = []
  round = 4100
  prices: unknown = undefined
  /** Runners whose app answers the pool's test with an error. */
  readonly failing = new Set<string>()
  #server: http.Server | undefined

  async start(port: number): Promise<string> {
    this.#server = http.createServer((req, res) => void this.#handle(req, res))
    await new Promise<void>((resolve) => this.#server?.listen(port, "127.0.0.1", resolve))
    const address = this.#server.address() as AddressInfo
    return `http://127.0.0.1:${address.port}`
  }

  async stop(): Promise<void> {
    await new Promise<void>((resolve) => {
      this.#server?.closeAllConnections()
      this.#server?.close(() => resolve())
    })
  }

  register(runner: Pick<FakeRunner, "runner_id" | "member_id" | "payout_address" | "app" | "version"> & Partial<FakeRunner>): void {
    const now = Date.now()
    this.runners = this.runners.filter((r) => r.runner_id !== runner.runner_id)
    this.runners.push({
      state: "probation", status: "ready", ready: true, mode: "single-shot", capacity: 2, capacity_registered: 2, capacity_used: 0,
      tunnel: true, tunnel_connected: true, remote_addr: "203.0.113.7:51820", registered_at_ms: now, last_heartbeat_ms: now,
      session_ids: [], static: false, ...runner,
    })
  }

  async #handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = new URL(req.url ?? "/", "http://orchestrator")
    const text = (status: number, body: string): void => {
      res.writeHead(status, { "content-type": "text/plain" }).end(body)
    }
    const json = (status: number, body: unknown): void => {
      res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body))
    }

    if (url.pathname === "/liveRunner/runners") return json(200, { runners: this.runners })
    if (url.pathname === "/currentRound") return text(200, String(this.round))
    if (url.pathname === "/liveRunner/prices" && req.method === "POST") {
      this.prices = JSON.parse(await readBody(req))
      return text(200, "ok")
    }
    if (url.pathname === "/liveRunner/setState" && req.method === "POST") {
      const form = new URLSearchParams(await readBody(req))
      const runner = this.runners.find((r) => r.runner_id === form.get("runner_id"))
      if (!runner) return text(404, "runner not found")
      runner.state = form.get("state") as FakeRunner["state"]
      return text(200, "ok")
    }
    if (url.pathname === "/liveRunner/evict" && req.method === "POST") {
      const form = new URLSearchParams(await readBody(req))
      const before = this.runners.length
      this.runners = this.runners.filter((r) => r.runner_id !== form.get("runner_id") && r.member_id !== form.get("member_id"))
      if (form.get("runner_id") && before === this.runners.length) return text(404, "runner not found")
      return text(200, "ok")
    }
    // The service port: a request to a runner's app, which is how the pool tests it.
    const app = /^\/apps\/([^/]+)\/app\//.exec(url.pathname)
    if (app) {
      const runnerId = decodeURIComponent(app[1] ?? "")
      if (!this.runners.some((r) => r.runner_id === runnerId)) return text(404, "runner not found")
      if (this.failing.has(runnerId)) return json(500, { error: "CUDA out of memory" })
      return json(200, { object: "list", data: [{ id: "model" }] })
    }
    return text(404, "not found")
  }
}
