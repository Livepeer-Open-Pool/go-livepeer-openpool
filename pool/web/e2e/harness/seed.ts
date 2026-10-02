import { type PrivateKeyAccount, privateKeyToAccount } from "viem/accounts"
import { ADMIN_EMAIL, eventually, type Region } from "./region.js"

// Well-known test keys (Hardhat's first accounts). They hold nothing anywhere.
export const alice = privateKeyToAccount("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
export const bob = privateKeyToAccount("0x59c6995e998f97a5a0044966f0945389dc9d86dae88c7a8412f4603b6b78690d")
export const carol = privateKeyToAccount("0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a")

const image = (name: string, fill: string): string => `${name}@sha256:${fill.repeat(32)}`

/** Signs the operator in at a region, reading the code from the service's log. */
export const adminToken = async (region: Region): Promise<string> => {
  await region.call("POST", "/auth/admin/code", undefined, { email: ADMIN_EMAIL })
  const session = await region.call<{ token: string }>("POST", "/auth/admin/verify", undefined, { email: ADMIN_EMAIL, code: region.latestCode() })
  return session.token
}

/** Signs a wallet in at a region, as the member site does. */
export const walletToken = async (region: Region, account: PrivateKeyAccount, host: string): Promise<string> => {
  const { nonce, sign_in } = await region.call<{ nonce: string; sign_in: { chain_id: number } }>("POST", "/auth/wallet/nonce", undefined)
  const message = [
    `${host} wants you to sign in with your Ethereum account:`, account.address, "", "Sign in to Open Pool.", "",
    `URI: http://${host}`, "Version: 1", `Chain ID: ${sign_in.chain_id}`, `Nonce: ${nonce}`, `Issued At: ${new Date().toISOString()}`,
  ].join("\n")
  const session = await region.call<{ token: string }>("POST", "/auth/wallet/verify", undefined, { message, signature: await account.signMessage({ message }) })
  return session.token
}

interface Seeded {
  readonly gpu4090: string
  readonly gpu3090: string
  readonly vllm: string
  readonly flux: string
}

/** The operator's setup of a region: GPU models, two apps with tests, and terms. */
export const seedCatalog = async (region: Region, admin: string, withRegionalTerms: boolean): Promise<Seeded> => {
  const gpu4090 = await region.call<{ id: string }>("POST", "/admin/gpus", admin, { name: "NVIDIA GeForce RTX 4090", vram_mb: 24564 })
  const gpu3090 = await region.call<{ id: string }>("POST", "/admin/gpus", admin, { name: "NVIDIA GeForce RTX 3090", vram_mb: 24576 })
  const test = { method: "GET", path: "/v1/models", expect_status: 200, expect_contains: ["data"], timeout_ms: 5000 }
  const vllm = await region.call<{ id: string }>("POST", "/admin/apps", admin, {
    name: "vllm/qwen2.5-7b-instruct", mode: "single-shot", price: { price: "0.50", unit: "hour" }, required_version: "1.0.0",
    container: { image: image("vllm/vllm-openai", "a1"), command: ["--model", "Qwen/Qwen2.5-7B-Instruct"], env: { HF_HOME: "/cache" }, port: 8000, health_path: "/health", gpus: 1, shm_size: "8g", cache_path: "/cache" },
    eligible_gpus: [gpu4090.id, gpu3090.id], default_capacity: 2, max_capacity: 4, test,
    discovery: "OpenAI-compatible chat completions on Qwen 2.5 7B Instruct.",
    description: "A 7-billion-parameter instruction model served with vLLM. Fits a 24 GB card with room for four concurrent chats.",
  })
  const flux = await region.call<{ id: string }>("POST", "/admin/apps", admin, {
    name: "flux/schnell", mode: "single-shot", price: { price: "0.002", unit: "fixed" }, required_version: "2.1.0",
    container: { image: image("blackforestlabs/flux-schnell", "b2"), port: 8000, health_path: "/health", gpus: 1, cache_path: "/models" },
    eligible_gpus: [gpu4090.id], default_capacity: 1, max_capacity: 2, test,
    discovery: "Text to image with FLUX.1 schnell.",
    description: "Four-step image generation. Needs an RTX 4090 or better.",
  })
  await region.call("POST", "/admin/terms", admin, {
    scope: "global",
    body: "Open Pool terms\n\n1. By applying you accept these terms.\n2. A member can be banned for any reason at any time. Malicious use is an immediate ban.\n3. The agent you install has access to your Docker daemon.\n4. Earnings are paid per payout window to the address you applied with.",
  })
  if (withRegionalTerms) {
    await region.call("POST", "/admin/terms", admin, { scope: "regional", body: `${region.name} terms\n\nMachines in this region must be located in it.` })
  }
  return { gpu4090: gpu4090.id, gpu3090: gpu3090.id, vllm: vllm.id, flux: flux.id }
}

interface Application {
  readonly id: string
}

/** A person applies with one machine, as the member site does: the region writes the message, the wallet signs it. */
export const apply = async (region: Region, account: PrivateKeyAccount, host: string, name: string, machine: { label: string; gpu: string; count: number }) => {
  const token = await walletToken(region, account, host)
  const draft = { display_name: name, machines: [{ label: machine.label, gpus: [{ gpu_id: machine.gpu, count: machine.count }] }] }
  const { message } = await region.call<{ message: string }>("POST", "/applications/message", token, draft)
  const application = await region.call<Application>("POST", "/applications", token, { ...draft, signature: await account.signMessage({ message }) })
  return { token, application }
}

/**
 * Fills a region with what a running pool has: an approved member whose
 * machine is connected and earning, an applicant waiting, a closed payout
 * window waiting for approval and an open one with earnings in it.
 */
export const seedRegion = async (region: Region, host: string, options: { regionalTerms: boolean; aliceFeeWei: string }) => {
  const admin = await adminToken(region)
  const catalog = await seedCatalog(region, admin, options.regionalTerms)

  // Alice is approved, with her machine assigned the chat app, and connects it.
  const { token: aliceToken, application } = await apply(region, alice, host, "Alice", { label: "garage-4090", gpu: catalog.gpu4090, count: 2 })
  const member = await region.call<{ id: string }>("POST", `/admin/applications/${application.id}/approve`, admin, {
    machines: [{ machine: 0, assignments: [{ app_id: catalog.vllm }] }],
  })
  const { machines } = await region.call<{ machines: Array<{ id: string }> }>("GET", "/me/machines", aliceToken)
  const machine = machines[0]
  if (!machine) throw new Error("Alice has no machine after approval")
  const credential = await region.call<{ secret: string }>("POST", `/me/machines/${machine.id}/credential`, aliceToken)
  await region.call("POST", "/agent/report", credential.secret, {
    agent_version: "0.1.0", generation: "seeded",
    containers: [{ service: "app-seeded", state: "running", health: "healthy" }, { service: "tunnel-seeded", state: "running" }],
    gpus: [{ name: "NVIDIA GeForce RTX 4090", memory_mb: 24564 }, { name: "NVIDIA GeForce RTX 4090", memory_mb: 24564 }],
  })
  const runner = `runner_${region.id}_alice`
  await region.admit(credential.secret, { runner_id: runner, app: "vllm/qwen2.5-7b-instruct", version: "1.0.0" })
  // The pool tests the new runner and, as it passes, lets it serve.
  await eventually(`${region.id} to promote Alice's runner`, async () => region.orchestrator.runners.find((r) => r.runner_id === runner)?.state === "active")

  // A first payout window: Alice earns, the node redeems tickets, the window closes.
  const first = region.orchestrator.round
  await region.paidSession({ runner, member: member.id, app: "vllm/qwen2.5-7b-instruct", round: first, feeWei: options.aliceFeeWei })
  await region.paidSession({ runner, member: member.id, app: "vllm/qwen2.5-7b-instruct", round: first + 1, feeWei: options.aliceFeeWei })
  await region.events([{ event_type: "orchestrator_ticket_redeemed", face_value_wei: "50000000000000000", tx_hash: "0x" + "11".repeat(32), round: first + 1, sender: "0x00000000000000000000000000000000000000c1" }])
  region.orchestrator.round = first + 7
  await eventually(`${region.id} to close its first payout window`, async () => {
    const { batches } = await region.call<{ batches: Array<{ status: string }> }>("GET", "/admin/batches", admin)
    return batches.some((b) => b.status === "ready")
  })
  // The window now running has earnings too, and a request the pool could not serve.
  await region.paidSession({ runner, member: member.id, app: "vllm/qwen2.5-7b-instruct", round: first + 7, feeWei: "3000000000000000" })
  await region.events([{ event_type: "live_runner_requests_refused", app: "flux/schnell", count: 4, round: first + 7 }])

  // Bob has applied and is waiting for the operator.
  await apply(region, bob, host, "Bob", { label: "rack-3090", gpu: catalog.gpu3090, count: 1 })

  return { admin, catalog, aliceMember: member.id, aliceMachine: machine.id, aliceRunner: runner }
}
