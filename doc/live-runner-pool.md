# Change request: a public GPU pool for Live Runners (revision 2)

**Status:** draft, 2026-10-01. Revises the 2026-09-30 draft (`go-livepeer-live-runner-pool.md` in the BlueClaw platform repository).\
**Repository:** this fork, branch `feat/open-pool-enhancements`, inspected at `5ed374e6` (v0.9.3 plus the Live Runner tunnel). Line numbers refer to that commit.\
**Reader:** the engineer building these changes in go-livepeer, and later the engineer building the pool service.

## What changed from the first draft

| # | First draft | This revision | Why |
| --- | --- | --- | --- |
| a | Change 7 (reverse tunnel) specified with a `transport` field and custom framing | Already built on this branch: `tunnel: true`, yamux over a WebSocket, `cmd/livepeer_runner_tunnel` | Cherry-picked from the BlueClaw fork; see `doc/live-runner.md`, "Tunneled runners" |
| b | Members may register a `runner_url` or use a tunnel; the Python SDK registers runners | Pool runners register only with `livepeer_runner_tunnel`; the orchestrator refuses a `runner_url` from them | A member-supplied URL lets a member point the orchestrator's proxy at internal addresses, including its own CLI port |
| c | `-orchSecret` still registers the operator's runners, skipping the webhook | With the webhook on, every dynamic registration goes through the webhook; `-orchSecret` registers nothing | `-orchSecret` is one value shared with every remote transcoder and AI worker; a leak would admit untested, active runners |
| d | The probe presents `-orchSecret` in `Authorization` | A separate `-liveRunnerPoolSecret`, shared only with the pool service, presented in a `Livepeer-Probe-Secret` header | Same leak concern; and customers of OpenAI-style apps already send `Authorization`, which is forwarded to the app |
| j | The webhook call carries no authentication | The orchestrator sends `-liveRunnerPoolSecret` as a bearer token on every webhook call | The webhook receives member credentials; without this anyone who can reach it could test credentials against it |
| k | The pool service is a separate repository, without payments or a UI | It lives in this repository under `pool/` as its own Go module, and includes on-chain payouts and a web UI | Decided 2026-10-01; kept separable so it can move out later |
| e | Only the price is pinned after registration | Everything a runner was admitted with is frozen; only `status`, `session_ids` and a lower `capacity` may change | Every heartbeat replaces the runner's record (`live_runner.go:468`), so a runner could pass probation as one app and become another |
| f | A runner may choose its `runner_id` | The orchestrator always generates the id for pool runners | Stops a member choosing an id that shadows something else, and makes "registration" unambiguous |
| g | App routing shares `/apps/{name}/…` with runner ids | App routing has its own prefix, `/pool/{app}/…`, with the app name base64url-encoded | App names contain `/`; a separate prefix needs no "runner or app?" guess |
| h | Operator runners may price an unconfigured app themselves | Every pool runner needs a configured price for its app | Follows from (c): there is no operator exception left |
| i | Kafka delivery flags assumed present | Commit `0777f2be` from the BlueClaw fork is cherry-picked first | It adds `-kafkaRequireDelivery`, `-kafkaEnqueueTimeout`, the queue reservation API and the test writer that change 6 uses |

Error responses on the Live Runner endpoints are the error text as a plain-text body, as they are today; where this document writes `403 unknown credential`, that is the status and the body.

## What we are building

Let GPU owners outside the operator's control ("pool members") attach Live Runners to an orchestrator, be tested before they take real work, be removed when they misbehave, and have every fee their runners earn recorded against them, so that a separate pool service can pay them a share of what the orchestrator redeems.

| # | Change | Where |
| --- | --- | --- |
| 0 | Cherry-pick `0777f2be` (Kafka delivery guarantees) | `monitor/kafka.go`, `monitor/census.go`, starter |
| 1 | Admission webhook: a pool-issued credential, checked by the pool service, which returns the member identity | `LiveRunnerRegistry.Heartbeat` |
| 2 | Orchestrator-set price per app | `normalizeHeartbeat`, a price table |
| 3 | Runner states `probation`, `active`, `suspended`, and an unpaid probe session | registry, the two reservation handlers |
| 4 | Administrative endpoints on the CLI port | `cliWebServerHandlers` |
| 5 | App-level routing under `/pool/{app}/…` | registry, new routes, discovery |
| 6 | Kafka events for registration, state, sessions and every fee debit | `monitor`, `reservePaidLiveRunnerSession`, registry |
| 7 | Reverse tunnel | done |
| 8 | Runner command: credential flag, pool error handling | `cmd/livepeer_runner_tunnel`, `ai/runner/tunnelclient` |

Build order: 0, then 1, 2, 3, 4, 6 and 8 (first milestone: a tunneled member runner joins, is probed, goes active, earns, and its fees appear as events), then 5.

"Pool mode" below means `-liveRunnerAuthWebhookUrl` is set. With none of the new flags set, the orchestrator behaves exactly as it does at `5ed374e6`.

Transcoding and the `-aiWorker` path are out of scope.

## Why

The branch already has most of a pool. A Live Runner registers with `POST /runners/heartbeat`, is issued a per-runner heartbeat secret, is dropped after its heartbeat TTL, and is reached through the orchestrator's proxy, over a tunnel the runner opens. A paid session is reserved on one runner, and each accounting tick lands in `livePaymentReceiver.AccountPayment` (`server/live_payment.go:156`).

What is missing is what turns a trusted internal runner into an accountable external one:

| Gap | Today |
| --- | --- |
| Runner identity | The first heartbeat is authenticated with the one shared `-orchSecret` (`live_runner.go:403-411`). No member field. |
| Admission policy | None. |
| Price control | The runner sends `price_info` and the orchestrator accepts it (`live_runner.go:915`). |
| Test before work | None; a runner is discoverable as soon as it is ready. |
| Removal | Only expiry and self-unregister (`live_runner.go:963`, `:1436`). |
| Integrity after admission | Every heartbeat replaces the runner's whole record (`live_runner.go:468`). |
| Per-runner accounting | The runner-to-session binding lives only in memory. No orchestrator-side event carries value. |
| Routing | One runner per route. The client picks a `runner_id`. |

Membership, approval, scoring, tallying and payout do not belong in go-livepeer. They go in a separate pool service that answers the webhook, consumes the events, and drives the orchestrator through the CLI-port endpoints. go-livepeer keeps no membership state: what it knows about a member arrives in the webhook response and dies with the runner.

## Concepts

- **Member:** an approved participant, identified by `member_id`, with a `payout_address` and a runner limit. The operator's own GPUs join the same way, under whatever `member_id` the pool service gives them.
- **Runner:** one `livepeer_runner_tunnel` process, serving one `app`, belonging to one member.
- **Credential:** an opaque string the pool service issues. The runner command presents it in `Authorization` on its registration heartbeat. Whether it is issued per member or per runner is the pool service's choice; go-livepeer only forwards it.
- **State:** `probation` (registered, invisible to customers, reachable only by a probe), `active` (takes real work), `suspended` (keeps its registration, takes no new work). The runner's own `status` (`ready` or not) is separate: it says whether the runner is available; the state says whether the orchestrator will use it.
- **Probe:** an unpaid session the pool service opens on one named runner to test it.

State is set by the webhook at registration and changed through the CLI port. When a runner expires and registers again, the webhook is asked again; the pool service is the durable record.

## Change 0: Kafka base

Cherry-pick `0777f2be` ("signer: Leave evidence when create_signed_ticket events are dropped") from `blueclaw/feat/blueclaw-enhancements`. It conflicts with `monitor/census.go` only where the tunnel metrics now sit beside its remote-signer metrics; keep both.

## Change 1: admission webhook

New flags:

- `-liveRunnerAuthWebhookUrl` (string, default empty). Empty: registration works as today.
- `-liveRunnerAuthWebhookTimeout` (duration, default `5s`).
- `-liveRunnerPoolSecret` (a value or a file path, read as `-orchSecret` is; default empty). The secret the orchestrator and the pool service share. The orchestrator sends it as `Authorization: Bearer <secret>` on webhook calls, and accepts it from the pool service for probes (change 3).

`-useLiveRunners` today requires `-orchSecret` (`starter.go:2058`); it must accept the webhook URL instead.

In pool mode:

**What counts as a registration.** A heartbeat with an empty `runner_id` is a registration. A heartbeat carrying a `runner_id` the registry does not hold gets `404 runner not found` and never reaches the webhook. Today such a heartbeat falls into the registration branch with the supplied id (`live_runner.go:395-399`).

**Checks on a registration, in order:**

1. Empty `Authorization` → `401 registration requires a credential`. The webhook is not called.
2. The heartbeat is normalized. It must carry `tunnel: true`; otherwise `400 pool runners must register with a tunnel`.
3. The orchestrator generates the `runner_id` and calls the webhook, without holding the registry lock.
4. Deny → `403 <reason>`. Webhook unreachable, non-200, timeout or malformed body → `503 admission unavailable`. Fail closed.
5. Member at its runner limit → `429 member runner limit`. Checked under the registry lock after the webhook returns, so two concurrent registrations cannot both pass.
6. The runner's `app` has no configured price (change 2) → `400 app not offered`. This is after the webhook so the pool service sees the attempt.
7. The runner is created with the webhook's `member_id`, `payout_address` and `state`.

Webhook request, `POST`, `Content-Type: application/json`, `Authorization: Bearer <pool secret>`:

```json
{
  "event": "register",
  "orch_address": "0x1234…abcd",
  "remote_addr": "203.0.113.7",
  "credential": "<the Authorization header value>",
  "runner": {
    "runner_id": "runner_3f9a…",
    "tunnel": true,
    "label": "",
    "version": "vllm-app:1.4.2",
    "metadata": "",
    "status": "ready",
    "mode": "single-shot",
    "app": "vllm/qwen2.5-7b-instruct",
    "capacity": 2
  }
}
```

Response `200`:

```json
{
  "allow": true,
  "member_id": "mbr_01J…",
  "payout_address": "0xabcd…1234",
  "max_runners": 4,
  "state": "probation"
}
```

or `{ "allow": false, "reason": "unknown credential" }`.

Rules:

- `member_id` is required on allow (non-empty, at most 128 bytes). `payout_address` may be empty. `max_runners` defaults to 1. `state` defaults to `probation` and must be one of the three states.
- `max_runners` counts the member's registered, non-removed runners on this node, across all apps. A pool-wide limit across several orchestrators is the webhook's to enforce.
- The response values are stored on the runner for its lifetime. A runner cannot assert its own member or state.
- Later heartbeats authenticate with the per-runner heartbeat secret as today; the webhook is not called again for that runner.
- The credential is never logged. Log `member_id`, `runner_id`, `app`, `remote_addr` and the decision.

**Frozen fields.** After registration a pool runner's heartbeat may change only `status`, `session_ids`, and `capacity` (to any value from 1 up to the capacity it registered with). A heartbeat whose `app`, `mode`, `version`, `label`, `proxy`, `metadata`, `gpu` or `tunnel` differs from the registered value gets `400 <field> cannot change after registration`; a capacity above the registered one gets `400 capacity cannot exceed the registered capacity`. `price_info` in a heartbeat is ignored (change 2). To change a frozen field the runner unregisters and registers again, which starts over at the webhook.

**Static runners** (`-liveRunnerConfig`, `POST /registerLiveRunners`) are untouched. They are the operator's own configuration, use no secret, and are reported as `member_id = "operator"`, `state = "active"`, `static: true`. They are not a pool feature.

## Change 2: orchestrator-set prices

New flag `-liveRunnerPrices` (path to a JSON file, default empty):

```json
{
  "apps": {
    "vllm/qwen2.5-7b-instruct": { "price": "0.50", "currency": "usd", "unit": "hour" },
    "echo":                     { "price": "0.001", "currency": "usd", "unit": "fixed" }
  }
}
```

Values are the existing `LiveRunnerPriceInfo` shape and go through `normalizeLiveRunnerPriceInfo`. The file is read at startup and replaced at runtime by `POST /liveRunner/prices` (change 4).

Rules:

- A runner whose `app` is in the table is priced from the table; the heartbeat's `price_info` is discarded before it is validated, so a pool runner need not send one. This applies to every runner of that app, static ones included, so an app in the table has exactly one price.
- A pool runner may register only an app that is in the table (change 1, step 6).
- The effective price is returned in a new heartbeat response field `price_info`, present only when a configured price applied.
- A price change takes effect for new sessions only; a running session keeps the price it was reserved at (`liveRunnerSession.priceInfo`). A reload updates registered runners at once, not at their next heartbeat, because static runners do not heartbeat.
- **Removing an app from the table:** its registered runners keep the price they have and stay reachable by `runner_id`; new registrations for it are refused; its `/pool/…` route (change 5) answers `404`. The operator evicts the runners if they should go.
- Off-chain orchestrators skip prices as today, but the "app not offered" check still applies, so admission can be tested off-chain.

`/discovery`, `PaymentInfo` and the 402 challenge read the runner's price from the registry and need no change.

## Change 3: runner states and the probe

Add `state` to `liveRunner`.

| | `probation` | `active` | `suspended` |
| --- | --- | --- | --- |
| Listed by `GET /discovery` | no | yes | no |
| Candidate for `/pool/{app}/…` (change 5) | no | yes | no |
| Paid reservation by `runner_id` | `404 runner not found` | yes | `409 runner suspended` |
| Probe | yes | yes | yes |
| Heartbeats, tunnel, sessions already running | unchanged | unchanged | unchanged; existing sessions finish |

A `probation` runner answers `404` to a non-probe request before any other check, so its existence and mode are not revealed. Today `RunnerMode` runs first and would answer `400` for a wrong-mode request (`server/ai_http.go:303-311`).

**The probe.** A probe is `POST /apps/{runner_id}/session` (persistent) or a request to `/apps/{runner_id}/app/{path}` (single-shot) carrying the header `Livepeer-Probe-Secret` with the value of `-liveRunnerPoolSecret`.

- Header present and the flag unset, or the value wrong → `401`. A request with the header never falls through to the paid path.
- Header valid: the 402 challenge and the payment ticker are skipped; no `ProcessPayment`, no `AccountPayment`. The session is otherwise ordinary: it counts against `capacity`, gets a session token, is proxied, and is released the same ways.
- The header is rejected with `400` on `/pool/…` routes: a probe names its runner.
- The session event (change 6) carries `paid: false` and an empty `sender`.

**The runner must not be able to tell a probe from real work.** The proxy forwards every inbound header to the runner (`server/ai_http.go:940-953`). Before forwarding to a pool runner it must remove `Livepeer-Probe-Secret` (otherwise members learn the secret), and `Livepeer-Payment` and `Livepeer-Segment` (otherwise their absence marks a probe, and a runner could behave well only when tested).

go-livepeer does not define or run tests. The pool service uses the probe to run the operator's per-app test through the same path a customer takes.

## Change 4: administrative endpoints on the CLI port

Registered in `cliWebServerHandlers` (`server/webserver.go`), beside `POST /registerLiveRunners`. They carry no authentication, like every CLI-port endpoint, so the CLI port must stay on loopback or a private network that only the pool service can reach. All responses are JSON.

`GET /liveRunner/runners` — every registered runner in every state:

```json
{
  "runners": [
    {
      "runner_id": "runner_3f9a…",
      "member_id": "mbr_01J…",
      "payout_address": "0xabcd…1234",
      "state": "active",
      "status": "ready",
      "app": "vllm/qwen2.5-7b-instruct",
      "mode": "single-shot",
      "version": "vllm-app:1.4.2",
      "capacity": 2,
      "capacity_registered": 2,
      "capacity_used": 1,
      "price_info": { "price": "0.50", "currency": "usd", "unit": "hour" },
      "tunnel": true,
      "tunnel_connected": true,
      "remote_addr": "203.0.113.7",
      "registered_at_ms": 1790000000000,
      "last_heartbeat_ms": 1790000012000,
      "session_ids": ["9d1f0c1a5b2e4c7d"],
      "static": false
    }
  ]
}
```

`POST /liveRunner/setState` — form fields `runner_id`, `state`, optional `reason`. `204`; `404` unknown runner; `400` bad state. Leaving `active` does not release running sessions.

`POST /liveRunner/evict` — form fields `runner_id` **or** `member_id`, optional `reason`. Removes the runner, or all of a member's runners, through the existing removal path (`removeRunnerWithRunnerLocked`, `live_runner.go:1515`): tunnel closed, sessions released, heartbeat secret invalid. `204`; `404` when nothing matched. A re-registration goes back through the webhook, which is where a ban lives.

`POST /liveRunner/prices` — body is the `-liveRunnerPrices` JSON; replaces the whole table atomically. `204`, or `400` with the first validation error and no change.

Every call emits the matching event of change 6 with `reason` copied through.

## Change 5: app-level routing

Today one route maps to one runner. A pool has many runners per app, and the customer should ask for the app.

New routes, beside the existing `/apps/{runner_id}/…` routes, which keep their meaning:

| Route | Does |
| --- | --- |
| `POST /pool/{app}/session` | Selects a persistent runner of the app and reserves a session on it |
| `POST /pool/{app}/session/{session_id}/payment` | Follow-up payment; resolves the runner holding that session |
| `/pool/{app}/app/{app_path...}` | Selects a single-shot runner of the app, per request, and proxies to it |

`{app}` is the app name encoded as base64url without padding (RFC 4648 §5), so `vllm/qwen2.5-7b-instruct` is `dmxsbS9xd2VuMi41LTdiLWluc3RydWN0`. Bad encoding → `400`. The orchestrator logs the decoded name. Clients do not build this by hand: discovery returns the URL.

Rules:

- Only an app in the price table is routable; otherwise `404 app not offered`.
- Selection: among runners with that `app`, the route's mode, `state == active`, ready, not expired, and a free slot: the fewest active sessions, ties broken uniformly at random. No candidate → `503 no capacity for app`. Selecting and reserving happen together, so two requests cannot take the same last slot.
- The 402 challenge quotes the app's configured price. Its `payment_url` is `/pool/{app}/session/{session_id}/payment`, because no runner is chosen until the paid retry arrives. The existing client uses the challenge's `payment_url` for later payments, so that route must resolve the session's runner.
- The session response gains `runner_id`; `control_url` and `app_url` are built on `/apps/{runner_id}/…` as today, so stop, channels and the session proxy are unchanged.

`GET /discovery` gains an `apps` array beside `runners` (which lists `active` runners only, in today's shape). One entry per app and mode:

```json
{
  "runners": [ … ],
  "apps": [
    {
      "app": "vllm/qwen2.5-7b-instruct",
      "url": "https://orch.example:8935/pool/dmxsbS9xd2VuMi41LTdiLWluc3RydWN0/app",
      "mode": "single-shot",
      "price_info": { "price": "0.50", "currency": "usd", "unit": "hour" },
      "runners": 5,
      "capacity": 10,
      "capacity_available": 7
    }
  ]
}
```

## Change 6: Kafka events

New flag `-kafkaOrchestratorTopic` (string, default empty). Events go to it through the existing producer, which today starts only when `-kafkaGatewayTopic` is set (`cmd/livepeer/starter/kafka.go:9`) and must start for either topic. With the topic empty nothing is emitted. One topic for all of an operator's nodes; the pool service tells them apart by `orch_address`.

Every event is a JSON object with this envelope plus the type's fields. Messages are keyed by `runner_id` so one runner's events stay in order.

```json
{
  "event_id": "5b0c…",
  "event_type": "live_runner_fee_debited",
  "orch_address": "0x1234…abcd",
  "ts_ms": 1790000012345,
  "runner_id": "runner_3f9a…",
  "member_id": "mbr_01J…",
  "app": "vllm/qwen2.5-7b-instruct"
}
```

`event_id` is a UUID, so the pool service can drop a redelivered message.

| `event_type` | Emitted when | Fields |
| --- | --- | --- |
| `live_runner_registered` | registration accepted | `payout_address`, `state`, `mode`, `capacity`, `version`, `remote_addr`, `price_info`, `tunnel`, `static` |
| `live_runner_state_changed` | `setState` | `from`, `to`, `reason` |
| `live_runner_removed` | runner leaves the registry | `reason`: `expired`, `unregistered`, `evicted`; `evict_reason` when evicted |
| `live_runner_session_reserved` | session reserved | `session_id`, `sender` (payer address, or `""`), `paid`, `price_info`, `via`: `runner` or `pool` |
| `live_runner_session_released` | session released | `session_id`, `duration_ms`, `reason`: `completed`, `stopped`, `payment_failed`, `runner_removed`, `runner_unhealthy`, `error`; `units_total`, `fee_wei_total` |
| `live_runner_fee_debited` | each successful debit for a live-runner session | `session_id`, `sender`, `units`, `fee_wei`, `balance_wei` |

Notes for the implementer:

- `AccountPayment` has no runner context and returns without debiting when the price is zero. Emit the fee event at its two call sites in `reservePaidLiveRunnerSession` (`server/ai_http.go:403`, `:422`), which know the runner, only after a debit happened.
- `releaseSessionLocked` (`live_runner.go:1534`) takes no reason today; the release reason has to be threaded through its callers (`live_runner.go:803`, `:1070`, `:1495`, `:1526` and the `ReleaseSession` calls in `server/ai_http.go`).
- `units_total` and `fee_wei_total` are kept on `liveRunnerSession` and are the second copy of the session's value: if per-tick events are lost, the pool service can settle from the released event. Fees are rationals; totals are summed exactly and each emitted value is rounded once to a whole-wei decimal string.

Delivery: `SendQueueEventAsync` by default; `ReserveQueueEvent` with `-kafkaEnqueueTimeout` when `-kafkaRequireDelivery` is set (that flag's startup check must accept the orchestrator topic). A failed enqueue never fails the session or the debit, since the payment has already been received. It increments a new `livepeer_orchestrator_events_dropped_total{event_type}` counter and logs `session_id`, `runner_id`, `member_id` and `fee_wei` at error level.

## Change 7: reverse tunnel

Built: `ai/runner/live_runner_tunnel.go`, `ai/runner/tunnelconn`, `ai/runner/tunnelclient`, `cmd/livepeer_runner_tunnel`, the route `GET /runners/{runner_id}/tunnel`. A tunneled runner is ready only while its tunnel is connected (`readyLocked`). Nothing in the tunnel depends on the runner's state, so a `probation` runner holds a tunnel and can be probed.

## Change 8: the runner command

`cmd/livepeer_runner_tunnel` is the only supported way to join the pool, for plain HTTP apps and live-video apps alike.

- New flag `-credential` (a value or a file path). `-orchSecret` stays for orchestrators without pool mode; exactly one of the two is given. Both go in the registration heartbeat's `Authorization`.
- `-price`, `-currency` and `-unit` become optional; a pool orchestrator ignores them. The command logs the `price_info` the heartbeat response returns.
- A refused registration logs the orchestrator's `error` text with the status (`403` denied, `429` limit, `400` app not offered, `503` admission unavailable) and keeps retrying with the existing backoff. Today it logs only the status code.

**Live-video apps.** The command registers with `-mode persistent`. Each request the app receives carries `Livepeer-Session-Id`, `Livepeer-Session-Token` and `Livepeer-Session-Control`; the app uses the token to create its media channels and connects outbound to the orchestrator for media. That part needs no registration and works behind the command. For remote members `-liveRunnerAddr` must be a public address, because the control URL and the internal media URLs handed to the app are built on it.

## Boundary: the pool service

The pool service lives in this repository under `pool/`, as its own Go module, so neither side can import the other and it can move to its own repository later. It talks to the orchestrator only through the webhook it answers, the CLI-port endpoints, the probe, and Kafka. Its design is specified separately; in outline it owns:

- Members: application, approval, credentials, `payout_address`, runner limits, blessed app versions, bans. It answers the admission webhook, including for the operator's own GPUs.
- Tests: the per-app probe (a request, an expected response, a latency bound), run with the probe secret at onboarding and periodically; it moves runners between states through `/liveRunner/setState`.
- Scoring: consumes the change-6 events and suspends or evicts.
- Tally and payout: consumes `live_runner_fee_debited` (falling back to `fee_wei_total` on the released event), sums fees per member per window, and pays each member its share of what the orchestrator redeemed in that window, on-chain, from a payout wallet.
- A web UI for the operator and for members.

go-livepeer persists none of it. If a runner registers again after an orchestrator restart, the webhook supplies the member and state again.

## Acceptance tests

Go tests beside `ai/runner/live_runner_test.go` and `server/ai_http_test.go`, with an `httptest` webhook and the test Kafka writer from change 0.

1. **No flags, no change.** With none of the new flags set, the existing tests pass unmodified: `-orchSecret` registers, a runner may choose its id and a `runner_url`, heartbeats may change any field, and responses carry no new fields.
2. **Webhook allow.** A registration with credential `c1` posts the documented body with a generated `runner_id`; on allow with `member_id m1`, `state probation`, the runner is registered, absent from `/discovery`, and shown by `GET /liveRunner/runners` as `m1`/`probation`. A second heartbeat with the heartbeat secret does not call the webhook.
3. **Webhook deny and outage.** Deny → `403` with the reason and no runner; timeout → `503 admission unavailable`; empty `Authorization` → `401` and no webhook call; `-orchSecret` as the credential is forwarded like any other and registers nothing unless the webhook allows it.
4. **Registration shape.** A heartbeat with an unknown non-empty `runner_id` → `404` and no webhook call; a supplied id is never adopted; a registration with a `runner_url` and no tunnel → `400`.
5. **Runner limit.** With `max_runners: 2`, the third runner for `m1` gets `429`; after one expires the next registers; two concurrent registrations at limit minus one admit exactly one.
6. **Frozen fields.** A later heartbeat changing `app`, `mode`, `version` or `metadata` → `400` naming the field and the record is unchanged; `capacity` may drop and return to the registered value but not exceed it; `status` changes are accepted.
7. **Price override.** A pool runner for a configured app is registered at the table price whatever it sends, the response echoes it, and the 402 challenge uses it. An unconfigured app → `400 app not offered` after the webhook was called.
8. **States.** `setState active` makes the runner discoverable; `suspended` hides it, a paid reservation gets `409`, and a running session continues and releases normally; `probation` gets `404` for a paid reservation of either mode.
9. **Probe.** With a valid `Livepeer-Probe-Secret` and no payment headers, a `probation` runner's session is reserved without a 402, counts against capacity, proxies, and is released; the reserved event has `paid: false`. Wrong secret, or the header with the flag unset → `401`. The runner receives neither the probe header nor, on a paid request, the payment headers. A customer `Authorization` header still reaches the app. Single-shot equivalent.
10. **Evict.** `evict` by `member_id` removes both of `m1`'s runners, closes their tunnels, releases their sessions, invalidates their heartbeat secrets, and emits two `live_runner_removed` events with `reason evicted`.
11. **Prices reload.** `POST /liveRunner/prices` changes the next challenge and not a running session's price; an invalid body → `400` and no change; removing an app refuses new registrations for it and leaves its runners reachable by id.
12. **Events.** A paid hour-priced session with three accounting ticks emits `reserved`, three `fee_debited` whose `fee_wei` equal the debited fees, and `released` with matching totals; all carry the same `orch_address`, `runner_id`, `member_id`, `session_id`, and distinct `event_id`s. With a full queue and `-kafkaRequireDelivery` off, the session succeeds and the drop counter increments.
13. **App routing.** Three `active` runners for app `a` with capacities 1, 2, 2: reservations through `/pool/<a>/session` land on the least-loaded runner each time, the sixth gets `503`, the response carries `runner_id`, a `probation` fourth runner is never chosen, and a follow-up payment to the challenge's `payment_url` reaches the right session. `/discovery` lists `apps[a]` with `runners 3`, `capacity 5` and a decreasing `capacity_available`. An app with a `/` in its name round-trips through the encoded route. A bad encoding → `400`; an unconfigured app → `404`.
14. **Command.** `livepeer_runner_tunnel -credential` registers against a pool-mode orchestrator without `-price`, logs the echoed price, logs the reason on a `403`, and keeps retrying.
15. **Box.** The `box` tunnel harness gains a stub webhook: a runner joins in `probation`, a probe succeeds, `setState active` lists it, a paid call succeeds, and `evict` removes it.

## Compatibility

- Every change is behind a flag or a new field. No new flag → identical behavior.
- Pool mode changes registration for every dynamic runner on that orchestrator: `-orchSecret` no longer registers Live Runners, and URL-registered dynamic runners are refused. Static runners are unaffected.
- No schema changes; go-livepeer stores nothing new on disk.

## Deferred and known limits

- **Session events to the app.** The orchestrator tells a runner when a session is reserved and released over a small channel (`o2r`). The command does not read it, so an app behind the command learns a session ended only when its media channels close. Forwarding these events to the app is a later addition to the command.
- **TLS verification.** The command defaults to `-insecure true` because orchestrators self-sign, so a member's credential is sent without verifying the server.
- **Capacity.** v1 trusts the registered `capacity`, checked only by the pool service's probe.
- **Attestation.** `version` is self-reported.
- **Customer data.** An untrusted runner sees customer inputs and outputs and can return bad results; probing and scoring are the only defence.
- **Customer-side clients.** Anything that calls the orchestrator needs to read `apps` from `/discovery` and `runner_id` from the session response to use change 5. Not needed for the first milestone.
- **Python SDK runner registration.** Not supported for the pool.
