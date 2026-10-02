# Unreleased Changes

## v0.X.X

### Breaking Changes 🚨🚨

### Features ⚒

#### General

- Remote signer: leave evidence when a `create_signed_ticket` event is dropped (`livepeer_remote_signer_events_dropped_total` counter, an error log with the ids needed to reconstruct the record) and expose `livepeer_remote_signer_event_queue_length`. Add `-kafkaRequireDelivery` and `-kafkaEnqueueTimeout` to refuse signing when the event cannot be enqueued.

#### Broadcaster

#### Orchestrator

- Live Runners can register with `tunnel: true` and receive their work over a connection they open (`GET /runners/{id}/tunnel`, WebSocket carrying a yamux session), so a runner needs no public address; the new `livepeer_runner_tunnel` command is the runner side for any plain HTTP app.
- Live Runner pool mode: with `-liveRunnerAuthWebhookUrl` set, every dynamic Live Runner registration is decided by a pool service's admission webhook, which returns the member the runner belongs to and the state it starts in (`probation`, `active` or `suspended`). In pool mode `-orchSecret` registers no Live Runner, runners must register with a tunnel, the orchestrator assigns runner ids, and a runner cannot change what it was admitted as. `-liveRunnerPoolSecret` identifies the orchestrator to the webhook. See `doc/live-runner-pool.md`.
- `-liveRunnerPrices` sets the operator's Live Runner price per app. A runner of a listed app is offered at that price whatever its heartbeat asks for, and the heartbeat response reports it in `price_info`. In pool mode a runner may only register a listed app.
- Live Runner probes: a request to `/apps/{runner_id}/session` or `/apps/{runner_id}/app/…` carrying `Livepeer-Probe-Secret: <-liveRunnerPoolSecret>` opens an unpaid session on that runner in any state, so the pool service can test a runner before and while it takes work. In pool mode the proxy no longer forwards the payment headers to runners, so a runner cannot tell a probe from paid work.
- Live Runner administration on the CLI port, for the pool service: `GET /liveRunner/runners` lists every runner in every state with its member, `POST /liveRunner/setState` moves a runner between `probation`, `active` and `suspended`, `POST /liveRunner/evict` removes a runner or all of a member's runners, and `POST /liveRunner/prices` replaces the price table.
- Orchestrator events: with `-kafkaOrchestratorTopic` set, the orchestrator publishes `live_runner_registered`, `live_runner_state_changed`, `live_runner_removed`, `live_runner_session_reserved`, `live_runner_session_released` and `live_runner_fee_debited` events, each naming the runner, its member and its app, so a pool service can record what each member's runners earn. A session's released event carries the totals of its fee events. Dropped events are counted in `livepeer_orchestrator_events_dropped_total`; `-kafkaRequireDelivery` makes an event wait up to `-kafkaEnqueueTimeout` for room in a full queue.
- `livepeer_runner_tunnel` joins a pool with `-credential` in place of `-orchSecret` and needs no `-price` there; it logs the price the orchestrator offers the runner at, and the orchestrator's reason when a registration is refused.
- Live Runner app-level routing: a customer can ask for an app instead of a runner. `POST /pool/{app}/session` and `/pool/{app}/app/…`, where `{app}` is the app name base64url-encoded, pick the least-loaded active runner of an app the operator has priced; the session response names the chosen runner in `runner_id`, and `GET /discovery` lists the apps with their URL, price and capacity in `apps`.
- Orchestrator events carry `round`, the current round. Two new events on `-kafkaOrchestratorTopic`: `orchestrator_ticket_redeemed`, emitted when the node redeems a winning ticket, with its face value, and `live_runner_requests_refused`, a count per app of requests answered `503` for lack of capacity.

#### Transcoder

### Bug Fixes 🐞

#### General

#### Broadcaster

#### CLI
