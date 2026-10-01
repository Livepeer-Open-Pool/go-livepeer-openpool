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

#### Transcoder

### Bug Fixes 🐞

#### General

#### Broadcaster

#### CLI
