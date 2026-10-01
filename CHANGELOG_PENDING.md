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

#### Transcoder

### Bug Fixes 🐞

#### General

#### Broadcaster

#### CLI
